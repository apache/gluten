/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{BaseScriptTransformationExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.{col, lit, map, struct, sum}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Every case runs with adaptive execution off and on. Most cases are rows of one table: a fixture,
 * a query and the exact declaration expected on each native scan, where an empty declaration means
 * the scan must be left whole. The class comment of ScanMapKeyPruning lists which shapes are
 * supported; the cases here follow that list.
 */
class ScanMapKeyPruningSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")

  private val pruningFlag = VeloxConfig.SCAN_MAP_KEY_PRUNING_ENABLED.key
  private val noBroadcast = "spark.sql.autoBroadcastJoinThreshold" -> "-1"

  // ---------------------------------------------------------------------------------------------
  // Fixtures: each writes a parquet table and hands its path to the body.
  // ---------------------------------------------------------------------------------------------

  private type Fixture = (String => Unit) => Unit

  private def table(rows: Long, columns: String*): Fixture = f =>
    withTempPath {
      path =>
        spark.range(0, rows).selectExpr(("id" +: columns): _*).write.parquet(path.getCanonicalPath)
        f(path.getCanonicalPath)
    }

  /** m: MAP<STRING, STRUCT<s STRING, t BIGINT>> with keys a, b, c. */
  private val mapTable = table(
    10000,
    "map('a', named_struct('s', concat('v', cast(id % 7 as string)), 't', id), " +
      "'b', named_struct('s', 'x', 't', id * 2), " +
      "'c', named_struct('s', 'y', 't', id + 1)) as m"
  )

  /** m: MAP<STRING, BIGINT>, a map whose values are plain numbers. */
  private val scalarMapTable = table(1000, "map('a', id, 'b', id * 2, 'c', id + 1) as m")

  /** m: MAP<BIGINT, STRUCT<s, t>>. */
  private val longKeyTable = table(
    10000,
    "map(1L, named_struct('s', concat('v', cast(id % 7 as string)), 't', id), " +
      "2L, named_struct('s', 'x', 't', id * 2)) as m")

  /** c: STRUCT<m: MAP<STRING, BIGINT>, v: BIGINT>. */
  private val nestedMapTable =
    table(1000, "named_struct('m', map('a', id, 'b', id * 2), 'v', id) as c")

  /** c: STRUCT<m: MAP<STRING, BIGINT>, s: STRUCT<n: MAP<STRING, BIGINT>, v: BIGINT>>. */
  private val siblingMapTable = table(
    1000,
    "named_struct('m', map('a', id, 'b', id * 2), " +
      "'s', named_struct('n', map('p', id), 'v', id)) as c")

  /** m: MAP<STRING, MAP<STRING, BIGINT>>. */
  private val mapOfMapTable =
    table(1000, "map('a', map('x', id, 'y', id * 2), 'b', map('z', id)) as m")

  /** m: MAP<STRING, ARRAY<BIGINT>>. */
  private val arrayMapTable = table(500, "map('a', array(id, id + 1), 'b', array(id * 2)) as m")

  /** m as in mapTable plus n: MAP<STRING, BIGINT>. */
  private val twoMapTable = table(
    10000,
    "map('a', named_struct('s', concat('v', cast(id % 7 as string)), 't', id), " +
      "'b', named_struct('s', 'x', 't', id * 2)) as m",
    "map('x', id, 'y', id * 2) as n"
  )

  // Built from code points rather than literals or unicode escapes, which scalastyle rejects.
  private val umlautUpper = 0xc4.toChar // A with diaeresis
  private val umlautLower = 0xe4.toChar // a with diaeresis

  /**
   * c: STRUCT<[umlautUpper]rger: MAP<STRING, BIGINT>, v: BIGINT>: a field name that only full
   * Unicode lowercasing changes, since the ASCII fold leaves the umlaut alone.
   */
  private val umlautFieldTable =
    table(1000, s"named_struct('${umlautUpper}rger', map('a', id, 'b', id * 2), 'v', id) as c")

  /** m: MAP<STRING, BIGINT> whose first key is a single byte that is not valid UTF-8. */
  private val invalidUtf8KeyTable =
    table(100, "map(cast(x'FF' as string), id, 'a', id * 2) as m")

  /** d: MAP<DATE, BIGINT>, e: MAP<DECIMAL(3, 1), BIGINT>: key types without a Velox subscript. */
  private val oddKeyTable = table(
    1000,
    "map(DATE '2020-01-01', id, DATE '2020-01-02', id * 2) as d",
    "map(CAST(1.5 AS DECIMAL(3, 1)), id, CAST(2.5 AS DECIMAL(3, 1)), id * 2) as e")

  // ---------------------------------------------------------------------------------------------
  // Plan inspection.
  // ---------------------------------------------------------------------------------------------

  /**
   * The declaration of every native scan in the executed plan (query stages and commands included),
   * each rendered in Velox Subfield syntax. At least one native scan must exist: a "nothing
   * declared" assertion over a plan whose scan fell back to Spark would prove nothing.
   */
  private def declaredPerScan(df: DataFrame): Seq[Map[String, Set[String]]] = {
    val scans = getExecutedPlan(df).collect { case scan: BasicScanExecTransformer => scan }
    assert(scans.nonEmpty, s"expected a native scan:\n${df.queryExecution.executedPlan}")
    scans.map(_.requiredMapSubfields.map { case (c, paths) => c -> paths.map(_.toString).toSet })
  }

  /** The declaration over all native scans, for single-scan plans. */
  private def declared(df: DataFrame): Map[String, Set[String]] = declaredPerScan(df).flatten.toMap

  private def planHas(df: DataFrame, cls: Class[_ <: SparkPlan]): Boolean =
    getExecutedPlan(df).exists(cls.isInstance)

  /** Per-scan declarations compared regardless of scan order. */
  private def multiset[T](xs: Seq[T]): Map[T, Int] = xs.groupBy(identity).map {
    case (k, v) => k -> v.size
  }

  // ---------------------------------------------------------------------------------------------
  // Table-driven cases.
  // ---------------------------------------------------------------------------------------------

  /**
   * One case: 'sql' over 'fixture', run with the flag on and compared with vanilla Spark, must
   * leave exactly 'expected' on the native scans (one entry per scan, order-free; an empty map is a
   * scan left whole) and, if given, must contain an operator of type 'requires' in its plan.
   */
  private case class Case(
      name: String,
      fixture: Fixture,
      sql: String => String,
      expected: Seq[Map[String, Set[String]]],
      conf: Seq[(String, String)] = Nil,
      requires: Option[Class[_ <: SparkPlan]] = None,
      noFallBack: Boolean = true,
      compare: Boolean = true,
      before: () => Unit = () => ())

  private def m(paths: String*): Map[String, Set[String]] = Map("m" -> paths.toSet)
  private def c(paths: String*): Map[String, Set[String]] = Map("c" -> paths.toSet)
  private val whole: Map[String, Set[String]] = Map.empty

  private val cases = Seq(
    // ----- supported shapes: the map is dropped by a Project before anything else sees it -----
    Case(
      "keyed filter with aggregates over keyed values",
      mapTable,
      p => s"SELECT count(*), sum(m['a'].t), max(m['a'].s) FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(m("m[\"a\"].s", "m[\"a\"].t"))
    ),
    Case(
      "filter only, map dropped above the filter",
      mapTable,
      p => s"SELECT id FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(m("m[\"a\"].s"))),
    Case(
      "count only",
      mapTable,
      p => s"SELECT count(*) FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(m("m[\"a\"].s"))),
    Case(
      "projected keyed value without aggregation",
      mapTable,
      p => s"SELECT id, m['a'].t FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(m("m[\"a\"].s", "m[\"a\"].t"))
    ),
    Case(
      "several keys",
      mapTable,
      p => s"SELECT sum(m['a'].t + m['b'].t) FROM parquet.`$p` WHERE m['c'].s = 'y'",
      Seq(m("m[\"a\"].t", "m[\"b\"].t", "m[\"c\"].s"))),
    Case(
      "constant key expressions are evaluated",
      mapTable,
      p => s"SELECT sum(m[concat('a', '')].t) FROM parquet.`$p` WHERE m[upper('a')].s = 'v3'",
      Seq(m("m[\"a\"].t", "m[\"A\"].s"))
    ),
    Case(
      "element_at with a constant key",
      mapTable,
      p =>
        s"SELECT sum(element_at(m, 'a').t) FROM parquet.`$p` " +
          s"WHERE element_at(m, 'a').s = 'v5'",
      Seq(m("m[\"a\"].s", "m[\"a\"].t"))
    ),
    Case(
      "inferred null check next to a keyed filter is dropped",
      mapTable,
      p =>
        s"SELECT count(*), sum(m['a'].t) FROM parquet.`$p` " +
          s"WHERE m IS NOT NULL AND m['a'].s = 'v3'",
      Seq(m("m[\"a\"].s", "m[\"a\"].t"))
    ),
    Case(
      "null check on a keyed path alone",
      mapTable,
      p => s"SELECT count(*) FROM parquet.`$p` WHERE m['a'] IS NULL",
      Seq(m("m[\"a\"]"))),
    Case(
      "long keys",
      longKeyTable,
      p => s"SELECT count(*), sum(m[1].t) FROM parquet.`$p` WHERE m[1].s = 'v3'",
      Seq(m("m[1].s", "m[1].t"))),
    Case(
      "scalar-valued map",
      scalarMapTable,
      p => s"SELECT sum(m['a']) FROM parquet.`$p` WHERE m['b'] > 10",
      Seq(m("m[\"a\"]", "m[\"b\"]"))),
    Case(
      "nested map value used whole under a key",
      mapOfMapTable,
      p => s"SELECT sum(cardinality(m['a'])), sum(m['a']['y']) FROM parquet.`$p`",
      Seq(m("m[\"a\"]", "m[\"a\"][\"y\"]"))
    ),
    Case(
      "map nested in a struct, through Spark's nested-column alias",
      nestedMapTable,
      p => s"SELECT sum(c.m['a']), sum(c.m['b']) FROM parquet.`$p`",
      Seq(c("c.m[\"a\"]", "c.m[\"b\"]"))
    ),
    Case(
      "sibling struct fields are declared alongside the keyed map",
      siblingMapTable,
      p => s"SELECT sum(c.m['a']), sum(c.s.v), sum(cardinality(c.s.n)) FROM parquet.`$p`",
      Seq(c("c.m[\"a\"]", "c.s.v", "c.s.n"))
    ),
    Case(
      "case-insensitive field access renders the schema's spelling",
      nestedMapTable,
      p => s"SELECT sum(c.M['a']) FROM parquet.`$p`",
      Seq(c("c.m[\"a\"]")),
      conf = Seq("spark.sql.caseSensitive" -> "false")
    ),
    Case(
      "collect limit at the root",
      mapTable,
      p => s"SELECT id FROM parquet.`$p` WHERE m['a'].s = 'v3' LIMIT 5",
      Seq(m("m[\"a\"].s"))),
    Case(
      "take ordered and project",
      mapTable,
      p => s"SELECT id FROM parquet.`$p` WHERE m['a'].s = 'v3' ORDER BY id LIMIT 5",
      Seq(m("m[\"a\"].s"))),
    Case(
      "window partitioned by a keyed value",
      scalarMapTable,
      p => s"SELECT id, rank() OVER (PARTITION BY m['a'] ORDER BY id) AS r FROM parquet.`$p`",
      Seq(m("m[\"a\"]"))
    ),
    Case(
      "generate over a keyed value, map still read above the generate",
      arrayMapTable,
      p => s"SELECT m['b'], explode(m['a']) FROM parquet.`$p`",
      Seq(m("m[\"a\"]", "m[\"b\"]")),
      requires = Some(classOf[GenerateExecTransformer])
    ),
    Case(
      "generate over a keyed value ends the chain when nothing above needs the map",
      arrayMapTable,
      p => s"SELECT explode(m['a']) FROM parquet.`$p`",
      Seq(m("m[\"a\"]")),
      requires = Some(classOf[GenerateExecTransformer])
    ),
    Case(
      "non-ASCII struct field name folded like the schema, case-insensitive",
      umlautFieldTable,
      // Non-ASCII identifiers must be quoted in Spark SQL.
      p =>
        s"SELECT sum(c.`${umlautLower}rger`['a']) FROM parquet.`$p` " +
          s"WHERE c.`${umlautUpper}RGER`['b'] > 10",
      // Plain concatenation: scalastyle's parser does not accept \" inside interpolated strings.
      Seq(c("c." + umlautLower + "rger[\"a\"]", "c." + umlautLower + "rger[\"b\"]")),
      conf = Seq("spark.sql.caseSensitive" -> "false")
    ),
    Case(
      "two map columns on one scan, one pruned and one read whole",
      twoMapTable,
      p => s"SELECT sum(m['a'].t), sum(size(n)) FROM parquet.`$p`",
      Seq(m("m[\"a\"].t"))
    ),
    Case(
      "shuffled join with keyed filters on both sides",
      mapTable,
      p =>
        s"SELECT count(*) FROM parquet.`$p` t1 JOIN parquet.`$p` t2 ON t1.id = t2.id " +
          s"WHERE t1.m['a'].s = 'v3' AND t2.m['b'].s = 'x'",
      Seq(m("m[\"a\"].s"), m("m[\"b\"].s")),
      conf = Seq(noBroadcast)
    ),
    Case(
      "inlined CTE read twice with different keys pushed below the join",
      mapTable,
      p =>
        s"WITH c AS (SELECT id, m FROM parquet.`$p`) " +
          s"SELECT sum(t1.m['a'].t + t2.m['b'].t) FROM c t1 JOIN c t2 ON t1.id = t2.id",
      Seq(m("m[\"a\"].t"), m("m[\"b\"].t")),
      conf = Seq(noBroadcast)
    ),
    Case(
      "identical pruned scans keep exchange reuse",
      mapTable,
      p =>
        s"WITH c AS (SELECT id, m['a'].t AS x FROM parquet.`$p` WHERE m['a'].s = 'v3') " +
          s"SELECT sum(t1.x + t2.x) FROM c t1 JOIN c t2 ON t1.id = t2.id",
      Seq(m("m[\"a\"].s", "m[\"a\"].t")),
      conf = Seq(noBroadcast),
      requires = Some(classOf[ReusedExchangeExec])
    ),
    Case(
      "union of keyed projections prunes each branch",
      mapTable,
      p =>
        s"SELECT m['a'].t AS v FROM parquet.`$p` WHERE m['a'].s = 'v3' " +
          s"UNION ALL SELECT m['b'].t FROM parquet.`$p` WHERE m['c'].s = 'y'",
      Seq(m("m[\"a\"].s", "m[\"a\"].t"), m("m[\"b\"].t", "m[\"c\"].s"))
    ),
    Case(
      "UDF in a partially offloaded project above the chain",
      mapTable,
      p => s"SELECT sum(mk_len(m['a'].s)), sum(id) FROM parquet.`$p` WHERE m['b'].s = 'x'",
      Seq(m("m[\"a\"].s", "m[\"b\"].s")),
      conf = Seq(GlutenConfig.ENABLE_COLUMNAR_PARTIAL_PROJECT.key -> "true"),
      requires = Some(classOf[ColumnarPartialProjectExec]),
      before = () => spark.udf.register("mk_len", (s: String) => if (s == null) 0 else s.length)
    ),
    Case(
      "script transformation above the chain",
      mapTable,
      p =>
        s"SELECT TRANSFORM(id, m['a'].s) USING 'cat' AS (a STRING, b STRING) " +
          s"FROM parquet.`$p` WHERE m['b'].s = 'x'",
      Seq(m("m[\"a\"].s", "m[\"b\"].s")),
      requires = Some(classOf[BaseScriptTransformationExec]),
      noFallBack = false
    ),
    // ----- unsupported shapes: the map stays whole -----
    Case(
      "whole-map use",
      mapTable,
      p => s"SELECT sum(size(m)) FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(whole)),
    Case(
      "non-constant key",
      mapTable,
      p => s"SELECT sum(m[if(id % 2 = 0, 'a', 'b')].t) FROM parquet.`$p`",
      Seq(whole)),
    Case(
      "map in the output",
      mapTable,
      p => s"SELECT m FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(whole)),
    Case(
      "alias still holding the map in the output",
      nestedMapTable,
      p => s"SELECT c.m AS mm FROM parquet.`$p` WHERE c.m['a'] > 10",
      Seq(whole)),
    Case(
      "project computing over the whole map",
      mapTable,
      p => s"SELECT map_keys(m) FROM parquet.`$p` WHERE m['a'].s = 'v3'",
      Seq(whole)),
    Case(
      "key-free path next to a keyed path prunes nothing",
      mapTable,
      p => s"SELECT sum(size(m)), sum(m['a'].t) FROM parquet.`$p`",
      Seq(whole)),
    Case(
      "key-free alias path next to a keyed path prunes nothing",
      nestedMapTable,
      p => s"SELECT sum(size(c.m)), sum(c.m['a']) FROM parquet.`$p`",
      Seq(whole)),
    Case(
      "bare null check next to a keyed null check prunes nothing",
      mapTable,
      p => s"SELECT count(*) FROM parquet.`$p` WHERE m IS NOT NULL AND m['a'] IS NULL",
      Seq(whole)
    ),
    Case(
      "generate over the whole map",
      scalarMapTable,
      p => s"SELECT id, explode(m) FROM parquet.`$p`",
      Seq(whole),
      requires = Some(classOf[GenerateExecTransformer])
    ),
    Case(
      "string key with invalid UTF-8 bytes",
      invalidUtf8KeyTable,
      p => s"SELECT sum(m[cast(x'FF' as string)]), sum(m['a']) FROM parquet.`$p`",
      Seq(whole),
      // Vanilla Spark itself returns null for this lookup after the Parquet round trip while the
      // native reader finds the entry; only the declaration is checked here.
      compare = false
    ),
    Case(
      "date keys",
      oddKeyTable,
      p => s"SELECT sum(d[DATE '2020-01-01']) FROM parquet.`$p`",
      Seq(whole),
      noFallBack = false),
    Case(
      "decimal keys",
      oddKeyTable,
      p => s"SELECT sum(e[CAST(1.5 AS DECIMAL(3, 1))]) FROM parquet.`$p`",
      Seq(whole),
      noFallBack = false),
    Case(
      "map forwarded through a shuffle and read above a join",
      mapTable,
      p =>
        s"SELECT sum(element_at(t1.m, 'a').t) FROM parquet.`$p` t1 " +
          s"JOIN parquet.`$p` t2 ON t1.id = t2.id",
      Seq(whole, whole),
      conf = Seq(noBroadcast)
    ),
    Case(
      "maps forwarded through a self join, one read whole",
      twoMapTable,
      p =>
        s"WITH c AS (SELECT id, m, n FROM parquet.`$p`) " +
          s"SELECT t1.m, t1.n['x'], t2.m['b'], t2.n['y'] FROM c t1 JOIN c t2 ON t1.id = t2.id",
      Seq(whole),
      conf = Seq(noBroadcast),
      requires = Some(classOf[ReusedExchangeExec])
    ),
    Case(
      "sort by a keyed value forwards the map through the shuffle",
      scalarMapTable,
      p => s"SELECT id FROM parquet.`$p` WHERE m['b'] > 10 ORDER BY m['a']",
      Seq(whole)
    )
  ) ++ Seq("true", "false").map {
    nativeUnion =>
      Case(
        s"union forwarding the map (native union = $nativeUnion)",
        mapTable,
        p =>
          s"SELECT size(m), m['b'].s FROM (SELECT m FROM parquet.`$p` WHERE m['a'].s = 'v0' " +
            s"UNION ALL SELECT m FROM parquet.`$p` WHERE m['c'].s = 'y')",
        Seq(whole, whole),
        conf = Seq(GlutenConfig.NATIVE_UNION_ENABLED.key -> nativeUnion)
      )
  }

  test("the native reader applies a declaration: undeclared keys are not read") {
    // Attaches a declaration to the scan directly, bypassing the rule, and reads the map whole.
    // Only the declared key may come back; if the transport or the native side ignored the
    // declaration, all three keys would.
    mapTable {
      path =>
        withSQLConf("spark.sql.adaptive.enabled" -> "false") {
          val df = spark.read.parquet(path).select("m")
          def keysPerRow(plan: SparkPlan): Set[Set[String]] =
            plan
              .executeCollect()
              .map(_.getMap(0).keyArray().toArray[UTF8String](StringType).map(_.toString).toSet)
              .toSet
          val plan = df.queryExecution.executedPlan
          assert(planHas(df, classOf[BasicScanExecTransformer]), s"expected a native scan:\n$plan")
          assert(keysPerRow(plan) == Set(Set("a", "b", "c")))
          val pruned = plan.transform {
            case scan: BasicScanExecTransformer =>
              scan.withRequiredMapSubfields(
                Map("m" -> Seq(SubfieldPath("m", Seq(SubfieldElement.StringKey("a"))))))
          }
          assert(keysPerRow(pruned) == Set(Set("a")), s"declaration not applied:\n$pruned")
        }
    }
  }

  Seq(false, true).foreach {
    adaptive =>
      val mode = s"adaptive execution ${if (adaptive) "on" else "off"}"
      val modeConf = "spark.sql.adaptive.enabled" -> adaptive.toString

      cases.foreach {
        cs =>
          test(s"${cs.name} ($mode)") {
            cs.before()
            cs.fixture {
              path =>
                withSQLConf((Seq(pruningFlag -> "true", modeConf) ++ cs.conf): _*) {
                  runQueryAndCompare(
                    cs.sql(path),
                    compareResult = cs.compare,
                    noFallBack = cs.noFallBack) {
                    df =>
                      val plan = df.queryExecution.executedPlan
                      cs.requires.foreach {
                        cls => assert(planHas(df, cls), s"expected ${cls.getSimpleName}:\n$plan")
                      }
                      val actual = declaredPerScan(df)
                      assert(
                        multiset(actual) == multiset(cs.expected),
                        s"declared $actual, expected ${cs.expected}\n$plan")
                  }
                }
            }
          }
      }

      // ----- cases that do not fit the table -----

      test(s"pruning is off by default ($mode)") {
        mapTable {
          path =>
            withSQLConf(modeConf) {
              runQueryAndCompare(
                s"SELECT sum(m['a'].t) FROM parquet.`$path` WHERE m['a'].s = 'v3'") {
                df => assert(declared(df).isEmpty)
              }
            }
        }
      }

      test(s"keys with quotes or backslashes survive the structural transport ($mode)") {
        // Built with the DataFrame API so the keys reach the plan verbatim, without SQL
        // string-literal escaping in between.
        val quoteKey = "q\"k"
        val backslashKey = "b\\s"
        withTempPath {
          dir =>
            val file = dir.getCanonicalPath
            spark
              .range(0, 100)
              .select(
                col("id"),
                map(
                  lit(quoteKey),
                  struct(lit("v").as("s"), col("id").as("t")),
                  lit(backslashKey),
                  struct(lit("w").as("s"), (col("id") * 2).as("t"))).as("m"))
              .write
              .parquet(file)
            def run(): (Row, Set[String]) = {
              val df = spark.read
                .parquet(file)
                .select(
                  sum(col("m").getItem(quoteKey).getField("t")).as("q"),
                  sum(col("m").getItem(backslashKey).getField("t")).as("b"))
              val row = df.collect().head
              (row, declared(df).getOrElse("m", Set.empty))
            }
            var expected: Row = null
            withSQLConf(pruningFlag -> "false", modeConf) {
              val (row, paths) = run()
              assert(paths.isEmpty)
              expected = row
            }
            withSQLConf(pruningFlag -> "true", modeConf) {
              val (row, paths) = run()
              assert(paths == Set("m[\"q\\\"k\"].t", "m[\"b\\\\s\"].t"), s"got: $paths")
              assert(row == expected, s"pruned result $row differs from $expected")
            }
        }
      }

      test(s"writing the whole map keeps it whole ($mode)") {
        // The Project feeding the write forwards the map, so the chain ends with it live.
        mapTable {
          path =>
            withTable("mk_write_whole") {
              spark.sql(
                "CREATE TABLE mk_write_whole " +
                  "(id BIGINT, m MAP<STRING, STRUCT<s: STRING, t: BIGINT>>) USING parquet")
              withSQLConf(pruningFlag -> "true", modeConf) {
                val df = spark.sql(
                  s"INSERT INTO mk_write_whole SELECT id, m FROM parquet.`$path` " +
                    s"WHERE m['a'].s = 'v3'")
                assert(declaredPerScan(df) == Seq(whole), s"map must stay whole: ${declared(df)}")
              }
              // Every written map still holds all three keys.
              checkAnswer(
                spark.sql("SELECT count(*) - sum(CASE WHEN size(m) = 3 THEN 1 ELSE 0 END) " +
                  "FROM mk_write_whole"),
                Row(0L) :: Nil)
            }
        }
      }

      test(s"writing keyed values prunes the scan ($mode)") {
        // The Project feeding the write computes m['a'].t and drops the map.
        mapTable {
          path =>
            withTable("mk_write_keyed") {
              spark.sql("CREATE TABLE mk_write_keyed (id BIGINT, t BIGINT) USING parquet")
              withSQLConf(pruningFlag -> "true", modeConf) {
                val df = spark.sql(
                  s"INSERT INTO mk_write_keyed SELECT id, m['a'].t FROM parquet.`$path` " +
                    s"WHERE m['a'].s = 'v3'")
                assert(
                  declared(df) == m("m[\"a\"].s", "m[\"a\"].t"),
                  s"expected pruning below the write: ${declared(df)}")
              }
              checkAnswer(
                spark.sql("SELECT count(*), sum(t) FROM mk_write_keyed"),
                spark.sql(
                  s"SELECT count(*), sum(m['a'].t) FROM parquet.`$path` WHERE m['a'].s = 'v3'"))
            }
        }
      }
  }
}
