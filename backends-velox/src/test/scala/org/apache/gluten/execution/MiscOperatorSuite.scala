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

import org.apache.gluten.config.{GlutenConfig, GlutenCoreConfig, VeloxConfig}
import org.apache.gluten.expression.VeloxDummyExpression
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class MiscOperatorSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
    VeloxDummyExpression.registerFunctions(spark.sessionState.functionRegistry)
  }

  override def afterAll(): Unit = {
    VeloxDummyExpression.unregisterFunctions(spark.sessionState.functionRegistry)
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro,parquet,csv")
      .set(GlutenConfig.NATIVE_ARROW_READER_ENABLED.key, "true")
  }

  test("select_part_column") {
    val df = runQueryAndCompare("select l_shipdate, l_orderkey from lineitem limit 1") {
      df =>
        {
          assert(df.schema.fields.length == 2)
        }
    }
    checkLengthAndPlan(df, 1)
  }

  test("select_as") {
    val df = runQueryAndCompare("select l_shipdate as my_col from lineitem limit 1") {
      df =>
        {
          assert(df.schema.fieldNames(0).equals("my_col"))
        }
    }
    checkLengthAndPlan(df, 1)
  }

  test("where") {
    val df = runQueryAndCompare("select * from lineitem where l_shipdate < '1998-09-02'") { _ => }
    checkLengthAndPlan(df, 59288)
  }

  test("is_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_comment is null") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)

    // Struct of array.
    val data =
      Row(Row(Array("a", "b", "c"), null)) ::
        Row(Row(Array("d", "e", "f"), Array(1, 2, 3))) ::
        Row(Row(null, null)) :: Nil

    val schema = new StructType()
      .add(
        "struct",
        new StructType()
          .add("a0", ArrayType(StringType))
          .add("a1", ArrayType(IntegerType)))

    val dataFrame = spark.createDataFrame(JavaConverters.seqAsJavaList(data), schema)

    withTempPath {
      path =>
        dataFrame.write.parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("select * from view where struct is null") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
        runQueryAndCompare("select * from view where struct.a0 is null") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
    }
  }

  test("is_null_has_null") {
    val data = Seq(Row(null), Row("data"), Row(null))
    val schema = StructType(Array(StructField("col1", StringType, nullable = true)))
    spark
      .createDataFrame(JavaConverters.seqAsJavaList(data), schema)
      .createOrReplaceTempView("temp_test_is_null")
    val df = runQueryAndCompare("select * from temp_test_is_null where col1 is null") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("is_not_null") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_comment is not null " +
        "and l_orderkey = 1") { _ => }
    checkLengthAndPlan(df, 6)

    // Struct of array.
    val data =
      Row(Row(Array("a", "b", "c"), null)) ::
        Row(Row(Array("d", "e", "f"), Array(1, 2, 3))) ::
        Row(Row(null, null)) :: Nil

    val schema = new StructType()
      .add(
        "struct",
        new StructType()
          .add("a0", ArrayType(StringType))
          .add("a1", ArrayType(IntegerType)))

    val dataFrame = spark.createDataFrame(JavaConverters.seqAsJavaList(data), schema)

    withTempPath {
      path =>
        dataFrame.write.parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("select * from view where struct is not null") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
        runQueryAndCompare("select * from view where struct.a0 is not null") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
    }
  }

  test("is_null and is_not_null coexist") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_comment is null and l_comment is not null") { _ => }
    checkLengthAndPlan(df, 0)
  }

  // TODO: fix on spark-4.0
  test("and pushdown") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem where l_orderkey > 2 " +
        "and l_orderkey < 2") { _ => }
    assert(df.isEmpty)
    checkLengthAndPlan(df, 0)
  }

  test("in") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("in_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674, 1062) and l_partkey in (1552, 674)") { _ => }
    checkLengthAndPlan(df, 73)
  }

  test("in_or") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 122)
  }

  test("in_or_and") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey in (1552, 674) or l_partkey in (1552) and l_orderkey > 1") { _ => }
    checkLengthAndPlan(df, 73)

    runQueryAndCompare(
      "select count(1) from lineitem " +
        "where (l_shipmode in ('TRUCK', 'MAIL') or l_shipmode in ('AIR', 'FOB')) " +
        "and l_shipmode in ('RAIL','SHIP')") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }
  }

  test("in_not") {
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) or l_partkey in (1552, 1062)") { _ => }
    checkLengthAndPlan(df, 60141)
  }

  test("not in") {
    // integral type
    val df = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674, 1062)") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }
    checkLengthAndPlan(df, 60053)

    val df2 = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) and l_partkey not in (1062)") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }
    checkLengthAndPlan(df2, 60053)

    val df3 = runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) and l_partkey != 1062") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }
    checkLengthAndPlan(df3, 60053)

    // string type
    val df4 =
      runQueryAndCompare("select o_orderstatus from orders where o_orderstatus not in ('O', 'F')") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
    checkLengthAndPlan(df4, 363)

    // bool type
    withTable("t") {
      sql("create table t (id int, b boolean) using parquet")
      sql("insert into t values (1, true), (2, false), (3, null)")
      runQueryAndCompare("select * from t where b not in (true)") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare("select * from t where b not in (true, false)") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
    }

    // mix not-in with range
    runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) and l_partkey >= 1552") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }

    // mix not-in with in
    runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) and l_partkey in (1552)") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }

    // not-in with or relation
    runQueryAndCompare(
      "select l_orderkey from lineitem " +
        "where l_partkey not in (1552, 674) or l_partkey in (1552)") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }
  }

  test("coalesce") {
    var df = runQueryAndCompare(
      "select l_orderkey, coalesce(l_comment, 'default_val') " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, l_comment, 'default_val') " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, l_comment) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, 1, 2) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
    df = runQueryAndCompare(
      "select l_orderkey, coalesce(null, null, null) " +
        "from lineitem limit 5") { _ => }
    checkLengthAndPlan(df, 5)
  }

  testWithMinSparkVersion("coalesce validation", "3.4") {
    withTempPath {
      path =>
        val data = "2019-09-09 01:02:03.456789"
        val df = Seq(data).toDF("strTs").selectExpr(s"CAST(strTs AS TIMESTAMP_NTZ) AS ts")
        df.coalesce(1).write.format("parquet").save(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).collect
    }
  }

  test("groupby") {
    val df = runQueryAndCompare(
      "select l_orderkey, sum(l_partkey) as sum from lineitem " +
        "where l_orderkey < 3 group by l_orderkey") { _ => }
    checkLengthAndPlan(df, 2)
  }

  test("group sets") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
        "where l_orderkey < 3 group by ROLLUP(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey ") { _ => }
  }

  test("orderby") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("orderby expression") {
    val df = runQueryAndCompare(
      "select l_suppkey from lineitem " +
        "where l_orderkey < 3 order by l_partkey / 2 ") { _ => }
    checkLengthAndPlan(df, 7)
  }

  test("window expression") {
    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_commitdate" +
        " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) from lineitem ") {
      df =>
        checkSparkPlan[WindowExecTransformer](df)
        assert(
          getExecutedPlan(df).collect {
            case s: SortExecTransformer if !s.global => s
          }.size == 1
        )
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey" +
        " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW), " +
        "min(l_comment) over" +
        " (partition by l_suppkey order by l_linenumber" +
        " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) from lineitem ") {
      checkSparkPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey" +
        " RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) from lineitem ") {
      checkSparkPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey" +
        " RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) from lineitem ") {
      checkSparkPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey" +
        " RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING) from lineitem ") {
      checkSparkPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey" +
        " RANGE BETWEEN 6 PRECEDING AND 3 PRECEDING) from lineitem ") {
      checkSparkPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey" +
        " RANGE BETWEEN 3 FOLLOWING AND 6 FOLLOWING) from lineitem ") {
      checkSparkPlan[WindowExecTransformer]
    }

    // DecimalType as order by column is not supported
    runQueryAndCompare(
      "select min(l_comment) over" +
        " (partition by l_suppkey order by l_discount" +
        " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) from lineitem ") {
      checkSparkPlan[WindowExec]
    }

    runQueryAndCompare(
      "select ntile(4) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select row_number() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select rank() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select dense_rank() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select percent_rank() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select cume_dist() over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") { _ => }

    runQueryAndCompare(
      "select l_suppkey, l_orderkey, nth_value(l_orderkey, 2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select l_suppkey, l_orderkey, nth_value(l_orderkey, 2) IGNORE NULLS over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select sum(l_partkey + 1) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select max(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select min(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select avg(l_partkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select lag(l_orderkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select lead(l_orderkey) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    // Test same partition/ordering keys.
    runQueryAndCompare(
      "select avg(l_partkey) over" +
        " (partition by l_suppkey order by l_suppkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    // Test overlapping partition/ordering keys.
    runQueryAndCompare(
      "select avg(l_partkey) over" +
        " (partition by l_suppkey order by l_suppkey, l_orderkey) from lineitem ") {
      checkGlutenPlan[WindowExecTransformer]
    }

    // Foldable input of nth_value is not supported.
    runQueryAndCompare(
      "select l_suppkey, l_orderkey, nth_value(1, 2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem ") {
      checkSparkPlan[WindowExec]
    }
  }

  test("df.count()") {
    val df = runQueryAndCompare("select * from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("union_all two tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union all
                         | select o_orderkey as orderkey from orders
                         |);
                         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).exists(
              plan => plan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined))
        }
    }
  }

  test("union_all two tables with known partitioning") {
    withSQLConf(GlutenConfig.NATIVE_UNION_ENABLED.key -> "true") {
      compareDfResultsAgainstVanillaSpark(
        () => {
          val df1 = spark.sql("select l_orderkey as orderkey from lineitem")
          val df2 = spark.sql("select o_orderkey as orderkey from orders")
          df1.repartition(5).union(df2.repartition(5))
        },
        compareResult = true,
        checkGlutenPlan[UnionExecTransformer]
      )

      compareDfResultsAgainstVanillaSpark(
        () => {
          val df1 = spark.sql("select l_orderkey as orderkey from lineitem")
          val df2 = spark.sql("select o_orderkey as orderkey from orders")
          df1.repartition(5).union(df2.repartition(6))
        },
        compareResult = true,
        checkGlutenPlan[ColumnarUnionExec]
      )
    }
  }

  test("union_all three tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union all
                         | select o_orderkey as orderkey from orders
                         | union all
                         | (select o_orderkey as orderkey from orders limit 100)
                         |);
                         |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined)
        }
    }
  }

  test("union two tables") {
    runQueryAndCompare("""
                         |select count(orderkey) from (
                         | select l_orderkey as orderkey from lineitem
                         | union
                         | select o_orderkey as orderkey from orders
                         |);
                         |""".stripMargin) {
      df =>
        {
          getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined)
        }
    }
  }

  test("test 'select global/local limit'") {
    runQueryAndCompare("""
                         |select * from (
                         | select * from lineitem limit 10
                         |) where l_suppkey != 0 limit 100;
                         |""".stripMargin) {
      checkGlutenPlan[LimitExecTransformer]
    }
  }

  test("bool scan") {
    withTempPath {
      path =>
        Seq(true, false, true, true, false, false)
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT a from view") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
    }
  }

  test("hash") {
    withTempView("t") {
      Seq[(Integer, String)]((1, "a"), (2, null), (null, "b"))
        .toDF("a", "b")
        .createOrReplaceTempView("t")
      runQueryAndCompare("select hash(a, b) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("select xxhash64(a, b) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("decimal abs") {
    runQueryAndCompare("""
                         |select abs(cast (l_quantity * (-1.0) as decimal(12, 2))),
                         |abs(cast (l_quantity * (-1.0) as decimal(22, 2))),
                         |abs(cast (l_quantity as decimal(12, 2))),
                         |abs(cast (l_quantity as decimal(12, 2))) from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        Seq(-3099.270000, -3018.367500, -2833.887500, -1304.180000, -1263.289167, -1480.093333)
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT abs(cast (a as decimal(19, 6))) from view") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("nested decimal arithmetics") {
    runQueryAndCompare("""
                         |SELECT
                         |  l_orderkey,
                         |  SUM(
                         |    (l_extendedprice * (1 - l_discount)) +
                         |    (l_extendedprice * (1 - l_discount) * 0.05)
                         |  ) AS total_revenue_with_tax
                         |FROM
                         |  lineitem
                         |GROUP BY
                         |  l_orderkey
                         |ORDER BY
                         |  l_orderkey
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
  }

  test("Cast double to decimal") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df
      .select($"DecimalCol".cast(DecimalType(38, 33)))
      .select(col("DecimalCol"))
      .agg(avg($"DecimalCol"))
    // Double precision loss:
    // https://github.com/facebookincubator/velox/pull/6051#issuecomment-1731028215.
    // assert(result.collect()(0).get(0).toString.equals("0.0345678900000000000000000000000000000"))
    assert((result.collect()(0).get(0).toString.toDouble - d).abs < 0.00000000001)
    checkGlutenPlan[HashAggregateExecTransformer](result)
  }

  test("orc scan") {
    val df = spark.read
      .format("orc")
      .load("../cpp/velox/benchmarks/data/bm_lineitem/orc/lineitem.orc")
    df.createOrReplaceTempView("lineitem_orc")
    runQueryAndCompare("select l_orderkey from lineitem_orc") {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[BatchScanExecTransformer]
              }) == 1)
        }
    }
  }

  test("combine small batches before shuffle") {
    val minBatchSize = 15
    withSQLConf(
      VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key -> "true",
      GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "2",
      VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE.key -> s"$minBatchSize"
    ) {
      val df = runQueryAndCompare(
        "select l_orderkey, sum(l_partkey) as sum from lineitem " +
          "where l_orderkey < 100 group by l_orderkey") { _ => }
      checkLengthAndPlan(df, 27)
      val ops = collect(df.queryExecution.executedPlan) { case p: VeloxResizeBatchesExec => p }
      assert(ops.size == 1)
      val op = ops.head
      assert(op.minOutputBatchSize == minBatchSize)
      val metrics = op.metrics
      assert(metrics("numInputRows").value == 27)
      assert(metrics("numInputBatches").value == 14)
      assert(metrics("numOutputRows").value == 27)
      assert(metrics("numOutputBatches").value == 2)
    }

    withSQLConf(
      VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key -> "true",
      GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "2"
    ) {
      val df = runQueryAndCompare(
        "select l_orderkey, sum(l_partkey) as sum from lineitem " +
          "where l_orderkey < 100 group by l_orderkey") { _ => }
      checkLengthAndPlan(df, 27)
      val ops = collect(df.queryExecution.executedPlan) { case p: VeloxResizeBatchesExec => p }
      assert(ops.size == 1)
      val op = ops.head
      assert(op.minOutputBatchSize == 1)
      val metrics = op.metrics
      assert(metrics("numInputRows").value == 27)
      assert(metrics("numInputBatches").value == 14)
      assert(metrics("numOutputRows").value == 27)
      assert(metrics("numOutputBatches").value == 14)
    }
  }

  test("test OneRowRelation") {
    val df = sql("SELECT 1")
    checkAnswer(df, Row(1))
    val plan = df.queryExecution.executedPlan
    if (isSparkVersionGE("4.1")) {
      assert(plan.find(_.getClass.getSimpleName == "OneRowRelationExec").isDefined)
    } else {
      assert(plan.find(_.isInstanceOf[RDDScanExec]).isDefined)
    }
    assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
    assert(plan.find(_.isInstanceOf[RowToVeloxColumnarExec]).isDefined)
  }

  test("equal null safe") {
    runQueryAndCompare("""
                         |select l_quantity <=> 1000 from lineitem;
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("test overlay function") {
    runQueryAndCompare("""
                         |select overlay(l_shipdate placing '_' from 0) from lineitem limit 1;
                         |""".stripMargin) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Improve the local sort ensure requirements") {
    withSQLConf(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
      withTable("t1", "t2") {
        sql("""
              |create table t1 using parquet as
              |select cast(id as int) as c1, cast(id as string) c2 from range(100)
              |""".stripMargin)
        sql("""
              |create table t2 using parquet as
              |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
              |""".stripMargin)

        runQueryAndCompare(
          """
            |select * from (select c1, max(c2) from t1 group by c1)t1
            |join t2 on t1.c1 = t2.c1 and t1.c1 > conv(t2.c1, 2, 10);
            |""".stripMargin
        ) {
          checkGlutenPlan[HashAggregateExecTransformer]
        }
      }
    }
  }

  test("Fix Generate fail when required child output is not same with child output") {
    withTable("t") {
      spark
        .range(10)
        .selectExpr("id as c1", "id as c2")
        .write
        .format("parquet")
        .saveAsTable("t")

      runQueryAndCompare("SELECT c1, explode(array(c2)) FROM t") {
        checkGlutenPlan[GenerateExecTransformer]
      }

      runQueryAndCompare("SELECT c1, explode(c3) FROM (SELECT c1, array(c2) as c3 FROM t)") {
        checkGlutenPlan[GenerateExecTransformer]
      }
    }
  }

  test("Validation should fail if unsupported expression is used for Generate.") {
    withTable("t") {
      spark
        .range(10)
        .selectExpr("id as c1", "id as c2")
        .write
        .format("parquet")
        .saveAsTable("t")

      // Add a simple UDF to generate the unsupported case
      val intToArrayFunc = udf((s: Int) => Array(s))
      spark.udf.register("intToArray", intToArrayFunc)

      // Testing unsupported case
      runQueryAndCompare("SELECT explode(intToArray(c1)) from t;") {
        df =>
          {
            getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[GenerateExec]).isDefined)
          }
      }

      // Testing unsupported case in case when
      runQueryAndCompare("""
                           |SELECT explode(case when size(intToArray(c1)) > 0
                           |then array(c1) else array(c2) end) from t;
                           |""".stripMargin) {
        df =>
          {
            getExecutedPlan(df).exists(plan => plan.find(_.isInstanceOf[GenerateExec]).isDefined)
          }
      }
    }
  }

  test("Support get native plan tree string, Velox single aggregation") {
    runQueryAndCompare("""
                         |select l_partkey + 1, count(*)
                         |from (select /*+ repartition(2) */ * from lineitem) group by l_partkey + 1
                         |""".stripMargin) {
      df =>
        val wholeStageTransformers = collect(df.queryExecution.executedPlan) {
          case w: WholeStageTransformer => w
        }
        assert(wholeStageTransformers.size == 3)
        val nativePlanString = wholeStageTransformers.head.nativePlanString()
        assert(nativePlanString.matches("[\\s\\S]*Aggregation\\[\\d+]\\[SINGLE[\\s\\S]*"))
        assert(nativePlanString.contains("ValueStream"))
        assert(wholeStageTransformers(1).nativePlanString().contains("ValueStream"))
        assert(wholeStageTransformers.last.nativePlanString().contains("TableScan"))
    }
  }

  test("Support StreamingAggregate if child output ordering is satisfied") {
    withTable("t") {
      spark
        .range(10000)
        .selectExpr(s"id % 999 as c1", "id as c2")
        .write
        .saveAsTable("t")

      withSQLConf(
        GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE.key -> "true",
        GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
      ) {
        val query =
          """
            |SELECT c1, count(*), sum(c2) FROM (
            |SELECT t1.c1, t2.c2 FROM t t1 JOIN t t2 ON t1.c1 = t2.c1
            |)
            |GROUP BY c1
            |""".stripMargin
        runQueryAndCompare(query) {
          df =>
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[SortMergeJoinExecTransformer]).isDefined)
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
        }
      }
    }
  }

  test("Verify parquet field name with special character") {
    withTable("t") {

      // https://github.com/apache/spark/pull/35229 Spark remove parquet field name check after 3.2
      if (!SparkShimLoader.getSparkVersion.startsWith("3.2")) {
        sql("create table t using parquet as select sum(l_partkey) from lineitem")
        runQueryAndCompare("select * from t") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
      } else {
        val msg = intercept[AnalysisException] {
          sql("create table t using parquet as select sum(l_partkey) from lineitem")
        }.message
        assert(msg.contains("contains invalid character"))
      }
    }
  }

  test("test explode/posexplode function") {
    Seq("explode", "posexplode").foreach {
      f =>
        Seq(true, false).foreach {
          isOuter =>
            val func = if (isOuter) s"${f}_outer" else f
            // Literal: func(literal)
            runQueryAndCompare(s"""
                                  |SELECT $func(array(1, 2, 3));
                                  |""".stripMargin) {
              checkGlutenPlan[GenerateExecTransformer]
            }
            runQueryAndCompare(s"""
                                  |SELECT $func(map(1, 'a', 2, 'b'));
                                  |""".stripMargin) {
              checkGlutenPlan[GenerateExecTransformer]
            }
            runQueryAndCompare(
              s"""
                 |SELECT
                 |  $func(array(map(1, 'a', 2, 'b'), map(3, 'c', 4, 'd'), map(5, '', 6, null)));
                 |""".stripMargin) {
              checkGlutenPlan[GenerateExecTransformer]
            }
            runQueryAndCompare(s"""
                                  |SELECT $func(map(1, array(1, 2), 2, array(3, 4), 3, array()));
                                  |""".stripMargin) {
              checkGlutenPlan[GenerateExecTransformer]
            }

            // CreateArray/CreateMap: func(array(col)), func(map(k, v))
            withTempView("t1") {
              sql("""select * from values (1), (2), (3), (4), (null)
                    |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t1")
              runQueryAndCompare(s"""
                                    |SELECT $func(array(a)) from t1;
                                    |""".stripMargin) {
                checkGlutenPlan[GenerateExecTransformer]
              }
              sql("""select * from values (1, 'a'), (2, 'b'), (3, null), (4, null)
                    |as tbl(a, b)
         """.stripMargin).createOrReplaceTempView("t1")
              runQueryAndCompare(s"""
                                    |SELECT $func(map(a, b)) from t1;
                                    |""".stripMargin) {
                checkGlutenPlan[GenerateExecTransformer]
              }
            }

            // AttributeReference: func(col)
            withTempView("t2") {
              sql("""select * from values
                    |  array(1, 2, 3),
                    |  array(4, null),
                    |  array(),
                    |  null
                    |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t2")
              runQueryAndCompare(s"""
                                    |SELECT $func(a) from t2;
                                    |""".stripMargin) {
                df =>
                  if (!isOuter) {
                    // No ProjectExecTransformer is introduced.
                    checkSparkOperatorChainMatch[GenerateExecTransformer, FilterExecTransformer](df)
                  }
                  checkGlutenPlan[GenerateExecTransformer](df)
              }
              sql("""select * from values
                    |  map(1, 'a', 2, 'b', 3, null),
                    |  map(4, null),
                    |  map(),
                    |  null
                    |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t2")
              runQueryAndCompare(s"""
                                    |SELECT $func(a) from t2;
                                    |""".stripMargin) {
                df =>
                  if (!isOuter) {
                    // No ProjectExecTransformer is introduced.
                    checkSparkOperatorChainMatch[GenerateExecTransformer, FilterExecTransformer](df)
                  }
                  checkGlutenPlan[GenerateExecTransformer](df)
              }

              runQueryAndCompare(
                s"""
                   |SELECT $func(${VeloxDummyExpression.VELOX_DUMMY_EXPRESSION}(a)) from t2;
                   |""".stripMargin) {
                checkGlutenPlan[GenerateExecTransformer]
              }
            }
        }
    }
  }

  test("test stack function") {
    withTempView("t1") {
      sql("""SELECT * from values
            |  (1, "james", 10, "lucy"),
            |  (2, "bond", 20, "lily")
            |as tbl(id, name, id1, name1)
         """.stripMargin).createOrReplaceTempView("t1")

      // Stack function with attributes as params.
      // Stack 4 attributes, no nulls need to be padded.
      runQueryAndCompare(s"""
                            |SELECT stack(2, id, name, id1, name1) from t1;
                            |""".stripMargin) {
        checkGlutenPlan[GenerateExecTransformer]
      }

      // Stack 3 attributes: there will be nulls.
      runQueryAndCompare(s"""
                            |SELECT stack(2, id, name, id1) from t1;
                            |""".stripMargin) {
        checkGlutenPlan[GenerateExecTransformer]
      }

      // Stack function with literals as params.
      runQueryAndCompare("SELECT stack(2, 1, 2, 3);") {
        checkGlutenPlan[GenerateExecTransformer]
      }

      // Stack function with params mixed with attributes and literals.
      runQueryAndCompare(s"""
                            |SELECT stack(2, id, name, 1) from t1;
                            |""".stripMargin) {
        checkGlutenPlan[GenerateExecTransformer]
      }
    }
  }

  test("test inline function") {
    Seq(true, false).foreach {
      isOuter =>
        val func = if (isOuter) "inline_outer" else "inline"

        // Literal: func(literal)
        runQueryAndCompare(s"""
                              |SELECT $func(array(
                              |  named_struct('c1', 0, 'c2', 1),
                              |  named_struct('c1', 2, 'c2', null)));
                              |""".stripMargin) {
          checkGlutenPlan[GenerateExecTransformer]
        }

        // CreateArray: func(array(col))
        withTempView("t1") {
          sql("""SELECT * from values
                |  (named_struct('c1', 0, 'c2', 1)),
                |  (named_struct('c1', 2, 'c2', null)),
                |  (null)
                |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t1")
          runQueryAndCompare(s"""
                                |SELECT $func(array(a)) from t1;
                                |""".stripMargin) {
            checkGlutenPlan[GenerateExecTransformer]
          }
        }

        withTempView("t2") {
          sql("""SELECT * from values
                |  array(
                |    named_struct('c1', 0, 'c2', 1),
                |    null,
                |    named_struct('c1', 2, 'c2', 3)
                |  ),
                |  array(
                |    null,
                |    named_struct('c1', 0, 'c2', 1),
                |    named_struct('c1', 2, 'c2', 3)
                |  ),
                |  array(),
                |  null
                |as tbl(a)
         """.stripMargin).createOrReplaceTempView("t2")
          runQueryAndCompare(s"""
                                |SELECT $func(a) from t2;
                                |""".stripMargin) {
            checkGlutenPlan[GenerateExecTransformer]
          }
        }

        // Fallback for array(struct(...), null) literal.
        runQueryAndCompare(s"""
                              |SELECT $func(array(
                              |  named_struct('c1', 0, 'c2', 1),
                              |  named_struct('c1', 2, 'c2', null),
                              |  null));
                              |""".stripMargin) {
          checkSparkPlan[GenerateExec]
        }
    }
  }

  test("test multi-generate") {
    withTable("t") {
      sql("CREATE TABLE t (col1 array<struct<a int, b string>>, col2 array<int>) using parquet")
      sql("INSERT INTO t VALUES (array(struct(1, 'a'), struct(2, 'b')), array(1, 2))")
      sql("INSERT INTO t VALUES (array(null, struct(3, 'c')), array(3, null))")

      runQueryAndCompare("""SELECT c1, c2, c3 FROM t
                           |LATERAL VIEW inline(col1) as c1, c2
                           |LATERAL VIEW explode(col2) as c3
                           |""".stripMargin) {
        checkGlutenPlan[GenerateExecTransformer]
      }
    }

    // More complex case which might cause projection name conflict.
    withTempView("script_trans") {
      sql("""SELECT * FROM VALUES
            |(1, 2, 3),
            |(4, 5, 6),
            |(7, 8, 9)
            |AS script_trans(a, b, c)
         """.stripMargin).createOrReplaceTempView("script_trans")
      runQueryAndCompare(s"""SELECT TRANSFORM(b, MAX(a), CAST(SUM(c) AS STRING), myCol, myCol2)
                            |  USING 'cat' AS (a STRING, b STRING, c STRING, d ARRAY<INT>, e STRING)
                            |FROM script_trans
                            |LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol
                            |LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2
                            |WHERE a <= 4
                            |GROUP BY b, myCol, myCol2
                            |HAVING max(a) > 1""".stripMargin) {
        checkSparkOperatorChainMatch[GenerateExecTransformer, FilterExecTransformer]
      }
    }
  }

  test("test array functions") {
    withTable("t") {
      sql("CREATE TABLE t (c1 ARRAY<INT>, c2 ARRAY<INT>, c3 STRING) using parquet")
      sql("INSERT INTO t VALUES (ARRAY(0, 1, 2, 3, 3), ARRAY(2, 2, 3, 4, 6), 'abc')")
      runQueryAndCompare("""
                           |SELECT array_except(c1, c2) FROM t;
                           |""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_distinct(c1), array_distinct(c2) FROM t;
                           |""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_position(c1, 3), array_position(c2, 2) FROM t;
                           |""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_repeat(c3, 5) FROM t;
                           |""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("""
                           |SELECT array_remove(c1, 3) FROM t;
                           |""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("array_sort") {
    withTable("t") {
      sql("create table t (a array<string>) using parquet")
      sql("insert into t values (array('a', 'acds', 'bcedf', 'dc'))")
      runQueryAndCompare(
        "select array_sort(a, (x, y) -> " +
          "if(length(x) > length(y), 1, if(length(x) < length(y), -1, 0))) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("Support bool type filter in scan") {
    withTable("t") {
      sql("create table t (id int, b boolean) using parquet")
      sql("insert into t values (1, true), (2, false), (3, null)")
      runQueryAndCompare("select * from t where b = true") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
      runQueryAndCompare("select * from t where b = false") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
      runQueryAndCompare("select * from t where b is NULL") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
    }
  }

  test("Support short int type filter in scan") {
    withTable("short_table") {
      sql("create table short_table (a short, b int) using parquet")
      sql(
        s"insert into short_table values " +
          s"(1, 1), (null, 2), (${Short.MinValue}, 3), (${Short.MaxValue}, 4)")
      runQueryAndCompare("select * from short_table where a = 1") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare("select * from short_table where a is NULL") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from short_table where a != ${Short.MinValue}") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from short_table where a != ${Short.MaxValue}") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
    }
  }

  test("Support int type filter in scan") {
    withTable("int_table") {
      sql("create table int_table (a int, b int) using parquet")
      sql(
        s"insert into int_table values " +
          s"(1, 1), (null, 2), (${Int.MinValue}, 3), (${Int.MaxValue}, 4)")
      runQueryAndCompare("select * from int_table where a = 1") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare("select * from int_table where a is NULL") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from int_table where a != ${Int.MinValue}") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      runQueryAndCompare(s"select * from int_table where a != ${Int.MaxValue}") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
    }
  }

  test("Fallback on timestamp column filter") {
    withTable("ts") {
      sql("create table ts (c1 int, c2 timestamp) using parquet")
      sql("insert into ts values (1, timestamp'2016-01-01 10:11:12.123456')")
      sql("insert into ts values (2, null)")
      sql("insert into ts values (3, timestamp'1965-01-01 10:11:12.123456')")

      runQueryAndCompare("select c1, c2 from ts where c1 = 1") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }

      // Fallback should only happen when there is a filter on timestamp column
      runQueryAndCompare(
        "select c1, c2 from ts where" +
          " c2 = timestamp'1965-01-01 10:11:12.123456'") { _ => }

      runQueryAndCompare(
        "select c1, c2 from ts where" +
          " c1 = 1 and c2 = timestamp'1965-01-01 10:11:12.123456'") { _ => }
    }
  }

  test("Test sample op") {
    withSQLConf(GlutenConfig.COLUMNAR_SAMPLE_ENABLED.key -> "true") {
      withTable("t") {
        sql("create table t (id int, b boolean) using parquet")
        sql("insert into t values (1, true), (2, false), (3, null), (4, true), (5, false)")
        runQueryAndCompare("select * from t TABLESAMPLE(20 PERCENT)", false) {
          checkGlutenPlan[SampleExecTransformer]
        }
      }
    }
  }

  test("test cross join") {
    withTable("t1", "t2") {
      sql("""
            |create table t1 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100)
            |""".stripMargin)
      sql("""
            |create table t2 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
            |""".stripMargin)

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "true") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkGlutenPlan[ShuffledHashJoinExecTransformer]
        }
      }

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1MB") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkGlutenPlan[BroadcastHashJoinExecTransformer]
        }
      }

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on t1.c1 = t2.c1;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 left semi join t2 on t1.c1 = t2.c1 and t1.c1 > 50;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }

      runQueryAndCompare(
        """
          |select * from t1 cross join t2;
          |""".stripMargin
      ) {
        checkGlutenPlan[CartesianProductExecTransformer]
      }

      runQueryAndCompare(
        """
          |select * from t1 cross join t2 on t1.c1 > t2.c1;
          |""".stripMargin
      ) {
        checkGlutenPlan[CartesianProductExecTransformer]
      }

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1MB") {
        runQueryAndCompare(
          """
            |select * from t1 cross join t2 on 2*t1.c1 > 3*t2.c1;
            |""".stripMargin
        ) {
          checkGlutenPlan[BroadcastNestedLoopJoinExecTransformer]
        }
      }
    }
  }

  test("test sort merge join") {
    withTable("t1", "t2") {
      sql("""
            |create table t1 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100)
            |""".stripMargin)
      sql("""
            |create table t2 using parquet as
            |select cast(id as int) as c1, cast(id as string) c2 from range(100) order by c1 desc;
            |""".stripMargin)
      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 inner join t2 on t1.c1 = t2.c1 and t1.c1 > 50;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 left join t2 on t1.c1 = t2.c1 and t1.c1 > 50;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 left semi join t2 on t1.c1 = t2.c1 and t1.c1 > 50;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 right join t2 on t1.c1 = t2.c1 and t1.c1 > 50;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }

      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          """
            |select * from t1 left anti join t2 on t1.c1 = t2.c1 and t1.c1 > 50;
            |""".stripMargin
        ) {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }
    }
  }

  test("Fix incorrect path by decode") {
    val c = "?.+<_>|/"
    val path = rootPath + "/test +?.+<_>|"
    val key1 = s"${c}key1 $c$c"
    val key2 = s"${c}key2 $c$c"
    val valueA = s"${c}some$c${c}value${c}A"
    val valueB = s"${c}some$c${c}value${c}B"
    val valueC = s"${c}some$c${c}value${c}C"
    val valueD = s"${c}some$c${c}value${c}D"

    val df1 = spark.range(3).withColumn(key1, lit(valueA)).withColumn(key2, lit(valueB))
    val df2 = spark.range(4, 7).withColumn(key1, lit(valueC)).withColumn(key2, lit(valueD))
    val df = df1.union(df2)
    df.write.partitionBy(key1, key2).format("parquet").mode("overwrite").save(path)

    spark.read.format("parquet").load(path).createOrReplaceTempView("test")
    runQueryAndCompare("select * from test") {
      checkGlutenPlan[FileSourceScanExecTransformer]
    }
  }

  test("partitioned write column order") {
    // Get a dataframe with a limited/known set of values for c_mktsegment and c_nationkey
    // to test the directory structure created for partitioned writes
    val df = sql(
      "SELECT c_name, c_nationkey, c_mktsegment FROM customer " +
        "WHERE c_mktsegment IN ('HOUSEHOLD', 'AUTOMOBILE') " +
        "AND c_nationkey < 5")

    withTempDir {
      tempDir =>
        val tempDirPath = tempDir.getPath
        df.write
          .format("parquet")
          .partitionBy("c_mktsegment", "c_nationkey")
          .mode("overwrite")
          .save(tempDirPath)

        // We expect the directory structure to look like:
        // {tempDirPath}/c_mktsegment=AUTOMOBILE/c_nationkey=[0-4]/*.parquet
        // {tempDirPath}/c_mktsegment=HOUSEHOLD/c_nationkey=[0-4]/*.parquet
        val expectedDirs = for {
          dir <- Seq("c_mktsegment=HOUSEHOLD", "c_mktsegment=AUTOMOBILE")
          subDir <- 0 to 4
        } yield Paths.get(tempDirPath, dir, "c_nationkey=" + subDir.toString).toString

        // Each directory should have .parquet file(s)
        expectedDirs.foreach {
          dir =>
            val path = Paths.get(dir)
            assert(Files.exists(path) && Files.isDirectory(path))
            val files = Files.list(path).iterator().asScala.toSeq
            assert(files.nonEmpty)
            val parquetFiles = files.filter(file => file.toString.contains(".parquet"))
            assert(parquetFiles.nonEmpty && parquetFiles.size == files.size)
        }

    }
  }

  test("timestamp cast fallback") {
    withTempPath {
      path =>
        (0 to 3).toDF("x").write.parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare(s"""
                              |SELECT x FROM view
                              |WHERE cast(x as timestamp)
                              |IN ('1970-01-01 08:00:00.001','1970-01-01 08:00:00.2')
                              |""".stripMargin)(_ => ())
    }
  }

  test("Columnar cartesian product with other join") {
    withTable("cartesian1", "cartesian2") {
      spark.sql("""
                  |CREATE TABLE cartesian1 USING PARQUET
                  |AS SELECT id as c1, id % 3 as c2 FROM range(20)
                  |""".stripMargin)
      spark.sql("""
                  |CREATE TABLE cartesian2 USING PARQUET
                  |AS SELECT id as c1, id % 3 as c2 FROM range(20)
                  |""".stripMargin)

      runQueryAndCompare(
        """
          |SELECT * FROM (
          | SELECT /*+ shuffle_replicate_nl(cartesian1) */ cartesian1.c1, cartesian2.c2
          | FROM cartesian1 join cartesian2
          |)tmp
          |join cartesian2 on tmp.c1 = cartesian2.c1
          |""".stripMargin)(df => checkFallbackOperators(df, 0))
    }
  }

  test("Support multi-children count") {
    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(l_shipdate, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment), count(l_shipdate, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey), count(l_shipdate, l_comment)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment), count(l_shipdate)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("Support multi-children count with row construct") {
    runQueryAndCompare(
      """
        |select l_orderkey, count(distinct l_partkey, l_comment), corr(l_partkey, l_partkey+1)
        |from lineitem group by l_orderkey
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("Remainder with non-foldable right side") {
    withTable("remainder") {
      spark.sql("""
                  |CREATE TABLE remainder USING PARQUET
                  |AS SELECT id as c1, id % 3 as c2 FROM range(3)
                  |""".stripMargin)
      spark.sql("INSERT INTO TABLE remainder VALUES(0, null)")

      runQueryAndCompare("SELECT c1 % c2 FROM remainder")(df => checkFallbackOperators(df, 0))
    }
  }

  test("Support Array type signature") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(id INT, l ARRAY<INT>) USING PARQUET")
      sql("INSERT INTO t1 VALUES(1, ARRAY(1, 2)), (2, ARRAY(3, 4))")
      runQueryAndCompare("SELECT first(l) FROM t1")(df => checkFallbackOperators(df, 0))

      sql("CREATE TABLE t2(id INT, l ARRAY<STRUCT<k: INT, v: INT>>) USING PARQUET")
      sql("INSERT INTO t2 VALUES(1, ARRAY(STRUCT(1, 100))), (2, ARRAY(STRUCT(2, 200)))")
      runQueryAndCompare("SELECT first(l) FROM t2")(df => checkFallbackOperators(df, 0))
    }
  }

  test("Fall back multiple expressions") {
    runQueryAndCompare(
      """
        |select (l_partkey % 10 + 5)
        |from lineitem
        |""".stripMargin
    )(checkGlutenPlan[ProjectExecTransformer])

    runQueryAndCompare(
      """
        |select l_partkey
        |from lineitem where (l_partkey % 10 + 5) > 6
        |""".stripMargin
    )(checkGlutenPlan[FilterExecTransformer])

    withSQLConf(GlutenConfig.COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD.key -> "2") {
      runQueryAndCompare(
        """
          |select (l_partkey % 10 + 5)
          |from lineitem
          |""".stripMargin
      )(checkSparkPlan[ProjectExec])

      runQueryAndCompare(
        """
          |select l_partkey
          |from lineitem where (l_partkey % 10 + 5) > 6
          |""".stripMargin
      )(checkSparkPlan[FilterExec])
    }
  }

  test("test array literal") {
    withTable("array_table") {
      sql("create table array_table(a array<bigint>) using parquet")
      sql("insert into table array_table select array(1)")
      runQueryAndCompare("select size(coalesce(a, array())) from array_table") {
        df =>
          {
            assert(getExecutedPlan(df).count(_.isInstanceOf[ProjectExecTransformer]) == 1)
          }
      }
    }
  }

  test("test map literal") {
    withTable("map_table") {
      sql("create table map_table(a map<bigint, string>) using parquet")
      sql("insert into table map_table select map(1, 'hello')")
      runQueryAndCompare("select size(coalesce(a, map())) from map_table") {
        df =>
          {
            assert(getExecutedPlan(df).count(_.isInstanceOf[ProjectExecTransformer]) == 1)
          }
      }
    }
  }

  test("Support In list option contains non-foldable expression") {
    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey - 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey + 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("Support StructType in HashAggregate") {
    runQueryAndCompare("""
                         |select s, count(1) from (
                         |   select named_struct('id', cast(id as int),
                         |   'id_str', cast(id as string)) as s from range(100)
                         |) group by s
                         |""".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
  }

  test("test RoundRobin repartition with sort") {
    def checkRoundRobinOperators(df: DataFrame): Unit = {
      checkGlutenPlan[SortExecTransformer](df)
      checkGlutenPlan[ColumnarShuffleExchangeExec](df)
    }

    // scalastyle:off
    runQueryAndCompare("SELECT /*+ REPARTITION(3) */ l_orderkey, l_partkey FROM lineitem") {
      /*
        ColumnarExchange RoundRobinPartitioning(3), REPARTITION_BY_NUM, [l_orderkey#16L, l_partkey#17L)
        +- ^(2) ProjectExecTransformer [l_orderkey#16L, l_partkey#17L]
          +- ^(2) SortExecTransformer [hash_partition_key#302 ASC NULLS FIRST], false, 0
            +- ^(2) ProjectExecTransformer [hash(l_orderkey#16L, l_partkey#17L) AS hash_partition_key#302, l_orderkey#16L, l_partkey#17L]
                +- ^(2) BatchScanExecTransformer[l_orderkey#16L, l_partkey#17L] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<l_orderkey:bigint,l_partkey:bigint>, PushedFilters: [] RuntimeFilters: []
       */
      checkRoundRobinOperators
    }
    // scalastyle:on

    withSQLConf("spark.sql.execution.sortBeforeRepartition" -> "false") {
      runQueryAndCompare("""SELECT /*+ REPARTITION(3) */
                           | l_orderkey, l_partkey FROM lineitem""".stripMargin) {
        df =>
          {
            assert(getExecutedPlan(df).count(_.isInstanceOf[SortExecTransformer]) == 0)
          }
      }
    }

    // Gluten-5206: test repartition on map type
    runQueryAndCompare(
      "SELECT /*+ REPARTITION(3) */ l_orderkey, map(l_orderkey, l_partkey) FROM lineitem")(
      checkRoundRobinOperators)
  }

  test("Support Map type signature") {
    // test map<str,str>
    withTempView("t1") {
      Seq[(Int, Map[String, String])]((1, Map("byte1" -> "aaa")), (2, Map("byte2" -> "bbbb")))
        .toDF("c1", "map_c2")
        .createTempView("t1")
      runQueryAndCompare("""
                           |SELECT c1, collect_list(map_c2) FROM t1 group by c1;
                           |""".stripMargin) {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
    // test map<str,map<str,str>>
    withTempView("t2") {
      Seq[(Int, Map[String, Map[String, String]])](
        (1, Map("byte1" -> Map("test1" -> "aaaa"))),
        (2, Map("byte2" -> Map("test1" -> "bbbb"))))
        .toDF("c1", "map_c2")
        .createTempView("t2")
      runQueryAndCompare("""
                           |SELECT c1, collect_list(map_c2) FROM t2 group by c1;
                           |""".stripMargin) {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
    // test map<map<str,str>,map<str,str>>
    withTempView("t3") {
      Seq[(Int, Map[Map[String, String], Map[String, String]])](
        (1, Map(Map("byte1" -> "aaaa") -> Map("test1" -> "aaaa"))),
        (2, Map(Map("byte2" -> "bbbb") -> Map("test1" -> "bbbb"))))
        .toDF("c1", "map_c2")
        .createTempView("t3")
      runQueryAndCompare("""
                           |SELECT collect_list(map_c2) FROM t3 group by c1;
                           |""".stripMargin) {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
    // test map<str,list<str>>
    withTempView("t4") {
      Seq[(Int, Map[String, Array[String]])](
        (1, Map("test1" -> Array("test1", "test2"))),
        (2, Map("test2" -> Array("test1", "test2"))))
        .toDF("c1", "map_c2")
        .createTempView("t4")
      runQueryAndCompare("""
                           |SELECT collect_list(map_c2) FROM t4 group by c1;
                           |""".stripMargin) {
        checkGlutenPlan[HashAggregateExecTransformer]
      }
    }
  }

  test("Cast date to string") {
    withTempPath {
      path =>
        Seq("2023-01-01", "2023-01-02", "2023-01-03")
          .toDF("dateColumn")
          .select(to_date($"dateColumn", "yyyy-MM-dd").as("dateColumn"))
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT cast(dateColumn as string) from view") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("Cast string to date") {
    withTempView("view") {
      Seq("2023-01-01", "2023-01-02", "-1", "-111-01-01")
        .toDF("dateColumn")
        .createOrReplaceTempView("view")
      runQueryAndCompare("SELECT cast(dateColumn as date) from view") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("Cast date to timestamp") {
    withTempPath {
      path =>
        Seq("2023-01-01", "2023-01-02", "2023-01-03")
          .toDF("dateColumn")
          .select(to_date($"dateColumn", "yyyy-MM-dd").as("dateColumn"))
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("SELECT cast(dateColumn as timestamp) from view") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("cast date to timestamp with timezone") {
    sql("SET spark.sql.session.timeZone = America/Los_Angeles")
    val dfInLA = sql("SELECT cast(date'2023-01-02 01:01:01' as timestamp) as ts")

    sql("SET spark.sql.session.timeZone = Asia/Shanghai")
    val dfInSH = sql("SELECT cast(date'2023-01-02 01:01:01' as timestamp) as ts")

    // Casting date to timestamp considers configured local timezone.
    // There is 16-hour difference between America/Los_Angeles & Asia/Shanghai.
    val timeInMillisInLA = dfInLA.collect()(0).getTimestamp(0).getTime()
    val timeInMillisInSH = dfInSH.collect()(0).getTimestamp(0).getTime()
    assert(TimeUnit.MILLISECONDS.toHours(timeInMillisInLA - timeInMillisInSH) == 16)

    // check ProjectExecTransformer
    val plan1 = dfInLA.queryExecution.executedPlan
    val plan2 = dfInSH.queryExecution.executedPlan
    assert(plan1.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
    assert(plan2.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("cast timestamp to date") {
    val query = "select cast(ts as date) from values (timestamp'2024-01-01 00:00:00') as tab(ts)"
    runQueryAndCompare(query) {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("timestamp broadcast join") {
    spark.range(0, 5).createOrReplaceTempView("right")
    spark.sql("SELECT id, timestamp_micros(id) as ts from right").createOrReplaceTempView("left")
    val expected = spark.sql("SELECT unix_micros(ts) from left")
    val df = spark.sql(
      "SELECT unix_micros(ts)" +
        " FROM left RIGHT OUTER JOIN right ON left.id = right.id")
    // Verify there is not precision loss for timestamp columns after data broadcast.
    checkAnswer(df, expected)
  }

  test("Test json_tuple function") {
    withTempView("t") {
      Seq[(String)](("{\"a\":\"b\"}"), (null), ("{\"b\":\"a\"}"))
        .toDF("json_field")
        .createOrReplaceTempView("t")
      runQueryAndCompare(
        "SELECT * from t lateral view json_tuple(json_field, 'a', 'b') as fa, fb") {
        checkGlutenPlan[GenerateExecTransformer]
      }
    }

    withTempView("json_tuple_test") {
      Seq[(String)](("{\"a.b\":\"b\"}"))
        .toDF("json_field")
        .createOrReplaceTempView("json_tuple_test")
      runQueryAndCompare("SELECT json_tuple(json_field, 'a.b') from json_tuple_test") {
        checkGlutenPlan[GenerateExecTransformer]
      }
    }

    runQueryAndCompare(
      """
        |SELECT
        | l_orderkey,
        | json_tuple('{"a" : 1, "b" : 2}', CAST(NULL AS STRING), 'b', CAST(NULL AS STRING), 'a')
        |from lineitem
        |""".stripMargin) {
      checkGlutenPlan[GenerateExecTransformer]
    }
  }

  test("Fix shuffle with null type failure") {
    // single and other partitioning
    Seq("1", "2").foreach {
      numShufflePartitions =>
        withSQLConf("spark.sql.shuffle.partitions" -> numShufflePartitions) {
          def checkNullTypeRepartition(df: => DataFrame, numProject: Int): Unit = {
            var expected: Array[Row] = null
            withSQLConf("spark.sql.execution.sortBeforeRepartition" -> "false") {
              expected = df.collect()
            }
            val actual = df
            checkAnswer(actual, expected)
            assert(
              collect(actual.queryExecution.executedPlan) {
                case p: ProjectExec => p
              }.size == numProject
            )
            assert(
              collect(actual.queryExecution.executedPlan) {
                case shuffle: ColumnarShuffleExchangeExec => shuffle
              }.size == 1
            )
          }

          // hash
          checkNullTypeRepartition(
            spark
              .table("lineitem")
              .selectExpr("l_orderkey", "null as x")
              .repartition($"l_orderkey"),
            0
          )
          // range
          checkNullTypeRepartition(
            spark
              .table("lineitem")
              .selectExpr("l_orderkey", "null as x")
              .repartitionByRange($"l_orderkey"),
            0
          )
          // round robin
          checkNullTypeRepartition(
            spark.table("lineitem").selectExpr("l_orderkey", "null as x").repartition(),
            0
          )
          checkNullTypeRepartition(
            spark.table("lineitem").selectExpr("null as x", "null as y").repartition(),
            0
          )
        }
    }
  }

  test("fix non-deterministic filter executed twice when push down to scan") {
    val df = sql("select * from lineitem where rand() <= 0.5")
    // plan check
    val plan = df.queryExecution.executedPlan
    val scans = plan.collect { case scan: FileSourceScanExecTransformer => scan }
    val filters = plan.collect { case filter: FilterExecTransformer => filter }
    assert(scans.size == 1)
    assert(filters.size == 1)
    assert(scans.head.filterExprs().size == 1)
    val remainingFilters = FilterHandler.subtractFilters(
      splitConjunctivePredicates(filters.head.condition),
      scans.head.filterExprs())
    assert(remainingFilters.isEmpty)

    // result length check, table lineitem has 60,000 rows
    val resultLength = df.collect().length
    assert(resultLength > 25000 && resultLength < 35000)
  }

  test("Deduplicate sorting keys") {
    runQueryAndCompare("select * from lineitem order by l_orderkey, l_orderkey") {
      checkGlutenPlan[SortExecTransformer]
    }
  }

  // Enable the test after fixing https://github.com/apache/incubator-gluten/issues/6827
  ignore("Test round expression") {
    val df1 = runQueryAndCompare("SELECT round(cast(0.5549999999999999 as double), 2)") { _ => }
    checkLengthAndPlan(df1, 1)
    val df2 = runQueryAndCompare("SELECT round(cast(0.19324999999999998 as double), 2)") { _ => }
    checkLengthAndPlan(df2, 1)
  }

  test("Fix wrong rescale") {
    withTable("t") {
      sql("create table t (col0 decimal(10, 0), col1 decimal(10, 0)) using parquet")
      sql("insert into t values (0, 0)")
      runQueryAndCompare("select col0 / (col1 + 1E-8) from t") { _ => }
    }
  }

  test("Fix struct field case error") {
    val excludedRules = "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates," +
      "org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughNonJoin"
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRules) {
      withTempPath {
        path =>
          sql("select named_struct('A', a) as c1 from values (1), (2) as data(a)").write.parquet(
            path.getAbsolutePath)
          val df = spark.read
            .parquet(path.getAbsolutePath)
            .union(spark.read.parquet(path.getAbsolutePath))
            .filter("c1.A > 1")
            .select("c1.A")
          checkAnswer(df, Seq(Row(2), Row(2)))
      }
    }
  }

  // Since https://github.com/apache/incubator-gluten/pull/7330.
  test("field names contain non-ASCII characters") {
    withTempPath {
      path =>
        // scalastyle:off nonascii
        Seq((1, 2, 3, 4)).toDF("товары", "овары", "国ⅵ", "中文").write.parquet(path.getCanonicalPath)
        // scalastyle:on
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("select * from view") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
    }

    withTempPath {
      path =>
        // scalastyle:off nonascii
        spark.range(10).toDF("中文").write.parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).filter("`中文`>1").createOrReplaceTempView("view")
        // scalastyle:on
        runQueryAndCompare("select * from view") {
          checkGlutenPlan[FileSourceScanExecTransformer]
        }
    }
  }

  test("test 'spark.gluten.enabled'") {
    withSQLConf(GlutenCoreConfig.GLUTEN_ENABLED.key -> "true") {
      runQueryAndCompare("select * from lineitem limit 1") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
      withSQLConf(GlutenCoreConfig.GLUTEN_ENABLED.key -> "false") {
        runQueryAndCompare("select * from lineitem limit 1") {
          checkSparkPlan[FileSourceScanExec]
        }
      }
      runQueryAndCompare("select * from lineitem limit 1") {
        checkGlutenPlan[FileSourceScanExecTransformer]
      }
    }
  }

  test("support null type in aggregate") {
    runQueryAndCompare("SELECT max(null), min(null) from range(10)".stripMargin) {
      checkGlutenPlan[HashAggregateExecTransformer]
    }
  }

  test("test get_struct_field with scalar function as input") {
    withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
      withTable("t") {
        withTempPath {
          path =>
            Seq[String](
              "{\"a\":1,\"b\":[10, 11, 12]}",
              "{\"a\":2,\"b\":[20, 21, 22]}"
            ).toDF("json_str").write.parquet(path.getCanonicalPath)
            spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("t")

            val query =
              """
                | select
                |  from_json(json_str, 'a INT, b ARRAY<INT>').a,
                |  from_json(json_str, 'a INT, b ARRAY<INT>').b
                | from t
                |""".stripMargin

            runQueryAndCompare(query)(
              df => {
                val executedPlan = getExecutedPlan(df)
                assert(executedPlan.count(_.isInstanceOf[ProjectExec]) == 0)
                assert(executedPlan.count(_.isInstanceOf[ProjectExecTransformer]) == 1)
              })
        }
      }
    }
  }

  test("Blacklist expression can be handled by ColumnarPartialProject") {
    withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "regexp_replace") {
      runQueryAndCompare(
        "SELECT c_custkey, c_name, regexp_replace(c_comment, '\\w', 'something') FROM customer") {
        df =>
          val executedPlan = getExecutedPlan(df)
          assert(executedPlan.count(_.isInstanceOf[ProjectExec]) == 0)
          assert(executedPlan.count(_.isInstanceOf[ColumnarPartialProjectExec]) == 1)
      }
    }
  }

  test("Check VeloxResizeBatches is added in ShuffleRead") {
    Seq(true, false).foreach(
      coalesceEnabled => {
        withSQLConf(
          VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT.key -> "true",
          SQLConf.SHUFFLE_PARTITIONS.key -> "10",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> coalesceEnabled.toString
        ) {
          runQueryAndCompare(
            "SELECT l_orderkey, count(1) from lineitem group by l_orderkey".stripMargin) {
            df =>
              val executedPlan = getExecutedPlan(df)
              if (coalesceEnabled) {
                // VeloxResizeBatches(AQEShuffleRead(ShuffleQueryStage(ColumnarShuffleExchange)))
                assert(executedPlan.sliding(4).exists {
                  case Seq(
                        _: ColumnarShuffleExchangeExec,
                        _: ShuffleQueryStageExec,
                        _: AQEShuffleReadExec,
                        _: VeloxResizeBatchesExec
                      ) =>
                    true
                  case _ => false
                })
              } else {
                // VeloxResizeBatches(ShuffleQueryStage(ColumnarShuffleExchange))
                assert(executedPlan.sliding(3).exists {
                  case Seq(
                        _: ColumnarShuffleExchangeExec,
                        _: ShuffleQueryStageExec,
                        _: VeloxResizeBatchesExec) =>
                    true
                  case _ => false
                })
              }
          }
        }
      })
  }

  test("RowToVeloxColumnar preferredBatchBytes") {
    Seq("1", "80", "100000000").foreach(
      preferredBatchBytes => {
        withSQLConf(
          VeloxConfig.COLUMNAR_VELOX_PREFERRED_BATCH_BYTES.key -> preferredBatchBytes
        ) {
          val df = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("Col").select($"Col".plus(1))
          assert(df.collect().length == 10)
          val ops = collect(df.queryExecution.executedPlan) { case p: RowToVeloxColumnarExec => p }
          assert(ops.size == 1)
          val op = ops.head
          val metrics = op.metrics
          // Each row consumes 16 bytes as an UnsafeRow.
          val expectedNumBatches = preferredBatchBytes match {
            case "1" => 10
            case "80" => 2
            case _ => 1
          }
          assert(metrics("numOutputBatches").value == expectedNumBatches)
        }
      })
  }

  test("Expression unsupported by backend can be handled by ColumnarPartialProject") {
    runQueryAndCompare(
      "SELECT c_custkey, map_from_arrays(array(c_name), array(c_comment)) FROM customer") {
      df =>
        val executedPlan = getExecutedPlan(df)
        assert(executedPlan.count(_.isInstanceOf[ProjectExec]) == 0)
        assert(executedPlan.count(_.isInstanceOf[ColumnarPartialProjectExec]) == 1)
    }
  }

  testWithMinSparkVersion("Left single join should not result into exception", "4.0") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      spark.sql("create temp view x (x1, x2) as values (1, 1), (2, 2);")
      spark.sql("create temp view y (y1, y2) as values (2, 0), (3, -1);")

      runQueryAndCompare(
        "select *," +
          " (select count(*) from y where x1 = y1 and y2 + 10 = x1 + 1 group by y2) from x") {
        df =>
          assert(
            getExecutedPlan(df).collect {
              case join: BaseJoinExec if join.joinType.toString == "LeftSingle" => join
            }.size == 1
          )
      }
    }
  }
}
