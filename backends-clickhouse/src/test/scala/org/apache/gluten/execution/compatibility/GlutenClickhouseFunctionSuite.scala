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
package org.apache.gluten.execution.compatibility

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ParquetSuite, ProjectExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.SQLConf

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickhouseFunctionSuite extends ParquetSuite {

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1073741824")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.minPartitionNum", "1")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.sql.warehouse.dir", warehouse)
      .setMaster("local[1]")
  }

  test("test uuid - write and read") {
    withSQLConf(
      (GlutenConfig.NATIVE_WRITER_ENABLED.key, "true"),
      (GlutenConfig.GLUTEN_ENABLED.key, "true")) {
      withTable("uuid_test") {
        spark.sql("create table if not exists uuid_test (id string) using parquet")

        val df = spark.sql("select regexp_replace(uuid(), '-', '') as id from range(1)")
        df.cache()
        df.write.insertInto("uuid_test")

        val df2 = spark.table("uuid_test")
        val diffCount = df.exceptAll(df2).count()
        assert(diffCount == 0)
      }
    }
  }

  test("https://github.com/apache/gluten/issues/6938") {
    val testSQL =
      s"""
         |select * from (
         |  select 1 as x, r_name as y, 's' as z from region
         |  union all
         |  select 2 as x, n_name as y, null as z from nation
         |) order by y,x,z
         |""".stripMargin
    runQueryAndCompare(testSQL)(_ => ())
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

  test("GLUTEN-5981 null value from get_json_object") {
    withTable("json_t1") {
      spark.sql("create table json_t1 (a string) using parquet")
      spark.sql("insert into json_t1 values ('{\"a\":null}')")
      runQueryAndCompare(
        """
          |SELECT get_json_object(a, '$.a') is null from json_t1
          |""".stripMargin
      )(df => checkFallbackOperators(df, 0))
    }
  }

  test("Fix arrayDistinct(Array(Nullable(Decimal))) core dump") {
    withTable("json_t1") {
      val create_sql =
        """
          |create table if not exists test(
          | dec array<decimal(10, 2)>
          |) using parquet
          |""".stripMargin
      val fill_sql =
        """
          |insert into test values(array(1, 2, null)), (array(null, 2,3, 5))
          |""".stripMargin
      val query_sql =
        """
          |select array_distinct(dec) from test;
          |""".stripMargin
      spark.sql(create_sql)
      spark.sql(fill_sql)
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    }
  }

  test("intersect all") {
    withTable("t1", "t2") {
      spark.sql("create table t1 (a int, b string) using parquet")
      spark.sql("insert into t1 values (1, '1'),(2, '2'),(3, '3'),(4, '4'),(5, '5'),(6, '6')")
      spark.sql("create table t2 (a int, b string) using parquet")
      spark.sql("insert into t2 values (4, '4'),(5, '5'),(6, '6'),(7, '7'),(8, '8'),(9, '9')")
      runQueryAndCompare(
        """
          |SELECT a,b FROM t1 INTERSECT ALL SELECT a,b FROM t2
          |""".stripMargin
      )(df => checkFallbackOperators(df, 0))
    }
  }

  test("array decimal32 CH column to row") {
    compareResultsAgainstVanillaSpark("SELECT array(1.0, 2.0)", true, { _ => })
    compareResultsAgainstVanillaSpark("SELECT map(1.0, '2', 3.0, '4')", true, { _ => })
  }

  test("array decimal32 spark row to CH column") {
    withTable("test_array_decimal") {
      sql("""
            |create table test_array_decimal(val array<decimal(5,1)>)
            |using parquet
            |""".stripMargin)
      sql("""
            |insert into test_array_decimal
            |values array(1.0, 2.0), array(3.0, 4.0),
            |array(5.0, 6.0), array(7.0, 8.0), array(7.0, 7.0)
            |""".stripMargin)
      // disable native scan so will get a spark row to CH column
      withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        val q = "SELECT max(val) from test_array_decimal"
        compareResultsAgainstVanillaSpark(q, true, { _ => }, false)
        val q2 = "SELECT max(val[0]) from test_array_decimal"
        compareResultsAgainstVanillaSpark(q2, true, { _ => }, false)
        val q3 = "SELECT max(val[1]) from test_array_decimal"
        compareResultsAgainstVanillaSpark(q3, true, { _ => }, false)
      }
    }
  }

  test("duplicate column name issue") {
    withTable("left_table", "right_table") {
      sql("create table left_table(id int, name string) using orc")
      sql("create table right_table(id int, book string) using orc")
      sql("insert into left_table values (1,'a'),(2,'b'),(3,'c'),(4,'d')")
      sql("insert into right_table values (1,'a'),(1,'b'),(2,'c'),(2,'d')")
      compareResultsAgainstVanillaSpark(
        """
          |select p1.id, p1.name, p2.book
          | from left_table p1 left join
          | (select id, id, book
          |    from right_table where id <= 2) p2
          | on p1.id=p2.id
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("function_input_file_expr") {
    withTable("test_table") {
      sql("create table test_table(a int) using parquet")
      sql("insert into test_table values (1), (2)")
      compareResultsAgainstVanillaSpark(
        """
          |select a,input_file_name(), input_file_block_start(),
          |input_file_block_length() from test_table
          |""".stripMargin,
        true,
        { _ => }
      )
      compareResultsAgainstVanillaSpark(
        """
          |select input_file_name(), input_file_block_start(),
          |input_file_block_length() from test_table
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7389: cast map to string diff with spark") {
    withTable("test_7389") {
      sql("create table test_7389(a map<string, int>) using parquet")
      sql("insert into test_7389 values(map('a', 1, 'b', 2))")
      compareResultsAgainstVanillaSpark(
        """
          |select cast(a as string) from test_7389
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7594: cast const map to string") {
    withSQLConf(
      (
        "spark.sql.optimizer.excludedRules",
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding," +
          "org.apache.spark.sql.catalyst.optimizer.NullPropagation")) {
      runQueryAndCompare("select cast(map(1,'2') as string)")(
        checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("GLUTEN-7550 get_json_object in IN") {
    withTable("test_7550") {
      sql("create table test_7550(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7550 values('{\'a\':\'1\'}'),('{\'a\':\'2\'}'),('{\'a\':\'3\'}')
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select a, get_json_object(a, '$.a') in ('1', '2') from test_7550
          |""".stripMargin,
        true,
        { _ => }
      )
      compareResultsAgainstVanillaSpark(
        """
          |select a in ('1', '2') from test_7550
          |where get_json_object(a, '$.a') in ('1', '2')
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7552 normalize json path") {
    withTable("test_7552") {
      sql("create table test_7552(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7552 values('{\'a\':\'1\'}')
          |,('{"a":3}')
          |,('{"3a":4}')
          |,('{"a c":5}')
          |,('{"3 d":6"}')
          |,('{"a:b":7}')
          |,('{"=a":8}')
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select a
          |, get_json_object(a, '$.a')
          |, get_json_object(a, '$.3a')
          |, get_json_object(a, '$.a c')
          |, get_json_object(a, '$.3 d')
          |, get_json_object(a, '$.a:b')
          |, get_json_object(a, '$.=a')
          |from test_7552
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7563 too large number in json") {
    withTable("test_7563") {
      sql("create table test_7563(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7563 values
          |('{"a":2.696539702293474E308}')
          |,('{"a":1232}')
          |,('{"a":1234xxx}')
          |,('{"a":2.696539702293474E30123}')
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select a, get_json_object(a, '$.a') from test_7563
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7591 get_json_object: normalize empty object fail") {
    withTable("test_7591") {
      sql("create table test_7591(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7591
          |select if(id < 10005, concat('{"a":', id), concat('{"a":', id , ', "b":{}}')) from
          |(SELECT explode(sequence(1, 10010)) as id);
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select get_json_object(a, '$.a') from test_7591
          |where get_json_object(a, '$.a') is not null
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7545: https://github.com/apache/gluten/issues/7545") {
    withTable("regexp_test") {
      sql("create table if not exists regexp_test (id string) using parquet")
      sql("insert into regexp_test values('1999-6-1')")
      compareResultsAgainstVanillaSpark(
        """
          |select regexp_replace(id,
          |'([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})',
          |'$1-$2-$3') from regexp_test
        """.stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-8148: Fix corr with NaN") {
    withTable("corr_nan") {
      sql("create table if not exists corr_nan (x double, y double) using parquet")
      sql("insert into corr_nan values(0,1)")
      compareResultsAgainstVanillaSpark(
        """
          |select corr(x,y), corr(y,x) from corr_nan
        """.stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7755: translate support args with unequal length") {
    withTable("test_7755") {
      sql("create table if not exists test_7755 (id string) using parquet")
      sql("insert into test_7755 values('aAbBcC')")
      compareResultsAgainstVanillaSpark(
        """
          |select translate(id, 'abc', '12') from test_7755
        """.stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7602: cast array to string") {
    withTable("test_7602") {
      sql("create table if not exists test_7602 (v ARRAY<STRING>) using parquet")
      sql("insert into test_7602 values(array('1', '2a', 'foo'));")
      compareResultsAgainstVanillaSpark(
        """
          |select cast(v as string) from test_7602
        """.stripMargin,
        true,
        { _ => }
      )
      val q = "select cast(a as string) from (select array('123',NULL) as a)"
      compareResultsAgainstVanillaSpark(q, true, { _ => })
    }
  }

  test("GLUTEN-9049: cast complex type to string") {
    withTable("test_9049") {
      sql("""
            |CREATE TABLE test_9049 (
            |  id INT,
            |  v1 array<string>,
            |  v2 array<array<string>>,
            |  v3 array<struct<s1:string, s2:int>>,
            |  v4 array<map<string, int>>,
            |  v5 map<string, array<string>>,
            |  v6 map<array<string>, map<string, struct<s1:string, s2:int>>>,
            |  v7 struct<s1:array<string>, s2:string, s3:map<string, int>, s4:struct<ss1:string, ss2:int>>
            |) using orc;
            |""".stripMargin)
      sql("""
            |insert overwrite table test_9049 values
            |(1,
            |array('123', '\'456\'', null),
            |array(array('abc', '\'edf\'', null), null),
            |array(struct("\'abc\'", 100), struct("\'edf\'", 200), null, struct("\'123\'", 300)),
            |array(map('k1', 1), map('\'k2\'', 2), map('k3', null), null),
            |map('k1', array('v1', 'v2', null), "'k2'", null),
            |map(array('a1', 'a2', null), map('aa1', struct('s1', 123))),
            |struct(array('sa1', null, "'sa2'"), null, map('sm1', null), struct("ss1", 123))
            |),
            |(2,
            |array('345', null, '\'678\''),
            |array(array('abc', '\'edf\'', null), null),
            |array(struct("\'abc\'", 400), struct("\'edf\'", 500), null, struct("\'123\'", 600)),
            |array(map('k1', 1), map('\'k2\'', 2), map('k3', null), null),
            |map('k1', array('v1', 'v2', null), "'k2'", null),
            |map(array('a1', 'a2', null), map('aa1', struct('s1', 234))),
            |struct(array('sa1', null, "'sa2'"), null, map('sm1', null), struct("ss1", 345))
            |),
            |(3, null, null, null, null, null, null, null);
            |""".stripMargin)
      val checkSql =
        """
          |select id,
          |cast(v1 as string), cast(v2 as string),
          |cast(v3 as string), cast(v4 as string),
          |cast(v5 as string), cast(v6 as string),
          |cast(v7 as string) from test_9049;
          |""".stripMargin
      compareResultsAgainstVanillaSpark(checkSql, true, { _ => })

      withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
        runQueryAndCompare(
          """
            |select
            |cast(array("'123'", null) as string),
            |cast(array(array('123\'')) as string),
            |cast(array(struct(1, '123\'')) as string),
            |cast(array(null) as string),
            |cast(map(array(1), map("aa", "123\'")) as string),
            |cast(named_struct("a", "test\'", "b", 1) as string),
            |cast(named_struct("a", "test\'", "b", 1, "c", struct("\'test"), "d", array('123\'')) as string)
            |""".stripMargin
        )(checkGlutenPlan[ProjectExecTransformer])
      }
    }
  }

  test("GLUTEN-8921: Type mismatch at checkDecimalOverflowSparkOrNull") {
    compareResultsAgainstVanillaSpark(
      """
        |select l_shipdate, avg(l_quantity), count(0) over() COU,
        |SUM(-1.1) over() SU, AVG(-2) over() AV,
        |max(-1.1) over() MA, min(-3) over() MI
        |from lineitem
        |where l_shipdate <= date'1998-09-02'
        |group by l_shipdate
        |order by l_shipdate
      """.stripMargin,
      true,
      { _ => }
    )
  }

  test("GLUTEN-8922: Incorrect result in lead function with constant col") {
    compareResultsAgainstVanillaSpark(
      """
        |select l_shipdate,
        |FIRST_VALUE(-2) over() FI,
        |LAST_VALUE(-2) over() LA,
        |lag(-2) over(order by l_shipdate) lag0,
        |lead(-2) over(order by l_shipdate) lead0
        |from lineitem
        |where l_shipdate <= date'1998-09-02'
        |group by l_shipdate
        |order by l_shipdate
      """.stripMargin,
      true,
      { _ => }
    )
  }

}
