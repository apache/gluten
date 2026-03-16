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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ColumnarPartialGenerateExec, ColumnarPartialProjectExec, GlutenQueryComparisonTest}
import org.apache.gluten.expression.UDFMappings
import org.apache.gluten.udf.{CustomerUDF, DuplicateArray}
import org.apache.gluten.udtf.{ConditionalOutputUDTF, CustomerUDTF, NoInputUDTF, SimpleUDTF}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.classic.ClassicTypes._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils

import java.io.File

import scala.collection.mutable
import scala.reflect.ClassTag

class GlutenHiveUDFSuite extends GlutenQueryComparisonTest with SQLTestUtils {
  private var _spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (_spark == null) {
      // By default, the classic SparkSession is constructed.
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("warn")

    createTestTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override protected def spark: ClassicSparkSession = _spark.asInstanceOf[ClassicSparkSession]

  protected def defaultSparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.sql.test", "")
      .set("spark.sql.testkey", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      .set(
        HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
        "org.apache.spark.sql.hive.execution.PairSerDe")
      // SPARK-8910
      .set(UI_ENABLED, false)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
      // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
      .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  protected def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
  }

  private def withTempFunction(funcName: String)(f: => Unit): Unit = {
    try f
    finally sql(s"DROP TEMPORARY FUNCTION IF EXISTS $funcName")
  }

  private def checkOperatorMatch[T <: SparkPlan](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  private def createTestTable(): Unit = {
    val table = "lineitem"
    val tableDir = getClass.getResource("/tpch-data-parquet").getFile
    val tablePath = new File(tableDir, table).getAbsolutePath
    val tableDF = spark.read.format("parquet").load(tablePath)
    tableDF.createOrReplaceTempView(table)
  }

  test("customer udf") {
    withTempFunction("testUDF") {
      sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[CustomerUDF].getName}'")
      val df = sql("select l_partkey, testUDF(l_comment) from lineitem")
      df.show()
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("customer udtf") {
    withTempFunction("testUDTF") {
      sql(s"CREATE TEMPORARY FUNCTION testUDTF AS '${classOf[CustomerUDTF].getName}';")
      runQueryAndCompare(
        "select l_partkey, col0, col1 from lineitem lateral view" +
          " testUDTF(l_partkey, l_comment) as col0, col1") {
        checkOperatorMatch[ColumnarPartialGenerateExec]
      }
    }
  }

  test("simple udtf") {
    withTempFunction("simpleUDTF") {
      sql(s"CREATE TEMPORARY FUNCTION simpleUDTF AS '${classOf[SimpleUDTF].getName}'")
      runQueryAndCompare(
        "select l_partkey, col0 from lineitem lateral view" +
          " simpleUDTF(l_orderkey) as col0") {
        checkOperatorMatch[ColumnarPartialGenerateExec]
      }
    }
  }

  test("no argument udtf") {
    withTempFunction("noInputUDTF") {
      sql(s"CREATE TEMPORARY FUNCTION noInputUDTF AS '${classOf[NoInputUDTF].getName}'")
      runQueryAndCompare(
        "select l_partkey, col0 from lineitem lateral view" +
          " noInputUDTF() as col0") {
        checkOperatorMatch[ColumnarPartialGenerateExec]
      }
    }
  }

  test("nested partial fallback") {
    withTempFunction("noInputUDTF") {
      val plusOne = udf((x: Long) => x + 1)
      spark.udf.register("plus_one", plusOne)
      sql(s"CREATE TEMPORARY FUNCTION noInputUDTF AS '${classOf[NoInputUDTF].getName}'")
      runQueryAndCompare("""
                           |select plus_one(col1) as col2, l_partkey from (
                           | select col1, l_partkey from lineitem lateral view noInputUDTF() as col1
                           |)""".stripMargin) {
        df =>
          {
            checkOperatorMatch[ColumnarPartialProjectExec](df)
            checkOperatorMatch[ColumnarPartialGenerateExec](df)
          }
      }
    }
  }

  test("lateral view outer udtf") {
    withTempFunction("conditionalOutputUDTF") {
      sql(
        s"CREATE TEMPORARY FUNCTION conditionalOutputUDTF" +
          s" AS '${classOf[ConditionalOutputUDTF].getName}'")
      runQueryAndCompare(
        "select l_partkey, col0 from lineitem lateral view outer" +
          " conditionalOutputUDTF(l_orderkey) as col0") {
        checkOperatorMatch[ColumnarPartialGenerateExec]
      }
    }
  }

  test("child of GenerateExec is not offloadable") {
    withTempFunction("testUDTF") {
      val plusOne = udf((x: Long) => x + 1)
      spark.udf.register("plus_one", plusOne)
      withSQLConf(
        GlutenConfig.ENABLE_COLUMNAR_PARTIAL_PROJECT.key -> "false"
      ) {
        sql(s"CREATE TEMPORARY FUNCTION testUDTF AS '${classOf[CustomerUDTF].getName}'")
        runQueryAndCompare(
          "select col0, col1 from (select plus_one(l_partkey) as " +
            "l_partkey, l_comment from lineitem) lateral view" +
            " testUDTF(l_partkey, l_comment) as col0, col1",
          noFallBack = false
        ) {
          df =>
            assert(
              df.queryExecution.executedPlan
                .find(_.isInstanceOf[ColumnarPartialGenerateExec])
                .isEmpty)
        }
      }
    }
  }

  test("customer udf wrapped in function") {
    withTempFunction("testUDF") {
      sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[CustomerUDF].getName}'")
      val df = sql("select l_partkey, hash(testUDF(l_comment)) from lineitem")
      df.show()
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("example") {
    withTempFunction("testUDF") {
      sql("CREATE TEMPORARY FUNCTION testUDF AS 'org.apache.hadoop.hive.ql.udf.UDFSubstr';")
      val df = sql("select testUDF('l_commen', 1, 5)")
      df.show()
      // It should not be converted to ColumnarPartialProjectExec, since
      // the UDF need all the columns in child output.
      assert(!getExecutedPlan(df).exists {
        case _: ColumnarPartialProjectExec => true
        case _ => false
      })
    }
  }

  test("udf with array") {
    withTempFunction("udf_sort_array") {
      sql("""
            |CREATE TEMPORARY FUNCTION udf_sort_array AS
            |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArray';
            |""".stripMargin)

      val df = sql("""
                     |SELECT
                     |  l_orderkey,
                     |  l_partkey,
                     |  udf_sort_array(array(10, l_orderkey, 1)) as udf_result
                     |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
                     |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(35, 5, mutable.WrappedArray.make(Array(1, 10, 35))),
          Row(321, 4, mutable.WrappedArray.make(Array(1, 10, 321))),
          Row(548, 2, mutable.WrappedArray.make(Array(1, 10, 548))),
          Row(640, 5, mutable.WrappedArray.make(Array(1, 10, 640))),
          Row(807, 2, mutable.WrappedArray.make(Array(1, 10, 807)))
        )
      )
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("udf with map") {
    withTempFunction("udf_str_to_map") {
      sql("""
            |CREATE TEMPORARY FUNCTION udf_str_to_map AS
            |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap';
            |""".stripMargin)

      val df = sql(
        """
          |SELECT
          |  l_orderkey,
          |  l_partkey,
          |  udf_str_to_map(
          |    concat_ws(',', array(concat('hello', l_partkey), 'world')), ',', 'l') as udf_result
          |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
          |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(321, 4, Map("he" -> "lo4", "wor" -> "d")),
          Row(35, 5, Map("he" -> "lo5", "wor" -> "d")),
          Row(548, 2, Map("he" -> "lo2", "wor" -> "d")),
          Row(640, 5, Map("he" -> "lo5", "wor" -> "d")),
          Row(807, 2, Map("he" -> "lo2", "wor" -> "d"))
        )
      )
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("prioritize offloading supported hive udf in ColumnarPartialProject") {
    withTempFunction("udf_substr") {
      withTempFunction("udf_substr2") {
        withTempFunction("udf_sort_array") {
          spark.sql(s"""
                       |CREATE TEMPORARY FUNCTION udf_sort_array AS
                       |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArray';
                       |""".stripMargin)
          // Mapping hive udf "udf_substr" to velox function "substring"
          UDFMappings.hiveUDFMap.put("udf_substr", "substring")
          Seq("udf_substr", "udf_substr2").foreach {
            testudf =>
              spark.sql(s"""CREATE TEMPORARY FUNCTION $testudf AS
                           |'org.apache.hadoop.hive.ql.udf.UDFSubstr';
                           |""".stripMargin)

              val df = spark.sql(s"""
                                    |select
                                    |  l_partkey,
                                    |  udf_sort_array(array(10, l_orderkey, 1)),
                                    |  $testudf(l_comment, 1, 5)
                                    |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
                                    |""".stripMargin)
              val executedPlan = getExecutedPlan(df)
              checkGlutenPlan[ColumnarPartialProjectExec](df)
              val partialProject = executedPlan
                .filter {
                  _ match {
                    case _: ColumnarPartialProjectExec => true
                    case _ => false
                  }
                }
                .head
                .asInstanceOf[ColumnarPartialProjectExec]

              if (testudf == "udf_substr") {
                // Since udf_substr is supported to transform,
                // then we should only partial project udf_sort_array.
                assert(partialProject.output.count(_.name.startsWith("_SparkPartialProject")) == 1)
              } else {
                // Since both udf_sort_array and udf_substr2 is not supported to transform,
                // then we should partial project udf_sort_array and udf_substr2.
                assert(partialProject.output.count(_.name.startsWith("_SparkPartialProject")) == 2)
              }
          }
        }
      }
    }
  }

  test("udf with map with null values") {
    withTempFunction("udf_map_values") {
      sql("""
            |CREATE TEMPORARY FUNCTION udf_map_values AS
            |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapValues';
            |""".stripMargin)

      runQueryAndCompare("""
                           |SELECT
                           |  l_partkey,
                           |  udf_map_values(map_data)
                           |FROM (
                           | SELECT l_partkey,
                           | map(
                           |   concat('hello', l_orderkey % 2),
                           |   CASE WHEN l_orderkey % 2 == 0 THEN l_orderkey ELSE null END,
                           |   concat('world', l_orderkey % 2),
                           |   CASE WHEN l_orderkey % 2 == 0 THEN l_orderkey ELSE null END
                           | ) as map_data
                           | FROM lineitem
                           |)
                           |""".stripMargin) {
        checkOperatorMatch[ColumnarPartialProjectExec]
      }
    }
  }

  test("udf with array with null values") {
    withTempFunction("udf_array_distinct") {
      sql(s"""
             |CREATE TEMPORARY FUNCTION udf_array_distinct AS '${classOf[DuplicateArray].getName}'
             |""".stripMargin)

      runQueryAndCompare("""
                           |SELECT
                           |  l_partkey,
                           |  udf_array_distinct(map_data)
                           |FROM (
                           | SELECT l_partkey,
                           | array(
                           |   l_orderkey % 2,
                           |   CASE WHEN l_orderkey % 2 == 0 THEN l_orderkey ELSE null END,
                           |   l_orderkey % 2,
                           |   CASE WHEN l_orderkey % 2 == 0 THEN l_orderkey ELSE null END
                           | ) as map_data
                           | FROM lineitem
                           |)
                           |""".stripMargin) {
        checkOperatorMatch[ColumnarPartialProjectExec]
      }
    }
  }
}
