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

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{FlattenedAnd, FlattenedOr}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenTestUtils, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.nio.file.Files
import java.sql.Date

import scala.reflect.ClassTag

class GlutenFunctionValidateSuite extends GlutenClickHouseWholeStageTransformerSuite {
  private var parquetPath: String = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set(GlutenConfig.GLUTEN_SUPPORTED_SCALA_UDFS.key, "compare_substrings:compare_substrings")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(
      Array(
        StructField("double_field1", DoubleType, nullable = true),
        StructField("int_field1", IntegerType, nullable = true),
        StructField("string_field1", StringType, nullable = true)
      ))
    val data = sparkContext.parallelize(
      Seq(
        Row(1.025, 1, "{\"a\":\"b\"}"),
        Row(1.035, 2, null),
        Row(1.045, 3, "{\"1a\":\"b\"}"),
        Row(1.011, 4, "{\"a 2\":\"b\"}"),
        Row(1.011, 5, "{\"a_2\":\"b\"}"),
        Row(1.011, 5, "{\"a\":\"b\", \"x\":{\"i\":1}}"),
        Row(1.011, 5, "{\"a\":\"b\", \"x\":{\"i\":2}}"),
        Row(1.011, 5, "{\"a\":1, \"x\":{\"i\":2}}"),
        Row(1.0, 5, "{\"a\":\"{\\\"x\\\":5}\"}"),
        Row(1.0, 6, "{\"a\":{\"y\": 5, \"z\": {\"m\":1, \"n\": {\"p\": \"k\"}}}"),
        Row(1.0, 7, "{\"a\":[{\"y\": 5}, {\"z\":[{\"m\":1, \"n\":{\"p\":\"k\"}}]}]}")
      ))
    val dfParquet = spark.createDataFrame(data, schema)
    dfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("json_test", parquetPath, fileFormat)

    val dateSchema = StructType(
      Array(
        StructField("ts", IntegerType, nullable = true),
        StructField("day", DateType, nullable = true),
        StructField("weekday_abbr", StringType, nullable = true)
      )
    )
    val dateRows = sparkContext.parallelize(
      Seq(
        Row(1546309380, Date.valueOf("2019-01-01"), "MO"),
        Row(1546273380, Date.valueOf("2019-01-01"), "TU"),
        Row(1546358340, Date.valueOf("2019-01-01"), "TH"),
        Row(1546311540, Date.valueOf("2019-01-01"), "WE"),
        Row(1546308540, Date.valueOf("2019-01-01"), "FR"),
        Row(1546319340, Date.valueOf("2019-01-01"), "SA"),
        Row(1546319940, Date.valueOf("2019-01-01"), "SU"),
        Row(1546323545, Date.valueOf("2019-01-01"), "MO"),
        Row(1546409940, Date.valueOf("2019-01-02"), "MM"),
        Row(1546496340, Date.valueOf("2019-01-03"), "TH"),
        Row(1546586340, Date.valueOf("2019-01-04"), "WE"),
        Row(1546676341, Date.valueOf("2019-01-05"), "FR"),
        Row(null, null, "SA"),
        Row(1546849141, Date.valueOf("2019-01-07"), null)
      )
    )
    val dateTableFile = Files.createTempFile("", ".parquet").toFile
    dateTableFile.deleteOnExit()
    val dateTableFilePath = dateTableFile.getAbsolutePath
    val dateTablePQ = spark.createDataFrame(dateRows, dateSchema)
    dateTablePQ
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(dateTableFilePath)
    spark.catalog.createTable("date_table", dateTableFilePath, fileFormat)
    val str2Mapfile = Files.createTempFile("", ".parquet").toFile
    str2Mapfile.deleteOnExit()
    val str2MapFilePath = str2Mapfile.getAbsolutePath
    val str2MapSchema = StructType(
      Array(
        StructField("str", StringType, nullable = true)
      ))
    val str2MapData = sparkContext.parallelize(
      Seq(
        Row("a:1,b:2,c:3"),
        Row("a:1,b:2"),
        Row("a:1;b:2"),
        Row("a:1,d:4"),
        Row("a:"),
        Row(null),
        Row(":,a:1"),
        Row(":"),
        Row("")
      ))
    val str2MapDfParquet = spark.createDataFrame(str2MapData, str2MapSchema)
    str2MapDfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(str2MapFilePath)
    spark.catalog.createTable("str2map_table", str2MapFilePath, fileFormat)

    val urlFile = Files.createTempFile("", ".parquet").toFile
    urlFile.deleteOnExit()
    val urlFilePath = urlFile.getAbsolutePath
    val urlTalbeSchema = StructType(
      Array(
        StructField("url", StringType, nullable = true)
      )
    )
    val urlTableData = sparkContext.parallelize(
      Seq(
        Row("http://www.gluten.com"),
        Row("www.gluten.com"),
        Row("http://www.gluten.com?x=1"),
        Row("http://www.gluten?x=1"),
        Row("http://www.gluten.com?x=1#Ref"),
        Row("http://www.gluten.com#Ref?x=1"),
        Row("http://www.gluten.com?x=1&y=2"),
        Row("https://www.gluten.com?x=1&y=2"),
        Row("file://www.gluten.com?x=1&y=2"),
        Row("hdfs://www.gluten.com?x=1&y=2"),
        Row("hdfs://www.gluten.com?x=1&y=2/a/b"),
        Row("hdfs://www.gluten.com/x/y"),
        Row("hdfs://xy:12@www.gluten.com/x/y"),
        Row("xy:12@www.gluten.com/x/y"),
        Row("www.gluten.com/x/y"),
        Row("www.gluten.com?x=1"),
        Row("www.gluten.com:999?x=1"),
        Row("www.gluten.com?x=1&y=2"),
        Row("heel?x=1&y=2"),
        Row("x=1&y=2"),
        Row("/a/b/cx=1&y=2"),
        Row("gluten?x=1&y=2"),
        Row("xxhhh"),
        Row(null)
      )
    )
    val urlPQFile = spark.createDataFrame(urlTableData, urlTalbeSchema)
    urlPQFile.coalesce(1).write.format("parquet").mode("overwrite").parquet(urlFilePath)
    spark.catalog.createTable("url_table", urlFilePath, fileFormat)
  }

  test("Test get_json_object 1") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.a') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 2") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.1a') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 3") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.a_2') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  ignore("Test get_json_object 4") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[a]') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 5") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[\\\'a\\\']') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 6") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[\\\'a 2\\\']') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 7") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$..') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test get_json_object 8") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$..') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test get_json_object 9") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.x[?(@.i == 1)]') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test nested get_json_object") {
    runQueryAndCompare(
      "SELECT get_json_object(get_json_object(string_field1, '$.a'), '$.x') from json_test") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("GLUTEN-8304: Optimize nested get_json_object") {
    def checkExpression(expr: Expression, path: String): Boolean = {
      expr match {
        case g: GetJsonObject
            if g.path.isInstanceOf[Literal] && g.path.dataType.isInstanceOf[StringType] =>
          g.path.asInstanceOf[Literal].value.toString.equals(path) || g.children.exists(
            c => checkExpression(c, path))
        case _ =>
          if (expr.children.isEmpty) {
            false
          } else {
            expr.children.exists(c => checkExpression(c, path))
          }
      }
    }

    def checkPlan(plan: LogicalPlan, path: String): Boolean = plan match {
      case p: Project =>
        p.projectList.exists(x => checkExpression(x, path)) || checkPlan(p.child, path)
      case f: Filter =>
        checkExpression(f.condition, path) || checkPlan(f.child, path)
      case _ =>
        if (plan.children.isEmpty) {
          false
        } else {
          plan.children.exists(c => checkPlan(c, path))
        }
    }

    def checkGetJsonObjectPath(df: DataFrame, path: String): Boolean = {
      checkPlan(df.queryExecution.analyzed, path)
    }

    withSQLConf((GlutenConfig.ENABLE_COLLAPSE_GET_JSON_OBJECT.key, "true")) {
      runQueryAndCompare(
        "select get_json_object(get_json_object(string_field1, '$.a'), '$.y') " +
          " from json_test where int_field1 = 6") {
        x => assert(checkGetJsonObjectPath(x, "$.a.y"))
      }
      runQueryAndCompare(
        "select get_json_object(get_json_object(string_field1, '$[a]'), '$[y]') " +
          " from json_test where int_field1 = 6") {
        x => assert(checkGetJsonObjectPath(x, "$[a][y]"))
      }
      runQueryAndCompare(
        "select get_json_object(get_json_object(get_json_object(string_field1, " +
          "'$.a'), '$.y'), '$.z') from json_test where int_field1 = 6") {
        x => assert(checkGetJsonObjectPath(x, "$.a.y.z"))
      }
      runQueryAndCompare(
        "select get_json_object(get_json_object(get_json_object(string_field1, '$.a')," +
          " string_field1), '$.z') from json_test where int_field1 = 6",
        noFallBack = false
      )(x => assert(checkGetJsonObjectPath(x, "$.a") && checkGetJsonObjectPath(x, "$.z")))
      runQueryAndCompare(
        "select get_json_object(get_json_object(get_json_object(string_field1, " +
          " string_field1), '$.a'), '$.z') from json_test where int_field1 = 6",
        noFallBack = false
      )(x => assert(checkGetJsonObjectPath(x, "$.a.z")))
      runQueryAndCompare(
        "select get_json_object(get_json_object(get_json_object(" +
          " substring(string_field1, 10), '$.a'), '$.z'), string_field1) " +
          " from json_test where int_field1 = 6",
        noFallBack = false
      )(x => assert(checkGetJsonObjectPath(x, "$.a.z")))
      runQueryAndCompare(
        "select get_json_object(get_json_object(string_field1, '$.a[0]'), '$.y') " +
          " from json_test where int_field1 = 7") {
        x => assert(checkGetJsonObjectPath(x, "$.a[0].y"))
      }
      runQueryAndCompare(
        "select get_json_object(get_json_object(get_json_object(string_field1, " +
          " '$.a[1]'), '$.z[1]'), '$.n') from json_test where int_field1 = 7") {
        x => assert(checkGetJsonObjectPath(x, "$.a[1].z[1].n"))
      }
      runQueryAndCompare(
        "select * from json_test where " +
          " get_json_object(get_json_object(get_json_object(string_field1, '$.a'), " +
          "'$.y'), '$.z') != null")(x => assert(checkGetJsonObjectPath(x, "$.a.y.z")))
    }
  }

  test("Test get_json_object 10") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.12345') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123.abc') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123.123') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123abc.123') from json_test") {
      _ =>
    }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.abc.123') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123[0]') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123[0].123') from json_test") {
      _ =>
    }
  }

  test("Test get_json_object 11") {
    runQueryAndCompare(
      "SELECT string_field1 from json_test where" +
        " get_json_object(string_field1, '$.a') is not null") { _ => }
  }

  test("Test get_json_object 12") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.a[*].y') from json_test where int_field1 = 7") {
      _ =>
    }
    runQueryAndCompare(
      "select get_json_object(string_field1, '$.a[*].z.n.p') from json_test where int_field1 = 7") {
      _ =>
    }
  }

  test("Test get_json_object 13") {
    val sql =
      """
        |SELECT
        | explode(array(get_json_object(get_json_object('{"a": "{\\\"b\\\":1}"}', '$.a'), '$.b')))
        | from range(1)
        |""".stripMargin
    runQueryAndCompare(sql) { df => }
  }

  test("GLUTEN-8557: Optimize nested and/or") {
    def checkFlattenedFunctions(plan: SparkPlan, functionName: String, argNum: Int): Boolean = {

      def checkExpression(expr: Expression, functionName: String, argNum: Int): Boolean =
        expr match {
          case s: FlattenedAnd if s.name.equals(functionName) && s.children.size == argNum =>
            true
          case o: FlattenedOr if o.name.equals(functionName) && o.children.size == argNum =>
            true
          case _ => expr.children.exists(c => checkExpression(c, functionName, argNum))
        }
      plan match {
        case f: FilterExecTransformer => return checkExpression(f.condition, functionName, argNum)
        case _ => return plan.children.exists(c => checkFlattenedFunctions(c, functionName, argNum))
      }
      false
    }
    runQueryAndCompare(
      "SELECT count(1) from json_test where int_field1 = 5 and double_field1 > 1.0" +
        " and string_field1 is not null") {
      x => assert(checkFlattenedFunctions(x.queryExecution.executedPlan, "and", 5))
    }
    runQueryAndCompare(
      "SELECT count(1) from json_test where int_field1 = 5 or double_field1 > 1.0" +
        " or string_field1 is not null") {
      x => assert(checkFlattenedFunctions(x.queryExecution.executedPlan, "or", 3))
    }
    runQueryAndCompare(
      "SELECT count(1) from json_test where int_field1 = 5 and double_field1 > 1.0" +
        " and double_field1 < 10 or int_field1 = 12 or string_field1 is not null") {
      x =>
        assert(
          checkFlattenedFunctions(
            x.queryExecution.executedPlan,
            "and",
            3) && checkFlattenedFunctions(x.queryExecution.executedPlan, "or", 3))
    }
  }

  test("Test covar_samp") {
    runQueryAndCompare("SELECT covar_samp(double_field1, int_field1) from json_test") { _ => }
  }

  test("Test covar_pop") {
    runQueryAndCompare("SELECT covar_pop(double_field1, int_field1) from json_test") { _ => }
  }

  test("test 'function xxhash64'") {
    val df1 = runQueryAndCompare(
      "select xxhash64(cast(id as int)), xxhash64(cast(id as byte)), " +
        "xxhash64(cast(id as short)), " +
        "xxhash64(cast(id as long)), xxhash64(cast(id as float)), xxhash64(cast(id as double)), " +
        "xxhash64(cast(id as string)), xxhash64(cast(id as binary)), " +
        "xxhash64(cast(from_unixtime(id) as date)), " +
        "xxhash64(cast(from_unixtime(id) as timestamp)), xxhash64(cast(id as decimal(5, 2))), " +
        "xxhash64(cast(id as decimal(10, 2))), xxhash64(cast(id as decimal(30, 2))) " +
        "from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df1, 10)

    val df2 = runQueryAndCompare(
      "select xxhash64(cast(id as int), 'spark'), xxhash64(cast(id as byte), 'spark'), " +
        "xxhash64(cast(id as short), 'spark'), xxhash64(cast(id as long), 'spark'), " +
        "xxhash64(cast(id as float), 'spark'), xxhash64(cast(id as double), 'spark'), " +
        "xxhash64(cast(id as string), 'spark'), xxhash64(cast(id as binary), 'spark'), " +
        "xxhash64(cast(from_unixtime(id) as date), 'spark'), " +
        "xxhash64(cast(from_unixtime(id) as timestamp), 'spark'), " +
        "xxhash64(cast(id as decimal(5, 2)), 'spark'), " +
        "xxhash64(cast(id as decimal(10, 2)), 'spark'), " +
        "xxhash64(cast(id as decimal(30, 2)), 'spark') from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df2, 10)
  }

  test("test function xxhash64 with complex types") {
    val sql =
      """
        |select
        |  xxhash64(array(id, null, id+1, 100)),
        |  xxhash64(array(cast(id as string), null, 'spark')),
        |  xxhash64(array(null)),
        |  xxhash64(cast(null as array<int>)),
        |  xxhash64(array(array(id, null, id+1))),
        |  xxhash64(cast(null as struct<a:int, b:string>)),
        |  xxhash64(struct(id, cast(id as string), 100, 'spark', null))
        |from range(10);
      """.stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test 'function murmur3hash'") {
    val df1 = runQueryAndCompare(
      "select hash(cast(id as int)), hash(cast(id as byte)), hash(cast(id as short)), " +
        "hash(cast(id as long)), hash(cast(id as float)), hash(cast(id as double)), " +
        "hash(cast(id as string)), hash(cast(id as binary)), " +
        "hash(cast(from_unixtime(id) as date)), " +
        "hash(cast(from_unixtime(id) as timestamp)), hash(cast(id as decimal(5, 2))), " +
        "hash(cast(id as decimal(10, 2))), hash(cast(id as decimal(30, 2))) from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df1, 10)

    val df2 = runQueryAndCompare(
      "select hash(cast(id as int), 'spark'), hash(cast(id as byte), 'spark'), " +
        "hash(cast(id as short), 'spark'), hash(cast(id as long), 'spark'), " +
        "hash(cast(id as float), 'spark'), hash(cast(id as double), 'spark'), " +
        "hash(cast(id as string), 'spark'), hash(cast(id as binary), 'spark'), " +
        "hash(cast(from_unixtime(id) as date), 'spark'), " +
        "hash(cast(from_unixtime(id) as timestamp), 'spark'), " +
        "hash(cast(id as decimal(5, 2)), 'spark'), hash(cast(id as decimal(10, 2)), 'spark'), " +
        "hash(cast(id as decimal(30, 2)), 'spark') from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df2, 10)
  }

  test("test function murmur3hash with complex types") {
    val sql =
      """
        |select
        |  hash(array(id, null, id+1, 100)),
        |  hash(array(cast(id as string), null, 'spark')),
        |  hash(array(null)),
        |  hash(cast(null as array<int>)),
        |  hash(array(array(id, null, id+1))),
        |  hash(cast(null as struct<a:int, b:string>)),
        |  hash(struct(id, cast(id as string), 100, 'spark', null))
        |from range(10);
      """.stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test next_day const") {
    runQueryAndCompare("select next_day(day, 'MO') from date_table") { _ => }
  }
  test("test next_day const all null") {
    runQueryAndCompare("select next_day(day, 'MM') from date_table") { _ => }
  }
  test("test next_day dynamic") {
    runQueryAndCompare("select next_day(day, weekday_abbr) from date_table") { _ => }
  }
  test("test last_day") {
    runQueryAndCompare("select last_day(day) from date_table") { _ => }
  }

  test("test issue: https://github.com/oap-project/gluten/issues/2340") {
    val sql =
      """
        |select array(null, array(id,2)) from range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test issue: https://github.com/oap-project/gluten/issues/2947") {
    val sql =
      """
        |select if(id % 2 = 0, null, array(id, 2, null)) from range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test str2map") {
    val sql1 =
      """
        |select str, str_to_map(str, ',', ':') from str2map_table
        |""".stripMargin
    runQueryAndCompare(sql1)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test str2map, regular expression") {
    val sql1 =
      """
        |select str_to_map('ab', '', ''),
        | str_to_map('a:b,c:d'),
        | str_to_map('ab', '', ':'),
        | str_to_map('a:,c:d,e', ',', ''),
        | str_to_map('a,b', ',', ''),
        | str_to_map('a:c|b:c', '\\|', ':')
        |""".stripMargin
    runQueryAndCompare(sql1)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test parse_url") {
    val sql1 =
      """
        | select url, parse_url(url, "HOST") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql1)(checkGlutenPlan[ProjectExecTransformer])

    val sql2 =
      """
        | select url, parse_url(url, "QUERY") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql2)(checkGlutenPlan[ProjectExecTransformer])

    val sql3 =
      """
        | select url, parse_url(url, "QUERY", "x") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql3)(checkGlutenPlan[ProjectExecTransformer])

    val sql5 =
      """
        | select url, parse_url(url, "FILE") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql5)(checkGlutenPlan[ProjectExecTransformer])

    val sql6 =
      """
        | select url, parse_url(url, "REF") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql6)(checkGlutenPlan[ProjectExecTransformer])

    val sql7 =
      """
        | select url, parse_url(url, "USERINFO") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql7)(checkGlutenPlan[ProjectExecTransformer])

    val sql8 =
      """
        | select url, parse_url(url, "AUTHORITY") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql8)(checkGlutenPlan[ProjectExecTransformer])

    val sql9 =
      """
        | select url, parse_url(url, "PROTOCOL") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql9)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test decode and encode") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      // Test codec with 'US-ASCII'
      runQueryAndCompare(
        "SELECT decode(encode('Spark SQL', 'US-ASCII'), 'US-ASCII')"
      )(checkGlutenPlan[ProjectExecTransformer])

      // Test codec with 'UTF-16'
      runQueryAndCompare(
        "SELECT decode(encode('Spark SQL', 'UTF-16'), 'UTF-16')"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test cast float string to int") {
    runQueryAndCompare(
      "select cast(concat(cast(id as string), '.1') as int) from range(10)"
    )(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test cast string to float") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select cast('7.921901' as float), cast('7.921901' as double)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test round issue: https://github.com/oap-project/gluten/issues/3462") {
    def checkResult(df: DataFrame, exceptedResult: Seq[Row]): Unit = {
      // check the result
      val result = df.collect()
      assert(result.length === exceptedResult.size)
      GlutenTestUtils.compareAnswers(result, exceptedResult)
    }

    runSql("select round(0.41875d * id , 4) from range(10);")(
      df => {
        checkGlutenPlan[ProjectExecTransformer](df)

        checkResult(
          df,
          Seq(
            Row(0.0),
            Row(0.4188),
            Row(0.8375),
            Row(1.2563),
            Row(1.675),
            Row(2.0938),
            Row(2.5125),
            Row(2.9313),
            Row(3.35),
            Row(3.7688)
          )
        )
      })

    runSql("select round(0.41875f * id , 4) from range(10);")(
      df => {
        checkGlutenPlan[ProjectExecTransformer](df)

        checkResult(
          df,
          Seq(
            Row(0.0f),
            Row(0.4188f),
            Row(0.8375f),
            Row(1.2562f),
            Row(1.675f),
            Row(2.0938f),
            Row(2.5125f),
            Row(2.9312f),
            Row(3.35f),
            Row(3.7688f)
          )
        )
      })
  }

  test("test date comparision expression override") {
    runQueryAndCompare("select * from date_table where to_date(from_unixtime(ts)) < '2019-01-02'") {
      _ =>
    }
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) <= '2019-01-02'") { _ => }
    runQueryAndCompare("select * from date_table where to_date(from_unixtime(ts)) > '2019-01-02'") {
      _ =>
    }
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) >= '2019-01-02'") { _ => }
    runQueryAndCompare("select * from date_table where to_date(from_unixtime(ts)) = '2019-01-01'") {
      _ =>
    }
    runQueryAndCompare(
      "select * from date_table where from_unixtime(ts) between '2019-01-01' and '2019-01-02'") {
      _ =>
    }
    runQueryAndCompare(
      "select * from date_table where from_unixtime(ts, 'yyyy-MM-dd') between" +
        " '2019-01-01' and '2019-01-02'") { _ => }
  }

  test("test element_at function") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      // input type is array<array<int>>
      runQueryAndCompare(
        "SELECT array(array(1,2,3), array(4,5,6))[1], " +
          "array(array(id,id+1,id+2), array(id+3,id+4,id+5)) from range(100)")(
        checkGlutenPlan[ProjectExecTransformer])

      // input type is array<array<string>>
      runQueryAndCompare(
        "SELECT array(array('1','2','3'), array('4','5','6'))[1], " +
          "array(array('1','2',cast(id as string)), array('4','5',cast(id as string)))[1] " +
          "from range(100)")(checkGlutenPlan[ProjectExecTransformer])

      // input type is array<map<string, int>>
      runQueryAndCompare(
        "SELECT array(map(cast(id as string), id), map(cast(id+1 as string), id+1))[1] " +
          "from range(100)")(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test common subexpression eliminate") {
    def checkOperatorCount[T <: TransformSupport](count: Int)(df: DataFrame)(implicit
        tag: ClassTag[T]): Unit = {
      if (spark33) {
        assert(
          getExecutedPlan(df).count(
            plan => {
              plan.getClass == tag.runtimeClass
            }) == count,
          s"executed plan: ${getExecutedPlan(df)}")
      }
    }

    withSQLConf((GlutenConfig.ENABLE_COMMON_SUBEXPRESSION_ELIMINATE.key, "true")) {
      // CSE in project
      runQueryAndCompare("select hash(id), hash(id)+1, hash(id)-1 from range(10)") {
        df => checkOperatorCount[ProjectExecTransformer](2)(df)
      }

      // CSE in filter(not work yet)
      // runQueryAndCompare(
      //   "select id from range(10) " +
      //     "where hex(id) != '' and upper(hex(id)) != '' and lower(hex(id)) != ''") { _ => }

      // CSE in window
      runQueryAndCompare(
        "SELECT id, AVG(id) OVER (PARTITION BY id % 2 ORDER BY id) as avg_id, " +
          "SUM(id) OVER (PARTITION BY id % 2 ORDER BY id) as sum_id FROM range(10)") {
        df => checkOperatorCount[ProjectExecTransformer](4)(df)
      }

      // CSE in aggregate
      runQueryAndCompare(
        "select id % 2, max(hash(id)), min(hash(id)) " +
          "from range(10) group by id % 2") {
        df => checkOperatorCount[ProjectExecTransformer](1)(df)
      }
      runQueryAndCompare(
        "select id % 10, sum(id +100) + max(id+100) from range(100) group by id % 10") {
        df => checkOperatorCount[ProjectExecTransformer](2)(df)
      }
      // issue https://github.com/oap-project/gluten/issues/4642
      runQueryAndCompare(
        "select id, if(id % 2 = 0, sum(id), max(id)) as s1, " +
          "if(id %2 = 0, sum(id+1), sum(id+2)) as s2 from range(10) group by id") {
        df => checkOperatorCount[ProjectExecTransformer](2)(df)
      }

      // CSE in sort
      runQueryAndCompare(
        "select id from range(10) " +
          "order by hash(id%10), hash(hash(id%10))") {
        df => checkOperatorCount[ProjectExecTransformer](3)(df)
      }

      runQueryAndCompare(s"""
                            |SELECT 'test' AS test
                            |  , Sum(CASE
                            |    WHEN name = '2' THEN 0
                            |      ELSE id
                            |    END) AS c1
                            |  , Sum(CASE
                            |    WHEN name = '2' THEN id
                            |      ELSE 0
                            |    END) AS c2
                            | , CASE WHEN name = '2' THEN Sum(id) ELSE 0
                            |   END AS c3
                            |FROM (select id, cast(id as string) name from range(10))
                            |GROUP BY name
                            |""".stripMargin) {
        df => checkOperatorCount[ProjectExecTransformer](4)(df)
      }

      runQueryAndCompare(
        s"""
           |select id % 2, max(hash(id)), min(hash(id)) from range(10) group by id % 2
           |""".stripMargin)(
        df => {
          df.queryExecution.optimizedPlan.collect {
            case Aggregate(_, aggregateExpressions, _) =>
              val result =
                aggregateExpressions
                  .map(a => a.asInstanceOf[Alias].child)
                  .filter(_.isInstanceOf[AggregateExpression])
                  .map(expr => expr.asInstanceOf[AggregateExpression].aggregateFunction)
                  .filter(aggFunc => aggFunc.children.head.isInstanceOf[AttributeReference])
                  .map(aggFunc => aggFunc.children.head.asInstanceOf[AttributeReference].name)
                  .distinct
              assertResult(1)(result.size)
          }
          checkOperatorCount[ProjectExecTransformer](1)(df)
        })
    }
  }

  test("test function getarraystructfields") {
    val sql =
      """
        |SELECT id,
        |       struct_array[0].field1,
        |       struct_array[0].field2
        |FROM (
        |  SELECT id,
        |         array(struct(id as field1, (id+1) as field2)) as struct_array
        |  FROM range(10)
        |) t
      """.stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test parse string with blank to integer") {
    // issue https://github.com/apache/incubator-gluten/issues/4956
    val sql = "select  cast(concat(' ', cast(id as string)) as bigint) from range(10)"
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("avg(bigint) overflow") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withTable("myitem") {
        sql("create table big_int(id bigint) using parquet")
        sql("""
              |insert into big_int values (9223372036854775807),
              |(9223372036854775807),
              |(9223372036854775807),
              |(9223372036854775807)
              |""".stripMargin)
        val q = "select avg(id) from big_int"
        runQueryAndCompare(q)(checkGlutenPlan[CHHashAggregateExecTransformer])
        val disinctSQL = "select count(distinct id), avg(distinct id), avg(id) from big_int"
        runQueryAndCompare(disinctSQL)(checkGlutenPlan[CHHashAggregateExecTransformer])
      }
    }
  }

  test("equalTo rewrite to isNaN") {
    withTable("tb_scrt") {
      sql("create table tb_scrt(id int) using parquet")
      sql("""
            |insert into tb_scrt values (-2147483648),(-2147483648)
            |""".stripMargin)
      val q = "select sqrt(id),sqrt(id)='NaN' from tb_scrt"
      runQueryAndCompare(q)(checkGlutenPlan[ProjectExecTransformer])
    }

  }

  test("array functions with lambda") {
    withTable("tb_array") {
      sql("create table tb_array(ids array<int>) using parquet")
      sql("""
            |insert into tb_array values (array(1,5,2,null, 3)), (array(1,1,3,2)), (null), (array())
            |""".stripMargin)
      val transform_sql = "select transform(ids, x -> x + 1) from tb_array"
      runQueryAndCompare(transform_sql)(checkGlutenPlan[ProjectExecTransformer])

      val filter_sql = "select filter(ids, x -> x % 2 == 1) from tb_array"
      runQueryAndCompare(filter_sql)(checkGlutenPlan[ProjectExecTransformer])

      val aggregate_sql = "select ids, aggregate(ids, 3, (acc, x) -> acc + x) from tb_array"
      runQueryAndCompare(aggregate_sql)(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("test issue: https://github.com/apache/incubator-gluten/issues/6561") {
    val sql =
      """
        |select
        | map_from_arrays(
        |   transform(map_keys(map('t1',id,'t2',id+1)), v->v),
        |   array('a','b')) as b from range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test function format_string") {
    val sql =
      """
        | SELECT
        |  format_string(
        |    'hello world %d %d %s %f',
        |    id,
        |    id,
        |    CAST(id AS STRING),
        |    CAST(id AS float)
        |  )
        |FROM range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test function array_except") {
    val sql =
      """
        |SELECT array_except(array(id, id+1, id+2), array(id+2, id+3))
        |FROM RANGE(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test functions unix_seconds/unix_date/unix_millis/unix_micros") {
    val sql =
      """
        |SELECT
        |  id,
        |  unix_seconds(cast(concat('2024-09-03 17:23:1',
        |     cast(id as string)) as timestamp)),
        |  unix_date(cast(concat('2024-09-1', cast(id as string)) as date)),
        |  unix_millis(cast(concat('2024-09-03 17:23:10.11',
        |     cast(id as string)) as timestamp)),
        |  unix_micros(cast(concat('2024-09-03 17:23:10.12345',
        |     cast(id as string)) as timestamp))
        |FROM range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test function arrays_zip") {
    val sql =
      """
        |SELECT arrays_zip(array(id, id+1, id+2), array(id, id-1, id-2))
        |FROM range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("test function timestamp_seconds/timestamp_millis/timestamp_micros") {
    val sql =
      """
        |SELECT
        |  id,
        |  timestamp_seconds(1725453790 + id) as ts_seconds,
        |  timestamp_millis(1725453790123 + id) as ts_millis,
        |  timestamp_micros(1725453790123456 + id) as ts_micros
        |from range(10);
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-7426 get_json_object") {
    val sql =
      """
        |select
        |get_json_object(a, '$.a b'),
        |get_json_object(a, '$.a b '),
        |get_json_object(a, '$.a b c'),
        |get_json_object(a, '$.a 1 c'),
        |get_json_object(a, '$.1 '),
        |get_json_object(a, '$.1 2'),
        |get_json_object(a, '$.1 2 c')
        |from values('{"a b":1}'), ('{"a b ":1}'), ('{"a b c":1}')
        |, ('{"a 1 c":1}'), ('{"1 ":1}'), ('{"1 2":1}'), ('{"1 2 c":1}')
        |as data(a)
    """.stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-7432 get_json_object returns array") {
    val sql =
      """
        |select
        |get_json_object(a, '$.a[*].x')
        |from values('{"a":[{"x":1}, {"x":5}]}'), ('{"a":[{"x":1}]}')
        |as data(a)
    """.stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  test("GLUTEN-7455 negative modulo") {
    withTable("test_7455") {
      spark.sql("create table test_7455(x bigint) using parquet")
      val insert_sql =
        """
          |insert into test_7455 values
          |(327696126396414574)
          |,(618162455457852376)
          |,(-1)
          |,(-2)
          |""".stripMargin
      spark.sql(insert_sql)
      val sql =
        """
          |select x,
          |x % 4294967296,
          |x % -4294967296,
          |x % 4294967295,
          |x % -4294967295,
          |x % 100,
          |x % -100
          |from test_7455
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("GLUTEN-7796 cast bool to string") {
    val sql = "select cast(id % 2 = 1 as string) from range(10)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-8598 Fix diff for cast string to long") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select cast(' \t2570852431\n' as long), cast('25708\t52431\n' as long)"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("Test map_concat") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select map_concat(map(1, 'a', 2, 'b'), map(3, null)), map_concat()"
      )(checkGlutenPlan[ProjectExecTransformer])
    }
  }

  test("Test transform_keys/transform_values") {
    val sql =
      """
        |select id, sort_array(map_entries(m1)), sort_array(map_entries(m2)) from(
        |select id, first(m1) as m1, first(m2) as m2 from(
        |select
        |  id,
        |  transform_keys(map_from_arrays(array(id+1, id+2, id+3),
        |    array(1, id+2, 3)), (k, v) -> k + 1) as m1,
        |  transform_values(map_from_arrays(array(id+1, id+2, id+3),
        |    array(1, id+2, 3)), (k, v) -> v + 1) as m2
        |from range(10)
        |) group by id
        |) order by id
        |""".stripMargin

    def checkProjects(df: DataFrame): Unit = {
      val projects = collectWithSubqueries(df.queryExecution.executedPlan) {
        case e: ProjectExecTransformer => e
      }
      assert(projects.nonEmpty)
    }

    compareResultsAgainstVanillaSpark(sql, compareResult = true, checkProjects, noFallBack = false)
  }

  test("GLUTEN-8406 replace from_json with get_json_object") {
    withTable("test_8406") {
      spark.sql("create table test_8406(x string) using parquet")
      val insert_sql =
        """
          |insert into test_8406 values
          |('{"a":1}'),
          |('{"a":2'),
          |('{"b":3}'),
          |('{"a":"5"}'),
          |('{"a":{"x":1}}')
          |""".stripMargin
      spark.sql(insert_sql)
      val sql =
        """
          |select from_json(x, 'Map<String, String>')['a'] from test_8406
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }
  }

  test("Test approx_count_distinct") {
    val sql = "select approx_count_distinct(id, 0.001), approx_count_distinct(id, 0.01), " +
      "approx_count_distinct(id, 0.1) from range(1000)"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("GLUTEN-8723 fix slice unexpected exception") {
    val create_sql = "create table t_8723 (full_user_agent string) using orc"
    val insert_sql = "insert into t_8723 values(NULL)"
    val select1_sql = "select " +
      "slice(split(full_user_agent, ';'), 2, size(split(full_user_agent, ';'))) from t_8723"
    val select2_sql = "select slice(split(full_user_agent, ';'), 0, 2) from t_8723"
    val drop_sql = "drop table t_8723"

    spark.sql(create_sql)
    spark.sql(insert_sql)
    compareResultsAgainstVanillaSpark(select1_sql, true, { _ => })
    compareResultsAgainstVanillaSpark(select2_sql, true, { _ => })
    spark.sql(drop_sql)
  }

  test("GLUTEN-8715 nan semantics") {
    withTable("test_8715") {
      spark.sql("create table test_8715(c1 int, c2 double) using parquet")
      val insert_sql =
        """
          |insert into test_8715 values
          |(1, double('infinity'))
          |,(2, double('infinity'))
          |,(3, double('inf'))
          |,(4, double('-inf'))
          |,(5, double('NaN'))
          |,(6, double('NaN'))
          |,(7, double('-infinity'))
          |""".stripMargin
      spark.sql(insert_sql)
      val sql =
        """
          |select c2 = cast('nan' as double) from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })
      val sql5 =
        """
          |select c2 <= cast('nan' as double) from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql5, true, { _ => })
      val sql6 =
        """
          |select c2 >= cast('nan' as double) from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql6, true, { _ => })
      val sql7 =
        """
          |select c2 > cast('1.1' as double) from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql7, true, { _ => })
      val sql9 =
        """
          |select c2 >= cast('1.1' as double) from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql9, true, { _ => })
      val sql8 =
        """
          |select cast('1.1' as double) < c2 from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql8, true, { _ => })
      val sql10 =
        """
          |select cast('1.1' as double) <= c2 from test_8715 where c1=5
          |order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql10, true, { _ => })
      val sql1 =
        """
          |select sum(c1) from test_8715
          |group by c2 order by c2 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql1, true, { _ => })
      val sql2 =
        """
          |select * from test_8715
          |order by c2 asc, c1 asc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql2, true, { _ => })
      val sql3 =
        """
          |select * from test_8715
          |order by c2 desc, c1 desc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql3, true, { _ => })
      val sql4 =
        """
          |select a.c1 as a_c1, a.c2 as a_c2,
          |b.c1 as b_c1, b.c2 as b_c2
          |from test_8715 a
          |join test_8715 b on a.c2 = b.c2
          |order by a.c1, b.c1 desc
          |""".stripMargin
      compareResultsAgainstVanillaSpark(sql4, true, { _ => })
    }
  }

  test("GLUTEN-8859 replace substrings comparison") {
    withTable("test_8859") {
      def isRewriteSubstringCompareProject(plan: SparkPlan): Boolean = {

        def hasSubstringComparison(e: Expression): Boolean = e match {
          case udf: ScalaUDF if udf.udfName.isDefined =>
            udf.udfName.get.equals("compare_substrings")
          case _ => e.children.exists(hasSubstringComparison)
        }

        plan match {
          case project: ProjectExecTransformer =>
            project.projectList.exists(e => hasSubstringComparison(e))
          case _ => false
        }
      }

      spark.sql("create table test_8859(c1 string, c2 string) using parquet")
      val insert_sql =
        """
          |insert into test_8859 values
          |('abcd', '1234'),
          |('bcde', '2345'),
          |('abcd', 'abcd')
          |""".stripMargin
      spark.sql(insert_sql)

      val sql1 =
        """
          |select substr(c1, 1, 2) = 'ab', substr(c1, 1, 3) < 'abc', substr(c1, 1, 4) > 'abcd'
          |from test_8859
          |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql1,
        true,
        {
          df =>
            val projects = df.queryExecution.executedPlan.collect {
              case project: ProjectExecTransformer if isRewriteSubstringCompareProject(project) =>
                project
            }
            assert(projects.length == 1)
        }
      )

      val sql2 =
        """
          |select substr(c1, 1, 2) = substr(c2, 1, 2), substr(c1, 1, 3) < substr(c2, 1, 3),
          |substr(c1, 1, 4) > substr(c2, 1, 4)
          |from test_8859
          |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql2,
        true,
        {
          df =>
            val projects = df.queryExecution.executedPlan.collect {
              case project: ProjectExecTransformer if isRewriteSubstringCompareProject(project) =>
                project
            }
            assert(projects.length == 1)
        }
      )

      val sql3 =
        """
          |select substr(c1, 1, 2) < 'abc'
          |from test_8859
          |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql3,
        true,
        {
          df =>
            val projects = df.queryExecution.executedPlan.collect {
              case project: ProjectExecTransformer if isRewriteSubstringCompareProject(project) =>
                project
            }
            assert(projects.isEmpty)
        }
      )
    }
  }

  test("Test partition values with special characters") {
    spark.sql("""
      CREATE TABLE tbl_9050 (
        product_id STRING,
        quantity INT
      ) using parquet
      PARTITIONED BY (year STRING)
    """)

    sql("INSERT INTO tbl_9050 PARTITION(year='%s') SELECT 'prod1', 1")
    sql("INSERT INTO tbl_9050 PARTITION(year='%%s') SELECT 'prod2', 2")
    sql("INSERT INTO tbl_9050 PARTITION(year='%25s') SELECT 'prod3', 3")
    sql("INSERT INTO tbl_9050 PARTITION(year=' s') SELECT 'prod3', 4")

    compareResultsAgainstVanillaSpark("select *, input_file_name() from tbl_9050", true, { _ => })

    sql("DROP TABLE tbl_9050")
  }

  test("Test array_sort without comparator") {
    // default comparator with array elements not nullable guaranteed
    val sql1 = "select array_sort(split(cast(id * 10 as string), '0')) from range(10)"
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })

    // default comparator without array elements not nullable guaranteed
    val sql2 = "select array_sort(array(id+1, null, id+2)) from range(10)"
    compareResultsAgainstVanillaSpark(sql2, true, { _ => })
  }

  test("Test SimplifySumRule for sum simplification") {
    val sql = "select sum(id / 3), sum(id * 7), sum(7 * id) from range(10)"

    def checkSimplifiedSum(df: DataFrame): Unit = {
      val projects = collectWithSubqueries(df.queryExecution.executedPlan) {
        case project: ProjectExecTransformer
            if project.child.isInstanceOf[CHHashAggregateExecTransformer] =>
          project
      }

      assert(projects.size == 1)
      assert(projects.head.projectList.size == 3)
      assert(projects.head.projectList.head.asInstanceOf[Alias].child.isInstanceOf[Divide])
      assert(projects.head.projectList(1).asInstanceOf[Alias].child.isInstanceOf[Multiply])
      assert(projects.head.projectList(2).asInstanceOf[Alias].child.isInstanceOf[Multiply])
    }

    withSQLConf((CHConfig.runtimeConfig("enable_simplify_sum"), "true")) {
      compareResultsAgainstVanillaSpark(sql, compareResult = true, checkSimplifiedSum)
    }
  }

  ignore("Test rewrite aggregate if to aggregate with filter") {
    val sql = "select sum(if(id % 2=0, id, null)), count(if(id % 2 = 0, 1, null)), " +
      "avg(if(id % 4 = 0, id, null)), sum(if(id % 3 = 0, id, 0)) from range(10)"

    def checkAggregateWithFilter(df: DataFrame): Unit = {
      val aggregates = collectWithSubqueries(df.queryExecution.executedPlan) {
        case agg: CHHashAggregateExecTransformer if agg.modes.contains(Partial) => agg
      }

      assert(aggregates.nonEmpty, "No aggregate operations found in the execution plan")
      aggregates.foreach {
        agg =>
          agg.aggregateExpressions.foreach {
            expr =>
              assert(expr.isInstanceOf[AggregateExpression], "AggregateExpression should be used")
              assert(expr.filter.isDefined, "AggregateExpression filter should not be None")
          }
      }
    }

    withSQLConf((CHConfig.runtimeConfig("enable_aggregate_if_to_filter"), "true")) {
      compareResultsAgainstVanillaSpark(sql, compareResult = true, checkAggregateWithFilter)
    }
  }

  test("Test map with nullable key") {
    val sql = "select map(string_field1, int_field1) from json_test where string_field1 is not null"
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("local digit date") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName),
      ("spark.sql.legacy.timeParserPolicy", "LEGACY")) {
      sql("create table tb_local_date(i bigint, d string) using parquet")
      sql("""
            |insert into tb_local_date values
            |(1, '2aLZoNmi2aQt2aDZpi3ZoNmh'),
            |(2, '2aLZoNmi2aQt2aHZoi3Zo9mh'),
            |(3, '2aLZoNmi2aQt2aHZoi3Zo9mh'),
            |(5, '27LbsNuy27Ut27HbsS3bsduz'),
            |(6, ''),
            |(7, '4KWo4KWm4KWo4KWrLeClp+Clpy3gpafgpak='),
            |(8, '4Z+i4Z+g4Z+i4Z+lLeGfoeGfoS3hn6Hhn6M='),
            |(9, null),
            |(10, '4Keo4Kem4Keo4KerLeCnp+Cnpy3gp6fgp6k='),
            |(11, 'MjAyNS0xMS0xMg=='),
            |(12, '4LmS4LmQ4LmS4LmVLeC5keC5kS3guZHguZM=')
            |""".stripMargin)
      // base64 inputs decode to local digit dates:
      // 1-3 Arabic-Indic, 5 Persian, 7 Devanagari, 8 Khmer, 10 Bengali, 11 ASCII, 12 Thai
      var query_sql = """
                        |select
                        |from_unixtime(unix_timestamp(cast(unbase64(d) as string), 'yyyy-MM-dd')),
                        |cast(unbase64(d) as string) from (
                        |select d, i
                        |from tb_local_date
                        |order by i)
                        |""".stripMargin
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })

      query_sql = """
                    |select from_unixtime(
                    | unix_timestamp(cast(unbase64('2aLZoNmi2aQt2aDZpi3ZoNmh') as string),
                    | 'yyyy-MM-dd')),
                    | cast(unbase64('2aLZoNmi2aQt2aDZpi3ZoNmh') as string)
                    |""".stripMargin
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })

      query_sql = """
                    |select from_unixtime(unix_timestamp('2020-01-01', 'yyyy-MM-dd'))
                    |""".stripMargin
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })

      query_sql = """
                    |select from_unixtime(
                    | unix_timestamp(
                    |   regexp_replace(
                    |     cast(unbase64('4LmS4LmQ4LmS4LmVLeC5keC5kS3guZHguZM=') as string),
                    |     '-0', '-'),
                    |   'yyyy-MM-dd'),
                    | 'yyyy-MM-dd')
                    |""".stripMargin
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })

      query_sql = """
                    |select from_unixtime(
                    | unix_timestamp(
                    |   regexp_replace(
                    |     cast(unbase64('4Z+i4Z+g4Z+i4Z+lLeGfoeGfoS3hn6Hhn6M=') as string),
                    |     '-0', '-'),
                    |   'yyyy-MM-dd'),
                    | 'yyyy-MM-dd')
                    |""".stripMargin
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })

      sql("drop table tb_local_date")
    }
  }

}
