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
package org.apache.gluten.execution.hive

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseWholeStageTransformerSuite, ProjectExecTransformer, TransformSupport}
import org.apache.gluten.test.AllDataTypesWithComplexType

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StructType}

import org.apache.hadoop.fs.Path

import java.io.{File, PrintWriter}

import scala.reflect.ClassTag

class GlutenClickHouseHiveTableSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with ReCreateHiveSession {

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "536870912")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set(
        "spark.sql.warehouse.dir",
        this.getClass.getResource("/").getPath + "tests-working-home/spark-warehouse")
      .set("spark.hive.exec.dynamic.partition.mode", "nonstrict")
      .set(GlutenConfig.GLUTEN_SUPPORTED_HIVE_UDFS.key, "my_add")
      .setCHConfig("use_local_format", true)
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .setMaster("local[*]")
  }

  private val txt_table_name = "hive_txt_test"
  private val txt_upper_table_name = "hive_txt_upper_test"
  private val txt_user_define_input = "hive_txt_user_define_input"
  private val json_table_name = "hive_json_test"
  private val parquet_table_name = "hive_parquet_test"

  private val txt_table_create_sql = genTableCreateSql(txt_table_name, "textfile")
  private val txt_upper_create_sql = genTableCreateUpperSql(txt_upper_table_name, "textfile")

  private val parquet_table_create_sql = genTableCreateSql(parquet_table_name, "parquet")
  private val json_table_create_sql = "create table if not exists %s (".format(json_table_name) +
    "string_field string," +
    "int_field int," +
    "long_field long," +
    "float_field float," +
    "double_field double," +
    "short_field short," +
    "byte_field byte," +
    "bool_field boolean," +
    "decimal_field decimal(23, 12)," +
    "date_field date," +
    "timestamp_field timestamp," +
    "array_field array<int>," +
    "array_field_with_null array<int>," +
    "map_field map<int,long>," +
    "map_field_with_null map<int,long>, " +
    "day string) partitioned by(day)" +
    "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'" +
    "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
    "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
  private val txt_table_user_define_create_sql =
    "create table if not exists %s (".format(txt_user_define_input) +
      "string_field string," +
      "int_field int," +
      "long_field long," +
      "float_field float," +
      "double_field double," +
      "short_field short," +
      "byte_field byte," +
      "bool_field boolean," +
      "decimal_field decimal(23, 12)," +
      "date_field date," +
      "timestamp_field timestamp," +
      "array_field array<int>," +
      "array_field_with_null array<int>," +
      "map_field map<int, long>," +
      "map_field_with_null map<int, long>) " +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.UserDefineTextInputFormat'" +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"

  def genTableCreateSql(tableName: String, fileFormat: String): String =
    "create table if not exists %s (".format(tableName) +
      "string_field string," +
      "int_field int," +
      "long_field long," +
      "float_field float," +
      "double_field double," +
      "short_field short," +
      "byte_field byte," +
      "bool_field boolean," +
      "decimal_field decimal(23, 12)," +
      "date_field date," +
      "timestamp_field timestamp," +
      "array_field array<int>," +
      "array_field_with_null array<int>," +
      "map_field map<int, long>," +
      "map_field_with_null map<int, long>) stored as %s".format(fileFormat)

  def genTableCreateUpperSql(tableName: String, fileFormat: String): String =
    "create table if not exists %s (".format(tableName) +
      "STRING_FIELD string," +
      "INT_FIELD int," +
      "LONG_FIELD long," +
      "FLOAT_FIELD float," +
      "DOUBLE_FIELD double," +
      "SHORT_FIELD short," +
      "BYTE_FIELD byte," +
      "BOOL_FIELD boolean," +
      "DECIMAL_FIELD decimal(23, 12)," +
      "DATE_FIELD date," +
      "TIMESTAMP_FIELD timestamp," +
      "ARRAY_FIELD array<int>," +
      "ARRAY_FIELD_WITH_NULL array<int>," +
      "MAP_FIELD map<int, long>," +
      "MAP_FIELD_WITH_NULL map<int, long>) stored as %s".format(fileFormat)

  protected def initializeTable(
      table_name: String,
      table_create_sql: String,
      partitions: Seq[String]): Unit = {
    spark
      .createDataFrame(AllDataTypesWithComplexType.genTestData())
      .createOrReplaceTempView("tmp_t")
    val truncate_sql = "truncate table %s".format(table_name)
    val drop_sql = "drop table if exists %s".format(table_name)
    spark.sql(drop_sql)
    spark.sql(table_create_sql)
    spark.sql(truncate_sql)
    if (partitions != null) {
      for (partition <- partitions) {
        spark.sql(
          "insert into %s PARTITION (day = '%s') select * from tmp_t".format(table_name, partition))
      }
    } else {
      spark.sql("insert into %s select * from tmp_t".format(table_name))
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTable(txt_table_name, txt_table_create_sql, null)
    initializeTable(txt_upper_table_name, txt_upper_create_sql, null)
    initializeTable(txt_user_define_input, txt_table_user_define_create_sql, null)
    initializeTable(
      json_table_name,
      json_table_create_sql,
      Seq("2023-06-05", "2023-06-06", "2023-06-07"))
    initializeTable(parquet_table_name, parquet_table_create_sql, null)
  }

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()
    super.afterAll()
  }

  test("test hive text table") {
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $txt_table_name
         | group by string_field
         | order by string_field
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test hive text table using user define input format") {
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $txt_user_define_input
         | group by string_field
         | order by string_field
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test hive text table with unordered columns") {
    val sql = "select decimal_field, short_field, double_field, float_field, long_field, " +
      s"int_field, string_field from $txt_table_name order by string_field"
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test hive text table with count(1)/count(*)") {
    val sql1 = s"select count(1), count(*) from $txt_table_name"
    compareResultsAgainstVanillaSpark(
      sql1,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )

    val sql2 = s"select count(*) from $txt_table_name where int_field >= 100"
    compareResultsAgainstVanillaSpark(
      sql2,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("fix bug: https://github.com/oap-project/gluten/issues/2022") {
    spark.sql(
      "create table if not exists test_empty_partitions" +
        "(uid string, mac string, country string) partitioned by (day string) stored as textfile")

    var sql = "select * from test_empty_partitions where day = '2023-01-01';"
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("fix bug: hive text table limit with fallback") {
    val sql =
      s"""
         | select string_field
         | from $txt_table_name
         | order by string_field
         | limit 10
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("hive text table select complex type columns with fallback") {
    val sql = s"select int_field, array_field, map_field from $txt_table_name order by int_field"
    compareResultsAgainstVanillaSpark(sql, true, { _ => }, false)
  }

  test("hive text table case-insensitive column matching") {
    val sql = s"select SHORT_FIELD, int_field, LONG_field from $txt_table_name order by int_field"
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test hive json table") {
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $json_table_name
         | where day = '2023-06-06'
         | group by string_field
         | order by string_field
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val jsonFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(jsonFileScan.size == 1)
      }
    )
  }

  test("test hive json table complex data type") {
    val sql =
      s"""
         | select array_field, map_field from $json_table_name
         | where string_field != '' and int_field > 0
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val jsonFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(jsonFileScan.size == 1)
      }
    )

  }

  test("GLUTEN-2019: Bug fix not allow quotes") {
    val default_quote_table_name = "test_2019_default"
    val allow_double_quote_table_name = "test_2019_allow_double"
    val allow_single_quote_table_name = "test_2019_allow_single"
    val default_data_path = getClass.getResource("/").getPath + "/text-data/default"
    val allow_double_data_path = getClass.getResource("/").getPath + "/text-data/double-quote"
    val allow_single_data_path = getClass.getResource("/").getPath + "/text-data/single-quote"
    val drop_default_table_sql = "drop table if exists %s".format(default_quote_table_name)
    val drop_double_quote_table_sql =
      "drop table if exists %s".format(allow_double_quote_table_name)
    val drop_single_quote_table_sql =
      "drop table if exists %s".format(allow_single_quote_table_name)
    val create_default_table_sql =
      "create table if not exists %s (".format(default_quote_table_name) +
        "a string," +
        "b string, " +
        "c string)" +
        " row format delimited fields terminated by ',' stored as textfile LOCATION \"%s\""
          .format(default_data_path)
    val create_double_quote_table_sql =
      "create table if not exists %s (".format(allow_double_quote_table_name) +
        "a string," +
        "b string, " +
        "c string)" +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'" +
        " WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '\"')" +
        " stored as textfile LOCATION \"%s\"".format(allow_double_data_path)
    val create_single_quote_table_sql =
      "create table if not exists %s (".format(allow_single_quote_table_name) +
        "a string," +
        "b string, " +
        "c string)" +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'" +
        " WITH SERDEPROPERTIES (\"separatorChar\" = \",\", \"quoteChar\" = \"\'\")" +
        " stored as textfile LOCATION \"%s\"".format(allow_single_data_path)

    spark.sql(drop_default_table_sql)
    spark.sql(drop_double_quote_table_sql)
    spark.sql(drop_single_quote_table_sql)
    spark.sql(create_default_table_sql)
    spark.sql(create_double_quote_table_sql)
    spark.sql(create_single_quote_table_sql)

    val sql1 = "select * from " + default_quote_table_name
    val sql2 = "select * from " + allow_double_quote_table_name
    val sql3 = "select * from " + allow_single_quote_table_name
    compareResultsAgainstVanillaSpark(
      sql1,
      compareResult = true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      }
    )
    compareResultsAgainstVanillaSpark(
      sql2,
      compareResult = true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      }
    )
    compareResultsAgainstVanillaSpark(
      sql3,
      compareResult = true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("text hive table with space/tab delimiter") {
    val txt_table_name_space_delimiter = "hive_txt_table_space_delimiter"
    val txt_table_name_tab_delimiter = "hive_txt_table_tab_delimiter"
    val drop_space_table_sql = "drop table if exists %s".format(txt_table_name_space_delimiter)
    val drop_tab_table_sql = "drop table if exists %s".format(txt_table_name_tab_delimiter)
    val create_space_table_sql =
      "create table if not exists %s (".format(txt_table_name_space_delimiter) +
        "int_field int," +
        "string_field string" +
        ") row format delimited fields terminated by ' ' stored as textfile"
    val create_tab_table_sql =
      "create table if not exists %s (".format(txt_table_name_tab_delimiter) +
        "int_field int," +
        "string_field string" +
        ") row format delimited fields terminated by '\t' stored as textfile"
    spark.sql(drop_space_table_sql)
    spark.sql(drop_tab_table_sql)
    spark.sql(create_space_table_sql)
    spark.sql(create_tab_table_sql)
    spark.sql("insert into %s values(1, 'ab')".format(txt_table_name_space_delimiter))
    spark.sql("insert into %s values(1, 'ab')".format(txt_table_name_tab_delimiter))
    val sql1 =
      s"""
         | select * from $txt_table_name_space_delimiter where int_field > 0
         |""".stripMargin
    val sql2 =
      s"""
         | select * from $txt_table_name_tab_delimiter where int_field > 0
         |""".stripMargin

    compareResultsAgainstVanillaSpark(
      sql1,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
    compareResultsAgainstVanillaSpark(
      sql2,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test hive table with illegal partition path") {
    val path = new Path(sparkConf.get("spark.sql.warehouse.dir"))
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    val tablePath = path.toUri.getPath + "/" + json_table_name
    val partitionPath = tablePath + "/" + "_temp_day=2023_06_05kids"
    val succ = fs.mkdirs(new Path(partitionPath))
    assert(succ, true)
    val partitionDataFilePath = partitionPath + "/abc.txt"
    val createSucc = fs.createNewFile(new Path(partitionDataFilePath))
    assert(createSucc, true)
    val sql =
      s"""
         | select string_field, day, count(*) from $json_table_name group by string_field, day
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("GLUTEN-7700: test hive table with partition values contain space") {
    val tbl = "test_7700"
    val create_table_sql =
      s"""
         |create table if not exists $tbl (
         |  id int
         |) partitioned by (itime string)
         |stored as orc;
         |""".stripMargin
    val insert_sql =
      s"""
         |insert overwrite table $tbl partition (itime = '2024-10-24 10:02:04')
         |select id from range(3)
         |""".stripMargin
    val select_sql =
      s"""
         |select * from $tbl
         |""".stripMargin
    val drop_sql = s"drop table if exists $tbl"

    spark.sql(create_table_sql)
    spark.sql(insert_sql)

    compareResultsAgainstVanillaSpark(
      select_sql,
      compareResult = true,
      df => assert(df.count() == 3)
    )
    spark.sql(drop_sql)
  }

  test("test hive compressed txt table") {
    withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "11") {
      Seq("DefaultCodec", "BZip2Codec").foreach {
        compress =>
          val txt_compressed_table_name = "hive_compressed_txt_test"
          val drop_table_sql = "drop table if exists %s".format(txt_compressed_table_name)
          val create_table_sql =
            "create table if not exists %s (".format(txt_compressed_table_name) +
              "id bigint," +
              "name string," +
              "sex string) stored as textfile"
          spark.sql(drop_table_sql)
          spark.sql(create_table_sql)
          spark.sql("SET hive.exec.compress.output=true")
          spark.sql("SET mapred.output.compress=true")
          spark.sql(s"SET mapred.output.compression.codec=org.apache.hadoop.io.compress.$compress")
          val insert_sql =
            s"""
               | insert into $txt_compressed_table_name values(1, "a", "b")
               |""".stripMargin
          spark.sql(insert_sql)

          val sql = "select * from " + txt_compressed_table_name
          compareResultsAgainstVanillaSpark(
            sql,
            compareResult = true,
            df => {
              val txtFileScan =
                collect(df.queryExecution.executedPlan) {
                  case l: HiveTableScanExecTransformer => l
                }
              assert(txtFileScan.size == 1)
            }
          )
      }
    }
  }

  test("text hive txt table with multiple compressed method") {
    val compressed_txt_table_name = "compressed_hive_txt_test"
    val compressed_txt_data_path = getClass.getResource("/").getPath + "/text-data/compressed"
    val drop_table_sql = "drop table if exists %s".format(compressed_txt_table_name)
    val create_table_sql =
      "create table if not exists %s (".format(compressed_txt_table_name) +
        "id bigint," +
        "name string," +
        "sex string) stored as textfile LOCATION \"%s\"".format(compressed_txt_data_path)
    spark.sql(drop_table_sql)
    spark.sql(create_table_sql)
    val sql = "select * from " + compressed_txt_table_name
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test orc/parquet table with null complex type values") {
    val create_template =
      """
        | CREATE TABLE test_%s(
        |   id INT,
        |   info STRUCT<name:STRING, age:INT>,
        |   data MAP<STRING, INT>,
        |   values ARRAY<INT>
        | ) stored as %s;
        |""".stripMargin
    val insert_template =
      """
        | INSERT OVERWRITE test_%s VALUES
        |   (1, struct('John', 25), map('A', 10, 'B', 20), array(1.0, 2.0, 3.0)),
        |   (2, struct('Alice', 30), map('C', 15, 'D', 25), array(4.0, 5.0, 6.0)),
        |   (3, struct('Bob', 35), map('E', 12, 'F', 18), array(7.0, 8.0, 9.0)),
        |   (4, struct('Jane', 40), map('G', 22, 'H', 30), array(10.0, 11.0, 12.0)),
        |   (5, struct('Kate', 45), map('I', 17, 'J', 28), array(13.0, 14.0, 15.0)),
        |   (6, null, null, null),
        |   (7, struct('Tank', 20), map('X', null, 'Y', null), array(1.0, 2.0, 3.0));
        |""".stripMargin

    val select1_template = "select id, info, info.age, data, values from test_%s"
    val select2_template = "select id, info.name from test_%s"
    val select3_template = "select id from test_%s where info.name = 'Bob'"

    val drop_template = "DROP TABLE test_%s"

    val formats = Array("orc", "parquet")
    for (format <- formats) {
      val create_sql = create_template.format(format, format)
      val insert_sql = insert_template.format(format)
      val select1_sql = select1_template.format(format)
      val select2_sql = select2_template.format(format)
      val select3_sql = select3_template.format(format)
      val drop_sql = drop_template.format(format)

      spark.sql(create_sql)
      spark.sql(insert_sql)

      compareResultsAgainstVanillaSpark(select1_sql, compareResult = true, _ => {})
      compareResultsAgainstVanillaSpark(select2_sql, compareResult = true, _ => {})
      compareResultsAgainstVanillaSpark(select3_sql, compareResult = true, _ => {})

      spark.sql(drop_sql)
    }
  }

  test("Gluten-2582: Fix crash in array<struct>") {
    val create_table_sql =
      """
        | create table test_tbl_2582(
        | id bigint,
        | d1 array<struct<a:int, b:string>>,
        | d2 map<string, struct<a:int, b:string>>) stored as parquet
        |""".stripMargin

    val insert_data_sql =
      """
        | insert into test_tbl_2582 values
        | (1, array(named_struct('a', 1, 'b', 'b1'), named_struct('a', 2, 'b', 'c1')),
        | map('a', named_struct('a', 1, 'b', 'b1'))),
        | (2, null, map('b', named_struct('a', 2, 'b','b2'))),
        | (3, array(null, named_struct('a', 3, 'b', 'b3')),
        |  map('c', named_struct('a', 3, 'b', 'b3'))),
        | (4, array(named_struct('a', 4, 'b', 'b4')), null),
        | (5, array(named_struct('a', 5, 'b', 'b5')),
        |  map('c', null, 'd', named_struct('a', 5, 'b', 'b5')))
        |""".stripMargin

    val select_sql_1 = "select * from test_tbl_2582 where d1[0].a = 1"
    val select_sql_2 = "select element_at(d1, 1).a from test_tbl_2582 where id = 1"
    val select_sql_3 = "select d2['a'].a from test_tbl_2582 where id = 1"
    val select_sql_4 = "select count(1) from test_tbl_2582 where d1[0].a = 1"
    val select_sql_5 = "select count(1) from test_tbl_2582 where (id = 2 or id = 3) and d1[0].a = 1"
    val select_sql_6 =
      "select count(1) from test_tbl_2582 where (id = 4 or id = 5) and d2['c'].a = 1"

    spark.sql(create_table_sql)
    spark.sql(insert_data_sql)

    compareResultsAgainstVanillaSpark(select_sql_1, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_2, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_3, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_4, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_5, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_6, compareResult = true, _ => {})
  }

  test("GLUTEN-2180: Test data field too much/few") {
    val test_table_name = "test_table_2180"
    val drop_table_sql = "drop table if exists %s".format(test_table_name)
    val test_data_path = getClass.getResource("/").getPath + "/text-data/field_too_much_few"
    val create_table_sql =
      "create table if not exists %s (".format(test_table_name) +
        "a string," +
        "b string," +
        "c string) stored as textfile LOCATION \"%s\"".format(test_data_path)
    spark.sql(drop_table_sql)
    spark.sql(create_table_sql)
    val sql = "select * from " + test_table_name
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("GLUTEN-2180: Test data field type not match") {
    val test_table_name = "test_table_2180"
    val drop_table_sql = "drop table if exists %s".format(test_table_name)
    val test_data_path = getClass.getResource("/").getPath + "/text-data/field_data_type_not_match"
    val create_table_sql =
      "create table if not exists %s (".format(test_table_name) +
        "a int," +
        "b string," +
        "c date, " +
        "d timestamp, " +
        "e boolean) stored as textfile LOCATION \"%s\"".format(test_data_path)
    spark.sql(drop_table_sql)
    spark.sql(create_table_sql)
    val sql = "select * from " + test_table_name
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      }
    )
  }

  test("test parquet push down filter skip row groups") {
    val currentTimestamp = System.currentTimeMillis() / 1000
    val sql =
      s"""
         | select count(1) from $parquet_table_name
         | where int_field > 1 and string_field > '1' and
         | bool_field = true and float_field > 3 and double_field > 5
         | and short_field > 1 and byte_field > 3
         | and timestamp_field <= to_timestamp($currentTimestamp)
         | and array_field[0] > 0 and map_field[9] >= 0 and decimal_field > 5
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, _ => {})
  }

  test("test parquet push down filter with null value") {
    val create_table_sql =
      """
        | create table test_tbl_2456(
        | id bigint,
        | name string,
        | age int,
        | day string,
        | hour string) partitioned by(day, hour) stored as parquet
        |""".stripMargin
    val insert_data_sql =
      """
        | insert into test_tbl_2456 values
        | (1, 'a', 12, '2023-08-01', '12'),
        | (2, 'b', null, '2023-08-01', '12'),
        | (3, 'c', null, '2023-08-01', '12'),
        | (4, 'd', null, '2023-08-01', '12'),
        | (5, null, 22, '2023-08-01', '12'),
        | (6, null, 23, '2023-08-01', '12'),
        | (7, 'e', 24, '2023-08-01', '12')
        |""".stripMargin

    val select_sql_1 = "select count(1) from test_tbl_2456 where name is null or age is null"
    val select_sql_2 = "select count(1) from test_tbl_2456 where age > 0 and day = '2023-08-01'"

    spark.sql(create_table_sql)
    spark.sql(insert_data_sql)
    compareResultsAgainstVanillaSpark(select_sql_1, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_2, compareResult = true, _ => {})
  }

  test("test parquet push down filter with multi-nested column types") {
    val create_table_sql =
      """
        | create table test_tbl_2457(
        | id bigint,
        | d1 array<array<int>>,
        | d2 array<Map<string, string>>,
        | d3 struct<a:int, b:array<map<string, string>>, c:map<string, array<map<string, string>>>>,
        | d4 struct<a:int, b:string, c:struct<d:string, e:string>>,
        | d5 map<string, array<int>>,
        | d6 map<string, map<string, string>>) stored as parquet
        |""".stripMargin

    val insert_data_sql =
      """
        | insert into test_tbl_2457 values
        | (1,
        |  array(array(1,2), array(3,4)),
        |  array(map('a', 'b', 'c', 'd'), map('e', 'f')),
        |  named_struct('a', 1, 'b', array(map('e', 'f')),
        |  'c', map('a', array(map('b', 'c'), map('d', 'e')), 'f', array(map('k', 'l')))),
        |  named_struct('a', 1, 'b', 'b1', 'c', named_struct('d', 'd1', 'e', 'e1')),
        |  map('f', array(3,4,5,6)),
        |  map('f', map('a', 'b'), 'g', map('d', 'e')))
        |""".stripMargin

    val select_sql_1 =
      """
        | select count(1) from test_tbl_2457
        | where d1[0][1]=2 and d2[1]['e'] = 'f'
        | and d3.c['a'][1]['d'] = 'e'
        | and d4.c.d = 'd1' and d5['f'][2] = 5
        | and d6['g']['d'] = 'e'
        |""".stripMargin
    spark.sql(create_table_sql)
    spark.sql(insert_data_sql)
    compareResultsAgainstVanillaSpark(select_sql_1, compareResult = true, _ => {})
  }

  test("fix reading string from number bug: https://github.com/oap-project/gluten/issues/3023") {
    val data_path = resPath + "/text-data/json-settings"
    spark.sql(s"""
                 |CREATE TABLE json_settings (
                 |  a string,
                 |  b string,
                 |  c int,
                 |  t struct<ta:string, tb:int, tc:float>)
                 |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
                 |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
                 |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 |LOCATION '$data_path'
      """.stripMargin)

    val select_sql = "select * from json_settings"
    compareResultsAgainstVanillaSpark(select_sql, compareResult = true, _ => {})
    spark.sql("DROP TABLE json_settings")
  }

  test("fix empty field delim bug: https://github.com/oap-project/gluten/issues/3098") {
    spark.sql(s"""
                 |CREATE TABLE a (
                 |  uid bigint,
                 |  appid int,
                 |  type int,
                 |  account string)
                 |ROW FORMAT SERDE
                 |  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                 |WITH SERDEPROPERTIES (
                 |  'field.delim'='',
                 |  'line.delim'='\n',
                 |  'serialization.format'='')
                 |STORED AS INPUTFORMAT
                 |  'org.apache.hadoop.mapred.TextInputFormat'
                 |OUTPUTFORMAT
                 |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      """.stripMargin)
    spark.sql("insert into a select id, id, id, cast(id as string) from range(10)")

    val select_sql = "select * from a"
    compareResultsAgainstVanillaSpark(select_sql, compareResult = true, _ => {})
    spark.sql("DROP TABLE a")
  }

  test("fix csv serde bug: https://github.com/oap-project/gluten/issues/3108") {
    spark.sql(s"""
                 |CREATE TABLE b (
                 |  uid bigint,
                 |  appid int,
                 |  type int,
                 |  account string)
                 |ROW FORMAT SERDE
                 |  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                 |STORED AS INPUTFORMAT
                 |  'org.apache.hadoop.mapred.TextInputFormat'
                 |OUTPUTFORMAT
                 |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      """.stripMargin)
    spark.sql("insert into b select id, id, id, cast(id as string) from range(10)")

    val sql = "select * from b"
    compareResultsAgainstVanillaSpark(sql, compareResult = true, _ => {})

    spark.sql("DROP TABLE b")
  }

  test("GLUTEN-3337: fix get_json_object for abnormal json") {
    val data_path = resPath + "/text-data/abnormal-json"
    spark.sql(s"""
                 |CREATE TABLE test_tbl_3337 (
                 |  id bigint,
                 |  data string) stored as textfile
                 |LOCATION '$data_path'
      """.stripMargin)

    val select_sql_1 = "select id, get_json_object(data, '$.data.v') from test_tbl_3337"
    val select_sql_2 = "select id, get_json_object(data, '$.v') from test_tbl_3337"
    val select_sql_3 = "select id, get_json_object(data, '$.123.234') from test_tbl_3337"
    val select_sql_4 = "select id, get_json_object(data, '$.v111') from test_tbl_3337"
    val select_sql_5 = "select id, get_json_object(data, 'v112') from test_tbl_3337"
    val select_sql_6 =
      "select id, get_json_object(data, '$.id') from test_tbl_3337 where id = 123"
    val select_sql_7 =
      "select id, get_json_object(data, '$.id') from test_tbl_3337"
    compareResultsAgainstVanillaSpark(select_sql_1, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_2, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_3, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_4, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_5, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_6, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(select_sql_7, compareResult = true, _ => {})

    spark.sql("DROP TABLE test_tbl_3337")
  }

  test("test hive read recursive dirs") {
    val path = new Path(sparkConf.get("spark.sql.warehouse.dir"))
    val create_test_file_recursive =
      "create external table if not exists test_file_recursive (" +
        "int_field int" +
        ") row format delimited fields terminated by ',' stored as textfile " +
        "LOCATION 'file://" + path + "/test_file_recursive'"
    spark.sql(create_test_file_recursive)

    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    val tablePath = path.toUri.getPath + "/test_file_recursive"
    val recursivePath = tablePath + "/subDir1/subDir2"
    val succ = fs.mkdirs(new Path(recursivePath))
    assert(succ, true)
    val recursiveFile = recursivePath + "/file1.txt"
    val writer = new PrintWriter(new File(recursiveFile))
    writer.write("10")
    writer.close()

    val sql =
      s"""
         | select int_field from test_file_recursive
         |""".stripMargin

    withSQLConf(("mapreduce.input.fileinputformat.input.dir.recursive", "true")) {
      compareResultsAgainstVanillaSpark(
        sql,
        compareResult = true,
        df => {
          assert(df.collect().length == 1)
        }
      )
    }
  }

  test("GLUTEN-3552: Bug fix csv field whitespaces") {
    val data_path = resPath + "/text-data/field_whitespaces"
    spark.sql(s"""
                 | CREATE TABLE test_tbl_3552(
                 | a string,
                 | b string,
                 | c string)
                 | ROW FORMAT SERDE
                 |  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                 |WITH SERDEPROPERTIES (
                 |   'field.delim'=','
                 | )
                 | STORED AS INPUTFORMAT
                 |  'org.apache.hadoop.mapred.TextInputFormat'
                 |OUTPUTFORMAT
                 |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 |LOCATION '$data_path'
                 |""".stripMargin)
    val select_sql = "select * from test_tbl_3552"
    compareResultsAgainstVanillaSpark(select_sql, compareResult = true, _ => {})
    spark.sql("DROP TABLE test_tbl_3552")
  }

  test("GLUTEN-3548: Bug fix csv allow cr end of line") {
    val data_path = resPath + "/text-data/cr_end_of_line"
    spark.sql(s"""
                 | CREATE TABLE test_tbl_3548(
                 | a string,
                 | b string,
                 | c string)
                 | ROW FORMAT SERDE
                 |  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                 |WITH SERDEPROPERTIES (
                 |   'field.delim'=','
                 | )
                 | STORED AS INPUTFORMAT
                 |  'org.apache.hadoop.mapred.TextInputFormat'
                 |OUTPUTFORMAT
                 |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                 |LOCATION '$data_path'
                 |""".stripMargin)
    val select_sql = "select * from test_tbl_3548"
    compareResultsAgainstVanillaSpark(select_sql, compareResult = true, _ => {})
    spark.sql("DROP TABLE test_tbl_3548")
  }

  test("test 'hive udf'") {
    val jarPath = "udfs/hive-test-udfs.jar"
    val jarUrl = s"file://$resPath/$jarPath"
    spark.sql(
      s"CREATE FUNCTION my_add as " +
        s"'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2' USING JAR '$jarUrl'")
    if (isSparkVersionLE("3.3")) {
      runQueryAndCompare("select MY_ADD(id, id+1) from range(10)")(
        checkGlutenPlan[ProjectExecTransformer])
    } else {
      runQueryAndCompare("select MY_ADD(id, id+1) from range(10)", noFallBack = false)(_ => {})
    }
  }

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

  test("GLUTEN-4333: fix CSE in aggregate operator") {
    val createTableSql =
      """
        |CREATE TABLE `test_cse`(
        |  `uid` bigint,
        |  `event` struct<time:bigint,event_info:map<string,string>>
        |)  PARTITIONED BY (
        |  `day` string)
        |ROW FORMAT SERDE
        |  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin
    spark.sql(createTableSql)
    val querySql =
      """
        |select day,uid,xxx3,type,
        |if(xxx1 is  null or xxx1 =''  or  xxx1='-1'   or  xxx1='0',-1,xxx1) xxx1,
        |if(xxx2 is  null or xxx2 ='' or  xxx2 =0 or  xxx2=-1 ,-1,xxx2) xxx2,
        |cast(if(xxx1 in (48,49),1,
        |if(xxx1 in (1,2),2,
        |if(xxx1 in (18,19),3,
        |if(xxx1 in (24,25),4,
        |if(xxx1 in (34,35),5,
        |if(xxx1 in (39,40),6,
        |if(xxx1 in (38,47),xxx1,-1))))))) as string) as xxx1_type,count(uid) as cnt
        |from
        |(
        |select
        |day
        |,uid
        |,event.event_info['xxx3'] xxx3
        |,event.event_info['type'] type
        |,event.event_info['xxx1'] xxx1
        |,event.event_info['xxx2'] xxx2
        |,round(event.time/1000)
        |from  test_cse
        |where
        |event.event_info['type']  in (1,2)
        |group by day
        |,uid
        |,event.event_info['xxx3']
        |,event.event_info['type']
        |,event.event_info['xxx1']
        |,event.event_info['xxx2']
        |,round(event.time/1000)
        |)
        |group by day,uid,xxx3,type,
        |if(xxx1 is  null or xxx1 =''  or  xxx1='-1'   or  xxx1='0',-1,xxx1),
        |if(xxx2 is  null or xxx2 ='' or  xxx2 =0 or  xxx2=-1 ,-1,xxx2),
        |cast(if(xxx1 in (48,49),1,
        |if(xxx1 in (1,2),2,
        |if(xxx1 in (18,19),3,
        |if(xxx1 in (24,25),4,
        |if(xxx1 in (34,35),5,
        |if(xxx1 in (39,40),6,
        |if(xxx1 in (38,47),xxx1,-1))))))) as string)
        """.stripMargin
    runQueryAndCompare(querySql)(df => checkOperatorCount[ProjectExecTransformer](2)(df))
  }

  test(
    "Gluten GetArrayStructFields: SPARK-33907: bad json input with " +
      "json pruning optimization: GetArrayStructFields") {
    val createSql =
      """
        |CREATE TABLE table_33907 AS
        |  SELECT  '[{"a": 1, "b": 2}, {"a": 3, "b": 4}]' AS value
      """.stripMargin
    spark.sql(createSql)

    val selectSql = "select from_json(value, 'array<struct<a:int,b:int>>').b from table_33907"
    Seq("true", "false").foreach {
      enabled =>
        withSQLConf(SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> enabled) {
          runQueryAndCompare(selectSql)(checkGlutenPlan[ProjectExecTransformer])
        }
    }
  }

  test(
    "Gluten GetArrayStructFields: SPARK-37450: " +
      "Prunes unnecessary fields from Explode for count aggregation") {
    val createSql1 =
      """
        |CREATE TABLE table_37450 using json AS
        |  SELECT '[{"itemId":1,"itemData":"a"},{"itemId":2,"itemData":"b"}]' as value
      """.stripMargin
    spark.sql(createSql1)

    val createSql2 =
      """
        |CREATE TABLE table_37450_orc using orc
        |  SELECT from_json(value, 'array<struct<itemId:long, itemData:string>>') as items
        |  FROM table_37450
      """.stripMargin
    spark.sql(createSql2)

    val selectSql = "SELECT count(*) FROM table_37450_orc LATERAL VIEW explode(items) as item"
    Seq("true", "false").foreach {
      enabled =>
        withSQLConf("spark.sql.orc.enableVectorizedReader" -> enabled) {
          compareResultsAgainstVanillaSpark(selectSql, compareResult = true, _ => {})
        }
    }
  }

  test(
    "Gluten GetArrayStructFields: SPARK-37450: " +
      "Prunes unnecessary fields from Explode for friend's middle name") {
    val createSql =
      """
        |CREATE TABLE contacts (
        |  `id` INT,
        |  `name` STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>,
        |  `address` STRING,
        |  `pets` INT,
        |  `friends` ARRAY<STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>>,
        |  `relatives` MAP<STRING, STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>>,
        |  `employer` STRUCT<`id`: INT, `company`: STRUCT<`name`: STRING, `address`: STRING>>,
        |  `relations` MAP<STRUCT<`first`: STRING, `middle`: STRING, `last`: STRING>,STRING>,
        |  `p` INT
        |) using parquet;
      """.stripMargin
    spark.sql(createSql)

    val insertSql =
      """
        |INSERT INTO contacts
        |VALUES
        |  (
        |    1,
        |    named_struct('first', 'John', 'middle', 'M', 'last', 'Doe'),
        |    '123 Main St',
        |    2,
        |    array(named_struct('first', 'Jane', 'middle', 'A', 'last', 'Smith')),
        |    map('Uncle', named_struct('first', 'Bob', 'middle', 'B', 'last', 'Johnson')),
        |    named_struct('id', 1, 'company',
        |      named_struct('name', 'ABC Corp', 'address', '456 Market St')),
        |    map(named_struct('first', 'Jane', 'middle', 'A', 'last', 'Smith'), 'Friend'),
        |    1
        |  ),
        |  (
        |    2,
        |    named_struct('first', 'Jane', 'middle', 'A', 'last', 'Smith'),
        |    '456 Market St',
        |    1,
        |    array(named_struct('first', 'John', 'middle', 'M', 'last', 'Doe')),
        |    map('Aunt', named_struct('first', 'Alice', 'middle', 'A', 'last', 'Johnson')),
        |    named_struct('id', 2, 'company',
        |      named_struct('name', 'XYZ Corp', 'address', '789 Broadway St')),
        |    map(named_struct('first', 'John', 'middle', 'M', 'last', 'Doe'), 'Friend'),
        |    2
        |  );
      """.stripMargin
    spark.sql(insertSql)

    val selectSql = "SELECT friend.MIDDLE FROM contacts LATERAL VIEW explode(friends) as friend"
    Seq("true", "false").foreach {
      enabled =>
        withSQLConf("spark.sql.parquet.enableVectorizedReader" -> enabled) {
          compareResultsAgainstVanillaSpark(selectSql, compareResult = true, _ => {})
        }
    }
  }

  test("GLUTEN-3452: Bug fix decimal divide") {
    val table_create_sql =
      """
        | create table test_tbl_3452(d1 decimal(12,2), d2 decimal(15,3)) stored as parquet;
        |""".stripMargin
    val data_insert_sql = "insert into test_tbl_3452 values(13.0, 0),(11, NULL), (12.3, 200)"
    spark.sql(table_create_sql)
    spark.sql(data_insert_sql)
    Seq("true", "false").foreach {
      s =>
        withSQLConf((SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key, s)) {
          val select_sql = "select d1/d2, d1/0, d1/cast(0 as decimal) from test_tbl_3452"
          compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
        }
    }
    spark.sql("drop table test_tbl_3452")
  }

  test("GLUTEN-6235: Fix crash on ExpandTransform::work()") {
    val tbl = "test_tbl_6235"
    sql(s"drop table if exists $tbl")
    val createSql =
      s"""
         |create table $tbl
         |stored as textfile
         |as select 1 as a1, 2 as a2, 3 as a3, 4 as a4, 5 as a5, 6 as a6, 7 as a7, 8 as a8, 9 as a9
         |""".stripMargin
    sql(createSql)
    val select_sql =
      s"""
         |select
         |a5,a6,a7,a8,a3,a4,a9
         |,count(distinct a2) as a2
         |,count(distinct a1) as a1
         |,count(distinct if(a3=1,a2,null)) as a33
         |,count(distinct if(a4=2,a1,null)) as a43
         |from $tbl
         |group by a5,a6,a7,a8,a3,a4,a9 with cube
         |""".stripMargin
    compareResultsAgainstVanillaSpark(select_sql, true, { _ => })
    sql(s"drop table if exists $tbl")
  }

  test("test mergetree write with column case sensitive on hive") {
    val dataPath = s"$dataHome/lineitem_mergetree_bucket"
    val sourceDF = spark.sql(s"""
                                |select
                                |  string_field,
                                |  int_field,
                                |  long_field,
                                |  date_field
                                | from $txt_table_name
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.numBuckets", "1")
      .option("clickhouse.bucketColumnNames", "STRING_FIELD")
      .mode(SaveMode.Overwrite)
      .save(dataPath)

    assert(new File(dataPath).listFiles().nonEmpty)

    val dataPath2 = s"$dataHome/lineitem_mergetree_bucket2"
    val df2 = spark.sql(s"""
                           |select
                           |  string_field STRING_FIELD,
                           |  int_field INT_FIELD,
                           |  long_field LONG_FIELD,
                           |  date_field DATE_FIELD
                           | from $txt_table_name
                           |""".stripMargin)

    df2.write
      .format("clickhouse")
      .partitionBy("DATE_FIELD")
      .option("clickhouse.numBuckets", "1")
      .option("clickhouse.bucketColumnNames", "STRING_FIELD")
      .option("clickhouse.orderByKey", "INT_FIELD,LONG_FIELD")
      .option("clickhouse.primaryKey", "INT_FIELD")
      .mode(SaveMode.Overwrite)
      .save(dataPath2)
    assert(new File(dataPath2).listFiles().nonEmpty)

    val dataPath3 = s"$dataHome/lineitem_mergetree_bucket3"
    val df3 = spark.sql(s"""
                           |select
                           |  string_field,
                           |  int_field,
                           |  long_field,
                           |  date_field
                           | from $txt_upper_table_name
                           |""".stripMargin)

    df3.write
      .format("clickhouse")
      .partitionBy("date_field")
      .option("clickhouse.numBuckets", "1")
      .option("clickhouse.bucketColumnNames", "string_field")
      .option("clickhouse.orderByKey", "int_field,LONG_FIELD")
      .option("clickhouse.primaryKey", "INT_FIELD")
      .mode(SaveMode.Overwrite)
      .save(dataPath3)
    assert(new File(dataPath3).listFiles().nonEmpty)

    val dataPath4 = s"$dataHome/lineitem_mergetree_bucket2"
    val df4 = spark
      .sql(s"""
              |select
              |  INT_FIELD ,
              |  STRING_FIELD,
              |  LONG_FIELD ,
              |  DATE_FIELD
              | from $txt_table_name
              | order by INT_FIELD
              |""".stripMargin)
      .toDF("INT_FIELD", "STRING_FIELD", "LONG_FIELD", "DATE_FIELD")

    df4.write
      .format("clickhouse")
      .partitionBy("DATE_FIELD")
      .option("clickhouse.numBuckets", "3")
      .option("clickhouse.bucketColumnNames", "STRING_FIELD")
      .option("clickhouse.orderByKey", "INT_FIELD,LONG_FIELD")
      .option("clickhouse.primaryKey", "INT_FIELD")
      .mode(SaveMode.Append)
      .save(dataPath4)
    assert(new File(dataPath4).listFiles().nonEmpty)
  }

  test("GLUTEN-6506: Orc read time zone") {
    val dataPath = s"$dataHome/orc-data/test_reader_time_zone.snappy.orc"
    val create_table_sql = ("create table test_tbl_6506(" +
      "id bigint, t timestamp) stored as orc location '%s'")
      .format(dataPath)
    val select_sql = "select * from test_tbl_6506"
    spark.sql(create_table_sql)
    compareResultsAgainstVanillaSpark(select_sql, compareResult = true, _ => {})
    spark.sql("drop table test_tbl_6506")
  }

  test("GLUTEN-7502: Orc write time zone") {
    val create_table_sql = "create table test_tbl_7502(id bigint, t timestamp) using orc"
    val insert_sql = "insert into test_tbl_7502 values(1, cast('2024-10-09 20:00:00' as timestamp))"
    val select_sql = "select * from test_tbl_7502"
    spark.sql(create_table_sql)
    spark.sql(insert_sql)
    compareResultsAgainstVanillaSpark(select_sql, compareResult = true, _ => {})
    spark.sql("drop table test_tbl_7502")
  }

  test("GLUTEN-6879: Fix partition value diff when it contains blanks") {
    val tableName = "test_tbl_6879"
    sql(s"drop table if exists $tableName")

    val createSql =
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING
         |) PARTITIONED BY (part STRING)
         |STORED AS PARQUET;
         |""".stripMargin
    sql(createSql)

    val insertSql =
      s"""
         |INSERT INTO $tableName PARTITION (part='part with spaces')
         |VALUES (1, 'John Doe');
         |""".stripMargin
    sql(insertSql)

    val selectSql = s"SELECT * FROM $tableName"
    compareResultsAgainstVanillaSpark(selectSql, compareResult = true, _ => {})
    sql(s"drop table if exists $tableName")
  }

  test("GLUTEN-7054: Fix exception when CSE meets common alias expression") {
    val createTableSql = """
                           |CREATE TABLE test_tbl_7054 (
                           |  day STRING,
                           |  event_id STRING,
                           |  event STRUCT<
                           |    event_info: MAP<STRING, STRING>
                           |  >
                           |) STORED AS PARQUET;
                           |""".stripMargin

    val insertDataSql = """
                          |INSERT INTO test_tbl_7054
                          |VALUES
                          |  ('2024-08-27', '011441004',
                          |     STRUCT(MAP('type', '1', 'action', '8', 'value_vmoney', '100'))),
                          |  ('2024-08-27', '011441004',
                          |     STRUCT(MAP('type', '2', 'action', '8', 'value_vmoney', '200'))),
                          |  ('2024-08-27', '011441004',
                          |     STRUCT(MAP('type', '4', 'action', '8', 'value_vmoney', '300')));
                          |""".stripMargin

    val selectSql = """
                      |SELECT
                      |  COALESCE(day, 'all') AS daytime,
                      |  COALESCE(type, 'all') AS type,
                      |  COALESCE(value_money, 'all') AS value_vmoney,
                      |  SUM(CASE
                      |      WHEN type IN (1, 2) AND action = 8 THEN value_vmoney
                      |      ELSE 0
                      |  END) / 60 AS total_value_vmoney
                      |FROM (
                      |  SELECT
                      |    day,
                      |    type,
                      |    NVL(CAST(value_vmoney AS BIGINT), 0) AS value_money,
                      |    action,
                      |    type,
                      |    CAST(value_vmoney AS BIGINT) AS value_vmoney
                      |  FROM (
                      |    SELECT
                      |      day,
                      |      event.event_info["type"] AS type,
                      |      event.event_info["action"] AS action,
                      |      event.event_info["value_vmoney"] AS value_vmoney
                      |    FROM test_tbl_7054
                      |    WHERE
                      |      day = '2024-08-27'
                      |      AND event_id = '011441004'
                      |      AND event.event_info["type"] IN (1, 2, 4)
                      |  ) a
                      |) b
                      |GROUP BY
                      |  day, type, value_money
                      |""".stripMargin

    spark.sql(createTableSql)
    spark.sql(insertDataSql)
    runQueryAndCompare(selectSql)(df => checkOperatorCount[ProjectExecTransformer](3)(df))
    spark.sql("DROP TABLE test_tbl_7054")
  }

  test("Nested column pruning for Project(Filter(Generate))") {
    spark.sql("drop table if exists aj")
    spark.sql(
      """
        |CREATE TABLE if not exists aj (
        |  country STRING,
        |  event STRUCT<time:BIGINT, lng:BIGINT, lat:BIGINT, net:STRING,
        |     log_extra:MAP<STRING, STRING>, event_id:STRING, event_info:MAP<STRING, STRING>>
        |)
        |USING orc
      """.stripMargin)

    spark.sql("""
                |INSERT INTO aj VALUES
                |  ('USA', named_struct('time', 1622547800, 'lng', -122, 'lat', 37, 'net',
                |    'wifi', 'log_extra', map('key1', 'value1'), 'event_id', 'event1',
                |    'event_info', map('tab_type', '5', 'action', '13'))),
                |  ('Canada', named_struct('time', 1622547801, 'lng', -79, 'lat', 43, 'net',
                |    '4g', 'log_extra', map('key2', 'value2'), 'event_id', 'event2',
                |    'event_info', map('tab_type', '4', 'action', '12')))
       """.stripMargin)

    val sql = """
                | SELECT * FROM (
                |  SELECT
                |    game_name,
                |    CASE WHEN
                |       event.event_info['tab_type'] IN (5) THEN '1' ELSE '0' END AS entrance
                |  FROM aj
                |  LATERAL VIEW explode(split(nvl(event.event_info['game_name'],'0'),','))
                |    as game_name
                |  WHERE event.event_info['action'] IN (13)
                |) WHERE game_name = 'xxx'
      """.stripMargin

    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val scan = df.queryExecution.executedPlan.collect {
          case scan: FileSourceScanExecTransformer => scan
        }.head
        val fieldType = scan.schema.fields.head.dataType.asInstanceOf[StructType]
        assert(fieldType.size == 1)
      }
    )

    spark.sql("drop table if exists aj")
  }

  test("Nested column pruning for Project(Filter(Generate)) on generator") {
    def assertFieldSizeAfterPruning(sql: String, expectSize: Int): Unit = {
      compareResultsAgainstVanillaSpark(
        sql,
        compareResult = true,
        df => {
          val scan = df.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExecTransformer => scan
          }.head

          val fieldType =
            scan.schema.fields.head.dataType
              .asInstanceOf[ArrayType]
              .elementType
              .asInstanceOf[StructType]
          assert(fieldType.size == expectSize)
        }
      )
    }

    spark.sql("drop table if exists ajog")
    spark.sql(
      """
        |CREATE TABLE if not exists ajog (
        |  country STRING,
        |  events ARRAY<STRUCT<time:BIGINT, lng:BIGINT, lat:BIGINT, net:STRING,
        |     log_extra:MAP<STRING, STRING>, event_id:STRING, event_info:MAP<STRING, STRING>>>
        |)
        |USING orc
      """.stripMargin)

    spark.sql("""
                |INSERT INTO ajog VALUES
                |  ('USA', array(named_struct('time', 1622547800, 'lng', -122, 'lat', 37, 'net',
                |    'wifi', 'log_extra', map('key1', 'value1'), 'event_id', 'event1',
                |    'event_info', map('tab_type', '5', 'action', '13')))),
                |  ('Canada', array(named_struct('time', 1622547801, 'lng', -79, 'lat', 43, 'net',
                |    '4g', 'log_extra', map('key2', 'value2'), 'event_id', 'event2',
                |    'event_info', map('tab_type', '4', 'action', '12'))))
       """.stripMargin)

    // Test nested column pruning on generator with single field extracted
    val sql1 = """
                 |select
                 |case when event.event_info['tab_type'] in (5) then '1' else '0' end as entrance
                 |from ajog
                 |lateral view explode(events)  as event
                 |where  event.event_info['action'] in (13)
      """.stripMargin
    assertFieldSizeAfterPruning(sql1, 1)

    // Test nested column pruning on generator with multiple field extracted,
    // which resolves SPARK-34956
    val sql2 = """
                 |select event.event_id,
                 |case when event.event_info['tab_type'] in (5) then '1' else '0' end as entrance
                 |from ajog
                 |lateral view explode(events)  as event
                 |where  event.event_info['action'] in (13)
      """.stripMargin
    assertFieldSizeAfterPruning(sql2, 2)

    // Test nested column pruning with two adjacent generate operator
    val sql3 = """
                 |SELECT
                 |abflag,
                 |event.event_info,
                 |event.log_extra
                 |FROM
                 |ajog
                 |LATERAL VIEW EXPLODE(events) AS event
                 |LATERAL VIEW EXPLODE(split(event.log_extra['key1'], ',')) AS abflag
                 |WHERE
                 |event.event_id = 'event1'
                 |AND event.event_info['tab_type'] IS NOT NULL
                 |AND event.event_info['tab_type'] != ''
                 |AND event.log_extra['key1'] = 'value1'
                 |LIMIT 100;
      """.stripMargin
    assertFieldSizeAfterPruning(sql3, 3)

    spark.sql("drop table if exists ajog")
  }

  test("test hive table scan nested column pruning") {
    val json_table_name = "test_tbl_7267_json"
    val pq_table_name = "test_tbl_7267_pq"
    val create_table_sql =
      s"""
         | create table if not exists %s(
         | id bigint,
         | d1 STRUCT<c: STRING, d: ARRAY<STRUCT<x: STRING, y: STRING>>>,
         | d2 STRUCT<c: STRING, d: Map<STRING, STRUCT<x: STRING, y: STRING>>>,
         | day string,
         | hour string
         | ) partitioned by(day, hour)
         |""".stripMargin
    val create_table_json = create_table_sql.format(json_table_name) +
      s"""
         | ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
         | STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
         | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
         |""".stripMargin
    val create_table_pq = create_table_sql.format(pq_table_name) + " Stored as PARQUET"
    val insert_sql =
      """
        | insert into %s values(1,
        | named_struct('c', 'c123', 'd', array(named_struct('x', 'x123', 'y', 'y123'))),
        | named_struct('c', 'c124', 'd', map('m124', named_struct('x', 'x124', 'y', 'y124'))),
        | '2024-09-26', '12'
        | )
        |""".stripMargin
    val select_sql =
      "select id, d1.c, d1.d[0].x, d2.d['m124'].y from %s where day = '2024-09-26' and hour = '12'"
    val table_names = Array.apply(json_table_name, pq_table_name)
    val create_table_sqls = Array.apply(create_table_json, create_table_pq)
    for (i <- table_names.indices) {
      val table_name = table_names(i)
      val create_table = create_table_sqls(i)
      spark.sql(create_table)
      spark.sql(insert_sql.format(table_name))
      withSQLConf(("spark.sql.hive.convertMetastoreParquet" -> "false")) {
        compareResultsAgainstVanillaSpark(
          select_sql.format(table_name),
          compareResult = true,
          df => {
            val scan = collect(df.queryExecution.executedPlan) {
              case l: HiveTableScanExecTransformer => l
            }
            assert(scan.size == 1)
          }
        )
      }
      spark.sql("drop table if exists %s".format(table_name))
    }
  }

  test("test input_file_name() in different formats") {
    val formats = Seq("textfile", "orc", "parquet")
    val tableNamePrefix = "sales_"

    formats.foreach {
      format =>
        val tableName = s"$tableNamePrefix${format.take(2)}"
        val createTableSql =
          s"""
             |CREATE TABLE $tableName (
             |  product_id STRING,
             |  quantity INT
             |) PARTITIONED BY (year STRING)
             |STORED AS $format
             |""".stripMargin

        val insertDataSql1 =
          s"""
             |INSERT INTO $tableName PARTITION(year='2001')
             |SELECT 'prod1', 100
             |""".stripMargin

        val insertDataSql2 =
          s"""
             |INSERT INTO $tableName PARTITION(year='2002')
             |SELECT 'prod1', 200
             |""".stripMargin

        val select1Sql = s"SELECT input_file_name() from $tableName"
        val select2Sql = s"SELECT input_file_block_start(), " +
          s"input_file_block_length() FROM $tableName"
        s"input_file_block_length() FROM $tableName"
        val dropSql = s"DROP TABLE IF EXISTS $tableName"

        spark.sql(createTableSql)
        spark.sql(insertDataSql1)
        spark.sql(insertDataSql2)

        if (format.equals("textfile")) {
          // When format is textfile, input_file_name() in vanilla returns paths like 'file:/xxx'
          // But in gluten it returns paths like 'file:///xxx'.
          val result = spark.sql(select1Sql)
          result
            .collect()
            .foreach(
              row => {
                assert(!row.isNullAt(0) && row.getString(0).nonEmpty)
              })
        } else {
          compareResultsAgainstVanillaSpark(select1Sql, compareResult = true, _ => {})
        }

        compareResultsAgainstVanillaSpark(select2Sql, compareResult = true, _ => {})

        spark.sql(dropSql)
    }
  }

  test("GLUTEN-9647: Fix SimplifySumRule on different types") {
    val sql =
      s"""
         | select sum(int_field * 2L),
         |        min(float_field / 2),
         |        max(double_field * 0.03),
         |        sum(short_field * 2),
         |        sum(decimal_field * 3L)
         | from $json_table_name
         | where day = '2023-06-06'
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val jsonFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(jsonFileScan.size == 1)
      }
    )
  }

}
