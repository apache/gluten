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
import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite
import org.apache.gluten.test.AllDataTypesWithComplexType.genTestData

import org.apache.spark.SparkConf
import org.apache.spark.gluten.NativeWriteChecker
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.types._

import java.io.File
import java.sql.Date

class GlutenClickHouseNativeWriteTableSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper
  with ReCreateHiveSession
  with NativeWriteChecker {

  override protected def sparkConf: SparkConf = {
    var sessionTimeZone = "GMT"
    if (isSparkVersionGE("3.5")) {
      sessionTimeZone = java.util.TimeZone.getDefault.getID
    }
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
      .set("spark.sql.warehouse.dir", getWarehouseDir)
      .set("spark.sql.session.timeZone", sessionTimeZone)
      .setMaster("local[1]")
  }

  private val table_name_template = "hive_%s_test"
  private val table_name_vanilla_template = "hive_%s_test_written_by_vanilla"

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()
    super.afterAll()
  }

  import collection.immutable.ListMap
  private val fields_ = ListMap(
    ("string_field", "string"),
    ("int_field", "int"),
    ("long_field", "long"),
    ("float_field", "float"),
    ("double_field", "double"),
    ("short_field", "short"),
    ("byte_field", "byte"),
    ("boolean_field", "boolean"),
    ("decimal_field", "decimal(23,12)"),
    ("date_field", "date")
  )

  private lazy val supplierSchema = StructType.apply(
    Seq(
      StructField.apply("s_suppkey", LongType, nullable = true),
      StructField.apply("s_name", StringType, nullable = true),
      StructField.apply("s_address", StringType, nullable = true),
      StructField.apply("s_nationkey", LongType, nullable = true),
      StructField.apply("s_phone", StringType, nullable = true),
      StructField.apply("s_acctbal", DecimalType(15, 2), nullable = true),
      StructField.apply("s_comment", StringType, nullable = true)
    ))

  private def supplierDF = {
    spark.read
      .option("delimiter", "|")
      .option("header", "false")
      .schema(supplierSchema)
      .csv(s"${resPath}csv-data/supplier.csv")
      .toDF()
  }

  test("supplier: csv to parquet- insert overwrite local directory") {
    val partitionNumber = 7
    withSource(supplierDF, "supplier") {
      nativeWrite2(
        format => {
          val sql =
            s"""insert overwrite local directory
               |'$dataHome/test_insert_into_${format}_supplier'
               |stored as $format
               |select /*+ REPARTITION($partitionNumber) */ * from supplier""".stripMargin
          (s"test_insert_into_${format}_supplier", null, sql)
        },
        (table_name, format) => {
          // spark 3.2 without orc or parquet suffix
          val files = recursiveListFiles(new File(s"$dataHome/$table_name"))
            .map(_.getName)
            .filterNot(s => s.endsWith(s".crc") || s.equals("_SUCCESS"))

          lazy val fileNames = {
            val dir = s"$dataHome/$table_name"
            recursiveListFiles(new File(dir))
              .map(f => f.getAbsolutePath.stripPrefix(dir))
              .sorted
              .mkString("\n")
          }

          lazy val errorMessage =
            s"Search $dataHome/$table_name with suffix .$format, all files: \n $fileNames"
          assert(files.length === partitionNumber, errorMessage)
        }
      )
    }
  }

  test("supplier: csv to parquet- insert into one partition") {
    val originViewName = "supplier"
    lazy val create_columns = supplierSchema
      .filterNot(f => f.name.equals("s_nationkey"))
      .map(f => s"${f.name} ${f.dataType.catalogString}")
      .mkString(",")
    lazy val all_columns = supplierSchema
      .filterNot(f => f.name.equals("s_nationkey"))
      .map(f => s"${f.name}")
      .mkString(",") + ", s_nationkey"
    withSource(supplierDF, originViewName) {
      nativeWrite2 {
        format =>
          val table_name = s"supplier_$format"
          val table_create_sql =
            s"""create table if not exists $table_name
               |($create_columns)
               |partitioned by (s_nationkey bigint) stored as $format""".stripMargin
          val insert_sql =
            s"""insert into $table_name
               |select $all_columns from $originViewName""".stripMargin
          (table_name, table_create_sql, insert_sql)
      }
    }
  }

  test("test insert into dir") {
    withSource(genTestData(), "origin_table") {
      nativeWrite {
        format =>
          Seq(
            s"""insert overwrite local directory '$dataHome/test_insert_into_${format}_dir1'
               |stored as $format select ${fields_.keys.mkString(",")}
               |from origin_table""".stripMargin,
            s"""insert overwrite local directory '$dataHome/test_insert_into_${format}_dir2'
               |stored as $format select string_field, sum(int_field) as x
               |from origin_table group by string_field""".stripMargin
          ).foreach(checkInsertQuery(_, checkNative = true))
      }
    }
  }

  test("test insert into partition") {
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"""create table if not exists $table_name
           |(${fields_.map(f => s"${f._1} ${f._2}").mkString(",")})
           |partitioned by (another_date_field date) stored as $format""".stripMargin
      val insert_sql =
        s"""insert into $table_name partition(another_date_field = '2020-01-01')
           | select ${fields_.keys.mkString(",")} from origin_table""".stripMargin
      (table_name, table_create_sql, insert_sql)
    }

    withSource(genTestData(), "origin_table", ("spark.sql.orc.compression.codec", "lz4")) {
      nativeWrite2(
        format => destination(format),
        (table_name, format) => {
          var files = recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
            .filter(_.getName.endsWith(s".$format"))
          if (format == "orc") {
            files = files.filter(_.getName.contains(".lz4"))
          }
          assertResult(1)(files.length)
          assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
        }
      )
    }
  }

  test("test CTAS") {
    withSource(genTestData(), "origin_table") {
      nativeWrite {
        format =>
          val table_name = table_name_template.format(format)
          val ctas_support_sql =
            s"create table $table_name using $format as select " +
              fields_
                .map(f => s"${f._1}")
                .mkString(",") +
              " from origin_table"
          val ctas_not_support_sql =
            s"create table $table_name as select " +
              fields_
                .map(f => s"${f._1}")
                .mkString(",") +
              " from origin_table"
          withDestinationTable(table_name) {
            checkInsertQuery(ctas_support_sql, checkNative = true)
          }
          withDestinationTable(table_name) {
            try {
              // hive format without format name not support
              checkInsertQuery(ctas_not_support_sql, checkNative = false)
            } catch {
              case _: UnsupportedOperationException => // expected
              case e: Exception => fail("should not throw exception", e)
            }
          }
      }
    }
  }

  test("test insert into partition, bigo's case which incur InsertIntoHiveTable") {
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql = s"create table if not exists $table_name (" + fields_
        .map(f => s"${f._1} ${f._2}")
        .mkString(",") + " ) partitioned by (another_date_field string)" +
        s"stored as $format"
      val insert_sql =
        s"insert overwrite table $table_name " +
          "partition(another_date_field = '2020-01-01') select " +
          fields_.keys.mkString(",") + " from (select " + fields_.keys.mkString(
            ",") + ", row_number() over (order by int_field desc) as rn  " +
          "from origin_table where float_field > 3 ) tt where rn <= 100"
      (table_name, table_create_sql, insert_sql)
    }

    withSource(
      genTestData(),
      "origin_table",
      ("spark.sql.hive.convertMetastoreParquet", "false"),
      ("spark.sql.hive.convertMetastoreOrc", "false")) {
      nativeWrite2(
        format => destination(format),
        (table_name, format) => {
          val files = recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
            .filter(_.getName.startsWith("part"))
          assertResult(1)(files.length)
          assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
        },
        isSparkVersionLE("3.3")
      )
    }
  }

  test("test 1-col partitioned table") {
    val origin_table = "origin_table"
    withSource(genTestData(), origin_table) {
      nativeWrite2(
        format => {
          val table_name = table_name_template.format(format)
          val table_create_sql =
            s"create table if not exists $table_name (" +
              fields_
                .filterNot(e => e._1.equals("date_field"))
                .map(f => s"${f._1} ${f._2}")
                .mkString(",") +
              " ) partitioned by (date_field date) " +
              s"stored as $format"
          val insert_sql =
            s"""insert overwrite $table_name select ${fields_.keys.toSeq.mkString(",")}
               |from $origin_table""".stripMargin
          (table_name, table_create_sql, insert_sql)
        })
    }
  }

  test("test 1-col partitioned table, partitioned by already ordered column") {
    val origin_table = "origin_table"
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"create table if not exists $table_name (" +
          fields_
            .filterNot(e => e._1.equals("date_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          " ) partitioned by (date_field date) " +
          s"stored as $format"
      val insert_sql =
        s"""insert overwrite $table_name select ${fields_.keys.mkString(",")}
           |from $origin_table order by date_field""".stripMargin
      (table_name, table_create_sql, insert_sql)
    }
    def check(table_name: String, format: String): Unit =
      compareSource(origin_table, table_name, fields_.keys.toSeq)
    withSource(genTestData(), origin_table) {
      nativeWrite2(destination, check)
    }
  }

  test("test 2-col partitioned table") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date"),
      ("byte_field", "byte")
    )
    val origin_table = "origin_table"
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"create table if not exists $table_name (" +
          fields
            .filterNot(e => e._1.equals("date_field") || e._1.equals("byte_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") + " ) partitioned by (date_field date, byte_field byte) " +
          s"stored as $format"
      val insert_sql =
        s"""insert overwrite $table_name select ${fields.keys.mkString(",")}
           |from $origin_table order by date_field""".stripMargin
      (table_name, table_create_sql, insert_sql)
    }
    def check(table_name: String, format: String): Unit =
      compareSource(origin_table, table_name, fields.keys.toSeq)
    withSource(genTestData(), origin_table) {
      nativeWrite2(destination, check)
    }
  }

  ignore(
    "test hive parquet/orc table, all types of columns being partitioned except the date_field," +
      " ignore because takes too long") {

    val fields: ListMap[String, String] = ListMap(
      ("date_field", "date"),
      ("timestamp_field", "timestamp"),
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)")
    )

    val origin_table = "origin_table"
    def destination(format: String, field: (String, String)): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"create table if not exists $table_name (" +
          " date_field date" + " ) partitioned by (" +
          field._1 + " " + field._2 + ") " +
          s"stored as $format"
      val insert_sql =
        s"""insert overwrite $table_name
           |select ${List("date_field", field._1).mkString(",")} from $origin_table""".stripMargin
      (table_name, table_create_sql, insert_sql)
    }
    def check(table_name: String, format: String, field: (String, String)): Unit =
      compareSource(origin_table, table_name, List("date_field", field._1))

    withSource(genTestData(), origin_table) {
      for (field <- fields.filterNot(e => e._1.equals("date_field"))) {
        nativeWrite2(
          format => destination(format, field),
          (table_name, format) => check(table_name, format, field))
      }
    }
  }

  // This test case will be failed with incorrect result randomly, ignore first.
  ignore("test hive parquet/orc table, all columns being partitioned. ") {
    val fields: ListMap[String, String] = ListMap(
      ("date_field", "date"),
      ("timestamp_field", "timestamp"),
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)")
    )
    val origin_table = "origin_table"
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"create table if not exists $table_name (" +
          " date_field date" + " ) partitioned by (" +
          fields
            .filterNot(e => e._1.equals("date_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          ") " +
          s"stored as $format"
      val insert_sql =
        s"""insert overwrite $table_name select ${fields.keys.mkString(",")}
           |from $origin_table order by date_field""".stripMargin
      (table_name, table_create_sql, insert_sql)
    }
    def check(table_name: String, format: String): Unit =
      compareSource(origin_table, table_name, fields.keys.toSeq)
    withSource(genTestData(), origin_table) {
      nativeWrite2(destination, check)
    }
  }

  test("test hive parquet/orc table with aggregated results") {
    val fields: ListMap[String, String] = ListMap(
      ("sum(int_field)", "bigint")
    )
    val origin_table = "origin_table"
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"create table if not exists $table_name (" +
          fields
            .map(f => s"${getColumnName(f._1)} ${f._2}")
            .mkString(",") +
          s" ) stored as $format"
      val insert_sql =
        s"insert overwrite $table_name select ${fields.keys.toSeq.mkString(",")} from $origin_table"
      (table_name, table_create_sql, insert_sql)
    }
    def check(table_name: String, format: String): Unit =
      compareSource(origin_table, table_name, fields.keys.toSeq)
    withSource(genTestData(), origin_table) {
      nativeWrite2(destination, check)
    }
  }

  test("test 1-col partitioned + 1-col bucketed table") {
    val origin_table = "origin_table"
    withSource(genTestData(), origin_table) {
      nativeWrite {
        format =>
          // spark write does not support bucketed table
          // https://issues.apache.org/jira/browse/SPARK-19256
          val table_name = table_name_template.format(format)
          writeAndCheckRead(origin_table, table_name, fields_.keys.toSeq) {
            fields =>
              spark
                .table("origin_table")
                .select(fields.head, fields.tail: _*)
                .write
                .format(format)
                .partitionBy("date_field")
                .bucketBy(2, "byte_field")
                .saveAsTable(table_name)
          }

          assertResult(2)(
            new File(getWarehouseDir + "/" + table_name)
              .listFiles()
              .filter(_.isDirectory)
              .filter(!_.getName.equals("date_field=__HIVE_DEFAULT_PARTITION__"))
              .head
              .listFiles()
              .count(!_.isHidden)
          ) // 2 bucket files
      }
    }
  }

  test("test table bucketed by all typed columns") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date")
      // ("timestamp_field", "timestamp")
      // FIXME https://github.com/apache/incubator-gluten/issues/8053
    )
    val origin_table = "origin_table"
    withSource(genTestData(), origin_table) {
      nativeWrite {
        format =>
          val table_name = table_name_template.format(format)
          val testFields = fields.keys.toSeq
          writeAndCheckRead(origin_table, table_name, testFields) {
            fields =>
              spark
                .table(origin_table)
                .select(fields.head, fields.tail: _*)
                .write
                .format(format)
                .bucketBy(10, fields.head, fields.tail: _*)
                .saveAsTable(table_name)
          }
          val table_name_vanilla = table_name_vanilla_template.format(format)
          spark.sql(s"drop table IF EXISTS $table_name_vanilla")
          withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")) {
            withNativeWriteCheck(checkNative = false) {
              spark
                .table("origin_table")
                .select(testFields.head, testFields.tail: _*)
                .write
                .format(format)
                .bucketBy(10, testFields.head, testFields.tail: _*)
                .saveAsTable(table_name_vanilla)
            }
          }
          compareWriteFilesSignature(format, table_name, table_name_vanilla, "sum(int_field)")
      }
    }
  }

  testWithMaxSparkVersion("test 1-col partitioned + 2-col bucketed table", "3.3") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date"),
      ("array", "array<int>"),
      ("map", "map<int, long>")
    )

    val origin_table = "origin_table"
    withSource(genTestData(), origin_table) {
      nativeWrite {
        format =>
          val table_name = table_name_template.format(format)
          writeAndCheckRead(origin_table, table_name, fields.keys.toSeq) {
            fields =>
              spark
                .table("origin_table")
                .select(fields.head, fields.tail: _*)
                .write
                .format(format)
                .partitionBy("date_field")
                .bucketBy(10, "byte_field", "string_field")
                .saveAsTable(table_name)
          }

          val table_name_vanilla = table_name_vanilla_template.format(format)
          spark.sql(s"drop table IF EXISTS $table_name_vanilla")
          withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")) {
            withNativeWriteCheck(checkNative = false) {
              spark
                .table("origin_table")
                .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
                .write
                .format(format)
                .partitionBy("date_field")
                .bucketBy(10, "byte_field", "string_field")
                .saveAsTable(table_name_vanilla)
            }
            compareWriteFilesSignature(format, table_name, table_name_vanilla, "sum(int_field)")
          }
      }
    }
  }

  test("test consecutive blocks having same partition value") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        withNativeWriteCheck(checkNative = true) {
          // 8096 row per block, so there will be blocks containing all 0 in p, all 1 in p
          spark
            .range(30000)
            .selectExpr("id", "id % 2 as p")
            .orderBy("p")
            .write
            .format(format)
            .partitionBy("p")
            .saveAsTable(table_name)
        }
        val ret = spark.sql(s"select sum(id) from $table_name").collect().apply(0).apply(0)
        assertResult(449985000)(ret)
    }
  }

  testWithMaxSparkVersion("test decimal with rand()", "3.3") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        withNativeWriteCheck(checkNative = true) {
          spark
            .range(200)
            .selectExpr("id", " cast((id + rand()) as decimal(23,12)) as p")
            .write
            .format(format)
            .partitionBy("p")
            .saveAsTable(table_name)
        }
        val ret = spark.sql(s"select max(p) from $table_name").collect().apply(0).apply(0)
    }
  }

  test("test partitioned by constant") {
    nativeWrite2 {
      format =>
        val table_name = s"tmp_123_$format"
        val create_sql =
          s"""create table tmp_123_$format(
             |x1 string, x2 bigint,x3 string, x4 bigint, x5 string )
             |partitioned by (day date) stored as $format""".stripMargin
        val insert_sql =
          s"""insert into tmp_123_$format partition(day)
             |select cast(id as string), id, cast(id as string),
             |       id, cast(id as string), '2023-05-09'
             |from range(10000000)""".stripMargin
        (table_name, create_sql, insert_sql)
    }
  }

  test("test partitioned with escaped characters") {

    val schema = StructType(
      Seq(
        StructField.apply("id", IntegerType, nullable = true),
        StructField.apply("escape", StringType, nullable = true),
        StructField.apply("bucket/col", StringType, nullable = true),
        StructField.apply("part=col1", DateType, nullable = true),
        StructField.apply("part_col2", StringType, nullable = true)
      ))

    val data: Seq[Row] = Seq(
      Row(1, "=", "00000", Date.valueOf("2024-01-01"), "2024=01/01"),
      Row(2, "/", "00000", Date.valueOf("2024-01-01"), "2024=01/01"),
      Row(3, "#", "00000", Date.valueOf("2024-01-01"), "2024#01:01"),
      Row(4, ":", "00001", Date.valueOf("2024-01-02"), "2024#01:01"),
      Row(5, "\\", "00001", Date.valueOf("2024-01-02"), "2024\\01\u000101"),
      Row(6, "\u0001", "000001", Date.valueOf("2024-01-02"), "2024\\01\u000101"),
      Row(7, "", "000002", null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.createOrReplaceTempView("origin_table")
    spark.sql("select * from origin_table").show()

    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        writeAndCheckRead("origin_table", table_name, schema.fieldNames.map(f => s"`$f`")) {
          _ =>
            spark
              .table("origin_table")
              .write
              .format(format)
              .partitionBy("part=col1", "part_col2")
              .bucketBy(2, "bucket/col")
              .saveAsTable(table_name)
        }

        val table_name_vanilla = table_name_vanilla_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name_vanilla")
        withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")) {
          withNativeWriteCheck(checkNative = false) {
            spark
              .table("origin_table")
              .write
              .format(format)
              .partitionBy("part=col1", "part_col2")
              .bucketBy(2, "bucket/col")
              .saveAsTable(table_name_vanilla)
          }
          compareWriteFilesSignature(format, table_name, table_name_vanilla, "sum(id)")
        }
    }
  }

  test("test bucketed by constant") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        withNativeWriteCheck(checkNative = true) {
          spark
            .range(10000000)
            .selectExpr("id", "cast('2020-01-01' as date) as p")
            .write
            .format(format)
            .bucketBy(2, "p")
            .saveAsTable(table_name)
        }
        assertResult(10000000)(spark.table(table_name).count())
    }
  }

  test("test consecutive null values being partitioned") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        withNativeWriteCheck(checkNative = true) {
          spark
            .range(30000)
            .selectExpr("id", "cast(null as string) as p")
            .write
            .format(format)
            .partitionBy("p")
            .saveAsTable(table_name)
        }
        assertResult(30000)(spark.table(table_name).count())
    }
  }

  test("test consecutive null values being bucketed") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        withNativeWriteCheck(checkNative = true) {
          spark
            .range(30000)
            .selectExpr("id", "cast(null as string) as p")
            .write
            .format(format)
            .bucketBy(2, "p")
            .saveAsTable(table_name)
        }
        assertResult(30000)(spark.table(table_name).count())
    }
  }

  test("test native write with empty dataset") {
    nativeWrite2(
      format => {
        val table_name = "t_" + format
        (
          table_name,
          s"create table $table_name (id int, str string) stored as $format",
          s"insert into $table_name select id, cast(id as string) from range(10) where id > 100"
        )
      },
      (table_name, _) => {
        assertResult(0)(spark.table(table_name).count())
      }
    )
  }

  test("test native write with union") {
    nativeWrite {
      format =>
        val table_name = "t_" + format
        withDestinationTable(
          table_name,
          Some(s"create table $table_name (id int, str string) stored as $format")) {
          checkInsertQuery(
            s"insert overwrite table $table_name " +
              "select id, cast(id as string) from range(10) union all " +
              "select 10, '10' from range(10)",
            checkNative = true)
          checkInsertQuery(
            s"insert overwrite table $table_name " +
              "select id, cast(id as string) from range(10) union all " +
              "select 10, cast(id as string) from range(10)",
            checkNative = true
          )
        }
    }
  }

  test("test native write and non-native read consistency") {
    nativeWrite2(
      {
        format =>
          val table_name = "t_" + format
          (
            table_name,
            s"create table $table_name (id int, name string, info char(4)) stored as $format",
            s"insert overwrite table $table_name " +
              "select id, cast(id as string), concat('aaa', cast(id as string)) from range(10)"
          )
      },
      (table_name, _) =>
        // https://github.com/apache/spark/pull/38151 add read-side char padding cause fallback.
        compareResultsAgainstVanillaSpark(
          s"select * from $table_name",
          compareResult = true,
          _ => {},
          isSparkVersionLE("3.3")
        )
    )
  }

  test("GLUTEN-4316: fix crash on dynamic partition inserting") {
    nativeWrite2(
      {
        format =>
          val table_name = "t_" + format
          val create_sql =
            s"""create table $table_name(
               | a int,
               | b map<string, string>,
               | c struct<d:string, e:string>
               | ) partitioned by (day string)
               | stored as $format""".stripMargin

          val insert_sql =
            s"""insert overwrite $table_name partition (day)
               |select id as a,
               |       str_to_map(concat('t1:','a','&t2:','b'),'&',':'),
               |       struct('1', null) as c,
               |       '2024-01-08' as day
               |from range(10)""".stripMargin
          (table_name, create_sql, insert_sql)
      },
      (table_name, _) =>
        compareResultsAgainstVanillaSpark(
          s"select * from $table_name",
          compareResult = true,
          _ => {})
    )
  }

  test("GLUTEN-2584: fix native write and read mismatch about complex types") {
    def table(format: String): String = s"t_2584_$format"
    def create(format: String, table_name: Option[String] = None): String =
      s"""CREATE TABLE ${table_name.getOrElse(table(format))}(
         |  id INT,
         |  info STRUCT<name:STRING, age:INT>,
         |  data MAP<STRING, INT>,
         |  values ARRAY<INT>
         |) stored as $format""".stripMargin
    def insert(format: String, table_name: Option[String] = None): String =
      s"""INSERT overwrite ${table_name.getOrElse(table(format))} VALUES
         |  (6, null, null, null);
            """.stripMargin

    nativeWrite2(
      format => (table(format), create(format), insert(format)),
      (table_name, format) => {
        val vanilla_table = s"${table_name}_v"
        val vanilla_create = create(format, Some(vanilla_table))
        vanillaWrite {
          withDestinationTable(vanilla_table, Option(vanilla_create)) {
            checkInsertQuery(insert(format, Some(vanilla_table)), checkNative = false)
          }
        }
        val rowsFromOriginTable =
          spark.sql(s"select * from $vanilla_table").collect()
        val dfFromWriteTable =
          spark.sql(s"select * from $table_name")
        checkAnswer(dfFromWriteTable, rowsFromOriginTable)
      }
    )
  }

  test(
    "GLUTEN-8021/8022/8032: fix orc read/write mismatch and parquet" +
      "read exception when written complex column contains null") {
    def table(format: String): String = s"t_8021_$format"
    def create(format: String, table_name: Option[String] = None): String =
      s"""CREATE TABLE ${table_name.getOrElse(table(format))}(
         |id int,
         |x int,
         |y int,
         |mp map<string, string>,
         |arr array<int>,
         |tup struct<x:int, y:int>,
         |arr_mp array<map<string, string>>,
         |mp_arr map<string, array<int>>,
         |tup_arr struct<a: array<int>>,
         |tup_map struct<m: map<string, string>>
         |) stored as $format""".stripMargin
    def insert(format: String, table_name: Option[String] = None): String =
      s"""INSERT OVERWRITE TABLE ${table_name.getOrElse(table(format))}
         |SELECT
         |  id, x, y,
         |  str_to_map(concat('x:', x, ',y:', y)) AS mp,
         |  IF(id % 4 = 0, NULL, array(x, y)) AS arr,
         |  IF(id % 4 = 1, NULL, struct(x, y)) AS tup,
         |  IF(id % 4 = 2, NULL, array(str_to_map(concat('x:', x, ',y:', y)))) AS arr_mp,
         |  IF(id % 4 = 3, NULL, map('x', array(x), 'y', array(y))) AS mp_arr,
         |  IF(id % 4 = 0, NULL, named_struct('a', array(x, y))) AS tup_arr,
         |  IF(id % 4 = 1, NULL, named_struct('m',
         |  str_to_map(concat('x:', x, ',y:', y)))) AS tup_map
         |FROM (
         |  SELECT
         |    id,
         |    IF(id % 3 = 1, NULL, id + 1) AS x,
         |    IF(id % 3 = 1, NULL, id + 2) AS y
         |  FROM range(100)
         |) AS data_source;""".stripMargin

    // TODO fix it in spark3.5
    if (!isSparkVersionGE("3.5")) {
      nativeWrite2(
        format => (table(format), create(format), insert(format)),
        (table_name, format) => {
          val vanilla_table = s"${table_name}_v"
          val vanilla_create = create(format, Some(vanilla_table))
          vanillaWrite {
            withDestinationTable(vanilla_table, Option(vanilla_create)) {
              checkInsertQuery(insert(format, Some(vanilla_table)), checkNative = false)
            }
          }
          val rowsFromOriginTable =
            spark.sql(s"select * from $vanilla_table").collect()
          val dfFromWriteTable =
            spark.sql(s"select * from $table_name")
          checkAnswer(dfFromWriteTable, rowsFromOriginTable)
        }
      )
    }
  }
}
