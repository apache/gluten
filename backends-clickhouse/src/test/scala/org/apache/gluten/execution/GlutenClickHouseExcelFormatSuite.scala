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

import org.apache.gluten.backendsapi.clickhouse.{CHConfig, RuntimeSettings}
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.{functions, DataFrame, Row}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.util

case class AllDataTypesWithNonPrimitiveType(
    string_field: String,
    int_field: java.lang.Integer,
    long_field: java.lang.Long,
    float_field: java.lang.Float,
    double_field: java.lang.Double,
    short_field: java.lang.Short,
    byte_field: java.lang.Byte,
    boolean_field: java.lang.Boolean,
    decimal_field: java.math.BigDecimal,
    date_field: java.sql.Date
    // TODO: support below data types
    // array: Seq[Int],
    // arrayContainsNull: Seq[Option[Int]],
    // map: Map[Int, Long],
    // mapValueContainsNull: Map[Int, Option[Long]],
    // data: (Seq[Int], (Int, String))
)

class GlutenClickHouseExcelFormatSuite extends GlutenClickHouseWholeStageTransformerSuite {
  import testImplicits._

  protected val orcDataPath: String = resPath + "orc-data"
  protected val csvDataPath: String = resPath + "csv-data"

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    super.sparkConf
      .set("spark.sql.adaptive.enabled", "true")
      .setCHSettings("date_time_input_format", "best_effort_us")
      .setCHSettings("use_excel_serialization", true)
  }

  // in this case, FakeRowAdaptor does R2C
  test("parquet native writer writing a in memory DF") {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {
      val filePath = dataHome + "/native_parquet_test"
      val format = "parquet"

      val df1 = spark.createDataFrame(genTestData())
      df1.write
        .mode("overwrite")
        .format("parquet")
        .save(filePath)
      val sql =
        s"""
           | select *
           | from $format.`$filePath`
           |""".stripMargin
      val df2 = spark.sql(sql)
      df2.collect()
      GlutenQueryComparisonTest.checkFallBack(df2)
      checkAnswer(df2, df1)
    }
  }

  // in this case, FakeRowAdaptor only wrap&transfer
  test("parquet native writer writing a DF from file") {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {

      val filePath = dataHome + "/native_parquet_test"
      val format = "parquet"

      val df1 = spark.read.parquet(testParquetAbsolutePath + "/customer")
      df1.write
        .mode("overwrite")
        .format("parquet")
        .save(filePath)
      val sql =
        s"""
           | select *
           | from $format.`$filePath`
           |""".stripMargin
      val df2 = spark.sql(sql)
      df2.collect()
      GlutenQueryComparisonTest.checkFallBack(df2)
      checkAnswer(df2, df1)
    }
  }

  // in this case, FakeRowAdaptor only wrap&transfer
  test("parquet native writer writing a DF from an aggregate") {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {

      val filePath = dataHome + "/native_parquet_test_agg"
      val format = "parquet"

      val df0 = spark
        .createDataFrame(genTestData())
      val df1 = df0
        .select("string_field", "int_field", "double_field")
        .groupBy("string_field")
        .agg(
          functions.sum("int_field").as("a"),
          functions.max("double_field").as("b"),
          functions.count("*").as("c"))
      df1.write
        .mode("overwrite")
        .format("parquet")
        .save(filePath)

      val sql =
        s"""
           | select *
           | from $format.`$filePath`
           |""".stripMargin
      val df2 = spark.sql(sql)
      df2.collect()
      GlutenQueryComparisonTest.checkFallBack(df2)
      checkAnswer(df2, df1)
    }
  }

  test("read data from csv file format") {
    val filePath = dataHome + "/csv_test.csv"
    val csvFileFormat = "csv"
    val sql =
      s"""
         | select *
         | from $csvFileFormat.`$filePath`
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(csvFileScan.size == 1)
      }
    )
  }

  // scalastyle:off line.size.limit
  test("GLUTEN-7032 timestamp in-filter test") {
    val filePath = resPath + "/csv-data/filter_timestamp.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("account_id", IntegerType, nullable = false),
        StructField.apply("record_time", DateType, nullable = false),
        StructField.apply("account_user_country", StringType, nullable = false),
        StructField.apply("account_date", DateType, nullable = false),
        StructField.apply("account_time", TimestampType, nullable = false)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")
    options.put("nullValue", "null")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(filePath)
      .toDF()
    df.createTempView("filter_timestamp")
    val sql1: String =
      "select * from filter_timestamp where account_time in ('2020-10-01 10:10:10', '2020-10-01 10:10:11')"
    val sql2: String =
      "select * from filter_timestamp where account_time in (timestamp'2020-10-01 10:10:10', timestamp'2020-10-01 10:10:11')"
    val sql3: String = "select * from filter_timestamp where account_time = '2020-10-01 10:10:10'"
    val sql4: String =
      "select * from filter_timestamp where account_time = timestamp'2020-10-01 10:10:10'"
    val sql5: String =
      "select * from filter_timestamp where account_date in ('2020-10-01', '2020-10-02')"
    runAndCompare(sql1)
    runAndCompare(sql2)
    runAndCompare(sql3)
    runAndCompare(sql4)
    runAndCompare(sql5)
  }
  // scalastyle:on line.size.limit

  test("read data from csv file format with filter") {
    val filePath = dataHome + "/csv_test_filter.csv"
    val csvFileFormat = "csv"
    val sql =
      s"""
         | select *
         | from $csvFileFormat.`$filePath`
         | where _c1 > 30
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(csvFileScan.size == 1)
      }
    )
  }

  test("read data from csv file format with agg") {
    val filePath = dataHome + "/csv_test_agg.csv"
    val csvFileFormat = "csv"
    val sql =
      s"""
         | select _c7, count(_c0), sum(_c1), avg(_c2), min(_c3), max(_c4), sum(_c5), sum(_c8)
         | from $csvFileFormat.`$filePath`
         | group by _c7
         |""".stripMargin
    testFileFormatBase(
      filePath,
      csvFileFormat,
      sql,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(csvFileScan.size == 1)
      }
    )
  }

  test("read normal csv") {
    val file_path = csvDataPath + "/normal_data.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("string_field", StringType, nullable = true),
        StructField.apply("int_field", IntegerType, nullable = true),
        StructField.apply("long_field", LongType, nullable = true),
        StructField.apply("float_field", FloatType, nullable = true),
        StructField.apply("double_field", DoubleType, nullable = true),
        StructField.apply("short_field", ShortType, nullable = true),
        StructField.apply("byte_field", ByteType, nullable = true),
        StructField.apply("boolean_field", BooleanType, nullable = true),
        StructField.apply("decimal_field", DecimalType.apply(10, 2), nullable = true),
        StructField.apply("date_field", DateType, nullable = true),
        StructField.apply("timestamp_field", TimestampType, nullable = true),
        StructField.apply("boolean_field2", BooleanType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")
    options.put("nullValue", "null")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(List(("spark.sql.legacy.timeParserPolicy", "LEGACY")) ++ vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .options(options)
        .schema(schema)
        .csv(file_path)
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("read excel csv with whitespace") {
    val file_path = csvDataPath + "/whitespace_data.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("int_field", IntegerType, nullable = true),
        StructField.apply("long_field", LongType, nullable = true),
        StructField.apply("float_field", FloatType, nullable = true),
        StructField.apply("double_field", DoubleType, nullable = true),
        StructField.apply("short_field", ShortType, nullable = true),
        StructField.apply("bool_field", BooleanType, nullable = true),
        StructField.apply("timestamp_field", TimestampType, nullable = true),
        StructField.apply("date_field", DateType, nullable = true),
        StructField.apply("string_field", StringType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()

    val tm1 = Timestamp.valueOf("2023-08-30 18:00:01")
    val dt1 = Date.valueOf("2023-08-30")
    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(1, 1.toLong, 1.toFloat, 1.toDouble, 1.toShort, true, tm1, dt1, null))
    dataCorrect.add(Row(2, 2.toLong, 2.toFloat, 2.toDouble, 2.toShort, false, tm1, dt1, null))

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("issues-2443 test1 for whitespace surrounding data") {
    val file_path = csvDataPath + "/whitespace_surrounding_data.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("int_field", IntegerType, nullable = true),
        StructField.apply("long_field", LongType, nullable = true),
        StructField.apply("short_field", ShortType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()

    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(1, 2.toLong, 3.toShort))
    dataCorrect.add(Row(1, 2.toLong, 3.toShort))
    dataCorrect.add(Row(1, 2.toLong, 4.toShort))

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("issues-2443 test2 for float to int data") {
    val file_path = csvDataPath + "/float_to_int_data.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("int_field", IntegerType, nullable = true),
        StructField.apply("long_field", LongType, nullable = true),
        StructField.apply("short_field", ShortType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", "|")
    options.put("quote", "\'")
    options.put("header", "false")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()

    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(1, 1.toLong, 10.toShort))
    dataCorrect.add(Row(1, null, 10.toShort))

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("issue-2670 test for special char surrounding int data") {
    val file_path = csvDataPath + "/special_char_surrounding_int_data.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("int_field", IntegerType, nullable = true),
        StructField.apply("short_field", ShortType, nullable = true),
        StructField.apply("long_field", LongType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("quote", "\"")
    options.put("header", "false")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()

    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(1, 2.toShort, 3.toLong))
    dataCorrect.add(Row(1, 2.toShort, 3.toLong))
    dataCorrect.add(Row(1, null, null))
    dataCorrect.add(Row(1, null, -100000.toLong))

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("issues-2677 test for ignoring special char around float value") {
    val file_path = csvDataPath + "/special_character_surrounding_float_data.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("float_field", FloatType, nullable = true),
        StructField.apply("double_field", DoubleType, nullable = true),
        StructField.apply("double_field2", DoubleType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")

    val df = spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()

    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(1.55.toFloat, 1.55.toDouble, -100.toDouble))
    dataCorrect.add(Row(1.55.toFloat, null, 100.toDouble))
    dataCorrect.add(Row(null, 1.55.toDouble, 98.88))

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("read excel export csv base") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
    }

    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", DateType, nullable = true),
        StructField.apply("c2", TimestampType, nullable = true),
        StructField.apply("c3", FloatType, nullable = true),
        StructField.apply("c4", DoubleType, nullable = true),
        StructField.apply("c5", IntegerType, nullable = true),
        StructField.apply("c6", LongType, nullable = true),
        StructField.apply("c7", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("nullValue", "null")
      .schema(schema)
      .csv(csvDataPath + "/excel_data_base.csv")
      .toDF()

    val result = df.collect()
    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }

    assert(csvFileScan.size == 1)
    assert(result.length == 21)
    assert(result.apply(0).getString(6) == null)
    assert(result.apply(0).getString(6) == null)
    assert(result.apply(16).getFloat(2) == -100000)
    assert(result.apply(16).getDouble(3) == -100000)
    assert(result.apply(16).getInt(4) == -100000)
    assert(result.apply(16).getLong(5) == -100000)
    assert(result.apply(18).getDate(0) == "2023-07-19".date)
    assert(result.apply(19).getDate(0) == "2023-07-01".date)
    assert(result.apply(20).getDate(0) == "2023-01-21".date)
  }

  test("read excel export csv delimiter") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", DateType, nullable = true),
        StructField.apply("b", TimestampType, nullable = true),
        StructField.apply("c", FloatType, nullable = true),
        StructField.apply("d", DoubleType, nullable = true),
        StructField.apply("e", IntegerType, nullable = true),
        StructField.apply("f", LongType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", "|")
      .schema(schema)
      .csv(csvDataPath + "/excel_data_delimiter.csv")
      .toDF()

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
    assert(df.collect().length == 12)
  }

  test("read csv with the sep option") {
    val schema = StructType(
      Seq(StructField("c1", StringType, nullable = true), StructField("c2", IntegerType)))
    // A plain separator and a tab written the escaped way Spark decodes, on both text readers.
    Seq(("|", "|"), ("\t", "\\t")).foreach {
      case (separator, sep) =>
        withTempDir {
          dir =>
            java.nio.file.Files.write(
              new java.io.File(dir, "part-0.csv").toPath,
              Seq("a", "b", "c").zipWithIndex
                .map { case (c, i) => s"$c$separator$i\n" }
                .mkString
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)
            )
            Seq("true", "false").foreach {
              excel =>
                withSQLConf(CHConfig.runtimeSettings("use_excel_serialization") -> excel) {
                  compareDfResultsAgainstVanillaSpark(
                    () => spark.read.option("sep", sep).schema(schema).csv(dir.getCanonicalPath),
                    compareResult = true,
                    df =>
                      assert(collect(df.queryExecution.executedPlan) {
                        case f: FileSourceScanExecTransformer => f
                      }.size == 1)
                  )
                }
            }
        }
    }
  }

  test("expected_end_of_line") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", IntegerType, nullable = true),
        StructField.apply("c2", StringType, nullable = true),
        StructField.apply("c3", StringType, nullable = true),
        StructField.apply("c4", StringType, nullable = true),
        StructField.apply("c5", StringType, nullable = true),
        StructField.apply("c6", StringType, nullable = true),
        StructField.apply("c7", StringType, nullable = true),
        StructField.apply("c8", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "false")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/expected_end_of_line.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("header", "false")
        .option("quote", "\"")
        .schema(schema)
        .csv(csvDataPath + "/expected_end_of_line.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)
  }

  test("csv pruning") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", StringType, nullable = true),
        StructField.apply("c2", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/double_quote.csv")
      .toDF()

    df.createTempView("pruning")

    compareResultsAgainstVanillaSpark(
      """
        |select
        |          c2
        |        from
        |          pruning
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("csv count(*)") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", StringType, nullable = true),
        StructField.apply("c2", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("quote", "\"")
      .schema(schema)
      .csv(csvDataPath + "/double_quote.csv")
      .toDF()

    df.createTempView("countallt")
    compareResultsAgainstVanillaSpark(
      """
        |select
        |          count(*)
        |        from
        |          countallt
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("csv \\r") {
    // scalastyle:off nonascii
    val csv_files = Seq("csv_r.csv", "中文.csv")
    // scalastyle:on nonascii
    csv_files.foreach(
      file => {
        val csv_path = csvDataPath + "/" + file
        val schema = StructType.apply(
          Seq(
            StructField.apply("c1", StringType, nullable = true)
          ))

        val df = spark.read
          .option("delimiter", ",")
          .option("header", "false")
          .schema(schema)
          .csv(csv_path)
          .toDF()

        var expectedAnswer: Seq[Row] = null
        withSQLConf(vanillaSparkConfs(): _*) {
          expectedAnswer = spark.read
            .option("delimiter", ",")
            .option("header", "false")
            .schema(schema)
            .csv(csv_path)
            .toDF()
            .collect()
        }
        checkAnswer(df, expectedAnswer)
      })
  }

  test("header size not equal csv first lines") {
    // In Csv file ,there is five field schema of header
    val schemaLessThanCsvHeader = StructType.apply(
      Seq(
        StructField.apply("c1", IntegerType, nullable = true),
        StructField.apply("c2", IntegerType, nullable = true),
        StructField.apply("c3", IntegerType, nullable = true),
        StructField.apply("c4", IntegerType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schemaLessThanCsvHeader)
      .csv(csvDataPath + "/header.csv")
      .toDF()

    df.createTempView("test_schema_header_less_than_csv_header")

    compareResultsAgainstVanillaSpark(
      """
        |select * from test_schema_header_less_than_csv_header
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )

    val schemaMoreThanCsvHeader = StructType.apply(
      Seq(
        StructField.apply("c1", IntegerType, nullable = true),
        StructField.apply("c2", IntegerType, nullable = true),
        StructField.apply("c3", IntegerType, nullable = true),
        StructField.apply("c4", IntegerType, nullable = true),
        StructField.apply("c5", IntegerType, nullable = true),
        StructField.apply("c6", IntegerType, nullable = true)
      ))

    val df2 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schemaMoreThanCsvHeader)
      .csv(csvDataPath + "/header.csv")
      .toDF()

    df2.createTempView("test_schema_header_More_than_csv_header")

    compareResultsAgainstVanillaSpark(
      """
        |select * from test_schema_header_More_than_csv_header
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("fix: read date field value wrong") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
      def timestamp: Timestamp = Timestamp.valueOf(s)
    }

    val csv_path = csvDataPath + "/field_value_wrong.csv"
    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")
    options.put("header", "false")

    val schema = StructType.apply(
      Seq(
        StructField.apply("a", DateType, nullable = true),
        StructField.apply("b", TimestampType, nullable = true)
      ))

    val data = new util.ArrayList[Row]()
    data.add(Row("2023-06-16".date, "2023-06-16 18:00:05".timestamp))

    spark
      .createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .format("csv")
      .options(options)
      .save(csv_path)

    spark.read
      .options(options)
      .schema(schema)
      .csv(csv_path)
      .toDF()
      .createTempView("field_read_wrong")

    compareResultsAgainstVanillaSpark(
      "select * from field_read_wrong",
      compareResult = true,
      _ => {})
  }

  test("cannot_parse_input") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", StringType, nullable = true),
        StructField.apply("c2", StringType, nullable = true),
        StructField.apply("c3", StringType, nullable = true),
        StructField.apply("c4", StringType, nullable = true),
        StructField.apply("c5", StringType, nullable = true),
        StructField.apply("c6", StringType, nullable = true),
        StructField.apply("c7", StringType, nullable = true),
        StructField.apply("c8", StringType, nullable = true),
        StructField.apply("c9", StringType, nullable = true),
        StructField.apply("c10", StringType, nullable = true),
        StructField.apply("c11", DoubleType, nullable = true),
        StructField.apply("c12", DoubleType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", "false")
      .schema(schema)
      .csv(csvDataPath + "/cannot_parse_input.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("header", "false")
        .schema(schema)
        .csv(csvDataPath + "/cannot_parse_input.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel quote") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val double_path = csvDataPath + "/double_quote.csv"
    val double_quote_option = new util.HashMap[String, String]()
    double_quote_option.put("delimiter", ",")
    double_quote_option.put("quote", "\"")

    val df1 = spark.read
      .options(double_quote_option)
      .schema(schema)
      .csv(double_path)
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .options(double_quote_option)
        .schema(schema)
        .csv(double_path)
        .toDF()
        .collect()
    }
    checkAnswer(df1, expectedAnswer)

    var csvFileScan = collect(df1.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)

    val single_path = csvDataPath + "/single_quote.csv"
    val single_quote_option = new util.HashMap[String, String]()
    single_quote_option.put("delimiter", ",")
    single_quote_option.put("quote", "\"")
    val df2 = spark.read
      .options(single_quote_option)
      .schema(schema)
      .csv(single_path)
      .toDF()

    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .options(single_quote_option)
        .schema(schema)
        .csv(single_path)
        .toDF()
        .collect()
    }
    checkAnswer(df2, expectedAnswer)

    csvFileScan = collect(df2.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)

    val no_quote_path = csvDataPath + "/no_quote.csv"
    val no_quote_option = new util.HashMap[String, String]()
    no_quote_option.put("delimiter", ",")
    no_quote_option.put("header", "false")
    no_quote_option.put("quote", "")

    val no_quote_schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val data = new util.ArrayList[Row]()
    data.add(Row("\'abc\'de\'", "\"abc\"de\""))

    spark
      .createDataFrame(data, schema)
      .write
      .mode("overwrite")
      .format("csv")
      .options(no_quote_option)
      .save(no_quote_path)

    spark.read
      .options(no_quote_option)
      .schema(no_quote_schema)
      .csv(no_quote_path)
      .toDF()
      .createTempView("no_quote_table")

    withSQLConf((CHConfig.runtimeSettings("use_excel_serialization.quote_strict"), "true")) {
      compareResultsAgainstVanillaSpark(
        "select * from no_quote_table",
        compareResult = true,
        _ => {})
    }
  }

  test("test read excel with header") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ";")
      .option("quote", "")
      .option("header", "true")
      .schema(schema)
      .csv(csvDataPath + "/with_header.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ";")
        .option("quote", "")
        .option("header", "true")
        .schema(schema)
        .csv(csvDataPath + "/with_header.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel with escape with quote") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("escape", "\\")
      .option("quote", "\'")
      .schema(schema)
      .csv(csvDataPath + "/escape_quote.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("quote", "\'")
        .option("escape", "\\")
        .schema(schema)
        .csv(csvDataPath + "/escape_quote.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("test read excel with escape without quote") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("a", StringType, nullable = true),
        StructField.apply("b", StringType, nullable = true)
      ))

    val df = spark.read
      .option("delimiter", ",")
      .option("escape", "\\")
      .schema(schema)
      .csv(csvDataPath + "/escape_without_quote.csv")
      .toDF()

    var expectedAnswer: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expectedAnswer = spark.read
        .option("delimiter", ",")
        .option("escape", "\\")
        .schema(schema)
        .csv(csvDataPath + "/escape_without_quote.csv")
        .toDF()
        .collect()
    }
    checkAnswer(df, expectedAnswer)

    val csvFileScan = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(csvFileScan.size == 1)
  }

  test("read data from csv file format with table") {
    val tableName = "csv_test"
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $tableName
         | group by string_field
         | order by string_field
         |""".stripMargin
    spark.createDataFrame(genTestData()).createOrReplaceTempView(tableName)
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      df => {
        val csvFileScan = collect(df.queryExecution.executedPlan) {
          case l: LocalTableScanExec => l
        }
        assert(csvFileScan.size == 1)
      })
  }

  test("knownfloatingpointnormalized") {
    val sql =
      s"""
         |select coalesce(t1.`i1`, 0) + coalesce(t2.`l1`, 0) `c1`,
         |       coalesce(t1.`d1`, t2.`d2`)                  sf
         |from (select double_field   d1,
         |             sum(int_field) i1
         |      from tt
         |      group by double_field) t1
         |         full join (select double_field    d2,
         |                           avg(long_field) l1
         |                    from tt
         |                    group by double_field) t2
         |                   on t1.d1 = t2.d2
         |""".stripMargin
    spark.createDataFrame(genTestData()).createOrReplaceTempView("tt")
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      _ => {}
    )
  }

  test("read data from orc file format") {
    val filePath = s"$orcDataPath/all_data_types_with_non_primitive_type.snappy.orc"
    val orcFileFormat = "orc"
    val sql =
      s"""
         | select *
         | from $orcFileFormat.`$filePath`
         | where long_field > 30
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, df => {})
  }

  // TODO: Fix: if the field names has upper case form, it will return null value
  ignore("read data from orc file format with upper case schema names") {
    val filePath = orcDataPath + "/case_insensitive_column_matching.orc"
    val orcFileFormat = "orc"
    val sql =
      s"""
         | select *
         | from $orcFileFormat.`$filePath`
         |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, df => {}, noFallBack = false)
  }

  test("ISSUE-2925 range partition with date32") {
    spark.createDataFrame(genTestData()).createOrReplaceTempView("t1")
    spark.createDataFrame(genTestData()).createTempView("t2")

    compareResultsAgainstVanillaSpark(
      """
        | select t1.date_field from t1 inner join t2 on t1.date_field = t2.date_field
        | group by t1.date_field
        | order by t1.date_field
        |
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  def testFileFormatBase(
      filePath: String,
      fileFormat: String,
      sql: String,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true
  ): Unit = {
    val data = genTestData()
    spark
      .createDataFrame(data)
      .write
      .mode("overwrite")
      .format(fileFormat)
      .option("quote", "\"")
      .save(filePath)
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      customCheck,
      noFallBack = noFallBack)
  }

  /** Generate test data for primitive type */
  def genTestData(): Seq[AllDataTypesWithNonPrimitiveType] = {
    (0 to 299).map {
      i =>
        if (i % 100 == 1) {
          // scalastyle:off nonascii
          AllDataTypesWithNonPrimitiveType(
            "测试中文",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null)
          // scalastyle:on nonascii
        } else if (i % 25 == 0) {
          if (i % 50 == 0) {
            AllDataTypesWithNonPrimitiveType(
              "",
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null)
          } else {
            AllDataTypesWithNonPrimitiveType(null, null, null, null, null, null, null, null, null,
              null)
          }
        } else {
          AllDataTypesWithNonPrimitiveType(
            s"$i",
            i,
            i.toLong,
            i.toFloat,
            i.toDouble,
            i.toShort,
            i.toByte,
            i % 2 == 0,
            new java.math.BigDecimal(i + ".56"),
            Date.valueOf(1950 + i / 3 + "-0" + (i % 3 + 1) + "-01"))
        }
    }
  }

  test("test_filter_not_null") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("int_field", IntegerType, nullable = true),
        StructField.apply("long_field", LongType, nullable = true),
        StructField.apply("bool_field", BooleanType, nullable = true)
      ))

    val data = new util.ArrayList[Row]()
    data.add(Row(1, 1.toLong, false))

    spark
      .createDataFrame(data, schema)
      .toDF()
      .createTempView("test_filter_not_null")

    compareResultsAgainstVanillaSpark(
      """
        | select
        |     sum(long_field) aa
        | from
        | (    select long_field,case when sum(int_field) > 0 then true else false end b
        |     from test_filter_not_null group by long_field) t where b
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("empty parquet") {
    val df = spark.read.parquet(createEmptyParquet()).toDF().select($"a")
    assert(df.collect().isEmpty)
  }

  test("issue-2881 null string test") {
    withSQLConf((CHConfig.runtimeSettings("use_excel_serialization.empty_as_null"), "true")) {
      val file_path = csvDataPath + "/null_string.csv"
      val schema = StructType.apply(
        Seq(
          StructField.apply("c1", StringType, nullable = true),
          StructField.apply("c2", ShortType, nullable = true),
          StructField.apply("c3", StringType, nullable = true)
        ))

      val options = new util.HashMap[String, String]()
      options.put("delimiter", ",")

      val df = spark.read
        .options(options)
        .schema(schema)
        .csv(file_path)
        .toDF()

      val dataCorrect = new util.ArrayList[Row]()
      dataCorrect.add(Row(null, 1.toShort, null))
      dataCorrect.add(Row(null, 2.toShort, "2"))
      dataCorrect.add(Row("1", null, null))
      dataCorrect.add(Row(null, null, null))

      var expectedAnswer: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
      }
      checkAnswer(df, expectedAnswer)
    }
  }

  test("issue-3542 null string test") {
    withSQLConf((CHConfig.runtimeSettings("use_excel_serialization.empty_as_null"), "false")) {
      val file_path = csvDataPath + "/null_string.csv"
      val schema = StructType.apply(
        Seq(
          StructField.apply("c1", StringType, nullable = true),
          StructField.apply("c2", ShortType, nullable = true),
          StructField.apply("c3", StringType, nullable = true)
        ))

      val options = new util.HashMap[String, String]()
      options.put("delimiter", ",")

      val df = spark.read
        .options(options)
        .schema(schema)
        .csv(file_path)
        .toDF()

      val dataCorrect = new util.ArrayList[Row]()
      dataCorrect.add(Row(null, 1.toShort, ""))
      dataCorrect.add(Row("", 2.toShort, "2"))
      dataCorrect.add(Row("1", null, null))
      dataCorrect.add(Row("", null, ""))

      var expectedAnswer: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
      }
      checkAnswer(df, expectedAnswer)
    }
  }

  test("test integer read with sign at the end of line") {
    val file_path = csvDataPath + "/sign_at_end_int.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", IntegerType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")

    spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()
      .createTempView("test_null_int")

    compareResultsAgainstVanillaSpark(
      """
        | select * from test_null_int
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("test float read with sign at the end of line") {
    val file_path = csvDataPath + "/sign_at_end_float.csv"
    val schema = StructType.apply(
      Seq(
        StructField.apply("c1", FloatType, nullable = true)
      ))

    val options = new util.HashMap[String, String]()
    options.put("delimiter", ",")

    spark.read
      .options(options)
      .schema(schema)
      .csv(file_path)
      .toDF()
      .createTempView("test_null_float")

    compareResultsAgainstVanillaSpark(
      """
        | select * from test_null_float
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }
  test("GLUTEN-7367: Memory limit exceeded") {
    val file_TEST_MEASURE = csvDataPath + "/default/TEST_MEASURE.csv"
    val TEST_MEASURE = StructType.apply(
      Seq(
        StructField.apply("ID1", LongType, nullable = false),
        StructField.apply("ID2", LongType, nullable = false),
        StructField.apply("ID3", LongType, nullable = false),
        StructField.apply("ID4", IntegerType, nullable = false),
        StructField.apply("PRICE1", FloatType, nullable = false),
        StructField.apply("PRICE2", DoubleType, nullable = false),
        StructField.apply("PRICE3", DecimalType(19, 4), nullable = false),
        StructField.apply("PRICE5", ShortType, nullable = false),
        StructField.apply("PRICE6", ByteType, nullable = false),
        StructField.apply("PRICE7", ShortType, nullable = false),
        StructField.apply("NAME1", StringType, nullable = true),
        StructField.apply("NAME2", StringType, nullable = true),
        StructField.apply("NAME3", StringType, nullable = true),
        StructField.apply("NAME4", ByteType, nullable = false),
        StructField.apply("TIME1", DateType, nullable = false),
        StructField.apply("TIME2", TimestampType, nullable = false),
        StructField.apply("FLAG", BooleanType, nullable = false)
      ))
    spark.read
      .schema(TEST_MEASURE)
      .csv(file_TEST_MEASURE)
      .toDF()
      .createTempView("TEST_MEASURE")
    val file_TEST_MEASURE1 = csvDataPath + "/default/TEST_MEASURE1.csv"
    val TEST_MEASURE1 = StructType.apply(
      Seq(
        StructField.apply("ID1", LongType, nullable = false),
        StructField.apply("ID2", LongType, nullable = false),
        StructField.apply("ID3", LongType, nullable = false),
        StructField.apply("ID4", IntegerType, nullable = false),
        StructField.apply("PRICE1", FloatType, nullable = false),
        StructField.apply("PRICE2", DoubleType, nullable = false),
        StructField.apply("PRICE3", DecimalType(19, 4), nullable = false),
        StructField.apply("PRICE5", ShortType, nullable = false),
        StructField.apply("PRICE6", ByteType, nullable = false),
        StructField.apply("PRICE7", ShortType, nullable = false),
        StructField.apply("NAME1", StringType, nullable = false),
        StructField.apply("NAME2", StringType, nullable = false),
        StructField.apply("NAME3", StringType, nullable = false),
        StructField.apply("NAME4", ByteType, nullable = false),
        StructField.apply("TIME1", DateType, nullable = false),
        StructField.apply("TIME2", TimestampType, nullable = false),
        StructField.apply("FLAG", BooleanType, nullable = false)
      ))
    spark.read
      .schema(TEST_MEASURE1)
      .csv(file_TEST_MEASURE1)
      .toDF()
      .createTempView("TEST_MEASURE1")

    withSQLConf(
      (CHConfig.runtimeSettings("use_excel_serialization"), "false"),
      (GlutenConfig.TEXT_INPUT_EMPTY_AS_DEFAULT.key, "true")) {
      compareResultsAgainstVanillaSpark(
        """
          | select * from TEST_MEASURE
          |""".stripMargin,
        compareResult = true,
        _ => {}
      )

      compareResultsAgainstVanillaSpark(
        """
          | select * from TEST_MEASURE1
          |""".stripMargin,
        compareResult = true,
        _ => {}
      )

      val sqlStr =
        """select `TEST_MEASURE`.`ID1`,
          | count(distinct `TEST_MEASURE`.`ID1`, `TEST_MEASURE`.`ID2`, `TEST_MEASURE`.`ID3`,
          |  `TEST_MEASURE`.`ID4`,`TEST_MEASURE`.`PRICE1`, `TEST_MEASURE`.`PRICE2`,
          |  `TEST_MEASURE`.`PRICE3`, `TEST_MEASURE`.`PRICE5`,`TEST_MEASURE`.`PRICE6`,
          |  `TEST_MEASURE`.`PRICE7`, `TEST_MEASURE`.`NAME1`, `TEST_MEASURE`.`NAME2`,
          |  `TEST_MEASURE`.`NAME3`, `TEST_MEASURE`.`NAME4`, `TEST_MEASURE`.`TIME1`,
          |  `TEST_MEASURE`.`TIME2`,`TEST_MEASURE`.`FLAG`),
          | 1
          |from `TEST_MEASURE`
          |         left join `TEST_MEASURE1` on `TEST_MEASURE`.`ID1` = `TEST_MEASURE1`.`ID1`
          |group by `TEST_MEASURE`.`ID1`""".stripMargin

      compareResultsAgainstVanillaSpark(sqlStr, compareResult = true, _ => {})
    }
  }

  test("issues-3609 int read test") {
    withSQLConf((CHConfig.runtimeSettings("use_excel_serialization.number_force"), "false")) {
      val csv_path = csvDataPath + "/int_special.csv"
      val options = new util.HashMap[String, String]()
      options.put("delimiter", ",")
      options.put("header", "false")
      val schema = StructType.apply(
        Seq(
          StructField.apply("a", IntegerType, nullable = true),
          StructField.apply("b", IntegerType, nullable = true),
          StructField.apply("c", IntegerType, nullable = true),
          StructField.apply("d", IntegerType, nullable = true)
        ))

      val df = spark.read
        .options(options)
        .schema(schema)
        .csv(csv_path)
        .toDF()

      val dataCorrect = new util.ArrayList[Row]()
      dataCorrect.add(Row(null, null, null, 15))

      var expectedAnswer: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
      }
      checkAnswer(df, expectedAnswer)
    }

    withSQLConf((CHConfig.runtimeSettings("use_excel_serialization.number_force"), "true")) {
      val csv_path = csvDataPath + "/int_special.csv"
      val options = new util.HashMap[String, String]()
      options.put("delimiter", ",")
      options.put("header", "false")
      val schema = StructType.apply(
        Seq(
          StructField.apply("a", IntegerType, nullable = true),
          StructField.apply("b", IntegerType, nullable = true),
          StructField.apply("c", IntegerType, nullable = true),
          StructField.apply("d", IntegerType, nullable = true)
        ))

      val df = spark.read
        .options(options)
        .schema(schema)
        .csv(csv_path)
        .toDF()

      val dataCorrect = new util.ArrayList[Row]()
      dataCorrect.add(Row(15, -1, 85, 15))

      var expectedAnswer: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        expectedAnswer = spark.createDataFrame(dataCorrect, schema).toDF().collect()
      }
      checkAnswer(df, expectedAnswer)
    }
  }

  def createEmptyParquet(): String = {
    val data = spark.sparkContext.emptyRDD[Row]
    val schema = new StructType()
      .add("a", StringType)

    val fileName = dataHome + "/parquet_test_" + System.currentTimeMillis() + "_empty.parquet"

    spark.createDataFrame(data, schema).toDF().write.parquet(fileName)
    fileName
  }

  test("write into hdfs") {

    /**
     * There is a bug in pipeline write to HDFS; when a pipeline returns column batch, it doesn't
     * close the hdfs file, and hence the file is not flushed.HDFS file is closed when LocalExecutor
     * is destroyed, but before that, the file moved by spark committer.
     */

    val tablePath = hdfsHelper.independentHdfsURL("write_into_hdfs")
    val format = "parquet"
    val sql =
      s"""
         | select *
         | from $format.`$tablePath`
         | where long_field > 30
         |""".stripMargin
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {
      testFileFormatBase(tablePath, format, sql, df => {})
    }
  }

  // TODO: pass spark configuration to FileFormatWriter in Spark 3.3 and 3.2
  testWithMinSparkVersion("write succeed even if set wrong snappy compression codec level", "3.5") {
    // TODO: remove duplicated test codes
    val tablePath = hdfsHelper.independentHdfsURL("failed_test")
    val format = "parquet"
    val sql =
      s"""
         | select *
         | from $format.`$tablePath`
         | where long_field > 30
         |""".stripMargin

    withSQLConf(
      (GlutenConfig.NATIVE_WRITER_ENABLED.key, "true"),
      (
        RuntimeSettings.OUTPUT_FORMAT_COMPRESSION_LEVEL.key,
        RuntimeSettings.OUTPUT_FORMAT_COMPRESSION_LEVEL.defaultValueString)
    ) {
      testFileFormatBase(tablePath, format, sql, df => {})
    }

    // we can't pass the configuration to FileFormatWriter in Spark 3.3 and 3.2
    withSQLConf(
      (GlutenConfig.NATIVE_WRITER_ENABLED.key, "true"),
      (RuntimeSettings.OUTPUT_FORMAT_COMPRESSION_LEVEL.key, "3")
    ) {
      testFileFormatBase(tablePath, format, sql, df => {})
    }
  }
}
