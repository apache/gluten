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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.datasource.ArrowCSVFileFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.{ArrowFileSourceScanExec, BaseArrowScanExec, ColumnarToRowExec}
import org.apache.spark.sql.execution.columnar.SparkCacheUtil
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.scalatest.Ignore

@Ignore
class ArrowCsvScanSuiteV1 extends ArrowCsvScanSuiteBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "csv")
  }

  test("csv scan v1") {
    val df = runAndCompare("select * from student")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(s => s.isInstanceOf[ColumnarToRowExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BaseArrowScanExec]).isDefined)
    val scan = plan.find(_.isInstanceOf[BaseArrowScanExec]).toList.head
    assert(
      scan
        .asInstanceOf[ArrowFileSourceScanExec]
        .relation
        .fileFormat
        .isInstanceOf[ArrowCSVFileFormat])
  }

  test("csv scan with schema v1") {
    val df = runAndCompare("select * from student_option_schema")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(s => s.isInstanceOf[ColumnarToRowExec]).isDefined)
    val scan = plan.find(_.isInstanceOf[BaseArrowScanExec])
    assert(scan.isDefined)
    assert(
      !scan.get
        .asInstanceOf[ArrowFileSourceScanExec]
        .original
        .relation
        .fileFormat
        .asInstanceOf[ArrowCSVFileFormat]
        .fallback)
  }
}

@Ignore
class ArrowCsvScanSuiteV2 extends ArrowCsvScanSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
  }

  test("csv scan") {
    runAndCompare("select * from student")
  }
}

@Ignore
class ArrowCsvScanWithTableCacheSuite extends ArrowCsvScanSuiteBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "csv")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // A common practice as well as in Spark tests, to clear the cache serializer
    // in case it was already set as the default row-based serializer.
    SparkCacheUtil.clearCacheSerializer()
  }

  override def afterAll(): Unit = {
    SparkCacheUtil.clearCacheSerializer()
    super.afterAll()
  }

  /**
   * Test for GLUTEN-8453: https://github.com/apache/gluten/issues/8453. To make sure no error is
   * thrown when caching an Arrow Java query plan.
   */
  test("csv scan v1 with table cache") {
    val df = spark.sql("select * from student")
    df.cache()
    assert(df.collect().length == 3)
  }
}

/** Since https://github.com/apache/gluten/pull/5850. */
@Ignore
abstract class ArrowCsvScanSuite extends ArrowCsvScanSuiteBase {

  test("csv scan with option string as null") {
    val df = runAndCompare("select * from student_option_str")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarToRowExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BaseArrowScanExec]).isDefined)
  }

  test("csv scan with option delimiter") {
    val df = runAndCompare("select * from student_option")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(s => s.isInstanceOf[ColumnarToRowExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BaseArrowScanExec]).isDefined)
  }

  test("csv scan with missing columns") {
    val df =
      runAndCompare("select languagemissing, language, id_new_col from student_option_schema_lm")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(s => s.isInstanceOf[VeloxColumnarToRowExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BaseArrowScanExec]).isDefined)
  }

  test("csv scan with different name") {
    val df = runAndCompare("select * from student_option_schema")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(s => s.isInstanceOf[ColumnarToRowExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BaseArrowScanExec]).isDefined)

    val df2 = runAndCompare("select * from student_option_schema")
    val plan2 = df2.queryExecution.executedPlan
    assert(plan2.find(s => s.isInstanceOf[ColumnarToRowExec]).isDefined)
    assert(plan2.find(_.isInstanceOf[BaseArrowScanExec]).isDefined)
  }

  test("csv scan with filter") {
    val df = runAndCompare("select * from student where Name = 'Peter'")
    assert(df.queryExecution.executedPlan.find(s => s.isInstanceOf[ColumnarToRowExec]).isEmpty)
    assert(
      df.queryExecution.executedPlan
        .find(s => s.isInstanceOf[BaseArrowScanExec])
        .isDefined)
  }

  test("insert into select from csv") {
    withTable("insert_csv_t") {
      spark.sql("create table insert_csv_t(Name string, Language string) using parquet;")
      runQueryAndCompare("""
                           |insert into insert_csv_t select * from student;
                           |""".stripMargin) {
        checkGlutenPlan[BaseArrowScanExec]
      }
    }
  }
}

abstract class ArrowCsvScanSuiteBase extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  protected val rootPath: String = getClass.getResource("/").getPath

  override def beforeAll(): Unit = {
    super.beforeAll()
    createCsvTables()
  }

  override def afterAll(): Unit = {
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
      .set(GlutenConfig.NATIVE_ARROW_READER_ENABLED.key, "true")
  }

  private def createCsvTables(): Unit = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(rootPath + "/datasource/csv/student.csv")
      .createOrReplaceTempView("student")

    spark.read
      .format("csv")
      .option("header", "true")
      .load(rootPath + "/datasource/csv/student_option_str.csv")
      .createOrReplaceTempView("student_option_str")

    spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(rootPath + "/datasource/csv/student_option.csv")
      .createOrReplaceTempView("student_option")

    spark.read
      .schema(
        new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("language", StringType))
      .format("csv")
      .option("header", "true")
      .load(rootPath + "/datasource/csv/student_option_schema.csv")
      .createOrReplaceTempView("student_option_schema")

    spark.read
      .schema(
        new StructType()
          .add("id_new_col", IntegerType)
          .add("name", StringType)
          .add("language", StringType)
          .add("languagemissing", StringType))
      .format("csv")
      .option("header", "true")
      .load(rootPath + "/datasource/csv/student_option_schema.csv")
      .createOrReplaceTempView("student_option_schema_lm")
  }
}
