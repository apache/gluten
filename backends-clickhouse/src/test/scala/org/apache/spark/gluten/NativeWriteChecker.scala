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
package org.apache.spark.gluten

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{CHColumnarToCarrierRowExec, GlutenClickHouseWholeStageTransformerSuite}

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, QueryExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.util.QueryExecutionListener

import java.io.File

import scala.reflect.runtime.universe.TypeTag

trait NativeWriteChecker
  extends GlutenClickHouseWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {

  private val formats: Seq[String] = Seq("orc", "parquet")

  // test non-ascii path, by the way
  // scalastyle:off nonascii
  protected def getWarehouseDir: String = dataHome + "/中文/spark-warehouse"
  // scalastyle:on nonascii

  def withNativeWriteCheck(checkNative: Boolean)(block: => Unit): Unit = {
    var nativeUsed = false

    val queryListener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {
        fail("query failed", e)
      }
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        if (!nativeUsed) {
          val executedPlan = stripAQEPlan(qe.executedPlan)
          nativeUsed = if (isSparkVersionGE("3.5")) {
            executedPlan.find(_.isInstanceOf[ColumnarWriteFilesExec]).isDefined
          } else {
            executedPlan.find(_.isInstanceOf[CHColumnarToCarrierRowExec]).isDefined
          }
        }
      }
    }
    try {
      spark.listenerManager.register(queryListener)
      block
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assertResult(checkNative)(nativeUsed)
    } finally {
      spark.listenerManager.unregister(queryListener)
    }
  }
  def checkInsertQuery(sqlStr: String, checkNative: Boolean): Unit =
    withNativeWriteCheck(checkNative) {
      spark.sql(sqlStr)
    }

  def withDestinationTable(table: String, createTableSql: Option[String] = None)(
      f: => Unit): Unit = {
    spark.sql(s"drop table IF EXISTS $table")
    createTableSql.foreach(spark.sql)
    f
  }

  def nativeWrite(f: String => Unit): Unit = {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {
      formats.foreach(f(_))
    }
  }

  def vanillaWrite(block: => Unit): Unit = {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")) {
      block
    }
  }

  def nativeWrite2(
      f: String => (String, String, String),
      extraCheck: (String, String) => Unit = null,
      checkNative: Boolean = true): Unit = nativeWrite {
    format =>
      val (table_name, table_create_sql, insert_sql) = f(format)
      withTable(table_name) {
        withDestinationTable(table_name, Option(table_create_sql)) {
          checkInsertQuery(insert_sql, checkNative)
          Option(extraCheck).foreach(_(table_name, format))
        }
      }
  }

  def withSource[A <: Product: TypeTag](data: Seq[A], viewName: String, pairs: (String, String)*)(
      block: => Unit): Unit =
    withSource(spark.createDataFrame(data), viewName, pairs: _*)(block)

  def withSource(df: Dataset[Row], viewName: String, pairs: (String, String)*)(
      block: => Unit): Unit = {
    withSQLConf(pairs: _*) {
      withTempView(viewName) {
        df.createOrReplaceTempView(viewName)
        block
      }
    }
  }

  def getColumnName(col: String): String = {
    col.replaceAll("\\(", "_").replaceAll("\\)", "_")
  }

  def compareSource(originTable: String, table: String, fields: Seq[String]): Unit = {
    def query(table: String, selectFields: Seq[String]): String = {
      s"select ${selectFields.mkString(",")} from $table"
    }
    val expectedRows = spark.sql(query(originTable, fields)).collect()
    val actual = spark.sql(query(table, fields.map(getColumnName)))
    checkAnswer(actual, expectedRows)
  }

  def writeAndCheckRead(
      original_table: String,
      table_name: String,
      fields: Seq[String],
      checkNative: Boolean = true)(write: Seq[String] => Unit): Unit = {
    withDestinationTable(table_name) {
      withNativeWriteCheck(checkNative) {
        write(fields)
      }
      compareSource(original_table, table_name, fields)
    }
  }

  def compareWriteFilesSignature(
      format: String,
      table: String,
      vanillaTable: String,
      sigExpr: String): Unit = {
    val tableFiles = recursiveListFiles(new File(getWarehouseDir + "/" + table))
      .filter(_.getName.endsWith(s".$format"))
    val sigsOfNativeWriter = getSignature(format, tableFiles, sigExpr).sorted
    val vanillaTableFiles = recursiveListFiles(new File(getWarehouseDir + "/" + vanillaTable))
      .filter(_.getName.endsWith(s".$format"))
    val sigsOfVanillaWriter = getSignature(format, vanillaTableFiles, sigExpr).sorted
    assertResult(sigsOfVanillaWriter)(sigsOfNativeWriter)
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getSignature(
      format: String,
      writeFiles: Array[File],
      sigExpr: String): Array[(Long, Long)] = {
    writeFiles.map(
      f => {
        val df = if (format.equals("parquet")) {
          spark.read.parquet(f.getAbsolutePath)
        } else {
          spark.read.orc(f.getAbsolutePath)
        }
        (df.count(), df.selectExpr(sigExpr).collect().apply(0).apply(0).asInstanceOf[Long])
      })
  }
}
