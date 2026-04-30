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
package org.apache.spark.sql

import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.Args
import org.scalatest.Status

import java.io.File
import java.io.PrintWriter

import scala.collection.mutable
import scala.reflect.ClassTag

trait GlutenExpressionOffloadTracker extends GlutenTestsTrait {

  protected def offloadCategory: String = "unknown"

  protected def panoramaMeta(expression: Expression): Map[String, String] =
    Map("expr" -> expression.getClass.getSimpleName)

  private case class OffloadRecord(
      method: String,
      expression: String,
      meta: Map[String, String],
      offload: String,
      failCause: String,
      failStackTrace: String)

  private case class TestOffloadResult(
      testName: String,
      records: Seq[OffloadRecord],
      status: String)

  private val currentTestRecords = mutable.ArrayBuffer[OffloadRecord]()
  private val allTestResults = mutable.ArrayBuffer[TestOffloadResult]()

  private def withOffloadLog[T](method: String, expression: Expression, resultDF: DataFrame)(
      body: => T): T = {
    val meta = panoramaMeta(expression)
    var failCause: String = null
    var failStackTrace: String = null
    try {
      body
    } catch {
      case e: Exception =>
        failCause = e.getMessage
        failStackTrace = e.getStackTrace.map(_.toString).mkString("\n")
        throw e
    } finally {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: ProjectExecTransformer => p
      }
      val offload = if (projectTransformer.size == 1) "OFFLOAD" else "FALLBACK"
      currentTestRecords += OffloadRecord(
        method,
        expression.toString,
        meta,
        offload,
        failCause,
        failStackTrace)
    }
  }

  override def runTest(testName: String, args: Args): Status = if (ansiTest) {
    currentTestRecords.clear()
    val status = super.runTest(testName, args)
    val result = if (status.succeeds()) "PASS" else "FAIL"
    allTestResults += TestOffloadResult(testName, currentTestRecords.toSeq, result)
    status
  } else {
    super.runTest(testName, args)
  }

  override protected def doCheckExpression(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow,
      resultDF: DataFrame): Unit = if (ansiTest) {
    withOffloadLog("checkExpression", expression, resultDF) {
      super.doCheckExpression(expression, expected, inputRow, resultDF)
    }
  } else {
    super.doCheckExpression(expression, expected, inputRow, resultDF)
  }

  override protected def doCheckExceptionInExpression[T <: Throwable: ClassTag](
      expression: Expression,
      inputRow: InternalRow,
      expectedErrMsg: String,
      resultDF: DataFrame): Unit = if (ansiTest) {
    withOffloadLog("checkException", expression, resultDF) {
      super.doCheckExceptionInExpression[T](expression, inputRow, expectedErrMsg, resultDF)
    }
  } else {
    super.doCheckExceptionInExpression[T](expression, inputRow, expectedErrMsg, resultDF)
  }

  override def afterAll(): Unit = if (ansiTest) {
    writeJsonOutput()
    super.afterAll()
  } else {
    super.afterAll()
  }

  private def writeJsonOutput(): Unit = {
    val suiteName = this.getClass.getSimpleName
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val testsJson = allTestResults.map {
      t =>
        val recordsJson = t.records.zipWithIndex.map {
          case (r, idx) =>
            val methodTag = if (r.method == "checkException") "E" else "N"
            val status = if (idx == t.records.size - 1) t.status else "PASS"
            val record = mutable.LinkedHashMap[String, Any](
              "method" -> methodTag,
              "expression" -> r.expression,
              "meta" -> r.meta,
              "offload" -> r.offload,
              "status" -> status
            )
            if (r.failCause != null) {
              record("failCause") = r.failCause
              record("failStackTrace") = r.failStackTrace
            }
            record
        }
        mutable.LinkedHashMap[String, Any](
          "name" -> t.testName,
          "status" -> t.status,
          "records" -> recordsJson
        )
    }

    val output = mutable.LinkedHashMap[String, Any](
      "suite" -> suiteName,
      "category" -> offloadCategory,
      "tests" -> testsJson
    )

    val dir = new File("target/ansi-offload")
    dir.mkdirs()
    val file = new File(dir, s"$suiteName.json")
    val writer = new PrintWriter(file)
    try {
      writer.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(output))
    } finally {
      writer.close()
    }
    logWarning(s"ANSI offload data written to ${file.getAbsolutePath}")
  }
}
