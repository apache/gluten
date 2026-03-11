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
package org.apache.gluten.integration

import org.apache.gluten.integration.metrics.MetricMapper

import org.apache.spark.sql.{RunResult, SparkQueryRunner, SparkSession}

import com.google.common.base.Preconditions
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File
import java.net.URI

class QueryRunner(val source: String, val dataPath: String) {
  import QueryRunner._

  def createTables(creator: TableCreator, analyzer: TableAnalyzer, spark: SparkSession): Unit = {
    creator.create(spark, source, dataPath)
    analyzer.analyze(spark)
  }

  def runQuery(
      spark: SparkSession,
      desc: String,
      query: Query,
      explain: Boolean = false,
      sqlMetricMapper: MetricMapper = MetricMapper.dummy,
      executorMetrics: Seq[String] = Nil,
      randomKillTasks: Boolean = false): QueryResult = {
    try {
      val path = new Path(dataPath)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      Preconditions.checkState(
        fs.exists(path),
        s"Data not found at $dataPath, try using command `<gluten-it> data-gen-only <options>` to generate it first.",
        Array(): _*)

      val r =
        SparkQueryRunner.runQuery(
          spark,
          desc,
          query,
          explain,
          sqlMetricMapper,
          executorMetrics,
          randomKillTasks)
      println(s"Successfully ran query ${query.id}. Returned row count: ${r.rows.length}")
      Success(query.id, r)
    } catch {
      case e: Exception =>
        println(s"Error running query ${query.id}. Error: ${ExceptionUtils.getStackTrace(e)}")
        Failure(query.id, e)
    }
  }

}

object QueryRunner {
  sealed trait QueryResult {
    def caseId(): String
    def succeeded(): Boolean
  }

  implicit class QueryResultOps(r: QueryResult) {
    def asSuccessOption(): Option[Success] = {
      r match {
        case s: Success => Some(s)
        case _: Failure => None
      }
    }

    def asFailureOption(): Option[Failure] = {
      r match {
        case _: Success => None
        case f: Failure => Some(f)
      }
    }

    def asSuccess(): Success = {
      asSuccessOption().get
    }

    def asFailure(): Failure = {
      asFailureOption().get
    }
  }

  case class Success(override val caseId: String, runResult: RunResult) extends QueryResult {
    override def succeeded(): Boolean = true
  }
  case class Failure(override val caseId: String, error: Exception) extends QueryResult {
    override def succeeded(): Boolean = false
  }
}
