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
package org.apache.spark.sql.delta

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.files.GlutenDeltaFileFormatWriter
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{GlutenDeltaLeafRunnableCommand, GlutenDeltaLeafV2CommandExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters._

class DeltaNativeWriteInvariantSuite extends DeltaSQLCommandTest {

  import testImplicits._

  private lazy val isMac = sys.props
    .get("os.name")
    .exists(_.toLowerCase(java.util.Locale.ROOT).contains("mac"))

  private def withNativeWriteOffloadConf[T](f: => T): T = {
    val confs = Seq(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false"
    ) ++
      (if (isMac) {
         Seq(GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false")
       } else {
         Seq.empty
       })

    var result: Option[T] = None
    withSQLConf(confs: _*) {
      result = Some(f)
    }
    result.get
  }

  private def hasGlutenDeltaWriteCommand(plan: SparkPlan): Boolean = {
    val nativeClassMatch = plan
      .collectFirst {
        case ExecutedCommandExec(_: GlutenDeltaLeafRunnableCommand) => true
        case _: GlutenDeltaLeafV2CommandExec => true
      }
      .getOrElse(false)

    val nativeNodeMatch = plan
      .collectFirst {
        case p if p.nodeName.startsWith("Execute GlutenDelta ") => true
        case p if p.nodeName.startsWith("GlutenDelta ") => true
      }
      .getOrElse(false)

    val nativeTreeMatch = plan.treeString.contains("GlutenDelta ")

    nativeClassMatch || nativeNodeMatch || nativeTreeMatch
  }

  private def collectExecutedPlans(action: => Unit): Seq[SparkPlan] = {
    val plans = new CopyOnWriteArrayList[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plans.add(qe.executedPlan)
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)
    try {
      action
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.asScala.toSeq
  }

  private def assertContainsNativeWriteCommand(plans: Seq[SparkPlan], context: String): Unit = {
    assert(
      plans.exists(hasGlutenDeltaWriteCommand),
      s"Expected native delta write command for $context, but got plans:\n" +
        plans.map(_.treeString).mkString("\n---\n")
    )
  }

  test("native delta write checks top-level NOT NULL without DeltaInvariantCheckerExec") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_not_null") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta_native_write_not_null " +
                s"(id BIGINT NOT NULL, value STRING) USING delta LOCATION '$path'")

            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            val plans = collectExecutedPlans {
              Seq((1L, "a"), (2L, "b"))
                .toDF("id", "value")
                .write
                .format("delta")
                .mode("append")
                .save(path)
            }

            assertContainsNativeWriteCommand(plans, "top-level NOT NULL append")
            val writerPlan = GlutenDeltaFileFormatWriter.getExecutedPlanForTesting
              .getOrElse(fail("Expected GlutenDeltaFileFormatWriter to record the executed plan"))
            val writerPlanString = writerPlan.treeString
            assert(
              !writerPlanString.contains("DeltaInvariantChecker"),
              s"Expected native invariant checker to avoid DeltaInvariantCheckerExec, but got:\n" +
                writerPlanString
            )
            assert(
              !writerPlanString.contains("ColumnarToRow"),
              s"Expected native invariant checker to avoid C2R transitions, but got:\n" +
                writerPlanString
            )

            val result = spark.read.format("delta").load(path)
            assert(result.collect().toSet == Set(Row(1L, "a"), Row(2L, "b")))
        }
      }
    }
  }

  test("native delta write keeps DeltaInvariantCheckerExec for CHECK constraints") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_check_constraint") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta_native_write_check_constraint " +
                s"(id BIGINT, value STRING) USING delta LOCATION '$path' " +
                s"TBLPROPERTIES ('delta.constraints.id_positive' = 'id > 0')")

            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            Seq((1L, "a"), (2L, "b"))
              .toDF("id", "value")
              .write
              .format("delta")
              .mode("append")
              .save(path)

            val writerPlan = GlutenDeltaFileFormatWriter.getExecutedPlanForTesting
              .getOrElse(fail("Expected GlutenDeltaFileFormatWriter to record the executed plan"))
            assert(
              writerPlan.treeString.contains("DeltaInvariantChecker"),
              s"Expected DeltaInvariantCheckerExec for unsupported CHECK constraint, but got:\n" +
                writerPlan.treeString
            )
        }
      }
    }
  }

  test("native delta write reports top-level NOT NULL violations") {
    withNativeWriteOffloadConf {
      withTable("delta_native_write_not_null_violation") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            sql(
              s"CREATE TABLE delta_native_write_not_null_violation " +
                s"(id BIGINT NOT NULL, value STRING) USING delta LOCATION '$path'")

            val schema = StructType(
              Seq(
                StructField("id", LongType, nullable = true),
                StructField("value", StringType, nullable = true)))
            val invalidData = spark.createDataFrame(
              spark.sparkContext.parallelize(Seq(Row(null, "bad"))),
              schema)

            GlutenDeltaFileFormatWriter.clearExecutedPlanForTesting()
            val exception = intercept[InvariantViolationException] {
              invalidData.write.format("delta").mode("append").save(path)
            }
            assert(exception.getMessage.toLowerCase(java.util.Locale.ROOT).contains("not null"))
            assert(exception.getMessage.contains("id"))

            val writerPlan = GlutenDeltaFileFormatWriter.getExecutedPlanForTesting
              .getOrElse(fail("Expected GlutenDeltaFileFormatWriter to record the executed plan"))
            assert(
              !writerPlan.treeString.contains("DeltaInvariantChecker"),
              s"Expected native invariant checker to avoid DeltaInvariantCheckerExec, but got:\n" +
                writerPlan.treeString
            )
        }
      }
    }
  }
}
