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
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial, PartialMerge, StddevSamp}
import org.apache.spark.sql.execution.ColumnarCollapseTransformStages
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import com.google.protobuf.Message
import io.substrait.proto.{Expression => SubstraitExpression}
import io.substrait.proto.Expression.Cast.FailureBehavior

import scala.collection.JavaConverters._

class GlutenAggregateCastModeSuite extends GlutenQueryComparisonTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "2")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "2")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.MERGE_TWO_PHASES_ENABLED.key, "false")
      .set(GlutenConfig.COLUMNAR_CUDF_ENABLED.key, "false")
  }

  private def collectCasts(message: Message): Seq[SubstraitExpression.Cast] = {
    val current = message match {
      case cast: SubstraitExpression.Cast => Seq(cast)
      case _ => Seq.empty
    }
    current ++ message.getAllFields.values().asScala.toSeq.flatMap {
      case child: Message => collectCasts(child)
      case children: java.util.List[_] =>
        children.asScala.toSeq.collect { case child: Message => child }.flatMap(collectCasts)
      case _ => Seq.empty
    }
  }

  for {
    ansiEnabled <- Seq(false, true)
    mode <- Seq(Partial, PartialMerge, Final)
  } {
    test(s"$mode statistical aggregate buffer casts respect ANSI mode $ansiEnabled") {
      assume(BackendTestUtils.isVeloxBackendLoaded())

      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        // DISTINCT introduces a partial-merge phase for the non-distinct aggregate.
        val query = """
                      |SELECT stddev_samp(id), count(DISTINCT id % 7)
                      |FROM range(0, 32, 1, 2)
                      |""".stripMargin
        val df = runAndCompare(query)
        val aggregates = collect(df.queryExecution.executedPlan) {
          case aggregate: HashAggregateExecBaseTransformer
              if aggregate.aggregateExpressions.exists(
                expression =>
                  expression.aggregateFunction.isInstanceOf[StddevSamp] &&
                    expression.mode == mode) => aggregate
        }
        assert(aggregates.nonEmpty, df.queryExecution.executedPlan.toString)

        val expectedBehavior = if (ansiEnabled) {
          FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION
        } else {
          FailureBehavior.FAILURE_BEHAVIOR_UNSPECIFIED
        }
        aggregates.foreach {
          aggregate =>
            // Isolate this aggregate so casts from its children cannot satisfy the assertions.
            val input =
              ColumnarCollapseTransformStages.wrapInputIteratorTransformer(aggregate.child)
            val isolated = aggregate
              .withNewChildren(Seq(input))
              .asInstanceOf[HashAggregateExecBaseTransformer]
            val relation = isolated.transform(new SubstraitContext).root.toProtobuf
            val casts = collectCasts(relation)
            if (mode == Partial || mode == PartialMerge) {
              // Velox stores the count as BIGINT; Spark's statistical buffer uses DOUBLE.
              val extractedCounts = casts.filter(_.getType.hasFp64)
              assert(extractedCounts.nonEmpty, relation.toString)
              extractedCounts.foreach(cast => assert(cast.getFailureBehavior == expectedBehavior))
            }
            if (mode == PartialMerge || mode == Final) {
              // Reconstructing the Velox buffer requires the reverse DOUBLE-to-BIGINT cast.
              val reconstructedCounts = casts.filter(_.getType.hasI64)
              assert(reconstructedCounts.nonEmpty, relation.toString)
              reconstructedCounts.foreach(
                cast => assert(cast.getFailureBehavior == expectedBehavior))
            }
        }
      }
    }
  }
}
