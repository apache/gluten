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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.ColumnarToRowExecBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

object GlutenWriterColumnarRules {

  private[datasources] def injectFakeRowAdaptor(command: SparkPlan, child: SparkPlan): SparkPlan = {
    child match {
      // if the child is columnar, we can just wrap & transfer the columnar data
      case c2r: ColumnarToRowExecBase =>
        command.withNewChildren(
          Array(BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(c2r.child)))
      // If the child is aqe, we make aqe "support columnar",
      // then aqe itself will guarantee to generate columnar outputs.
      // So FakeRowAdaptor will always consumes columnar data,
      // thus avoiding the case of c2r->aqe->r2c->writer
      case aqe: AdaptiveSparkPlanExec =>
        command.withNewChildren(
          Array(
            BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(
              AdaptiveSparkPlanExec(
                aqe.inputPlan,
                aqe.context,
                aqe.preprocessingRules,
                aqe.isSubquery,
                supportsColumnar = true
              ))))
      case other =>
        command.withNewChildren(
          Array(BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(other)))
    }
  }

  // TODO: This makes FileFormatWriter#write caller-sensitive.
  //  Remove this workaround once we have a better solution.
  def injectSparkLocalProperty(
      spark: SparkSession,
      format: Option[String],
      numStaticPartitions: Option[Int]): Unit = {
    if (format.isDefined) {
      spark.sparkContext.setLocalProperty("isNativeApplicable", true.toString)
      spark.sparkContext.setLocalProperty("nativeFormat", format.get)
      // The flag for static-only write is not used by Velox backend where
      // the static partition write is already supported.
      spark.sparkContext.setLocalProperty(
        "staticPartitionWriteOnly",
        BackendsApiManager.getSettings.staticPartitionWriteOnly().toString)
      spark.sparkContext.setLocalProperty(
        "numStaticPartitionCols",
        numStaticPartitions.getOrElse(0).toString)
    } else {
      spark.sparkContext.setLocalProperty("isNativeApplicable", null)
      spark.sparkContext.setLocalProperty("nativeFormat", null)
      spark.sparkContext.setLocalProperty("staticPartitionWriteOnly", null)
      spark.sparkContext.setLocalProperty("numStaticPartitionCols", null)
    }
  }
}
