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
package org.apache.spark.sql.delta.sources

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.execution.datasources.v2.DeltaV2WriteOperators
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode

// Delta's V1 streaming sink starts transactions outside physical write command execution
case class OffloadDeltaStreamingWrite() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!OffloadDeltaStreamingWrite.enabled(SQLConf.get)) {
      return plan
    }
    plan.transformDown {
      case write: WriteToStream if write.outputMode == OutputMode.Append() =>
        write.sink match {
          case sink: DeltaSink => write.copy(sink = new GlutenDeltaSink(sink))
          case _ => write
        }
    }
  }
}

object OffloadDeltaStreamingWrite {
  private[sources] def enabled(sqlConf: SQLConf): Boolean = {
    val conf = new GlutenConfig(sqlConf)
    // This sink bypasses physical write command validation, so retain its session level gates
    conf.enableGluten && new VeloxDeltaConfig(sqlConf).enableNativeWrite &&
    !conf.enableScanOnly && !(conf.enableAnsiMode && conf.enableAnsiFallback)
  }
}

// Keep Delta's schema checks, epoch deduplication and commit protocol in the original sink
class GlutenDeltaSink(delegate: DeltaSink) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (OffloadDeltaStreamingWrite.enabled(data.sparkSession.sessionState.conf)) {
      DeltaV2WriteOperators.withColumnarTransaction {
        delegate.addBatch(batchId, data)
      }
    } else {
      delegate.addBatch(batchId, data)
    }
  }

  override def toString: String = s"Gluten$delegate"
}
