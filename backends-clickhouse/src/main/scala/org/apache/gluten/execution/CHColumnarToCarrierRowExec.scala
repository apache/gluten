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

import org.apache.gluten.backendsapi.clickhouse.{CHBatchType, CHCarrierRowType}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq, Transitions}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CHColumnarToCarrierRowExec(override val child: SparkPlan)
  extends ColumnarToCarrierRowExecBase {
  override protected def fromBatchType(): Convention.BatchType = CHBatchType
  override def rowType(): Convention.RowType = CHCarrierRowType
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
  // Since https://github.com/apache/gluten/pull/1595.
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      val r2c = Transitions.enforceReq(
        child,
        ConventionReq.ofBatch(ConventionReq.BatchType.Is(CHBatchType)))
      r2c.executeColumnar()
    }
  }
}

object CHColumnarToCarrierRowExec {
  def enforce(child: SparkPlan): SparkPlan = {
    Transitions.enforceReq(child, ConventionReq.ofRow(ConventionReq.RowType.Is(CHCarrierRowType)))
  }
}
