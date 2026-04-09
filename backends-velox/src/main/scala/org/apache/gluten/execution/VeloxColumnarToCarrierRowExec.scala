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

import org.apache.gluten.backendsapi.velox.{VeloxBatchType, VeloxCarrierRowType}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq, Transitions}

import org.apache.spark.sql.execution.SparkPlan

case class VeloxColumnarToCarrierRowExec(override val child: SparkPlan)
  extends ColumnarToCarrierRowExecBase {
  override protected def fromBatchType(): Convention.BatchType = VeloxBatchType
  override def rowType(): Convention.RowType = VeloxCarrierRowType
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object VeloxColumnarToCarrierRowExec {
  def enforce(child: SparkPlan): SparkPlan = {
    Transitions.enforceReq(
      child,
      ConventionReq.ofRow(ConventionReq.RowType.Is(VeloxCarrierRowType)))
  }
}
