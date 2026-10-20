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
package org.apache.gluten.backendsapi.bolt

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes
import org.apache.gluten.execution.{ArrowColumnarToBoltColumnarExec, BoltColumnarToRowExec, RowToBoltColumnarExec}
import org.apache.gluten.extension.columnar.transition.{Convention, Transition}

object BoltBatchType extends Convention.BatchType {
  override protected def registerTransitions(): Unit = {
    fromRow(Convention.RowType.VanillaRowType, RowToBoltColumnarExec.apply)
    toRow(Convention.RowType.VanillaRowType, BoltColumnarToRowExec.apply)
    fromBatch(ArrowBatchTypes.ArrowNativeBatchType, ArrowColumnarToBoltColumnarExec.apply)
    toBatch(ArrowBatchTypes.ArrowNativeBatchType, Transition.empty)
  }
}
