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

import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

/**
 * A convenience trait for [[GlutenPlan]] to implement [[ColumnarToColumnarTransition]] at the same
 * time. Note the implementation class will be seen by
 * [[org.apache.gluten.extension.columnar.transition.RemoveTransitions]] and removed when that rule
 * is executed.
 */
trait GlutenColumnarToColumnarTransition extends ColumnarToColumnarTransition with GlutenPlan {
  protected val from: Convention.BatchType
  protected val to: Convention.BatchType

  override def batchType(): Convention.BatchType = to

  override def rowType(): Convention.RowType = {
    Convention.RowType.None
  }

  override def requiredChildConvention(): Seq[ConventionReq] = {
    List(ConventionReq.ofBatch(ConventionReq.BatchType.Is(from)))
  }
}
