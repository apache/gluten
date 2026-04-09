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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.sql.execution.SparkPlan

/**
 * Base interface for Query plan that defined by backends.
 *
 * The following Spark APIs are marked final so forbidden from overriding:
 *   - supportsColumnar
 *   - supportsRowBased (Spark version >= 3.3)
 *
 * Instead, subclasses are expected to implement the following APIs:
 *   - batchType
 *   - rowType
 *   - requiredChildConvention (optional)
 *
 * With implementations of the APIs provided, Gluten query planner will be able to find and insert
 * proper transitions between different plan nodes.
 *
 * Implementing `requiredChildConvention` is optional while the default implementation is a sequence
 * of convention reqs that are exactly the same with the output convention. If it's not the case for
 * some plan types, then the API should be overridden. For example, a typical row-to-columnar
 * transition is at the same time a query plan node that requires for row input however produces
 * columnar output.
 */
trait GlutenPlan
  extends SparkPlan
  with Convention.KnownBatchType
  with Convention.KnownRowType
  with GlutenPlan.SupportsRowBasedCompatible
  with ConventionReq.KnownChildConvention {

  final override val supportsColumnar: Boolean = {
    batchType() != Convention.BatchType.None
  }

  final override val supportsRowBased: Boolean = {
    rowType() != Convention.RowType.None
  }

  override def batchType(): Convention.BatchType

  override def rowType(): Convention.RowType

  override def requiredChildConvention(): Seq[ConventionReq] = {
    // In the normal case, children's convention should follow parent node's convention.
    val childReq = Convention.of(rowType(), batchType()).asReq()
    Seq.tabulate(children.size)(
      _ => {
        childReq
      })
  }
}

object GlutenPlan {
  // To be compatible with Spark (version < 3.3)
  trait SupportsRowBasedCompatible {
    def supportsRowBased(): Boolean = {
      throw new GlutenException("Illegal state: The method is not expected to be called")
    }
  }
}
