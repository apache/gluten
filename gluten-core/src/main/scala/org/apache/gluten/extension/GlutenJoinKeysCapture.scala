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
package org.apache.gluten.extension

import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Strategy to capture join keys from logical plan before Spark's JoinSelection transforms them.
 * This strategy runs early in the planning phase to preserve the original join keys before any
 * transformations like rewriteKeyExpr.
 */
case class GlutenJoinKeysCapture() extends SparkStrategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    if (!plan.isInstanceOf[Join]) {
      return Nil
    }

    plan match {

      case ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, _, left, right, _) =>
        if (leftKeys.nonEmpty) {
          left.setTagValue(JoinKeysTag.ORIGINAL_JOIN_KEYS, leftKeys)
        }
        if (rightKeys.nonEmpty) {
          right.setTagValue(JoinKeysTag.ORIGINAL_JOIN_KEYS, rightKeys)
        }

        Nil

      case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys) =>
        if (leftKeys.nonEmpty) {
          j.left.setTagValue(JoinKeysTag.ORIGINAL_JOIN_KEYS, leftKeys)
        }
        if (rightKeys.nonEmpty) {
          j.right.setTagValue(JoinKeysTag.ORIGINAL_JOIN_KEYS, rightKeys)
        }

        Nil

      // For non-equi-join or other plan nodes, return Nil.
      case _ => Nil
    }
  }
}
