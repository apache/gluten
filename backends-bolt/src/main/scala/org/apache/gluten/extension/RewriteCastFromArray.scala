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

import org.apache.gluten.config.BoltConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ArrayJoin, Cast, Concat, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CAST
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StringType}

/**
 * Bolt does not support cast Array to String. Before bolt support, temporarily add this rule to
 * replace `cast(array as String)` with `concat('[', array_join(array, ', ', 'null'), ']')` to
 * support offload.
 */
case class RewriteCastFromArray(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !BoltConfig.get.enableRewriteCastArrayToString ||
      SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)
    ) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(CAST)) {
      case p =>
        p.transformExpressionsUpWithPruning(_.containsPattern(CAST)) {
          case Cast(child, StringType, timeZoneId, evalMode)
              if child.dataType.isInstanceOf[ArrayType] =>
            val joinChild = child.dataType.asInstanceOf[ArrayType].elementType match {
              case StringType =>
                child
              case _ =>
                Cast(child, ArrayType(StringType), timeZoneId, evalMode)
            }
            val arrayJoin = ArrayJoin(joinChild, Literal(", "), Some(Literal("null")))
            Concat(Seq(Literal("["), arrayJoin, Literal("]")))
        }
    }
  }
}
