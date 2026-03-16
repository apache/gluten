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
package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{Alias, BitwiseAnd, BitwiseOr, Cast, Expression, ShiftLeft}
import org.apache.spark.sql.types.IntegralType

object SparkHashJoinUtils {

  // Copy from org.apache.spark.sql.execution.joins.HashJoin#canRewriteAsLongType
  // we should keep consistent with it to identify the LongHashRelation.
  def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    // TODO: support BooleanType, DateType and TimestampType
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
    keys.map(_.dataType.defaultSize).sum <= 8
  }

  def getOriginalKeysFromPacked(expr: Expression): Seq[Expression] = {

    def unwrap(e: Expression): Expression = e match {
      case Cast(child, _, _, _) => unwrap(child)
      case Alias(child, _) => unwrap(child)
      case BitwiseAnd(child, _) => unwrap(child)
      case other => other
    }

    expr match {
      case BitwiseOr(ShiftLeft(left, _), rightPart) =>
        getOriginalKeysFromPacked(left) :+ unwrap(rightPart)
      case BitwiseOr(left, rightPart) =>
        getOriginalKeysFromPacked(left) :+ unwrap(rightPart)
      case other =>
        Seq(unwrap(other))
    }
  }

}
