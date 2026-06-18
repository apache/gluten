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
package org.apache.gluten.expression

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ScalarSubquery

case class ScalarSubqueryTransformer(substraitExprName: String, query: ScalarSubquery)
  extends LeafExpressionTransformer {
  override def original: Expression = query

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    // don't trigger collect when in validation phase
    if (TransformerState.underValidationState) {
      return ExpressionBuilder.makeLiteral(null, query.dataType, true)
    }
    // After https://github.com/apache/gluten/pull/5862, we do not need to execute
    // subquery manually so the exception behavior is same with vanilla Spark.
    // Note that, this code change is just for simplify. The subquery has already been materialized
    // before doing transform.
    val result = query.eval(InternalRow.empty)
    ExpressionBuilder.makeLiteral(result, query.dataType, result == null)
  }
}
