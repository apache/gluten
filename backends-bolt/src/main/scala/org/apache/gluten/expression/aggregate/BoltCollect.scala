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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.expressions.{ArrayDistinct, AttributeReference, Concat, CreateArray, Expression, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, DataType}

abstract class BoltCollect(child: Expression)
  extends DeclarativeAggregate
  with UnaryLike[Expression] {

  protected lazy val buffer: AttributeReference = AttributeReference("buffer", dataType)()

  override def dataType: DataType = ArrayType(child.dataType, false)

  override def nullable: Boolean = false

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(buffer)

  override lazy val initialValues: Seq[Expression] = Seq(Literal.create(Array(), dataType))

  override lazy val updateExpressions: Seq[Expression] = Seq(
    If(
      IsNull(child),
      buffer,
      Concat(Seq(buffer, CreateArray(Seq(child), useStringTypeWhenEmpty = false))))
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    Concat(Seq(buffer.left, buffer.right))
  )

  override def defaultResult: Option[Literal] = Option(Literal.create(Array(), dataType))
}

case class BoltCollectSet(child: Expression) extends BoltCollect(child) {

  override lazy val evaluateExpression: Expression =
    ArrayDistinct(buffer)

  override def prettyName: String = "bolt_collect_set"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class BoltCollectList(child: Expression) extends BoltCollect(child) {

  override val evaluateExpression: Expression = buffer

  override def prettyName: String = "bolt_collect_list"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
