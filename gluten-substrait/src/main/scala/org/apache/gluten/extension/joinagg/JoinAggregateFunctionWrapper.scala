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
package org.apache.gluten.extension.joinagg

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CreateStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DataType

import java.util.Locale

import scala.collection.mutable

object JoinAggregateFunctionWrapper {
  // The wrapper is used in exactly two logical phases:
  //   - PartialPhase: a pushed aggregate below / through joins
  //   - FinalPhase: the aggregate above the join that restores the original query semantics
  sealed trait TargetPhase {
    def sqlName: String
  }

  case object PartialPhase extends TargetPhase {
    override val sqlName: String = "PARTIAL"
  }

  case object FinalPhase extends TargetPhase {
    override val sqlName: String = "FINAL"
  }

  def wrapperPartial(
      innerAgg: DeclarativeAggregate,
      wrapperKey: String = "0"): JoinAggregateFunctionWrapper = {
    JoinAggregateFunctionWrapper(
      innerAgg = innerAgg,
      targetPhase = PartialPhase,
      inputBuffer = None,
      wrapperKey = wrapperKey)
  }

  def wrapperFinal(
      innerAgg: DeclarativeAggregate,
      inputBuffer: Expression,
      wrapperKey: String = "0"): JoinAggregateFunctionWrapper = {
    JoinAggregateFunctionWrapper(
      innerAgg = innerAgg,
      targetPhase = FinalPhase,
      inputBuffer = Some(inputBuffer),
      wrapperKey = wrapperKey)
  }

  // Translate Spark's physical aggregate mode together with the wrapper semantic phase into the
  // actual Spark aggregate mode that should be used for the wrapped aggregate. For example, a
  // FinalPhase wrapper running in Spark's Partial mode is really doing a partial merge of the
  // pushed aggregate buffers.
  def semanticMode(actualMode: AggregateMode, targetPhase: TargetPhase): AggregateMode = {
    // The wrapper phase says what the logical plan wants to do with the aggregate state across
    // the join. `actualMode` says which physical aggregate stage Spark is currently building.
    // The combination determines the real wrapped aggregate mode that should run physically.
    (actualMode, targetPhase) match {
      case (Partial, PartialPhase) => Partial
      case (PartialMerge, PartialPhase) => PartialMerge
      case (Final, PartialPhase) => PartialMerge
      case (Complete, PartialPhase) => Partial
      case (Partial, FinalPhase) => PartialMerge
      case (PartialMerge, FinalPhase) => PartialMerge
      case (Final, FinalPhase) => Final
      case (Complete, FinalPhase) => Final
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported wrapper semantic mode mapping: actualMode=$actualMode, " +
            s"targetPhase=$targetPhase")
    }
  }
}

case class JoinAggregateFunctionWrapper(
    innerAgg: DeclarativeAggregate,
    targetPhase: JoinAggregateFunctionWrapper.TargetPhase,
    inputBuffer: Option[Expression],
    wrapperKey: String = "0")
  extends DeclarativeAggregate {
  /*
   * Logical wrapper around a Spark declarative aggregate used by the join-aggregate rewrite.
   *
   * Why the wrapper exists:
   *   - Below the join, we want to aggregate early and carry the aggregate buffer through the
   *     join as a single value.
   *   - Above the join, we want to merge / evaluate that buffer and recover the original
   *     aggregate result.
   *
   * The wrapper therefore changes only the *logical contract* across the join:
   *   - PartialPhase exposes the wrapped aggregate buffer as a single struct-valued output.
   *   - FinalPhase consumes that struct-valued buffer and delegates merge / evaluate semantics
   *     back to the wrapped Spark aggregate.
   *
   * `ImplementJoinAggregate` later lowers this wrapper back into normal Spark physical aggregates
   * by packing / unpacking the struct around the aggregate buffers.
   */
  import JoinAggregateFunctionWrapper._

  private val wrappedBufferAttrs: Seq[AttributeReference] =
    innerAgg.aggBufferAttributes.zipWithIndex.map {
      case (attr, index) =>
        AttributeReference(attr.name, attr.dataType, attr.nullable)()
    }

  private def outputBufferExpr: Expression =
    inputBuffer.getOrElse(CreateStruct(innerAgg.inputAggBufferAttributes))

  override lazy val nullable: Boolean = true

  override lazy val dataType: DataType = targetPhase match {
    case PartialPhase =>
      // The pushed phase carries the aggregate buffer through the plan as a single struct-valued
      // payload so the join sees one logical column per pushed aggregate.
      CreateStruct(wrappedBufferAttrs).dataType
    case FinalPhase =>
      innerAgg.dataType
  }

  override def children: Seq[Expression] = targetPhase match {
    case PartialPhase => innerAgg.children
    case FinalPhase => Seq(outputBufferExpr)
  }

  override lazy val aggBufferAttributes: Seq[AttributeReference] = wrappedBufferAttrs

  override lazy val initialValues: Seq[Expression] = {
    rewrite(innerAgg.initialValues, childReplacements = Map.empty, useInputBufferField = false)
  }

  override lazy val updateExpressions: Seq[Expression] = targetPhase match {
    case PartialPhase =>
      // Update the wrapped aggregate buffer from the original aggregate children.
      rewrite(
        innerAgg.updateExpressions,
        childReplacements = innerAgg.children.zip(children).toMap,
        useInputBufferField = false
      )
    case FinalPhase =>
      // Merge expressions read from the struct-valued input buffer produced by the pushed phase.
      rewrite(innerAgg.mergeExpressions, childReplacements = Map.empty, useInputBufferField = true)
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    rewrite(innerAgg.mergeExpressions, childReplacements = Map.empty, useInputBufferField = false)
  }

  override lazy val evaluateExpression: Expression = targetPhase match {
    case PartialPhase =>
      // The pushed phase returns the entire aggregate buffer, not the final aggregate value.
      CreateStruct(aggBufferAttributes)
    case FinalPhase =>
      rewrite(
        innerAgg.evaluateExpression,
        childReplacements = Map.empty,
        useInputBufferField = false)
  }

  override def nodeName: String = "JoinAggregateWrapper"

  override def prettyName: String =
    s"join_agg_wrapper_${targetPhase.sqlName.toLowerCase(Locale.ROOT)}"

  override def sql: String = {
    s"$prettyName(${innerAgg.sql(false)})"
  }

  override lazy val deterministic: Boolean = innerAgg.deterministic

  override lazy val defaultResult: Option[Literal] = targetPhase match {
    case PartialPhase => None
    case FinalPhase => innerAgg.defaultResult
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    targetPhase match {
      case PartialPhase =>
        val newInner = innerAgg.withNewChildren(newChildren).asInstanceOf[DeclarativeAggregate]
        copy(innerAgg = newInner, inputBuffer = None)
      case FinalPhase =>
        if (newChildren.size != 1) {
          throw new IllegalArgumentException(
            s"Final JoinAggregateWrapper expects exactly one child, got ${newChildren.size}")
        }
        copy(inputBuffer = Some(newChildren.head))
    }
  }

  private def rewrite(
      exprs: Seq[Expression],
      childReplacements: Map[Expression, Expression],
      useInputBufferField: Boolean): Seq[Expression] = {
    exprs.map(rewrite(_, childReplacements, useInputBufferField))
  }

  private def rewrite(
      expr: Expression,
      childReplacements: Map[Expression, Expression],
      useInputBufferField: Boolean): Expression = {
    // Rebind every attribute reference in the wrapped aggregate expression tree to the
    // corresponding wrapper-side attribute:
    //   - wrapped buffer attrs when we are updating / evaluating the wrapper buffer
    //   - struct-field reads when FinalPhase is consuming the pushed buffer payload
    //   - original aggregate children when PartialPhase still reads the raw input rows
    val innerToWrappedBuffer = innerAgg.aggBufferAttributes.zip(aggBufferAttributes)
    val innerToInputBuffer = innerAgg.inputAggBufferAttributes.zipWithIndex.map {
      case (attr, index) =>
        if (useInputBufferField) {
          attr -> GetStructField(outputBufferExpr, index, Some(attr.name))
        } else {
          attr -> inputAggBufferAttributes(index)
        }
    }
    val attrRewriteMap = mutable.ArrayBuffer.empty[(Attribute, Expression)]
    attrRewriteMap ++= innerToWrappedBuffer
    attrRewriteMap ++= innerToInputBuffer
    val childRewriteSeq = childReplacements.toSeq

    // Rewrite in two passes:
    //   1. swap original aggregate child expressions when PartialPhase still reads raw input rows;
    //   2. rebind buffer/input attributes to wrapper-side attrs or struct-field reads.
    //
    // Using semantic equality here is important because the wrapped aggregate expression tree is
    // copied and re-bound several times while the rule and strategy move between logical and
    // physical representations.
    childRewriteSeq
      .foldLeft(expr) {
        case (curExpr, (from, to)) =>
          curExpr.transformUp {
            case e if e.semanticEquals(from) => to
          }
      }
      .transformUp {
        case a: Attribute =>
          attrRewriteMap
            .collectFirst {
              case (from, to) if a.semanticEquals(from) => to
            }
            .getOrElse(a)
      }
  }
}
