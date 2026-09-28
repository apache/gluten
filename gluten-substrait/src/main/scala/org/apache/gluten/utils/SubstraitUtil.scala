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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.`type`.{ColumnTypeNode, TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}

import com.google.protobuf.{Any, DoubleValue, Int32Value, Int64Value, Message, StringValue}
import io.substrait.proto.{JoinRel, NamedStruct, NestedLoopJoinRel, Type}

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Collections, List => JList}

import scala.collection.JavaConverters._

object SubstraitUtil {
  def toSubstrait(sparkJoin: JoinType): JoinRel.JoinType = sparkJoin match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      // The right side is required to be used for building hash table in Substrait plan.
      // Therefore, for RightOuter Join, the left and right relations are exchanged and the
      // join type is reverted.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    case _ =>
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  def toNestedLoopJoinSubstrait(sparkJoin: JoinType): NestedLoopJoinRel.JoinType = sparkJoin match {
    case _: InnerLike =>
      NestedLoopJoinRel.JoinType.JOIN_TYPE_INNER
    case LeftOuter | RightOuter =>
      // since we always assume build right side in substrait,
      // the left and right relations are exchanged and the
      // join type is reverted.
      NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi | ExistenceJoin(_) =>
      NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case FullOuter =>
      NestedLoopJoinRel.JoinType.JOIN_TYPE_OUTER
    case _ =>
      NestedLoopJoinRel.JoinType.UNRECOGNIZED
  }

  def createEnhancement(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map {
      attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
    }
    // Normally, the enhancement node is only used for plan validation. But here the enhancement
    // is also used in the execution phase. In this case, an empty typeUrlPrefix needs to be passed,
    // so that it can be correctly parsed into JSON string on the cpp side.
    BackendsApiManager.getTransformerApiInstance.packPBMessage(
      TypeBuilder.makeStruct(false, inputTypeNodes.asJava).toProtobuf)
  }

  def toSubstraitExpression(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      context: SubstraitContext): ExpressionNode = {
    ExpressionConverter
      .replaceWithExpressionTransformer(expr, attributeSeq)
      .doTransform(context)
  }

  def createNameStructBuilder(
      types: JList[TypeNode],
      names: JList[String],
      columnTypeNodes: JList[ColumnTypeNode]): NamedStruct.Builder = {
    val structBuilder = Type.Struct.newBuilder
    types.asScala.foreach(t => structBuilder.addTypes(t.toProtobuf))

    val namedStructBuilder = NamedStruct.newBuilder
    namedStructBuilder.setStruct(structBuilder.build())
    names.asScala.foreach(n => namedStructBuilder.addNames(n))
    columnTypeNodes.asScala.foreach(c => namedStructBuilder.addColumnTypes(c.toProtobuf))
    namedStructBuilder
  }

  /** create table named struct */
  def toNameStruct(output: JList[Attribute]): NamedStruct = {
    val typeList = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithExprId(output)
    createNameStructBuilder(typeList, nameList, Collections.emptyList()).build()
  }

  def convertJavaObjectToAny(obj: AnyRef): Any = {
    if (obj == null) return null
    val msg: Message = obj match {
      case s: String => StringValue.newBuilder.setValue(s).build
      case i: Integer => Int32Value.newBuilder.setValue(i).build
      case l: JLong => Int64Value.newBuilder.setValue(l).build
      case d: JDouble => DoubleValue.newBuilder.setValue(d).build
      case _ =>
        // TODO: generate the message according to the object type
        StringValue.newBuilder.setValue(obj.toString).build
    }
    BackendsApiManager.getTransformerApiInstance.packPBMessage(msg)
  }
}
