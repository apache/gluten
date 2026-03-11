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

import org.apache.gluten.expression.{AttributeReferenceTransformer, ExpressionConverter}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.DataType

import com.google.protobuf.Any
import io.substrait.proto.{CrossRel, JoinRel}

import scala.collection.JavaConverters._

object JoinUtils {

  def createExtensionNode(output: Seq[Attribute], validation: Boolean): AdvancedExtensionNode = {
    // Use field [enhancement] in a extension node for input type validation.
    if (validation) {
      ExtensionBuilder.makeAdvancedExtension(SubstraitUtil.createEnhancement(output))
    } else {
      null
    }
  }

  def preProjectionNeeded(keyExprs: Seq[Expression]): Boolean = {
    !keyExprs.forall(_.isInstanceOf[AttributeReference])
  }

  private def createPreProjectionIfNeeded(
      keyExprs: Seq[Expression],
      inputNode: RelNode,
      inputNodeOutput: Seq[Attribute],
      partialConstructedJoinOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      validation: Boolean): (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]) = {
    if (!preProjectionNeeded(keyExprs)) {
      // Skip pre-projection if all keys are [AttributeReference]s,
      // which can be directly converted into SelectionNode.
      val keys = keyExprs.map {
        expr =>
          (
            ExpressionConverter
              .replaceWithExpressionTransformer(expr, partialConstructedJoinOutput)
              .asInstanceOf[AttributeReferenceTransformer]
              .doTransform(substraitContext),
            expr.dataType)
      }
      (keys, inputNode, inputNodeOutput)
    } else {
      // Pre-projection is constructed from original columns followed by join-key expressions.
      val selectOrigins = inputNodeOutput.indices.map(ExpressionBuilder.makeSelection(_))
      val appendedKeys = keyExprs.flatMap {
        case _: AttributeReference => None
        case expr =>
          Some(
            (
              ExpressionConverter
                .replaceWithExpressionTransformer(expr, inputNodeOutput)
                .doTransform(substraitContext),
              expr.dataType))
      }
      val preProjectNode = RelBuilder.makeProjectRel(
        inputNode,
        new java.util.ArrayList[ExpressionNode]((selectOrigins ++ appendedKeys.map(_._1)).asJava),
        createExtensionNode(inputNodeOutput, validation),
        substraitContext,
        operatorId,
        inputNodeOutput.size
      )

      // Compute index for join keys in join outputs.
      val offset = partialConstructedJoinOutput.size
      val appendedKeysAndIndices = appendedKeys.zipWithIndex.iterator
      val keys = keyExprs.map {
        case a: AttributeReference =>
          // The selection index for original AttributeReference is unchanged.
          (
            ExpressionConverter
              .replaceWithExpressionTransformer(a, partialConstructedJoinOutput)
              .asInstanceOf[AttributeReferenceTransformer]
              .doTransform(substraitContext),
            a.dataType)
        case _ =>
          val (key, idx) = appendedKeysAndIndices.next()
          (ExpressionBuilder.makeSelection(idx + offset), key._2)
      }
      (
        keys,
        preProjectNode,
        inputNodeOutput ++
          appendedKeys.zipWithIndex.map {
            case (key, idx) =>
              // Create output attributes for appended keys.
              // This is used as place holder for finding the right column indexes in post-join
              // filters.
              AttributeReference(s"col_${idx + offset}", key._2)()
          })
    }
  }

  private def createJoinExtensionNode(
      joinParameters: Any,
      output: Seq[Attribute]): AdvancedExtensionNode = {
    // Use field [optimization] in a extension node
    // to send some join parameters through Substrait plan.
    val enhancement = SubstraitUtil.createEnhancement(output)
    ExtensionBuilder.makeAdvancedExtension(joinParameters, enhancement)
  }

  // Return the direct join output.
  private def getDirectJoinOutput(
      joinType: JoinType,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      callerClassName: String = null): (Seq[Attribute], Seq[Attribute]) = {
    joinType match {
      case _: InnerLike =>
        (leftOutput, rightOutput)
      case LeftOuter =>
        (leftOutput, rightOutput.map(_.withNullability(true)))
      case RightOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput)
      case FullOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput.map(_.withNullability(true)))
      case j: ExistenceJoin =>
        (leftOutput :+ j.exists, Nil)
      case LeftExistence(_) =>
        // LeftSemi | LeftAnti | ExistenceJoin.
        (leftOutput, Nil)
      // LeftSingle is a Spark 4.0+ join type with same output schema as LeftOuter.
      // The join will fallback to Spark execution because substraitJoinType returns UNRECOGNIZED.
      case leftSingle if SparkShimLoader.getSparkShims.isLeftSingleJoinType(leftSingle) =>
        (leftOutput, rightOutput.map(_.withNullability(true)))
      case x =>
        val joinClass = Option(callerClassName).getOrElse(this.getClass.getSimpleName)
        throw new IllegalArgumentException(s"$joinClass not take $x as the JoinType")
    }
  }

  def getDirectJoinOutputSeq(
      joinType: JoinType,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      joinClassName: String = null): Seq[Attribute] = {
    val (left, right) = getDirectJoinOutput(joinType, leftOutput, rightOutput, joinClassName)
    left ++ right
  }

  // scalastyle:off argcount
  def createJoinRel(
      streamedKeyExprs: Seq[Expression],
      buildKeyExprs: Seq[Expression],
      condition: Option[Expression],
      substraitJoinType: JoinRel.JoinType,
      exchangeTable: Boolean,
      joinType: JoinType,
      joinParameters: Any,
      inputStreamedRelNode: RelNode,
      inputBuildRelNode: RelNode,
      inputStreamedOutput: Seq[Attribute],
      inputBuildOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      hashTableId: String = "",
      validation: Boolean = false): RelNode = {
    // scalastyle:on argcount
    // Create pre-projection for build/streamed plan. Append projected keys to each side.
    val (streamedKeys, streamedRelNode, streamedOutput) = createPreProjectionIfNeeded(
      streamedKeyExprs,
      inputStreamedRelNode,
      inputStreamedOutput,
      inputStreamedOutput,
      substraitContext,
      operatorId,
      validation)

    val (buildKeys, buildRelNode, buildOutput) = createPreProjectionIfNeeded(
      buildKeyExprs,
      inputBuildRelNode,
      inputBuildOutput,
      streamedOutput ++ inputBuildOutput,
      substraitContext,
      operatorId,
      validation)

    // Combine join keys to make a single expression.
    val joinExpressionNode = streamedKeys
      .zip(buildKeys)
      .map {
        case ((leftKey, leftType), (rightKey, rightType)) =>
          HashJoinLikeExecTransformer.makeEqualToExpression(
            leftKey,
            leftType,
            rightKey,
            rightType,
            substraitContext)
      }
      .reduce((l, r) => HashJoinLikeExecTransformer.makeAndExpression(l, r, substraitContext))

    // Create post-join filter, which will be computed in hash join.
    val postJoinFilter =
      condition.map {
        SubstraitUtil.toSubstraitExpression(_, streamedOutput ++ buildOutput, substraitContext)
      }

    // Create JoinRel.
    val joinRel = RelBuilder.makeJoinRel(
      streamedRelNode,
      buildRelNode,
      substraitJoinType,
      joinExpressionNode,
      postJoinFilter.orNull,
      createJoinExtensionNode(joinParameters, streamedOutput ++ buildOutput),
      hashTableId,
      substraitContext,
      operatorId
    )

    createProjectRelPostJoinRel(
      exchangeTable,
      joinType,
      inputStreamedOutput,
      inputBuildOutput,
      substraitContext,
      operatorId,
      joinRel,
      streamedOutput,
      buildOutput,
      validation
    )
  }

  def createProjectRelPostJoinRel(
      exchangeTable: Boolean,
      joinType: JoinType,
      inputStreamedOutput: Seq[Attribute],
      inputBuildOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      joinRel: RelNode,
      streamedOutput: Seq[Attribute],
      buildOutput: Seq[Attribute],
      validation: Boolean = false
  ): RelNode = {
    // Result projection will drop the appended keys, and exchange columns order if BuildLeft.
    val resultProjection = if (exchangeTable) {
      val (leftOutput, rightOutput) =
        getDirectJoinOutput(joinType, inputBuildOutput, inputStreamedOutput)
      joinType match {
        case _: ExistenceJoin =>
          inputBuildOutput.indices.map(ExpressionBuilder.makeSelection(_)) :+
            ExpressionBuilder.makeSelection(buildOutput.size)
        case LeftSemi | LeftAnti =>
          // When the left semi/anti join support the BuildLeft
          leftOutput.indices.map(idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size))
        case LeftExistence(_) =>
          leftOutput.indices.map(ExpressionBuilder.makeSelection(_))
        case _ =>
          // Exchange the order of build and streamed.
          leftOutput.indices.map(
            idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size)) ++
            rightOutput.indices
              .map(ExpressionBuilder.makeSelection(_))
      }
    } else {
      val (leftOutput, rightOutput) =
        getDirectJoinOutput(joinType, inputStreamedOutput, inputBuildOutput)
      if (joinType.isInstanceOf[ExistenceJoin]) {
        inputStreamedOutput.indices.map(ExpressionBuilder.makeSelection(_)) :+
          ExpressionBuilder.makeSelection(streamedOutput.size)
      } else {
        leftOutput.indices.map(ExpressionBuilder.makeSelection(_)) ++
          rightOutput.indices.map(idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size))
      }
    }

    val directJoinOutputs = if (exchangeTable) {
      getDirectJoinOutputSeq(joinType, buildOutput, streamedOutput)
    } else {
      getDirectJoinOutputSeq(joinType, streamedOutput, buildOutput)
    }
    RelBuilder.makeProjectRel(
      joinRel,
      new java.util.ArrayList[ExpressionNode](resultProjection.asJava),
      createExtensionNode(directJoinOutputs, validation),
      substraitContext,
      operatorId,
      directJoinOutputs.size
    )
  }

  def createCrossRel(
      substraitJoinType: CrossRel.JoinType,
      condition: Option[Expression],
      inputStreamedRelNode: RelNode,
      inputBuildRelNode: RelNode,
      inputStreamedOutput: Seq[Attribute],
      inputBuildOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      joinParameters: Any,
      validation: Boolean = false
  ): RelNode = {
    val expressionNode =
      condition.map {
        SubstraitUtil.toSubstraitExpression(
          _,
          inputStreamedOutput ++ inputBuildOutput,
          substraitContext)
      }

    val extensionNode =
      createJoinExtensionNode(joinParameters, inputStreamedOutput ++ inputBuildOutput)

    RelBuilder.makeCrossRel(
      inputStreamedRelNode,
      inputBuildRelNode,
      substraitJoinType,
      expressionNode.orNull,
      extensionNode,
      substraitContext,
      operatorId
    )
  }
}
