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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression._
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode, ScalarFunctionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.VeloxIntermediateData

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.sql.hive.HiveUDAFInspector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.google.protobuf.StringValue

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

abstract class HashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override def output: Seq[Attribute] = {
    // TODO: We should have a check to make sure the returned schema actually matches the output
    //  data. Since "resultExpressions" is not actually in used by Velox.
    super.output
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getAggRel(context, operatorId, aggParams, childCtx.root)
    TransformContext(output, relNode)
  }

  // Return whether the outputs partial aggregation should be combined for Velox computing.
  // When the partial outputs are multiple-column, row construct is needed.
  private def rowConstructNeeded(): Boolean = aggregateExpressions.exists {
    case AggregateExpression(aggFunc, PartialMerge | Final, _, _, _) =>
      aggFunc.inputAggBufferAttributes.size > 1
    case _ => false
  }

  /**
   * Returns whether extracting subfield from struct is needed. True when the intermediate type of
   * Velox aggregation is a compound type.
   * @return
   *   extracting needed or not.
   */
  private def extractStructNeeded(): Boolean = aggregateExpressions.exists {
    case AggregateExpression(aggFunc, Partial | PartialMerge, _, _, _) =>
      aggFunc.aggBufferAttributes.size > 1
    case _ => false
  }

  /**
   * Add a projection after aggregation to extract subfields from Struct.
   * @param context
   *   the Substrait context
   * @param aggRel
   *   the aggregation rel
   * @param operatorId
   *   the operator id
   * @return
   *   a project rel
   */
  private def applyExtractStruct(
      context: SubstraitContext,
      aggRel: RelNode,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val expressionNodes = new JArrayList[ExpressionNode]()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      val groupingExpr: ExpressionNode = ExpressionBuilder.makeSelection(colIdx)
      expressionNodes.add(groupingExpr)
      colIdx += 1
    }

    for (expr <- aggregateExpressions) {
      val aggFunc = expr.aggregateFunction
      aggFunc match {
        case _ @VeloxIntermediateData.Type(veloxTypes: Seq[DataType]) =>
          val (sparkOrders, sparkTypes) =
            aggFunc.aggBufferAttributes.map(attr => (attr.name, attr.dataType)).unzip
          val veloxOrders = VeloxIntermediateData.veloxIntermediateDataOrder(aggFunc)
          val adjustedOrders = sparkOrders.map(VeloxIntermediateData.getAttrIndex(veloxOrders, _))
          sparkTypes.zipWithIndex.foreach {
            case (sparkType, idx) =>
              val veloxType = veloxTypes(adjustedOrders(idx))
              if (veloxType != sparkType) {
                // Velox and Spark have different type, adding a cast expression
                expressionNodes.add(
                  ExpressionBuilder
                    .makeCast(
                      ConverterUtils.getTypeNode(sparkType, nullable = false),
                      ExpressionBuilder.makeSelection(colIdx, adjustedOrders(idx)),
                      SQLConf.get.ansiEnabled))
              } else {
                // Velox and Spark have the same type
                expressionNodes.add(ExpressionBuilder.makeSelection(colIdx, adjustedOrders(idx)))
              }
          }
          colIdx += 1
        case _ =>
          expressionNodes.add(ExpressionBuilder.makeSelection(colIdx))
          colIdx += 1
      }
    }
    if (!validation) {
      RelBuilder.makeProjectRel(
        aggRel,
        expressionNodes,
        context,
        operatorId,
        groupingExpressions.size + aggregateExpressions.size)
    } else {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, getPartialAggOutTypes).toProtobuf))
      RelBuilder.makeProjectRel(
        aggRel,
        expressionNodes,
        extensionNode,
        context,
        operatorId,
        groupingExpressions.size + aggregateExpressions.size)
    }
  }

  // Whether the output data allows to be just pre-aggregated rather than
  // fully aggregated. If true, aggregation could flush its in memory
  // aggregated data whenever is needed rather than waiting for all input
  // to be read.
  protected def allowFlush: Boolean

  private def formatExtOptimizationString(isStreaming: Boolean): String = {
    val isStreamingStr = if (isStreaming) "1" else "0"
    val allowFlushStr = if (allowFlush) "1" else "0"
    s"isStreaming=$isStreamingStr\nallowFlush=$allowFlushStr\n"
  }

  // Create aggregate function node.
  private def makeFunctionNode(
      context: SubstraitContext,
      aggregateFunction: AggregateFunction,
      childrenNodeList: JList[ExpressionNode],
      aggregateMode: AggregateMode): AggregateFunctionNode = {

    val outputTypeNode = aggregateMode match {
      case Partial | PartialMerge if aggregateFunction.aggBufferAttributes.size > 1 =>
        VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction)
      case Partial | PartialMerge =>
        ConverterUtils.getTypeNode(
          aggregateFunction.inputAggBufferAttributes.head.dataType,
          aggregateFunction.inputAggBufferAttributes.head.nullable)
      case Final | Complete =>
        ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
    }
    ExpressionBuilder.makeAggregateFunction(
      VeloxAggregateFunctionsBuilder.create(context, aggregateFunction, aggregateMode),
      childrenNodeList,
      modeToKeyWord(aggregateMode),
      outputTypeNode
    )
  }

  /**
   * Return the output types after partial aggregation through Velox.
   * @return
   */
  private def getPartialAggOutTypes: JList[TypeNode] = {
    val typeNodeList = new JArrayList[TypeNode]()
    groupingExpressions.foreach(
      expression => {
        typeNodeList.add(ConverterUtils.getTypeNode(expression.dataType, expression.nullable))
      })

    aggregateExpressions.foreach(
      expression => {
        val aggregateFunction = expression.aggregateFunction
        aggregateFunction match {
          case _ if aggregateFunction.aggBufferAttributes.size > 1 =>
            expression.mode match {
              case Partial | PartialMerge =>
                typeNodeList.add(VeloxIntermediateData.getIntermediateTypeNode(aggregateFunction))
              case Final | Complete =>
                typeNodeList.add(
                  ConverterUtils
                    .getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
              case other =>
                throw new GlutenNotSupportException(s"$other is not supported.")
            }
          case _ =>
            typeNodeList.add(
              ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable))
        }
      })
    typeNodeList
  }

  // Return a scalar function node representing row construct function in Velox.
  private def getRowConstructNode(
      context: SubstraitContext,
      childNodes: JList[ExpressionNode],
      rowConstructAttributes: Seq[Attribute],
      aggFunc: AggregateFunction): ScalarFunctionNode = {
    val functionName = ConverterUtils.makeFuncName(
      VeloxIntermediateData.getRowConstructFuncName(aggFunc),
      rowConstructAttributes.map(attr => attr.dataType))
    val functionId = context.registerFunction(functionName)

    // Use struct type to represent Velox RowType.
    val structTypeNodes = rowConstructAttributes
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      .asJava

    ExpressionBuilder.makeScalarFunction(
      functionId,
      childNodes,
      TypeBuilder.makeStruct(false, structTypeNodes))
  }

  // Add a projection node before aggregation for row constructing.
  // Mainly used for aggregation whose intermediate type is a compound type in Velox.
  // Pre-projection is never required for final stages.
  private def applyRowConstruct(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      inputRel: RelNode,
      validation: Boolean): RelNode = {
    val exprNodes = new JArrayList[ExpressionNode]()
    groupingExpressions.foreach(
      expr => {
        exprNodes.add(
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, originalInputAttributes)
            .doTransform(context))
      })

    for (aggregateExpression <- aggregateExpressions) {
      val aggFunc = aggregateExpression.aggregateFunction
      val functionInputAttributes = aggFunc.inputAggBufferAttributes
      aggFunc match {
        case _ if aggregateExpression.mode == Partial || aggregateExpression.mode == Complete =>
          val childNodes = aggFunc.children
            .map(
              ExpressionConverter
                .replaceWithExpressionTransformer(_, originalInputAttributes)
                .doTransform(context)
            )
            .asJava
          exprNodes.addAll(childNodes)

        case _: HyperLogLogPlusPlus if aggFunc.aggBufferAttributes.size != 1 =>
          throw new GlutenNotSupportException("Only one input attribute is expected.")

        case _ @VeloxIntermediateData.Type(veloxTypes: Seq[DataType]) =>
          val rewrittenInputAttributes =
            rewriteAggBufferAttributes(functionInputAttributes, originalInputAttributes)
          // The process of handling the inconsistency in column types and order between
          // Spark and Velox is exactly the opposite of applyExtractStruct.
          aggregateExpression.mode match {
            case PartialMerge | Final =>
              val newInputAttributes = new ArrayBuffer[Attribute]()
              val childNodes = new JArrayList[ExpressionNode]()
              val (sparkOrders, sparkTypes) =
                aggFunc.aggBufferAttributes.map(attr => (attr.name, attr.dataType)).unzip
              val veloxOrders = VeloxIntermediateData.veloxIntermediateDataOrder(aggFunc)
              val adjustedOrders = veloxOrders.map(o => sparkOrders.indexOf(o.head))
              veloxTypes.zipWithIndex.foreach {
                case (veloxType, idx) =>
                  val adjustedIdx = adjustedOrders(idx)
                  if (adjustedIdx == -1) {
                    // The Velox aggregate intermediate buffer column not found in Spark.
                    // For example, skewness and kurtosis share the same aggregate buffer in Velox,
                    // and Kurtosis additionally requires the buffer column of m4, which is
                    // always 0 for skewness. In Spark, the aggregate buffer of skewness does not
                    // have the column of m4, thus a placeholder m4 with a value of 0 must be passed
                    // to Velox, and this value cannot be omitted. Velox will always read m4 column
                    // when accessing the intermediate data.
                    val extraAttr = AttributeReference(veloxOrders(idx).head, veloxType)()
                    newInputAttributes += extraAttr
                    val lt = Literal.default(veloxType)
                    childNodes.add(ExpressionBuilder.makeLiteral(lt.value, lt.dataType, false))
                  } else {
                    val sparkType = sparkTypes(adjustedIdx)
                    val attr = rewrittenInputAttributes(adjustedIdx)
                    val aggFuncInputAttrNode = ExpressionConverter
                      .replaceWithExpressionTransformer(attr, originalInputAttributes)
                      .doTransform(context)
                    val expressionNode = if (sparkType != veloxType) {
                      newInputAttributes +=
                        attr.copy(dataType = veloxType)(attr.exprId, attr.qualifier)
                      ExpressionBuilder.makeCast(
                        ConverterUtils.getTypeNode(veloxType, attr.nullable),
                        aggFuncInputAttrNode,
                        SQLConf.get.ansiEnabled)
                    } else {
                      newInputAttributes += attr
                      aggFuncInputAttrNode
                    }
                    childNodes.add(expressionNode)
                  }
              }
              exprNodes.add(
                getRowConstructNode(context, childNodes, newInputAttributes.toSeq, aggFunc))
            case other =>
              throw new GlutenNotSupportException(s"$other is not supported.")
          }

        case _ =>
          val rewrittenInputAttributes =
            rewriteAggBufferAttributes(functionInputAttributes, originalInputAttributes)
          val childNodes = rewrittenInputAttributes
            .map(
              ExpressionConverter
                .replaceWithExpressionTransformer(_, originalInputAttributes)
                .doTransform(context)
            )
            .asJava
          exprNodes.addAll(childNodes)
      }
    }

    RelBuilder.makeProjectRel(
      originalInputAttributes.asJava,
      inputRel,
      exprNodes,
      context,
      operatorId,
      validation)
  }

  /**
   * Create and return the Rel for the aggregation.
   * @param context
   *   the Substrait context
   * @param operatorId
   *   the operator id
   * @param aggParams
   *   the params for aggregation mainly used for metrics updating
   * @param input
   *   tht input rel node
   * @param validation
   *   whether this is for native validation
   * @return
   *   the rel node for this aggregation
   */
  override protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output

    val finalInput = if (rowConstructNeeded()) {
      aggParams.rowConstructionNeeded = true
      applyRowConstruct(context, originalInputAttributes, operatorId, input, validation)
    } else {
      input
    }

    var aggRel = getAggRelInternal(
      context,
      originalInputAttributes,
      operatorId,
      finalInput,
      validation,
      aggParams.rowConstructionNeeded)

    if (extractStructNeeded()) {
      aggParams.extractionNeeded = true
      aggRel = applyExtractStruct(context, aggRel, operatorId, validation)
    }

    context.registerAggregationParam(operatorId, aggParams)
    aggRel
  }

  private def rewriteAggBufferAttributes(
      inputAggBufferAttributes: Seq[AttributeReference],
      originalInputAttributes: Seq[Attribute]): Seq[AttributeReference] = {
    inputAggBufferAttributes.map {
      attr =>
        val sameAttr = originalInputAttributes.find(_.exprId == attr.exprId)
        if (sameAttr.isEmpty) {
          // 1. When aggregateExpressions includes subquery, Spark's PlanAdaptiveSubqueries
          // Rule will transform the subquery within the final agg. The aggregateFunction
          // in the aggregateExpressions of the final aggregation will be cloned, resulting
          // in creating new aggregateFunction object. The inputAggBufferAttributes will
          // also generate new AttributeReference instances with larger exprId, which leads
          // to a failure in binding with the output of the partial agg. We need to adapt
          // to this situation; when encountering a failure to bind, it is necessary to
          // allow the binding of inputAggBufferAttribute with the same name but different
          // exprId.
          // 2. After apply `PullOutPreProject`, the aggregate expression may be created a new
          // instance.
          val attrsWithSameName =
            originalInputAttributes.drop(groupingExpressions.size).collect {
              case a if a.name == attr.name => a
            }
          val aggBufferAttrsWithSameName = aggregateExpressions.toIndexedSeq
            .flatMap(_.aggregateFunction.inputAggBufferAttributes)
            .filter(_.name == attr.name)
          assert(
            attrsWithSameName.size == aggBufferAttrsWithSameName.size,
            "The attribute with the same name in final agg inputAggBufferAttribute must" +
              "have the same size of corresponding attributes in originalInputAttributes."
          )
          attrsWithSameName(aggBufferAttrsWithSameName.indexOf(attr))
            .asInstanceOf[AttributeReference]
        } else {
          attr
        }
    }
  }

  private def getAggRelInternal(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean,
      rowConstructed: Boolean): RelNode = {
    var colIdx = -1
    val toExpressionNode: Expression => ExpressionNode = if (rowConstructed) {
      // If the input is row constructed, use selection to get the column.
      (_: Expression) =>
        colIdx += 1
        ExpressionBuilder.makeSelection(colIdx)
    } else {
      (expr: Expression) =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, originalInputAttributes)
          .doTransform(context)
    }

    val groupingList = groupingExpressions.map(toExpressionNode).asJava
    // Get the aggregate function nodes.
    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          aggFilterList.add(toExpressionNode(aggExpr.filter.get))
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = aggExpr.mode match {
          case Partial | Complete =>
            aggregateFunc.children.toList.map(toExpressionNode)
          case PartialMerge | Final =>
            if (rowConstructed) {
              // Only occupies one column due to intermediate results are combined
              // by previous row construct projection.
              Seq(toExpressionNode.apply(null))
            } else {
              rewriteAggBufferAttributes(
                aggregateFunc.inputAggBufferAttributes,
                originalInputAttributes).map(toExpressionNode)
            }
          case other =>
            throw new GlutenNotSupportException(s"$other not supported.")
        }
        aggregateFunctionList.add(
          makeFunctionNode(context, aggregateFunc, childrenNodes.asJava, aggExpr.mode))
      })

    val extensionNode = getAdvancedExtension(validation && !rowConstructed, originalInputAttributes)
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  private def getAdvancedExtension(
      validation: Boolean = false,
      originalInputAttributes: Seq[Attribute] = Seq.empty): AdvancedExtensionNode = {
    val enhancement = if (validation) {
      // Use an extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)
    } else {
      null
    }

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder
          .setValue(formatExtOptimizationString(isCapableForStreamingAggregation))
          .build)
    ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)
  }

  def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)
}

/** An aggregation function builder specifically used by Velox backend. */
object VeloxAggregateFunctionsBuilder {

  /**
   * Create a scalar function for the input aggregate function.
   * @param context:
   *   the SubstraitContext.
   * @param aggregateFunc:
   *   the input aggregate function.
   * @param mode:
   *   the mode of input aggregate function.
   * @return
   */
  def create(
      context: SubstraitContext,
      aggregateFunc: AggregateFunction,
      mode: AggregateMode): Long = {
    val (sigName, aggFunc) =
      try {
        (AggregateFunctionsBuilder.getSubstraitFunctionName(aggregateFunc), aggregateFunc)
      } catch {
        case e: GlutenNotSupportException =>
          HiveUDAFInspector.getUDAFClassName(aggregateFunc) match {
            case Some(udafClass) if UDFResolver.UDAFNames.contains(udafClass) =>
              (udafClass, UDFResolver.getUdafExpression(udafClass)(aggregateFunc.children))
            case _ => throw e
          }
        case e: Throwable => throw e
      }

    context.registerFunction(
      ConverterUtils.makeFuncName(
        // Substrait-to-Velox procedure will choose appropriate companion function if needed.
        sigName,
        VeloxIntermediateData.getInputTypes(aggFunc, mode == PartialMerge || mode == Final),
        FunctionConfig.REQ
      )
    )
  }
}

// Hash aggregation that emits full-aggregated data, this works like regular hash
// aggregation in Vanilla Spark.
case class RegularHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override protected def allowFlush: Boolean = false

  override def simpleString(maxFields: Int): String =
    s"${super.simpleString(maxFields)}"

  override def verboseString(maxFields: Int): String =
    s"${super.verboseString(maxFields)}"

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer = {
    copy(child = newChild)
  }
}

// Hash aggregation that emits pre-aggregated data which allows duplications on grouping keys
// among its output rows.
case class FlushableHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override protected def allowFlush: Boolean = true

  override def simpleString(maxFields: Int): String =
    s"Flushable${super.simpleString(maxFields)}"

  override def verboseString(maxFields: Int): String =
    s"Flushable${super.verboseString(maxFields)}"

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExecTransformer = {
    copy(child = newChild)
  }
}

case class HashAggregateExecPullOutHelper(
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute])
  extends HashAggregateExecPullOutBaseHelper {

  /** This method calculates the output attributes of Aggregation. */
  override protected def getAttrForAggregateExprs: List[Attribute] = {
    aggregateExpressions.zipWithIndex.flatMap {
      case (expr, index) =>
        expr.mode match {
          case Partial | PartialMerge =>
            expr.aggregateFunction.aggBufferAttributes
          case Final | Complete =>
            Seq(aggregateAttributes(index))
          case other =>
            throw new GlutenNotSupportException(s"Unsupported aggregate mode: $other.")
        }
    }.toList
  }
}
