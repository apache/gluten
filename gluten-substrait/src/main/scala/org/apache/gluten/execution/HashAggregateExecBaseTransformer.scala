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
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression._
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.rel.RelNode

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.types._

/** Columnar Based HashAggregateExec. */
abstract class HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends BaseAggregateExec
  with UnaryTransformSupport {

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetrics(sparkContext)

  protected def isCapableForStreamingAggregation: Boolean = {
    if (!glutenConf.getConf(GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE)) {
      return false
    }
    if (groupingExpressions.isEmpty) {
      return false
    }

    val childOrdering = child match {
      case agg: HashAggregateExecBaseTransformer
          if agg.groupingExpressions == this.groupingExpressions =>
        // If the child aggregate supports streaming aggregate then the ordering is not changed.
        // So we can propagate ordering if there is no shuffle exchange between aggregates and
        // they have same grouping keys,
        agg.child.outputOrdering
      case _ => child.outputOrdering
    }
    val requiredOrdering = groupingExpressions.map(expr => SortOrder.apply(expr, Ascending))
    SortOrder.orderingSatisfies(childOrdering, requiredOrdering)
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetricsUpdater(metrics)

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"HashAggregateTransformer(keys=$keyString, " +
        s"functions=$functionString, " +
        s"isStreamingAgg=$isCapableForStreamingAggregation, " +
        s"output=$outputString)"
    } else {
      s"HashAggregateTransformer(keys=$keyString, " +
        s"functions=$functionString, " +
        s"isStreamingAgg=$isCapableForStreamingAggregation)"
    }
  }

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  protected def checkType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | StringType | TimestampType | DateType | BinaryType =>
        true
      case _: NumericType => true
      case _: ArrayType => true
      case _: StructType => true
      case _: NullType => true
      case _ => false
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val aggParams = new AggregationParams
    val relNode = getAggRel(substraitContext, operatorId, aggParams, null, validation = true)

    val unsupportedAggExprs = aggregateAttributes.filterNot(attr => checkType(attr.dataType))
    if (unsupportedAggExprs.nonEmpty) {
      return ValidationResult.failed(
        "Found unsupported data type in aggregation expression: " +
          unsupportedAggExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    val unsupportedGroupExprs = groupingExpressions.filterNot(attr => checkType(attr.dataType))
    if (unsupportedGroupExprs.nonEmpty) {
      return ValidationResult.failed(
        "Found unsupported data type in grouping expression: " +
          unsupportedGroupExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    aggregateExpressions.foreach {
      expr =>
        if (!checkAggFuncModeSupport(expr.aggregateFunction, expr.mode)) {
          throw new GlutenNotSupportException(
            s"Unsupported aggregate mode: ${expr.mode} for ${expr.aggregateFunction.prettyName}")
        }
    }
    doNativeValidation(substraitContext, relNode)
  }

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  protected def checkAggFuncModeSupport(
      aggFunc: AggregateFunction,
      mode: AggregateMode): Boolean = {
    aggFunc match {
      case s: Sum if s.prettyName.equals("try_sum") => false
      case bloom if bloom.getClass.getSimpleName.equals("BloomFilterAggregate") =>
        mode match {
          case Partial | Final | Complete => true
          case _ => false
        }
      case _ => true
    }
  }

  protected def modeToKeyWord(aggregateMode: AggregateMode): String = {
    aggregateMode match {
      case Partial => "PARTIAL"
      case PartialMerge => "PARTIAL_MERGE"
      case Complete => "COMPLETE"
      case Final => "FINAL"
      case other =>
        throw new GlutenNotSupportException(s"not currently supported: $other.")
    }
  }

  protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode
}

object HashAggregateExecBaseTransformer {

  private def getInitialInputBufferOffset(agg: BaseAggregateExec): Int = agg match {
    case a: HashAggregateExec => a.initialInputBufferOffset
    case a: ObjectHashAggregateExec => a.initialInputBufferOffset
    case a: SortAggregateExec => a.initialInputBufferOffset
  }

  def from(agg: BaseAggregateExec): HashAggregateExecBaseTransformer = {
    BackendsApiManager.getSparkPlanExecApiInstance
      .genHashAggregateExecTransformer(
        agg.requiredChildDistributionExpressions,
        agg.groupingExpressions,
        agg.aggregateExpressions,
        agg.aggregateAttributes,
        getInitialInputBufferOffset(agg),
        agg.resultExpressions,
        agg.child
      )
  }

  def fromSortAggregate(agg: BaseAggregateExec): HashAggregateExecBaseTransformer = {
    BackendsApiManager.getSparkPlanExecApiInstance
      .genSortAggregateExecTransformer(
        agg.requiredChildDistributionExpressions,
        agg.groupingExpressions,
        agg.aggregateExpressions,
        agg.aggregateAttributes,
        getInitialInputBufferOffset(agg),
        agg.resultExpressions,
        agg.child
      )
  }
}

/**
 * Marker trait for hash aggregate transformers that were offloaded from a SortAggregateExec. This
 * allows sort elimination rules to distinguish aggregates that were originally sort-based (and thus
 * can safely eliminate their upstream sort) from regular hash aggregates (which must not).
 */
trait SortAggregateExecTransformer extends HashAggregateExecBaseTransformer {}

trait HashAggregateExecPullOutBaseHelper {
  // The direct outputs of Aggregation.
  def allAggregateResultAttributes(groupingExpressions: Seq[NamedExpression]): List[Attribute] =
    groupingExpressions.map(ConverterUtils.getAttrFromExpr(_)).toList :::
      getAttrForAggregateExprs

  /** This method calculates the output attributes of Aggregation. */
  protected def getAttrForAggregateExprs: List[Attribute]
}
