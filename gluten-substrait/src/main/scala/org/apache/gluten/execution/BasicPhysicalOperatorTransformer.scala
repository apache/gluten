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
import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.SparkContextUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.utils.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

abstract class FilterExecTransformerBase(val cond: Expression, val input: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetrics(sparkContext)

  // Split out all the IsNotNulls from condition.
  protected val (notNullPreds, _) = splitConjunctivePredicates(cond).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  protected val notNullAttributes: Seq[ExprId] =
    notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def isNoop: Boolean = getRemainingCondition == null

  override def metricsUpdater(): MetricsUpdater = if (isNoop) {
    MetricsUpdater.None
  } else {
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)
  }

  def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, originalInputAttributes)
      .doTransform(context)
    RelBuilder.makeFilterRel(
      context,
      condExprNode,
      originalInputAttributes.asJava,
      operatorId,
      input,
      validation
    )
  }

  override def output: Seq[Attribute] = {
    child.output.map {
      a =>
        if (a.nullable && notNullAttributes.contains(a.exprId)) {
          a.withNullability(false)
        } else {
          a
        }
    }
  }

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = child.output

  // FIXME: Should use field "condition" to store the actual executed filter expressions.
  //  To make optimization easier (like to remove filter when it actually does nothing)
  protected def getRemainingCondition: Expression = {
    val scanFilters = child match {
      // Get the filters including the manually pushed down ones.
      case basicScanExecTransformer: BasicScanExecTransformer =>
        basicScanExecTransformer.filterExprs()
      // For fallback scan, we need to keep original filter.
      case _ =>
        Seq.empty[Expression]
    }
    if (scanFilters.isEmpty) {
      cond
    } else {
      val remainingFilters =
        FilterHandler.subtractFilters(splitConjunctivePredicates(cond), scanFilters)
      remainingFilters.reduceLeftOption(And).orNull
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val remainingCondition = getRemainingCondition
    if (remainingCondition == null) {
      // All the filters can be pushed down and the computing of this Filter
      // is not needed.
      return ValidationResult.succeeded
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode = getRelNode(
      substraitContext,
      remainingCondition,
      child.output,
      operatorId,
      null,
      validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    if (isNoop) {
      // The computing for this filter is not needed.
      // Since some columns' nullability will be removed after this filter, we need to update the
      // outputAttributes of child context.
      return TransformContext(output, childCtx.root)
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    val remainingCondition = getRemainingCondition
    val currRel = getRelNode(
      context,
      remainingCondition,
      child.output,
      operatorId,
      childCtx.root,
      validation = false)
    assert(currRel != null, "Filter rel should be valid.")
    TransformContext(output, currRel)
  }
}

abstract class ProjectExecTransformerBase(val list: Seq[NamedExpression], val input: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    failValidationWithException {
      val relNode =
        getRelNode(substraitContext, list, child.output, operatorId, null, validation = true)
      // Then, validate the generated plan in native engine.
      doNativeValidation(substraitContext, relNode)
    }()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, list, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Project Rel should be valid")
    TransformContext(output, currRel)
  }

  override def output: Seq[Attribute] = list.map(_.toAttribute)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = list

  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(projectList, originalInputAttributes)
    val projExprNodeList = columnarProjExprs.map(_.doTransform(context)).asJava
    RelBuilder.makeProjectRel(
      originalInputAttributes.asJava,
      input,
      projExprNodeList,
      context,
      operatorId,
      validation)
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", list)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }
}

// An alternative for UnionExec.
case class ColumnarUnionExec(children: Seq[SparkPlan], partitioning: Partitioning)
  extends ValidatablePlan {
  require(children.nonEmpty, "ColumnarUnionExec requires at least one child.")
  children.foreach {
    case w: WholeStageTransformer =>
      // FIXME: Avoid such practice for plan immutability.
      w.setOutputSchemaForPlan(output)
    case _ =>
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override def outputPartitioning: Partitioning = partitioning

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map {
      attrs =>
        val firstAttr = attrs.head
        val nullable = attrs.exists(_.nullable)
        val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.unionLikeMerge)
        if (firstAttr.dataType == newDt) {
          firstAttr.withNullability(nullable)
        } else {
          AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
            firstAttr.exprId,
            firstAttr.qualifier)
        }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): ColumnarUnionExec =
    copy(children = newChildren)

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (outputPartitioning.isInstanceOf[UnknownPartitioning]) {
      sparkContext.union(children.map(c => c.executeColumnar()))
    } else {
      // This union has a known partitioning, i.e., its children have the same partitioning
      // in semantics so this union can choose not to change the partitioning by using a
      // custom partitioning aware union RDD.
      val nonEmptyRdds = children.map(_.executeColumnar()).filter(!_.partitions.isEmpty)
      SparkContextUtils.createPartitioningAwareUnionRDD(
        sparkContext,
        nonEmptyRdds,
        outputPartitioning.numPartitions)
    }
  }
}

object ColumnarUnionExec {
  def from(union: UnionExec): ColumnarUnionExec = {
    val children = union.children
    val outputPartitioning: Partitioning = union.outputPartitioning
    ColumnarUnionExec(children, outputPartitioning)
  }
}

/**
 * Contains functions for the comparision and separation of the filter conditions in Scan and
 * Filter. Contains the function to manually push down the conditions into Scan.
 */
object FilterHandler extends PredicateHelper {

  /**
   * Get the original filter conditions in Scan for the comparison with those in Filter.
   *
   * @param plan
   *   : the Spark plan
   * @return
   *   If the plan is FileSourceScanExec or BatchScanExec, return the filter conditions in it.
   *   Otherwise, return empty sequence.
   */
  def getScanFilters(plan: SparkPlan): Seq[Expression] = {
    plan match {
      case fileSourceScan: FileSourceScanExec =>
        fileSourceScan.dataFilters
      case batchScan: BatchScanExec =>
        batchScan.scan match {
          case scan: FileScan =>
            scan.dataFilters
          case _ =>
            throw new GlutenNotSupportException(
              s"${batchScan.scan.getClass.toString} is not supported")
        }
      case _ =>
        Seq()
    }
  }

  def subtractFilters(left: Seq[Expression], right: Seq[Expression]): Seq[Expression] = {
    val scanSet = right.map(_.canonicalized).toSet
    left.filter(f => !scanSet.contains(f.canonicalized))
  }

  def combineFilters(left: Seq[Expression], right: Seq[Expression]): Seq[Expression] = {
    (left.toSet ++ right.toSet).toSeq
  }
}
