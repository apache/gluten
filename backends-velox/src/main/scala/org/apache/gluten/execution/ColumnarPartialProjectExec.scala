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
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{ArrowProjection, ConverterUtils, ExpressionConverter, ExpressionMappings, ExpressionUtils, TransformerState}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.vectorized.{ArrowColumnarRow, ArrowWritableColumnVector}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{ExplainUtils, OrderPreservingNodeShim, PartitioningPreservingNodeShim, ProjectExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.hive.{HiveUDFTransformer, VeloxHiveUDFTransformer}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * By rule <PartialProjectRule>, the project not offload-able that is changed to
 * ProjectExecTransformer + ColumnarPartialProjectExec e.g. sum(myudf(a) + b + hash(c)), child is
 * (a, b, c) ColumnarPartialProjectExec (a, b, c, myudf(a) as _SparkPartialProject1),
 * ProjectExecTransformer(_SparkPartialProject1 + b + hash(c))
 *
 * @param projectList
 *   The project output, with this argument in case class, function QueryPlan.expressions can return
 *   the Expression list correctly, then the function executeQuery can find the SubQuery from
 *   Expression
 * @param child
 *   child plan
 */
case class ColumnarPartialProjectExec(projectList: Seq[Expression], child: SparkPlan)(
    replacedAlias: Seq[Alias])
  extends UnaryExecNode
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with ValidatablePlan {

  private val projectAttributes: ListBuffer[Attribute] = ListBuffer()
  private val projectIndexInChild: ListBuffer[Int] = ListBuffer()
  private var attrNotExists = false
  private var hasUnsupportedDataType = false
  getProjectIndexInChildOutput(replacedAlias)

  @transient override lazy val metrics = Map(
    "time" -> SQLMetrics.createTimingMetric(sparkContext, "total time of partial project"),
    "velox_to_arrow_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of velox to Arrow ColumnarBatch"),
    "arrow_to_velox_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of Arrow ColumnarBatch to velox")
  )

  override def output: Seq[Attribute] = child.output ++ replacedAlias.map(_.toAttribute)

  override def doCanonicalize(): ColumnarPartialProjectExec = {
    super
      .doCanonicalize()
      .asInstanceOf[ColumnarPartialProjectExec]
      .copy()(replacedAlias = replacedAlias.map(QueryPlan.normalizeExpressions(_, child.output)))
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  final override protected def otherCopyArgs: Seq[AnyRef] = {
    replacedAlias :: Nil
  }

  private def getProjectIndexInChildOutput(exprs: Seq[Expression]): Unit = {
    exprs.forall {
      case a: AttributeReference =>
        val index = child.output.indexWhere(s => s.exprId.equals(a.exprId))
        // Some child operator as HashAggregateTransformer will not have udf child column
        if (index < 0) {
          attrNotExists = true
          log.debug(s"Expression $a should exist in child output ${child.output}")
          false
        } else if (
          BackendsApiManager.getValidatorApiInstance.doSchemaValidate(a.dataType).isDefined
        ) {
          hasUnsupportedDataType = true
          log.debug(s"Expression $a contains unsupported data type ${a.dataType}")
          false
        } else if (!projectIndexInChild.contains(index)) {
          projectAttributes.append(a.toAttribute)
          projectIndexInChild.append(index)
          true
        } else true
      case p =>
        getProjectIndexInChildOutput(p.children)
        true
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (attrNotExists) {
      return ValidationResult.failed(
        "Attribute in the partial projected expressions does not exists in its child")
    }
    if (hasUnsupportedDataType) {
      return ValidationResult.failed(
        "Attribute in the partial projected expressions contains unsupported type")
    }
    if (projectAttributes.size == child.output.size) {
      return ValidationResult.failed(
        "The partial projected expressions need all the columns in child output")
    }
    if (replacedAlias.isEmpty) {
      return ValidationResult.failed("No UDF or blacklisted expressions")
    }
    if (replacedAlias.size > projectList.size) {
      // e.g. udf1(col) + udf2(col), it will introduce 2 cols for a2c
      return ValidationResult.failed("Number of RowToColumn columns is more than ProjectExec")
    }
    if (!projectList.forall(ColumnarPartialProjectExec.validateExpression)) {
      return ValidationResult.failed("Contains expression not supported")
    }
    if (
      ExpressionUtils.hasComplexExpressions(
        projectList,
        GlutenConfig.get.fallbackExpressionsThreshold)
    ) {
      return ValidationResult.failed("Fallback by complex expression")
    }
    ValidationResult.succeeded
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val totalTime = longMetric("time")
    val c2a = longMetric("velox_to_arrow_time")
    val a2c = longMetric("arrow_to_velox_time")
    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          // select part of child output and child data
          val projection: ArrowProjection =
            ArrowProjection.create(replacedAlias, projectAttributes.toSeq)

          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows == 0) {
              Iterator.empty
            } else {
              val start = System.currentTimeMillis()
              val childData = ColumnarBatches
                .select(BackendsApiManager.getBackendName, batch, projectIndexInChild.toArray)
              try {
                val projectedBatch = getProjectedBatchArrow(childData, projection, c2a, a2c)
                val batchIterator = projectedBatch.map {
                  b =>
                    if (b.numCols() != 0) {
                      val compositeBatch = VeloxColumnarBatches.compose(batch, b)
                      b.close()
                      compositeBatch
                    } else {
                      b.close()
                      ColumnarBatches.retain(batch)
                      batch
                    }
                }
                totalTime += System.currentTimeMillis() - start
                batchIterator
              } finally {
                childData.close()
              }
            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
          // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
          .recyclePayload(_.close())
          .create()

    }
  }

  private def getProjectedBatchArrow(
      childData: ColumnarBatch,
      proj: ArrowProjection,
      c2a: SQLMetric,
      a2c: SQLMetric): Iterator[ColumnarBatch] = {
    val numRows = childData.numRows()
    val start = System.currentTimeMillis()
    val sparkColumnarBatch = if (childData.numCols() == 0) {
      childData
    } else {
      ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), childData)
    }
    // In spark with version belows 4.0, the `ColumnarRow`'s get method doesn't check whether the
    // column to get is null, so we change it to `ArrowColumnarBatch` manually. `ArrowColumnarBatch`
    // returns `ArrowColumnarRow`, which fixes the bug.
    val arrowBatch = ColumnarBatches.convertToArrowColumnarBatch(sparkColumnarBatch)
    c2a += System.currentTimeMillis() - start

    val schema =
      SparkShimLoader.getSparkShims.structFromAttributes(replacedAlias.map(_.toAttribute))
    val vectors: Array[ArrowWritableColumnVector] = ArrowWritableColumnVector
      .allocateColumns(numRows, schema)
      .map {
        vector =>
          vector.setValueCount(numRows)
          vector
      }
    val targetRow = new ArrowColumnarRow(vectors)
    for (i <- 0 until numRows) {
      targetRow.rowId = i
      proj.target(targetRow).apply(arrowBatch.getRow(i))
    }
    targetRow.finishWriteRow()
    val targetBatch = new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), numRows)
    val start2 = System.currentTimeMillis()
    val veloxBatch = VeloxColumnarBatches.toVeloxBatch(
      ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), targetBatch))
    a2c += System.currentTimeMillis() - start2
    Iterators
      .wrap(Iterator.single(veloxBatch))
      .recycleIterator({
        arrowBatch.close()
        targetBatch.close()
      })
      .create()
    // TODO: should check the size <= 1, but now it has bug, will change iterator to empty
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("UDF", replacedAlias)}
       |${ExplainUtils.generateFieldString("ProjectOutput", projectAttributes)}
       |${ExplainUtils.generateFieldString("ProjectInputIndex", projectIndexInChild)}
       |""".stripMargin
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + " PartialProject " + replacedAlias

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarPartialProjectExec = {
    copy(child = newChild)(replacedAlias)
  }

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = child.output ++ replacedAlias
}

object ColumnarPartialProjectExec {

  val projectPrefix = "_SparkPartialProject"

  val dummyPrefix = "_dummy"

  def validateExpression(expr: Expression): Boolean = {
    expr.deterministic && !expr.isInstanceOf[LambdaFunction] && expr.children
      .forall(validateExpression)
  }

  /** Check if it's a hive udf but not transformable */
  private def containsUnsupportedHiveUDF(h: Expression): Boolean = {
    HiveUDFTransformer.isHiveUDF(h) && !VeloxHiveUDFTransformer.isSupportedHiveUDF(h)
  }

  private def isBlacklistExpression(e: Expression): Boolean = {
    ExpressionMappings.blacklistExpressionMap.contains(e.getClass)
  }

  private def containsUDFOrBlacklistExpression(expr: Expression): Boolean = {
    if (expr == null) return false
    expr match {
      case _: ScalaUDF => true
      case h if containsUnsupportedHiveUDF(h) => true
      case e if isBlacklistExpression(e) => true
      case p => p.children.exists(c => containsUDFOrBlacklistExpression(c))
    }
  }

  private def replaceByAlias(expr: Expression, replacedAliasUdf: ListBuffer[Alias]): Expression = {
    val replaceIndex = replacedAliasUdf.indexWhere(r => r.child.equals(expr))
    if (replaceIndex == -1) {
      val replace = Alias(expr, s"$projectPrefix${replacedAliasUdf.size}")()
      replacedAliasUdf.append(replace)
      replace.toAttribute
    } else {
      replacedAliasUdf(replaceIndex).toAttribute
    }
  }

  private def isConditionalExpression(expr: Expression): Boolean = expr match {
    case _: If => true
    case _: CaseWhen => true
    case _: NaNvl => true
    case _: Coalesce => true
    case _ => false
  }

  private def replaceExpression(expr: Expression, replacedAlias: ListBuffer[Alias]): Expression = {
    if (expr == null) return null
    expr match {
      case u: ScalaUDF =>
        replaceByAlias(u, replacedAlias)
      case h if containsUnsupportedHiveUDF(h) =>
        replaceByAlias(h, replacedAlias)
      case e if isBlacklistExpression(e) =>
        replaceByAlias(e, replacedAlias)
      case au @ Alias(_: ScalaUDF, _) =>
        val replaceIndex = replacedAlias.indexWhere(r => r.exprId == au.exprId)
        if (replaceIndex == -1) {
          replacedAlias.append(au)
          au.toAttribute
        } else {
          replacedAlias(replaceIndex).toAttribute
        }
      // Alias(HiveSimpleUDF) not exists, only be Alias(ToPrettyString(HiveSimpleUDF)),
      // so don't process this condition
      case x if isConditionalExpression(x) =>
        // For example:
        // myudf is udf((x: Int) => x + 1)
        // if (isnull(cast(l_extendedprice#9 as bigint))) null
        // else myudf(knownnotnull(cast(l_extendedprice#9 as bigint)))
        // if we extract else branch, and use the data child l_extendedprice,
        // the result is incorrect for null value
        if (containsUDFOrBlacklistExpression(expr)) {
          replaceByAlias(expr, replacedAlias)
        } else expr
      case p => p.withNewChildren(p.children.map(c => replaceExpression(c, replacedAlias)))
    }
  }

  private def doNativeValidateExpression(
      expr: Expression,
      replacedAlias: ListBuffer[Alias],
      childOutput: Seq[Attribute]): Boolean = {
    val substraitContext = new SubstraitContext
    val output = childOutput ++ replacedAlias.map(_.toAttribute)
    val exprTransformer = ExpressionConverter.replaceWithExpressionTransformer(expr, output)
    val inputTypeNodeList = TypeBuilder.makeStruct(
      false,
      output
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava)
    BackendsApiManager.getValidatorApiInstance.doNativeValidateExpression(
      substraitContext,
      exprTransformer.doTransform(substraitContext),
      inputTypeNodeList)
  }

  /**
   * Traverse up the expression (post-order). If the function finds an expression that is not
   * supported by the native backend, then the function replaces the expression by `Alias`.
   *
   * @return
   *   the new expression
   */
  private def traverseUpExpression(
      expr: Expression,
      replacedAlias: ListBuffer[Alias],
      childOutput: Seq[Attribute]): Expression = {
    val newExpr = expr.withNewChildren(expr.children.map {
      case a: AttributeReference => a
      case child =>
        val newChild = child.withNewChildren(
          child.children.map(c => traverseUpExpression(c, replacedAlias, childOutput)))
        // To prevent nested expressions be validated multiple times, before doing the validation,
        // we replace it by `Alias`.
        val tempAttributes = new ListBuffer[Attribute]()
        val tempChildren = child.children.zipWithIndex.map(
          c => {
            val child = c._1
            val a = AttributeReference(s"$dummyPrefix${c._2}", child.dataType, child.nullable)()
            tempAttributes.append(a)
            a
          })
        // For CreateNamedStruct, we need to create it with `CreateStruct.apply`.
        val toValidatedExpression = child match {
          case CreateNamedStruct(_) => CreateStruct(tempChildren)
          case _ => child.withNewChildren(tempChildren)
        }
        if (
          !doNativeValidateExpression(
            toValidatedExpression,
            replacedAlias,
            childOutput ++ tempAttributes)
        ) {
          replaceByAlias(newChild, replacedAlias)
        } else {
          newChild
        }
    })
    if (!doNativeValidateExpression(newExpr, replacedAlias, childOutput)) {
      replaceByAlias(newExpr, replacedAlias)
    } else {
      newExpr
    }
  }

  private def replaceExpression(
      expr: Expression,
      childOutput: Seq[Attribute],
      replacedAlias: ListBuffer[Alias]): Expression = {
    if (expr == null) return null
    val newExpr = replaceExpression(expr, replacedAlias)
    if (!GlutenConfig.get.enableNativeValidation || !validateExpression(newExpr)) {
      return newExpr
    }
    newExpr match {
      case _: AttributeReference => newExpr
      case alias @ Alias(child, name) =>
        val newChild = replaceExpression(child, childOutput, replacedAlias)
        Alias(newChild, name)(
          alias.exprId,
          alias.qualifier,
          alias.explicitMetadata,
          alias.nonInheritableMetadataKeys)
      case x if isConditionalExpression(x) =>
        try {
          TransformerState.enterValidation
          if (!doNativeValidateExpression(x, replacedAlias, childOutput)) {
            replaceByAlias(x, replacedAlias)
          } else {
            x
          }
        } catch {
          case _: Throwable =>
            // If the process of conversion of the expression throws exception, then we need to
            // fallback the whole operator.
            newExpr
        } finally {
          TransformerState.finishValidation
        }
      case p =>
        try {
          TransformerState.enterValidation
          if (doNativeValidateExpression(p, replacedAlias, childOutput)) {
            // Fast path: if the expression is supported by the native backend,
            // then we don't need to traverse up the expression.
            newExpr
          } else {
            // The expression is not supported by the native backend, then we traverse down the
            // expression to find which expression the native backend does not support。
            traverseUpExpression(p, replacedAlias, childOutput)
          }
        } catch {
          case _: Throwable =>
            // If the process of conversion of the expression throws exception, then we need to
            // fallback the whole operator. The unsupported expression may cause the calculation
            // crash. For example, the result data type of the expression is decimal and the scale
            // of the decimal is negative, but velox don't allow decimal type with negative scale.
            newExpr
        } finally {
          TransformerState.finishValidation
        }
    }
  }

  def create(original: ProjectExec): ProjectExecTransformer = {
    val replacedAlias: ListBuffer[Alias] = ListBuffer()
    val newProjectList = original.projectList.map {
      p => replaceExpression(p, original.child.output, replacedAlias).asInstanceOf[NamedExpression]
    }
    val partialProject =
      ColumnarPartialProjectExec(original.projectList, original.child)(replacedAlias.toSeq)
    ProjectExecTransformer(newProjectList, partialProject)
  }
}
