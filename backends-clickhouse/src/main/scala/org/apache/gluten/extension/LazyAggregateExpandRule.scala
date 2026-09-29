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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/*
 * For aggregation with grouping sets, we need to expand the grouping sets
 * to individual group by.
 * 1. It need to make copies of the original data.
 * 2. run the aggregation on the multi copied data.
 * Both of these two are expensive.
 *
 * We could do this as following
 * 1. Run the aggregation on full grouping keys.
 * 2. Expand the aggregation result to the full grouping sets.
 * 3. Run the aggregation on the expanded data.
 *
 * So the plan is transformed from
 *   expand -> partial aggregating -> shuffle -> final merge aggregating
 * to
 *   partial aggregating -> expand -> shuffle -> final merge aggregating
 *
 * Notice:
 * If the aggregation involves distinct, we can't do this optimization.
 */

case class LazyAggregateExpandRule(session: SparkSession) extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    logDebug(s"xxx enable lazy aggregate expand: ${CHBackendSettings.enableLazyAggregateExpand}")
    if (!CHBackendSettings.enableLazyAggregateExpand) {
      return plan
    }
    plan.transformUp {
      case shuffle @ ColumnarShuffleExchangeExec(
            HashPartitioning(hashExpressions, _),
            CHHashAggregateExecTransformer(
              _,
              groupingExpressions,
              aggregateExpressions,
              _,
              _,
              resultExpressions,
              ExpandExecTransformer(projections, output, child)),
            _,
            _,
            _,
            _
          ) =>
        logDebug(s"xxx match plan:$shuffle")
        val partialAggregate = shuffle.child.asInstanceOf[CHHashAggregateExecTransformer]
        val expand = partialAggregate.child.asInstanceOf[ExpandExecTransformer]
        logDebug(
          s"xxx partialAggregate: groupingExpressions:" +
            s"${partialAggregate.groupingExpressions}\n" +
            s"aggregateAttributes:${partialAggregate.aggregateAttributes}\n" +
            s"aggregateExpressions:${partialAggregate.aggregateExpressions}\n" +
            s"resultExpressions:${partialAggregate.resultExpressions}")
        if (doValidation(partialAggregate, expand, shuffle)) {

          val attributesToReplace = buildReplaceAttributeMap(expand)
          logDebug(s"xxx attributesToReplace: $attributesToReplace")

          val newPartialAggregate = buildAheadAggregateExec(
            partialAggregate,
            expand,
            attributesToReplace
          )

          val newExpand = buildPostExpandExec(
            expand,
            partialAggregate,
            newPartialAggregate,
            attributesToReplace
          )

          val newShuffle = shuffle.copy(child = newExpand)
          logDebug(s"xxx new plan: $newShuffle")
          newShuffle
        } else {
          shuffle
        }
      case shuffle @ ColumnarShuffleExchangeExec(
            HashPartitioning(hashExpressions, _),
            CHHashAggregateExecTransformer(
              _,
              groupingExpressions,
              aggregateExpressions,
              _,
              _,
              resultExpressions,
              FilterExecTransformer(_, ExpandExecTransformer(projections, output, child))),
            _,
            _,
            _,
            _
          ) =>
        val partialAggregate = shuffle.child.asInstanceOf[CHHashAggregateExecTransformer]
        val filter = partialAggregate.child.asInstanceOf[FilterExecTransformer]
        val expand = filter.child.asInstanceOf[ExpandExecTransformer]
        logDebug(
          s"xxx partialAggregate: groupingExpressions:" +
            s"${partialAggregate.groupingExpressions}\n" +
            s"aggregateAttributes:${partialAggregate.aggregateAttributes}\n" +
            s"aggregateExpressions:${partialAggregate.aggregateExpressions}\n" +
            s"resultExpressions:${partialAggregate.resultExpressions}")
        if (doValidation(partialAggregate, expand, shuffle)) {
          val attributesToReplace = buildReplaceAttributeMap(expand)
          logDebug(s"xxx attributesToReplace: $attributesToReplace")

          val newPartialAggregate = buildAheadAggregateExec(
            partialAggregate,
            expand,
            attributesToReplace
          )

          val newExpand = buildPostExpandExec(
            expand,
            partialAggregate,
            newPartialAggregate,
            attributesToReplace
          )

          val newFilter = filter.copy(child = newExpand)
          newFilter.copyTagsFrom(filter)

          val newShuffle = shuffle.copy(child = newFilter)
          newShuffle.copyTagsFrom(shuffle)
          logDebug(s"xxx new plan: $newShuffle")
          newShuffle

        } else {
          shuffle
        }
    }
  }

  // Just enable for simple cases. Some of cases that are not supported:
  // 1. select count(a),count(b), count(1), count(distinct(a)), count(distinct(b)) from values
  //    (1, null), (2,2) as data(a,b);
  // 2. select n_name, count(distinct n_regionkey) as col1,
  //    count(distinct concat(n_regionkey, n_nationkey)) as col2 from
  //    nation group by n_name;
  def doValidation(
      aggregate: CHHashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      shuffle: ColumnarShuffleExchangeExec): Boolean = {
    // all grouping keys must be attribute references
    val expandOutputAttributes = expand.child.output.toSet
    if (
      !aggregate.groupingExpressions.forall(
        e => e.isInstanceOf[Attribute] || e.isInstanceOf[Literal])
    ) {
      logDebug(s"xxx Not all grouping expression are attribute references")
      return false
    }
    // all shuffle keys must be attribute references
    if (
      !shuffle.outputPartitioning
        .asInstanceOf[HashPartitioning]
        .expressions
        .forall(e => e.isInstanceOf[Attribute] || e.isInstanceOf[Literal])
    ) {
      logDebug(s"xxx Not all shuffle hash expression are attribute references")
      return false
    }

    // 1. for safty, we don't enbale this optimization for all aggregate functions.
    // 2. if any aggregate function uses attributes which is not from expand's child, we don't
    // enable this
    if (
      !aggregate.aggregateExpressions.forall {
        e =>
          isValidAggregateFunction(e) &&
          e.aggregateFunction.references.forall(
            attr => expandOutputAttributes.find(_.semanticEquals(attr)).isDefined)
      }
    ) {
      logDebug(s"xxx Some aggregate functions are not supported")
      return false
    }

    // get the group id's position in the expand's output
    val gidIndex = findGroupingIdIndex(expand)
    gidIndex != -1
  }

  // group id column doesn't have a fixed position, so we need to find it.
  def findGroupingIdIndex(expand: ExpandExecTransformer): Int = {
    def isValidGroupIdColumn(e: Expression, gids: Set[Long]): Long = {
      if (!e.isInstanceOf[Literal]) {
        return -1
      }
      val literalValue = e.asInstanceOf[Literal].value
      e.dataType match {
        case _: LongType =>
          if (gids.contains(literalValue.asInstanceOf[Long])) {
            -1
          } else {
            literalValue.asInstanceOf[Long]
          }
        case _: IntegerType =>
          if (gids.contains(literalValue.asInstanceOf[Int].toLong)) {
            -1
          } else {
            literalValue.asInstanceOf[Int].toLong
          }
        case _ => -1
      }
    }

    var groupIdIndexes = Seq[Int]()
    for (col <- 0 until expand.output.length) {
      val expandCol = expand.projections(0)(col)
      // gids should be unique
      var gids = Set[Long]()
      if (isValidGroupIdColumn(expandCol, gids) != -1) {
        if (
          expand.projections.forall {
            projection =>
              val res = isValidGroupIdColumn(projection(col), gids)
              gids += res
              res != -1
          }
        ) {
          groupIdIndexes +:= col
        }
      }
    }
    if (groupIdIndexes.length == 1) {
      logDebug(s"xxx  gid is at pos ${groupIdIndexes(0)}")
      groupIdIndexes(0)
    } else {
      -1
    }
  }

  // Some of aggregate functions' output columns are not consistent with the output of gluten.
  // - average: in partial aggregation, the outputs are sum and count, but gluten only generates one
  //   column, avg.
  // - sum: if the input's type is decimal, the output are sum and isEmpty, but gluten doesn't use
  //   the isEmpty column.
  def isValidAggregateFunction(aggregateExpression: AggregateExpression): Boolean = {
    if (aggregateExpression.filter.isDefined) {
      return false
    }
    aggregateExpression.aggregateFunction match {
      case _: Count => true
      case _: Max => true
      case _: Min => true
      case _: Average => true
      case _: Sum => true
      case _ => false
    }
  }

  def getReplaceAttribute(
      toReplace: Attribute,
      attributesToReplace: Map[Attribute, Attribute]): Attribute = {
    val kv = attributesToReplace.find(kv => kv._1.semanticEquals(toReplace))
    kv match {
      case Some((_, v)) => v
      case None => toReplace
    }
  }

  def buildReplaceAttributeMap(expand: ExpandExecTransformer): Map[Attribute, Attribute] = {
    var fullExpandProjection = Seq[Expression]()
    for (i <- 0 until expand.projections(0).length) {
      val attr = expand.projections.find(x => x(i).isInstanceOf[Attribute]) match {
        case Some(projection) => projection(i).asInstanceOf[Attribute]
        case None => null
      }
      fullExpandProjection = fullExpandProjection :+ attr
    }

    var attributeMap = Map[Attribute, Attribute]()
    for (i <- 0 until expand.output.length) {
      if (fullExpandProjection(i).isInstanceOf[Attribute]) {
        attributeMap += (expand.output(i) -> fullExpandProjection(i).asInstanceOf[Attribute])
      }
    }
    attributeMap
  }

  def buildPostExpandProjections(
      originalExpandProjections: Seq[Seq[Expression]],
      originalExpandOutput: Seq[Attribute],
      newExpandOutput: Seq[Attribute]): Seq[Seq[Expression]] = {
    val newExpandProjections = originalExpandProjections.map {
      projection =>
        newExpandOutput.map {
          attr =>
            val index = originalExpandOutput.indexWhere(_.semanticEquals(attr))
            if (index != -1) {
              projection(index)
            } else {
              attr
            }
        }
    }
    newExpandProjections
  }

  // 1. make expand's child be aggregate's child
  // 2. replace the attributes in groupingExpressions and resultExpressions as needed
  def buildAheadAggregateExec(
      partialAggregate: CHHashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      attributesToReplace: Map[Attribute, Attribute]): SparkPlan = {
    val groupIdAttribute = expand.output(findGroupingIdIndex(expand))

    // New grouping expressions should include the group id column
    val groupingExpressions =
      partialAggregate.groupingExpressions
        .filter(
          e =>
            !e.toAttribute.semanticEquals(groupIdAttribute) &&
              attributesToReplace.find(kv => kv._1.semanticEquals(e.toAttribute)).isDefined)
        .map(e => getReplaceAttribute(e.toAttribute, attributesToReplace))
        .distinct
    logDebug(
      s"xxx newGroupingExpresion: $groupingExpressions,\n" +
        s"groupingExpressions: ${partialAggregate.groupingExpressions}")

    // Remove group id column from result expressions
    val groupingKeysCount =
      partialAggregate.resultExpressions.length - partialAggregate.aggregateExpressions.length
    var resultExpressions = partialAggregate.resultExpressions
      .slice(0, groupingKeysCount)
      .filter(
        e =>
          !e.toAttribute.semanticEquals(groupIdAttribute) &&
            attributesToReplace.find(kv => kv._1.semanticEquals(e.toAttribute)).isDefined)
      .map(e => getReplaceAttribute(e.toAttribute, attributesToReplace))
    resultExpressions = resultExpressions ++ partialAggregate.resultExpressions
      .slice(groupingKeysCount, partialAggregate.resultExpressions.length)
      .map(e => getReplaceAttribute(e.toAttribute, attributesToReplace))
    logDebug(
      s"xxx newResultExpressions: $resultExpressions\n" +
        s"resultExpressions:${partialAggregate.resultExpressions}")
    partialAggregate.copy(
      groupingExpressions = groupingExpressions,
      resultExpressions = resultExpressions.distinct,
      child = expand.child)
  }

  def buildPostExpandExec(
      expand: ExpandExecTransformer,
      partialAggregate: CHHashAggregateExecTransformer,
      child: SparkPlan,
      attributesToReplace: Map[Attribute, Attribute]): SparkPlan = {
    // The output of the native plan is not completely consistent with Spark.
    val aggregateOutput = partialAggregate.output
    logDebug(s"xxx aggregateResultAttributes: ${partialAggregate.aggregateResultAttributes}")
    logDebug(s"xxx aggregateOutput: $aggregateOutput")

    val expandProjections = buildPostExpandProjections(
      expand.projections,
      expand.output,
      aggregateOutput
    )
    logDebug(s"xxx expandProjections: $expandProjections\nprojections:${expand.projections}")
    ExpandExecTransformer(expandProjections, aggregateOutput, child)
  }

}
