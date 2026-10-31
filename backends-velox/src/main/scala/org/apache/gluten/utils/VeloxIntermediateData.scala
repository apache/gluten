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

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.expression.aggregate.VeloxCollectSet
import org.apache.gluten.extension.CollectRewriteRule
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object VeloxIntermediateData {
  // Agg functions with inconsistent ordering of intermediate data between Velox and Spark. The
  // strings in the Seq comes from the aggBufferAttributes of Spark's aggregate function, and they
  // are arranged in the order of fields in Velox's Accumulator. The reason for using a
  // two-dimensional Seq is that in some cases, a field in Velox will be mapped to multiple
  // Attributes in Spark's aggBufferAttributes. For example, the fourth field of Velox's RegrSlope
  // Accumulator is mapped to both xAvg and avg in Spark's RegrSlope aggBufferAttributes. In this
  // scenario, when passing the output of Spark's partial aggregation to Velox, we only need to
  // take one of them.
  // Corr, RegrR2
  private val veloxCorrIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "xMk", "yMk", "xAvg", "yAvg").map(Seq(_))
  // CovPopulation, CovSample
  private val veloxCovarIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "xAvg", "yAvg").map(Seq(_))
  // Skewness, Kurtosis
  private val veloxCentralMomentAggIntermediateDataOrder: Seq[Seq[String]] =
    Seq("n", "avg", "m2", "m3", "m4").map(Seq(_))
  // RegrSlope, RegrIntercept
  private val veloxRegrIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "m2", "xAvg:avg", "yAvg").map(attr => attr.split(":").toSeq)
  // RegrSXY
  // Use "undefined" to represent variables in the accumulator that do not exist in Spark. These
  // variables will not affect the final result and are considered redundant data.
  private val veloxRegrSXYIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "undefined", "xAvg", "yAvg", "undefined").map(Seq(_))

  // Agg functions with inconsistent types of intermediate data between Velox and Spark.
  // StddevSamp, StddevPop, VarianceSamp, VariancePop
  private val veloxVarianceIntermediateTypes: Seq[DataType] = Seq(LongType, DoubleType, DoubleType)
  // CovPopulation, CovSample
  private val veloxCovarIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType)
  // Corr
  private val veloxCorrIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType, DoubleType)
  // Skewness, Kurtosis
  private val veloxCentralMomentAggIntermediateTypes: Seq[DataType] =
    Seq(LongType, DoubleType, DoubleType, DoubleType, DoubleType)
  // RegrSlope, RegrIntercept
  private val veloxRegrIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType)
  // RegrSXY
  private val veloxRegrSXYIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType, DoubleType)

  def getAttrIndex(intermediateDataOrder: Seq[Seq[String]], attr: String): Int =
    intermediateDataOrder.zipWithIndex
      .find { case (innerSeq, _) => innerSeq.contains(attr) }
      .map(_._2)
      .getOrElse(-1)

  /**
   * Return the intermediate columns order of Velox aggregation functions, with special matching
   * required for some aggregation functions where the intermediate columns order are inconsistent
   * with Spark.
   * @param aggFunc
   *   Spark aggregation function
   * @return
   *   the intermediate columns order of Velox aggregation functions
   */
  def veloxIntermediateDataOrder(aggFunc: AggregateFunction): Seq[Seq[String]] = {
    aggFunc match {
      case _: PearsonCorrelation =>
        veloxCorrIntermediateDataOrder
      case _: CovPopulation | _: CovSample =>
        veloxCovarIntermediateDataOrder
      case _: Skewness | _: Kurtosis =>
        veloxCentralMomentAggIntermediateDataOrder
      // The reason for using class names to match aggFunc here is because these aggFunc come from
      // certain versions of Spark, and SparkShim is not dependent on the backend-velox module. It
      // is not convenient to include Velox-specific logic in SparkShim. Using class names to match
      // aggFunc is reliable in this case, as there are no cases of duplicate names.
      case _
          if aggFunc.getClass.getSimpleName.equals("RegrSlope") ||
            aggFunc.getClass.getSimpleName.equals("RegrIntercept") =>
        veloxRegrIntermediateDataOrder
      case _ if aggFunc.getClass.getSimpleName.equals("RegrSXY") =>
        veloxRegrSXYIntermediateDataOrder
      case _ =>
        aggFunc.aggBufferAttributes.map(_.name).map(Seq(_))
    }
  }

  /**
   * Get the arguments passed to a Velox aggregate function when it consumes raw input (Partial or
   * Complete mode).
   *
   * @param aggregateFunc
   *   The input aggregate function.
   * @return
   *   The raw input arguments of a Velox aggregate function.
   */
  def getRawInputs(aggregateFunc: AggregateFunction): Seq[Expression] = aggregateFunc match {
    // Velox's collect_set takes RESPECT NULLS as a constant `ignoreNulls` argument. Window
    // functions keep Spark's CollectSet, because CollectRewriteRule only rewrites aggregates.
    case c: VeloxCollectSet if !c.ignoreNulls => Seq(c.child, Literal.FalseLiteral)
    case c: CollectSet if !CollectRewriteRule.getIgnoreNulls(c) =>
      Seq(c.child, Literal.FalseLiteral)
    case _ => aggregateFunc.children
  }

  /**
   * Get the compatible input types for a Velox aggregate function.
   *
   * @param aggregateFunc
   *   The input aggregate function.
   * @param forMergeCompanion
   *   Whether this is a special case to solve mixed aggregation phases.
   * @return
   *   The input types of a Velox aggregate function.
   */
  def getInputTypes(aggregateFunc: AggregateFunction, forMergeCompanion: Boolean): Seq[DataType] = {
    if (!forMergeCompanion) {
      return getRawInputs(aggregateFunc).map(_.dataType)
    }
    aggregateFunc match {
      case _ @Type(veloxDataTypes: Seq[DataType]) =>
        Seq(StructType(veloxDataTypes.map(StructField("", _)).toArray))
      case _ =>
        // Not use StructType for single column agg intermediate data
        aggregateFunc.aggBufferAttributes.map(_.dataType)
    }
  }

  /**
   * Return the intermediate type node of a partial aggregation in Velox.
   *
   * @param aggFunc
   *   Spark aggregation function.
   * @return
   *   The type of partial outputs.
   */
  def getIntermediateTypeNode(aggFunc: AggregateFunction): TypeNode = {
    val structTypeNodes =
      aggFunc match {
        case _ @Type(dataTypes: Seq[DataType]) =>
          dataTypes.map(ConverterUtils.getTypeNode(_, nullable = false))
        case _ =>
          throw new UnsupportedOperationException("Can not get velox intermediate types.")
      }
    TypeBuilder.makeStruct(false, structTypeNodes.asJava)
  }

  /**
   * Obtain the name of the RowConstruct function, only decimal avg and sum currently require the
   * use of row_constructor, while the rest use the Gluten custom modified
   * row_constructor_with_null.
   */
  def getRowConstructFuncName(aggFunc: AggregateFunction): String = aggFunc match {
    case _: Average | _: Sum if aggFunc.dataType.isInstanceOf[DecimalType] =>
      "row_constructor"
    // For agg function min_by/max_by, it needs to keep rows with null value but non-null
    // comparison, such as <null, 5>. So we set the struct to null when all of the arguments
    // are null
    case _: MaxMinBy =>
      "row_constructor_with_all_null"
    case _ => "row_constructor_with_null"
  }

  object Type {

    /**
     * Return the intermediate types of Velox agg functions, with special matching required for some
     * aggregation functions where the intermediate results are inconsistent with Spark. Only return
     * if the intermediate result has multiple columns.
     * @param aggFunc
     *   Spark aggregation function
     * @return
     *   the intermediate types of Velox aggregation functions.
     */
    def unapply(aggFunc: AggregateFunction): Option[Seq[DataType]] = {
      aggFunc match {
        case _: PearsonCorrelation =>
          Some(veloxCorrIntermediateTypes)
        case _ if aggFunc.getClass.getSimpleName.equals("RegrSXY") =>
          // RegrSXY extends Covariance, it must be placed before Covariance.
          Some(veloxRegrSXYIntermediateTypes)
        case _: Covariance =>
          Some(veloxCovarIntermediateTypes)
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          Some(veloxVarianceIntermediateTypes)
        case _: Skewness | _: Kurtosis =>
          Some(veloxCentralMomentAggIntermediateTypes)
        case _
            if aggFunc.getClass.getSimpleName.equals("RegrSlope") ||
              aggFunc.getClass.getSimpleName.equals("RegrIntercept") =>
          Some(veloxRegrIntermediateTypes)
        case _ if aggFunc.aggBufferAttributes.size > 1 =>
          Some(aggFunc.aggBufferAttributes.map(_.dataType))
        case _ => None
      }
    }
  }
}
