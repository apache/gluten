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
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object BoltIntermediateData {
  // Agg functions with inconsistent ordering of intermediate data between Bolt and Spark. The
  // strings in the Seq comes from the aggBufferAttributes of Spark's aggregate function, and they
  // are arranged in the order of fields in Bolt's Accumulator. The reason for using a
  // two-dimensional Seq is that in some cases, a field in Bolt will be mapped to multiple
  // Attributes in Spark's aggBufferAttributes. For example, the fourth field of Bolt's RegrSlope
  // Accumulator is mapped to both xAvg and avg in Spark's RegrSlope aggBufferAttributes. In this
  // scenario, when passing the output of Spark's partial aggregation to Bolt, we only need to
  // take one of them.
  // Corr, RegrR2
  private val boltCorrIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "xMk", "yMk", "xAvg", "yAvg").map(Seq(_))
  // CovPopulation, CovSample
  private val boltCovarIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "xAvg", "yAvg").map(Seq(_))
  // Skewness, Kurtosis
  private val boltCentralMomentAggIntermediateDataOrder: Seq[Seq[String]] =
    Seq("n", "avg", "m2", "m3", "m4").map(Seq(_))
  // RegrSlope, RegrIntercept
  private val boltRegrIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "m2", "xAvg:avg", "yAvg").map(attr => attr.split(":").toSeq)
  // RegrSXY
  // Use "undefined" to represent variables in the accumulator that do not exist in Spark. These
  // variables will not affect the final result and are considered redundant data.
  private val boltRegrSXYIntermediateDataOrder: Seq[Seq[String]] =
    Seq("ck", "n", "undefined", "xAvg", "yAvg", "undefined").map(Seq(_))

  // Agg functions with inconsistent types of intermediate data between Bolt and Spark.
  // StddevSamp, StddevPop, VarianceSamp, VariancePop
  private val boltVarianceIntermediateTypes: Seq[DataType] = Seq(LongType, DoubleType, DoubleType)
  // CovPopulation, CovSample
  private val boltCovarIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType)
  // Corr
  private val boltCorrIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType, DoubleType)
  // Skewness, Kurtosis
  private val boltCentralMomentAggIntermediateTypes: Seq[DataType] =
    Seq(LongType, DoubleType, DoubleType, DoubleType, DoubleType)
  // RegrSlope, RegrIntercept
  private val boltRegrIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType)
  // RegrSXY
  private val boltRegrSXYIntermediateTypes: Seq[DataType] =
    Seq(DoubleType, LongType, DoubleType, DoubleType, DoubleType, DoubleType)

  def getAttrIndex(intermediateDataOrder: Seq[Seq[String]], attr: String): Int =
    intermediateDataOrder.zipWithIndex
      .find { case (innerSeq, _) => innerSeq.contains(attr) }
      .map(_._2)
      .getOrElse(-1)

  /**
   * Return the intermediate columns order of Bolt aggregation functions, with special matching
   * required for some aggregation functions where the intermediate columns order are inconsistent
   * with Spark.
   * @param aggFunc
   *   Spark aggregation function
   * @return
   *   the intermediate columns order of Bolt aggregation functions
   */
  def boltIntermediateDataOrder(aggFunc: AggregateFunction): Seq[Seq[String]] = {
    aggFunc match {
      case _: PearsonCorrelation =>
        boltCorrIntermediateDataOrder
      case _: CovPopulation | _: CovSample =>
        boltCovarIntermediateDataOrder
      case _: Skewness | _: Kurtosis =>
        boltCentralMomentAggIntermediateDataOrder
      // The reason for using class names to match aggFunc here is because these aggFunc come from
      // certain versions of Spark, and SparkShim is not dependent on the backend-bolt module. It
      // is not convenient to include Bolt-specific logic in SparkShim. Using class names to match
      // aggFunc is reliable in this case, as there are no cases of duplicate names.
      case _
          if aggFunc.getClass.getSimpleName.equals("RegrSlope") ||
            aggFunc.getClass.getSimpleName.equals("RegrIntercept") =>
        boltRegrIntermediateDataOrder
      case _ if aggFunc.getClass.getSimpleName.equals("RegrSXY") =>
        boltRegrSXYIntermediateDataOrder
      case _ =>
        aggFunc.aggBufferAttributes.map(_.name).map(Seq(_))
    }
  }

  /**
   * Get the compatible input types for a Bolt aggregate function.
   *
   * @param aggregateFunc
   *   The input aggregate function.
   * @param forMergeCompanion
   *   Whether this is a special case to solve mixed aggregation phases.
   * @return
   *   The input types of a Bolt aggregate function.
   */
  def getInputTypes(aggregateFunc: AggregateFunction, forMergeCompanion: Boolean): Seq[DataType] = {
    if (!forMergeCompanion) {
      return aggregateFunc.children.map(_.dataType)
    }
    aggregateFunc match {
      case _ @Type(boltDataTypes: Seq[DataType]) =>
        Seq(StructType(boltDataTypes.map(StructField("", _)).toArray))
      case _ =>
        // Not use StructType for single column agg intermediate data
        aggregateFunc.aggBufferAttributes.map(_.dataType)
    }
  }

  /**
   * Return the intermediate type node of a partial aggregation in Bolt.
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
          throw new UnsupportedOperationException("Can not get bolt intermediate types.")
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
    // For first/last the intermediate is (value, valueSet). The bolt guarantees the
    // struct is always non-null and valueSet is always non-null (false or true), so no
    // implicit re-nulling by the row constructor is needed. row_constructor leaves struct
    // nullness entirely to the producer.
    case _: First | _: Last =>
      "row_constructor"
    case _ => "row_constructor_with_null"
  }

  object Type {

    /**
     * Return the intermediate types of Bolt agg functions, with special matching required for some
     * aggregation functions where the intermediate results are inconsistent with Spark. Only return
     * if the intermediate result has multiple columns.
     * @param aggFunc
     *   Spark aggregation function
     * @return
     *   the intermediate types of Bolt aggregation functions.
     */
    def unapply(aggFunc: AggregateFunction): Option[Seq[DataType]] = {
      aggFunc match {
        case _: PearsonCorrelation =>
          Some(boltCorrIntermediateTypes)
        case _ if aggFunc.getClass.getSimpleName.equals("RegrSXY") =>
          // RegrSXY extends Covariance, it must be placed before Covariance.
          Some(boltRegrSXYIntermediateTypes)
        case _: Covariance =>
          Some(boltCovarIntermediateTypes)
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          Some(boltVarianceIntermediateTypes)
        case _: Skewness | _: Kurtosis =>
          Some(boltCentralMomentAggIntermediateTypes)
        case _
            if aggFunc.getClass.getSimpleName.equals("RegrSlope") ||
              aggFunc.getClass.getSimpleName.equals("RegrIntercept") =>
          Some(boltRegrIntermediateTypes)
        case _ if aggFunc.aggBufferAttributes.size > 1 =>
          Some(aggFunc.aggBufferAttributes.map(_.dataType))
        case _ => None
      }
    }
  }
}
