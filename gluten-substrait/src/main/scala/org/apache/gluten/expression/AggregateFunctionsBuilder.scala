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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DataType

object AggregateFunctionsBuilder {
  def create(context: SubstraitContext, aggregateFunc: AggregateFunction): Long = {
    // First handle the custom aggregate functions
    val substraitAggFuncName = getSubstraitFunctionName(aggregateFunc)

    // Check whether each backend supports this aggregate function.
    if (
      !BackendsApiManager.getValidatorApiInstance.doExprValidate(
        substraitAggFuncName,
        aggregateFunc)
    ) {
      throw new GlutenNotSupportException(s"Aggregate function not supported for $aggregateFunc.")
    }

    val inputTypes: Seq[DataType] = aggregateFunc.children.map(child => child.dataType)

    context.registerFunction(
      ConverterUtils.makeFuncName(substraitAggFuncName, inputTypes, FunctionConfig.REQ))
  }

  def getSubstraitFunctionName(aggregateFunc: AggregateFunction): String = {
    aggregateFunc match {
      case First(_, ignoreNulls) if ignoreNulls =>
        ExpressionNames.FIRST_IGNORE_NULL
      case Last(_, ignoreNulls) if ignoreNulls =>
        ExpressionNames.LAST_IGNORE_NULL
      case _ if isDeltaBitmapAggregator(aggregateFunc) =>
        ExpressionNames.BITMAP_AGGREGATOR
      case _ =>
        val nameOpt = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
        if (nameOpt.isEmpty) {
          throw new GlutenNotSupportException(
            s"Could not find a valid substrait mapping name for $aggregateFunc.")
        }
        nameOpt.get match {
          case ExpressionNames.UDAF_PLACEHOLDER => aggregateFunc.prettyName
          case name => name
        }
    }
  }

  /**
   * Matches Delta's `BitmapAggregator` -- the aggregate that builds a deletion-vector roaring
   * bitmap for DELETE/MoR. Matched by `prettyName` so this common module (`gluten-substrait`) stays
   * free of a `delta-spark` dependency.
   *
   * No serialization-format check is needed. Delta's only construction site
   * (`DMLWithDeletionVectorsHelper`) always uses the `Portable` format, which is exactly the layout
   * the native aggregate reads; and a non-Portable payload could not slip through silently anyway,
   * since the native `RoaringBitmapArray::deserialize` rejects any buffer whose magic number is not
   * the Portable one.
   */
  private def isDeltaBitmapAggregator(aggregateFunc: AggregateFunction): Boolean =
    aggregateFunc.prettyName == ExpressionNames.BITMAP_AGGREGATOR
}
