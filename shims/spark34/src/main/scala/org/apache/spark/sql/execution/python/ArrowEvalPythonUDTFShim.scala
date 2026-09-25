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
package org.apache.spark.sql.execution.python

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Evaluates an Arrow-optimized Python UDTF. Each returned batch holds the flattened UDTF result
 * columns of a single `eval` (one per input row, in input order) or `terminate` call.
 */
trait ArrowPythonUDTFEvaluator extends Serializable {
  def evaluate(iter: Iterator[InternalRow], context: TaskContext): Iterator[ColumnarBatch]
}

/** Python UDTFs are not available before Spark 3.5. */
object ArrowEvalPythonUDTFShim {
  def unapply(
      plan: SparkPlan): Option[(Expression, Seq[Attribute], Seq[Attribute], SparkPlan, Int)] =
    None

  def createEvaluator(
      plan: SparkPlan,
      udtf: Expression,
      evalType: Int,
      childOutput: Seq[Attribute],
      resultAttrs: Seq[Attribute],
      pythonMetrics: Map[String, SQLMetric]): ArrowPythonUDTFEvaluator =
    throw new UnsupportedOperationException("Python UDTF is not supported before Spark 3.5")
}
