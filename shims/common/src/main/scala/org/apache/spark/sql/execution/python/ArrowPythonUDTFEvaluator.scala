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
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Evaluates an Arrow-optimized Python UDTF (`SQL_ARROW_TABLE_UDF`) with Spark's
 * ArrowPythonUDTFRunner, which is created by the per-Spark-version ArrowEvalPythonUDTFShim.
 *
 * Returns the runner's raw output: each batch has a single struct column holding the UDTF result
 * columns of one `eval` call (one per input row, in input order) or of the `terminate` call. The
 * batches are owned by the runner and only valid until the next one is requested.
 */
trait ArrowPythonUDTFEvaluator extends Serializable {
  def evaluate(iter: Iterator[InternalRow], context: TaskContext): Iterator[ColumnarBatch]
}
