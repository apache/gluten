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

import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, IcebergEqualityDeleteWrite}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

case class VeloxIcebergEqualityDeleteExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: IcebergEqualityDeleteWrite)
  extends ColumnarV2TableWriteExec {

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = write.createWriterFactory(schema)

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory =
    throw new UnsupportedOperationException("SQL DELETE is a batch operation")

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}
