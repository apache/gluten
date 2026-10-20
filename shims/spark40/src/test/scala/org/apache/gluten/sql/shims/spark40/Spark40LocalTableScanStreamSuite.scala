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
package org.apache.gluten.sql.shims.spark40

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Spark 4.0+ only: LocalTableScanExec carries an optional streaming source (`stream`). This suite
 * verifies the Spark40Shims accessor that VeloxSparkPlanExecApi.isSupportLocalTableScanExec uses to
 * skip offload for streaming sources. The Velox backend test lives here (not in the shared
 * cross-version suite) because the `stream` constructor parameter does not exist on Spark 3.x.
 */
class Spark40LocalTableScanStreamSuite extends AnyFunSuite {

  private val shims = new Spark40Shims

  private def output = Seq(AttributeReference("id", IntegerType)())

  private val stubStream: SparkDataStream = new SparkDataStream {
    override def initialOffset(): Offset = null
    override def deserializeOffset(json: String): Offset = null
    override def commit(end: Offset): Unit = {}
    override def stop(): Unit = {}
  }

  test("getLocalTableScanStream returns the stream for a streaming LocalTableScanExec") {
    val plan = LocalTableScanExec(output, Seq.empty[InternalRow], Some(stubStream))
    val stream = shims.getLocalTableScanStream(plan)
    assert(stream.isDefined, "Streaming LocalTableScanExec must be detected as a streaming source")
    assert(stream.get eq stubStream)
  }

  test("getLocalTableScanStream returns None for a batch LocalTableScanExec") {
    val plan = LocalTableScanExec(output, Seq.empty[InternalRow], None)
    assert(
      shims.getLocalTableScanStream(plan).isEmpty,
      "Batch LocalTableScanExec must not be detected as a streaming source")
  }
}
