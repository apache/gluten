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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.delta.{DeltaLog, GlutenOptimisticTransaction, OptimisticTransaction, TransactionExecutionObserver}
import org.apache.spark.sql.delta.sources.{DeltaSink, GlutenDeltaSink, OffloadDeltaStreamingWrite}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.test.SharedSparkSession

import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{doAnswer, mock}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class GlutenDeltaStreamingWriteSuite extends SparkFunSuite with SharedSparkSession {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.ui.enabled", "false")
    .set(GlutenConfig.GLUTEN_ENABLED.key, "true")
    .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
    .set(SQLConf.ANSI_ENABLED.key, "false")
    .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "true")
    .set(GlutenConfig.COLUMNAR_SCAN_ONLY_ENABLED.key, "false")

  private def writePlan(sink: Sink, mode: OutputMode = OutputMode.Append()): WriteToStream =
    WriteToStream(
      name = "delta-stream",
      resolvedCheckpointLocation = "unused-checkpoint",
      sink = sink,
      outputMode = mode,
      deleteCheckpointOnStop = false,
      inputQuery = LocalRelation(),
      catalogAndIdent = None,
      catalogTable = None
    )

  test("only append-mode Delta sinks are wrapped, once, without changing the write plan") {
    val sink = mock(classOf[DeltaSink])
    val plan = writePlan(sink)
    val rule = OffloadDeltaStreamingWrite()
    val rewritten = rule(plan).asInstanceOf[WriteToStream]
    assert(rewritten.sink.isInstanceOf[GlutenDeltaSink])
    assert(rewritten.copy(sink = sink) == plan)
    assert(rule(rewritten) eq rewritten)
    Seq(OutputMode.Complete(), OutputMode.Update()).foreach {
      mode =>
        val unsupported = writePlan(sink, mode)
        assert(rule(unsupported) eq unsupported)
    }
    val other = writePlan(mock(classOf[Sink]))
    assert(rule(other) eq other)
  }

  test("streaming gates apply to analysis and each batch without leaking the transaction observer") {
    withTempDir {
      dir =>
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val data = spark.range(1).toDF()
        val sink = mock(classOf[DeltaSink])
        val failure = new IllegalStateException("failed streaming batch")
        var failBatch = false
        var transaction: OptimisticTransaction = null
        doAnswer(new Answer[Void] {
          override def answer(call: InvocationOnMock): Void = {
            assert(call.getArgument[Long](0) == 42L)
            assert(call.getArgument[DataFrame](1) eq data)
            transaction = deltaLog.startTransaction(catalogTableOpt = None)
            if (failBatch) throw failure
            null
          }
        }).when(sink).addBatch(anyLong(), any(classOf[DataFrame]))
        val wrapped = new GlutenDeltaSink(sink)
        val original = TransactionExecutionObserver.getObserver
        val cases = Seq(
          Seq.empty[(String, String)] -> true,
          Seq(GlutenConfig.GLUTEN_ENABLED.key -> "false") -> false,
          Seq(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") -> false,
          Seq(GlutenConfig.COLUMNAR_SCAN_ONLY_ENABLED.key -> "true") -> false,
          Seq(SQLConf.ANSI_ENABLED.key -> "true") -> false,
          Seq(
            SQLConf.ANSI_ENABLED.key -> "true",
            GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") -> true
        )
        cases.foreach {
          case (settings, expectedNative) =>
            withSQLConf(settings: _*) {
              val plan = writePlan(sink)
              val rewritten = OffloadDeltaStreamingWrite()(plan).asInstanceOf[WriteToStream]
              assert(rewritten.sink.isInstanceOf[GlutenDeltaSink] == expectedNative)
              if (!expectedNative) assert(rewritten eq plan)
              wrapped.addBatch(42L, data)
              assert(transaction.isInstanceOf[GlutenOptimisticTransaction] == expectedNative)
              assert(TransactionExecutionObserver.getObserver eq original)
            }
        }
        failBatch = true
        assert(intercept[IllegalStateException](wrapped.addBatch(42L, data)) eq failure)
        assert(transaction.isInstanceOf[GlutenOptimisticTransaction])
        assert(TransactionExecutionObserver.getObserver eq original)
    }
  }
}
