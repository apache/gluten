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
package org.apache.spark.sql.delta

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

import io.delta.sql.DeltaSparkSessionExtension

@ExtendedSQLTest
class VeloxDeltaTransactionObserverSuite extends GlutenQueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", classOf[DeltaSparkSessionExtension].getName)
      .set("spark.sql.catalog.spark_catalog", classOf[DeltaCatalog].getName)
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.ansi.enabled", "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
  }

  test("native Delta DELETE rewrites rows while preserving the phase-locking observer") {
    withTempDir {
      dir =>
        withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
          spark
            .range(4)
            .coalesce(1)
            .write
            .format("delta")
            .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "false")
            .save(dir.getCanonicalPath)
        }
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val originalFiles = deltaLog.update().allFiles.collect()
        assert(originalFiles.length == 1)
        val observer = new PhaseLockingTransactionExecutionObserver(
          OptimisticTransactionPhases.forName("native-delete")) {
          override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
            val transaction = super.startingTransaction(f)
            assert(transaction.isInstanceOf[GlutenOptimisticTransaction])
            transaction
          }
        }
        observer.phaseLocks.foreach(_.entryBarrier.unblock())

        TransactionExecutionObserver.withObserver(observer) {
          sql(s"DELETE FROM delta.`${dir.getCanonicalPath}` WHERE id < 2").collect()
          assert(TransactionExecutionObserver.getObserver eq observer)
        }
        assert(observer.allPhasesHavePassed)
        val remainingFiles = deltaLog.update().allFiles.collect()
        assert(remainingFiles.nonEmpty)
        assert(remainingFiles.forall(_.path != originalFiles.head.path))
        assert(remainingFiles.forall(_.deletionVector == null))
        checkAnswer(
          spark.read.format("delta").load(dir.getCanonicalPath),
          Seq(Row(2L), Row(3L)))
    }
  }
}
