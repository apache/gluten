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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.execution.datasources.v2.DeltaV2WriteOperators.withColumnarTransaction
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sql.DeltaSparkSessionExtension

import scala.collection.mutable.ListBuffer

class DeltaTransactionObserverSuite extends SparkFunSuite with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", classOf[DeltaSparkSessionExtension].getName)
      .set("spark.sql.catalog.spark_catalog", classOf[DeltaCatalog].getName)
      .set("spark.ui.enabled", "false")
  }

  private class RecordingObserver extends TransactionExecutionObserver {
    val events: ListBuffer[String] = ListBuffer.empty
    var child: Option[TransactionExecutionObserver] = None

    override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
      events += "starting"
      val transaction = f
      events += "started"
      transaction
    }

    override def preparingCommit[T](f: => T): T = {
      events += "preparing"
      val result = f
      events += "prepared"
      result
    }

    override def beginDoCommit(): Unit = { events += "commit" }
    override def beginBackfill(): Unit = { events += "backfill" }
    override def beginPostCommit(): Unit = { events += "postCommit" }
    override def transactionCommitted(): Unit = { events += "committed" }
    override def transactionAborted(): Unit = { events += "aborted" }

    override def createChild(): TransactionExecutionObserver = {
      events += "child"
      child.getOrElse(TransactionExecutionObserver.getObserver)
    }
  }

  private def phaseObserver(name: String): PhaseLockingTransactionExecutionObserver = {
    val observer =
      new PhaseLockingTransactionExecutionObserver(OptimisticTransactionPhases.forName(name))
    observer.phaseLocks.foreach(_.entryBarrier.unblock())
    observer
  }

  private def finishCommit(observer: TransactionExecutionObserver): Unit = {
    observer.preparingCommit(())
    observer.beginDoCommit()
    observer.beginBackfill()
    observer.beginPostCommit()
    observer.transactionCommitted()
  }

  test("native transactions preserve the existing observer and all lifecycle callbacks") {
    withTempDir {
      dir =>
        val original = new RecordingObserver
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        TransactionExecutionObserver.withObserver(original) {
          withColumnarTransaction {
            val transaction = deltaLog.startTransaction()
            assert(transaction.isInstanceOf[GlutenOptimisticTransaction])
            val observer = TransactionExecutionObserver.getObserver
            assert(transaction.executionObserver eq observer)
            assert(original.events.toList == List("starting", "started"))
            assert(observer.preparingCommit(42) == 42)
            observer.beginDoCommit()
            observer.beginBackfill()
            observer.beginPostCommit()
            observer.transactionCommitted()
            observer.transactionAborted()
          }
          assert(TransactionExecutionObserver.getObserver eq original)
        }
        assert(
          original.events.toList == List(
            "starting",
            "started",
            "preparing",
            "prepared",
            "commit",
            "backfill",
            "postCommit",
            "committed",
            "aborted"))
    }
  }

  test("creation, preparation, and command exceptions retain their identity and observer scope") {
    val original = new RecordingObserver
    val failure = new IllegalStateException("observer test")
    TransactionExecutionObserver.withObserver(original) {
      assert(intercept[IllegalStateException] {
        withColumnarTransaction {
          TransactionExecutionObserver.getObserver.startingTransaction(throw failure)
        }
      } eq failure)
      assert(TransactionExecutionObserver.getObserver eq original)

      assert(intercept[IllegalStateException] {
        withColumnarTransaction {
          TransactionExecutionObserver.getObserver.preparingCommit(throw failure)
        }
      } eq failure)
      assert(TransactionExecutionObserver.getObserver eq original)

      assert(intercept[IllegalStateException] {
        withColumnarTransaction(throw failure)
      } eq failure)
      assert(TransactionExecutionObserver.getObserver eq original)
    }
  }

  for (aborted <- Seq(false, true)) {
    test(s"Delta's phase observer advances after completion: aborted=$aborted") {
      withTempDir {
        dir =>
          val original = phaseObserver("first")
          val next = phaseObserver("next")
          original.setNextObserver(next, autoAdvance = true)
          original.phases.postCommitPhase.exitBarrier.unblock()
          val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

          TransactionExecutionObserver.withObserver(original) {
            withColumnarTransaction {
              val first = deltaLog.startTransaction()
              assert(first.isInstanceOf[GlutenOptimisticTransaction])
              val firstObserver = first.executionObserver
              if (aborted) {
                firstObserver.transactionAborted()
              } else {
                finishCommit(firstObserver)
              }

              assert(TransactionExecutionObserver.getObserver ne firstObserver)
              val second = deltaLog.startTransaction()
              assert(second.isInstanceOf[GlutenOptimisticTransaction])
              finishCommit(second.executionObserver)
              assert(next.allPhasesHavePassed)
            }
            assert(TransactionExecutionObserver.getObserver eq next)
          }
      }
    }
  }

  test("nested native commands reuse the observer and preserve advancement on scope exit") {
    val original = new RecordingObserver
    val next = new RecordingObserver
    TransactionExecutionObserver.withObserver(original) {
      withColumnarTransaction {
        val observer = TransactionExecutionObserver.getObserver
        withColumnarTransaction {
          assert(TransactionExecutionObserver.getObserver eq observer)
          observer.setNextObserver(next)
          TransactionExecutionObserver.advanceToNextObserver()
          TransactionExecutionObserver.getObserver.beginDoCommit()
        }
        assert(TransactionExecutionObserver.getObserver ne observer)
        assert(TransactionExecutionObserver.getObserver ne next)
        TransactionExecutionObserver.getObserver.beginBackfill()
      }
      assert(TransactionExecutionObserver.getObserver eq next)
    }
    assert(next.events.toList == List("commit", "backfill"))
  }

  test("native child observers preserve Delta's current-thread and custom-child behavior") {
    val original = new RecordingObserver
    val child = new RecordingObserver
    TransactionExecutionObserver.withObserver(original) {
      withColumnarTransaction {
        val observer = TransactionExecutionObserver.getObserver
        assert(observer.createChild() eq observer)
        original.child = Some(child)
        val childObserver = observer.createChild()
        assert(childObserver ne child)
        childObserver.beginDoCommit()
        assert(child.events.toList == List("commit"))
        assert(TransactionExecutionObserver.getObserver eq observer)
      }
      assert(TransactionExecutionObserver.getObserver eq original)
    }
  }

  test("a callback exception does not discard an observer advancement") {
    val next = new RecordingObserver
    val failure = new IllegalStateException("commit callback")
    val original = new RecordingObserver {
      override def transactionCommitted(): Unit = {
        advanceToNextThreadObserver()
        throw failure
      }
    }
    original.setNextObserver(next)
    TransactionExecutionObserver.withObserver(original) {
      assert(intercept[IllegalStateException] {
        withColumnarTransaction {
          TransactionExecutionObserver.getObserver.transactionCommitted()
        }
      } eq failure)
      assert(TransactionExecutionObserver.getObserver eq next)
    }
  }

  test("callbacks outside a native command do not install a native observer") {
    val original = new RecordingObserver
    TransactionExecutionObserver.withObserver(original) {
      val observer = withColumnarTransaction {
        TransactionExecutionObserver.getObserver
      }
      observer.transactionCommitted()
      observer.transactionAborted()
      assert(TransactionExecutionObserver.getObserver eq original)
    }
    assert(original.events.toList == List("committed", "aborted"))
  }
}
