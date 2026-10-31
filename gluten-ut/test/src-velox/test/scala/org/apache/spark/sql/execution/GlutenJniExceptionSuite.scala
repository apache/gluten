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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.{NativeMemoryManager, NativeMemoryManagerJniWrapper}
import org.apache.gluten.memory.listener.ReservationListener
import org.apache.gluten.memory.memtarget.Spiller
import org.apache.gluten.runtime.{Runtime, RuntimeJniWrapper, Runtimes}
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.threads.NativeThreadManager
import org.apache.gluten.utils.{BackendTestUtils, ConfigUtil}
import org.apache.gluten.vectorized.{ColumnarBatchInIterator, ColumnarBatchOutIterator, NativePlanEvaluator, PlanEvaluatorJniWrapper}

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkConf, SparkNumberFormatException, SparkThrowable}
import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources

import java.io.PrintWriter
import java.util.{Collections, Iterator => JIterator}

import scala.collection.JavaConverters._

class GlutenJniExceptionSuite extends GlutenQueryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      // TaskResources.runUnsafe uses stage -1, which would match the default dump stage.
      .set(GlutenConfig.BENCHMARK_TASK_STAGEID.key, "0")
  }

  private def withNativeIterator(input: JIterator[ColumnarBatch])(
      f: ColumnarBatchOutIterator => Unit): Unit = {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withTempDir {
      spillDir =>
        TaskResources.runUnsafe {
          val context = new SubstraitContext
          val names = Collections.singletonList("value")
          val read = RelBuilder.makeReadRelForInputIterator(
            Collections.singletonList(TypeBuilder.makeI32(false)),
            names,
            context,
            context.nextOperatorId("exception-test"))
          val plan = PlanBuilder.makePlan(context, Collections.singletonList(read), names)
          val backend = BackendsApiManager.getBackendName
          val out = NativePlanEvaluator
            .create(backend)
            .createKernelWithBatchIterator(
              plan.toProtobuf.toByteArray,
              Array.empty[Array[Byte]],
              Array(new ColumnarBatchInIterator(backend, input)),
              0,
              spillDir.getCanonicalPath)
          try {
            out.noMoreSplits()
            f(out)
          } finally {
            out.close()
          }
        }
    }
  }

  private def failingInput(
      original: Throwable,
      failInHasNext: Boolean): JIterator[ColumnarBatch] = {
    new JIterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        if (failInHasNext) {
          throw original
        }
        true
      }

      override def next(): ColumnarBatch = throw original
    }
  }

  private def withReservationRuntime(listener: ReservationListener)(f: Runtime => Unit): Unit = {
    TaskResources.runUnsafe {
      val backend = BackendsApiManager.getBackendName
      val threadOwner = Runtimes.contextInstance(backend, "jni-reservation-test")
      val conf = ConfigUtil.serialize(
        Map(
          "spark.sql.session.timeZone" -> "UTC",
          "spark.gluten.memory.backtrace.allocation" -> "false").asJava)
      val memoryHandle = NativeMemoryManagerJniWrapper.create(backend, listener, conf)
      try {
        val runtimeHandle = RuntimeJniWrapper.createRuntime(
          backend,
          memoryHandle,
          threadOwner.threadManager().getHandle(),
          conf)
        try {
          val nativeMemory = new NativeMemoryManager {
            override def addSpiller(spiller: Spiller): Unit =
              throw new UnsupportedOperationException("This test does not install spillers")
            override def hold(): Unit = NativeMemoryManagerJniWrapper.hold(memoryHandle)
            override def getHandle(): Long = memoryHandle
            override def release(): Unit = NativeMemoryManagerJniWrapper.release(memoryHandle)
          }
          f(new Runtime {
            override def memoryManager(): NativeMemoryManager = nativeMemory
            override def threadManager(): NativeThreadManager = threadOwner.threadManager()
            override def getHandle(): Long = runtimeHandle
          })
        } finally {
          RuntimeJniWrapper.releaseRuntime(runtimeHandle)
        }
      } finally {
        NativeMemoryManagerJniWrapper.release(memoryHandle)
      }
    }
  }

  private def withNativeCount(runtime: Runtime)(f: ColumnarBatchOutIterator => Unit): Unit = {
    withTempDir {
      spillDir =>
        val context = new SubstraitContext
        val read = RelBuilder.makeReadRelForInputIterator(
          Collections.emptyList[TypeNode](),
          Collections.emptyList[String](),
          context,
          context.nextOperatorId("reservation-input"))
        val count = ExpressionBuilder.makeAggregateFunction(
          context.registerFunction("count"),
          Collections.emptyList[ExpressionNode](),
          "COMPLETE",
          TypeBuilder.makeI64(false))
        val aggregate = RelBuilder.makeAggregateRel(
          read,
          Collections.emptyList[ExpressionNode](),
          Collections.singletonList(count),
          Collections.singletonList[ExpressionNode](null),
          null,
          context,
          context.nextOperatorId("reservation-aggregate")
        )
        val plan = PlanBuilder.makePlan(
          context,
          Collections.singletonList(aggregate),
          Collections.singletonList("count"))
        val input = new ColumnarBatch(Array.empty[ColumnVector], 1)
        val handle = PlanEvaluatorJniWrapper
          .create(runtime)
          .nativeCreateKernelWithIterator(
            plan.toProtobuf.toByteArray,
            Array.empty[Array[Byte]],
            Array(new ColumnarBatchInIterator(
              BackendsApiManager.getBackendName,
              Collections.singletonList(input).iterator())),
            0,
            0,
            0L,
            false,
            spillDir.getCanonicalPath
          )
        val out = new ColumnarBatchOutIterator(runtime, handle)
        try {
          out.noMoreSplits()
          f(out)
        } finally {
          out.close()
          input.close()
        }
    }
  }

  private class FailingReservationListener(val original: RuntimeException)
    extends ReservationListener {
    var failReserve = false
    var failUnreserve = false
    var failures = 0
    private var used = 0L

    override def reserve(size: Long): Long = {
      if (failReserve) {
        failures += 1
        throw original
      }
      used += size
      size
    }

    override def unreserve(size: Long): Long = {
      used -= size
      if (failUnreserve) {
        failures += 1
        throw original
      }
      size
    }

    override def getUsedBytes: Long = used
  }

  for (failInHasNext <- Seq(true, false)) {
    test(s"Java throwable identity and Spark metadata survive JNI (hasNext=$failInHasNext)") {
      val errors = Seq(
        new SparkArithmeticException(
          "CAST_OVERFLOW",
          Map(
            "value" -> "128",
            "sourceType" -> "\"INT\"",
            "targetType" -> "\"TINYINT\"",
            "ansiConfig" -> "\"spark.sql.ansi.enabled\""),
          Array.empty[QueryContext],
          ""
        ),
        new SparkNumberFormatException(
          "CAST_INVALID_INPUT",
          Map(
            "expression" -> "'invalid'",
            "sourceType" -> "\"STRING\"",
            "targetType" -> "\"INT\"",
            "ansiConfig" -> "\"spark.sql.ansi.enabled\""),
          Array.empty[QueryContext],
          ""
        )
      )
      errors.foreach {
        original =>
          val cause = new IllegalStateException("original cause")
          val suppressed = new IllegalArgumentException("original suppressed exception")
          original.initCause(cause)
          original.addSuppressed(suppressed)
          val stack = original.getStackTrace.toSeq
          val parameters = original.getMessageParameters
          val errorClass = original.getErrorClass
          val sqlState = original.getSqlState
          withNativeIterator(failingInput(original, failInHasNext)) {
            out =>
              val actual = intercept[Exception] {
                out.hasNext0()
              }
              assert(actual eq original)
              assert(actual.getClass == original.getClass)
              assert(actual.getCause eq cause)
              assert(actual.getSuppressed.toSeq == Seq(suppressed))
              assert(actual.getStackTrace.toSeq == stack)
              actual match {
                case sparkError: SparkThrowable =>
                  assert(sparkError.getErrorClass == errorClass)
                  assert(sparkError.getMessageParameters == parameters)
                  assert(sparkError.getSqlState == sqlState)
                case _ => fail("Original SparkThrowable was lost")
              }
          }
      }
    }
  }

  test("nested native iterators retain the actual Java cause") {
    val original = new ArithmeticException("original arithmetic error")
    withNativeIterator(failingInput(original, failInHasNext = true)) {
      inner =>
        withNativeIterator(inner) {
          outer =>
            val actual = intercept[GlutenException] {
              outer.hasNext()
            }
            assert(actual.getCause.isInstanceOf[GlutenException])
            assert(actual.getCause.getCause eq original)
        }
    }
  }

  test("JNI restores Errors as well as Exceptions") {
    val original = new AssertionError("original error")
    withNativeIterator(failingInput(original, failInHasNext = false)) {
      out =>
        val actual = intercept[AssertionError] {
          out.next0()
        }
        assert(actual eq original)
    }
  }

  test("a failed stack trace describer does not replace the original throwable") {
    val original = new ArithmeticException("original exception") {
      override def printStackTrace(writer: PrintWriter): Unit = {
        throw new IllegalStateException("stack trace description failed")
      }
    }
    withNativeIterator(failingInput(original, failInHasNext = true)) {
      out =>
        val actual = intercept[ArithmeticException] {
          out.hasNext0()
        }
        assert(actual eq original)
    }
  }

  test("normal end of input is unchanged") {
    withNativeIterator(Collections.emptyList[ColumnarBatch]().iterator()) {
      out => assert(!out.hasNext())
    }
  }

  test("native operator reservations preserve the actual Java throwable") {
    val cause = new ArithmeticException("reservation cause")
    val original = new IllegalStateException("reservation failure", cause)
    val listener = new FailingReservationListener(original)
    withReservationRuntime(listener) {
      runtime =>
        withNativeCount(runtime) {
          out =>
            listener.failReserve = true
            try {
              val actual = intercept[IllegalStateException] {
                out.hasNext0()
              }
              assert(listener.failures > 0)
              assert(actual eq original)
              assert(actual.getCause eq cause)
            } finally {
              listener.failReserve = false
            }
        }
    }
  }

  test("direct JNI unreserve preserves the actual Java throwable") {
    val original = new IllegalStateException("unreserve failure")
    val listener = new FailingReservationListener(original)
    withReservationRuntime(listener) {
      runtime =>
        withNativeCount(runtime) {
          out =>
            assert(out.hasNext0())
            val batch = out.next0()
            batch.close()
            assert(!out.hasNext0())
        }
        assert(listener.getUsedBytes > 0)
        listener.failUnreserve = true
        try {
          val actual = intercept[IllegalStateException] {
            NativeMemoryManagerJniWrapper.shrink(runtime.memoryManager().getHandle(), 0)
          }
          assert(listener.failures > 0)
          assert(actual eq original)
        } finally {
          listener.failUnreserve = false
        }
    }
  }
}
