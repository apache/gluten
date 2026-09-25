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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.utils.BackendTestUtils
import org.apache.gluten.vectorized.{ColumnarBatchInIterator, ColumnarBatchOutIterator, NativePlanEvaluator}

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkConf, SparkNumberFormatException, SparkThrowable}
import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import java.io.PrintWriter
import java.util.{Collections, Iterator => JIterator}

class GlutenJniExceptionSuite extends GlutenQueryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
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
}
