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

import org.apache.gluten.columnarbatch.{BoltColumnarBatches, ColumnarBatches}
import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowColumnVectors

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Generator}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.execution.{GenerateExec, LeafExecNode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

/** Returns no eval rows and one final count, including for an empty partition. */
private case class BoltBufferedCount(input: Expression) extends Generator with CodegenFallback {
  private var count = 0
  private var terminated = false
  override def children: Seq[Expression] = Seq(input)
  override def elementSchema: StructType = new StructType().add("count", IntegerType)
  override def eval(row: InternalRow): Seq[InternalRow] = {
    require(!terminated)
    input.eval(row)
    count += 1
    Seq.empty
  }
  override def terminate(): Seq[InternalRow] = {
    require(!terminated, "Generator must terminate exactly once")
    terminated = true
    Seq(InternalRow(count))
  }
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(input = newChildren.head)
}

/** Recycling is deliberate: advancing this producer closes the previous native input batch. */
private case class BoltTerminationInput(
    override val output: Seq[Attribute],
    batches: Seq[Seq[(Int, String)]]) extends LeafExecNode {
  override def supportsColumnar: Boolean = true
  override protected def doExecute(): RDD[InternalRow] = {
    sparkContext.parallelize(Seq(batches), 1).flatMap {
      partition =>
        partition.flatten.map { case (id, value) => InternalRow(id, UTF8String.fromString(value)) }
    }
  }
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val schema = ExpressionUtils.structFromAttributes(output)
    sparkContext.parallelize(Seq(batches), 1).mapPartitions {
      partitions =>
        val native = partitions.flatMap(_.iterator).map {
          rows =>
            val arrow = ArrowColumnVectors.fromRows(
              schema,
              rows.iterator.map {
                case (id, value) => InternalRow(id, UTF8String.fromString(value))
              })
            BoltColumnarBatches.toBoltBatch(
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), arrow))
        }
        Iterators.wrap(native).protectInvocationFlow().recyclePayload(_.close()).create()
    }
  }
}

class BoltPartialGenerateTerminationSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private val cases: Seq[(String, Seq[Seq[(Int, String)]], Option[String], Int)] = Seq(
    (
      "trailing empty batches",
      Seq(Seq((1, "first"), (2, "retained-last")), Nil, Nil),
      Some("retained-last"),
      2),
    ("replaced last batch", Seq(Seq((1, "old")), Nil, Seq((2, "new")), Nil), Some("new"), 2),
    ("only empty batches", Seq(Nil, Nil), None, 0),
    ("no batches", Nil, None, 0)
  )

  cases.foreach {
    case (name, input, lastValue, expectedCount) =>
      test(s"partial generate terminates once with $name") {
        val id = AttributeReference("id", IntegerType, nullable = false)()
        val label = AttributeReference("label", StringType, nullable = true)()
        val count = AttributeReference("count", IntegerType, nullable = false)()
        // Spark can emit termination rows without input only when no child output is required.
        val requiredChildOutput = if (input.exists(_.nonEmpty)) Seq(label) else Nil
        val countOrdinal = requiredChildOutput.size
        def result(row: InternalRow): (Option[String], Int) = {
          require(row.numFields == countOrdinal + 1)
          val last =
            if (countOrdinal == 0 || row.isNullAt(0)) None
            else Some(row.getUTF8String(0).toString)
          (last, row.getInt(countOrdinal))
        }
        def plan(): GenerateExec = {
          val child = BoltTerminationInput(Seq(id, label), input)
          GenerateExec(BoltBufferedCount(id), requiredChildOutput, false, Seq(count), child)
        }
        val expected = Seq((lastValue, expectedCount))
        val sparkRows = plan().executeCollect().map(result).toSeq
        assert(sparkRows == expected)
        val vanilla = plan()
        val native = ColumnarPartialGenerateExec(vanilla, vanilla.child)
        val actual = native.executeColumnar().mapPartitions {
          batches =>
            batches.flatMap {
              batch =>
                val loaded = ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), batch)
                (0 until loaded.numRows()).map {
                  rowId => result(loaded.getRow(rowId))
                }.iterator
            }
        }.collect().toSeq
        assert(actual == expected)
      }
  }

  cases.filter(_._2.forall(_.isEmpty)).foreach {
    case (name, input, _, _) =>
      test(s"partial generate preserves Spark's missing-child failure with $name") {
        val id = AttributeReference("id", IntegerType, nullable = false)()
        val label = AttributeReference("label", StringType, nullable = true)()
        val count = AttributeReference("count", IntegerType, nullable = false)()
        def plan(): GenerateExec = {
          val child = BoltTerminationInput(Seq(id, label), input)
          GenerateExec(BoltBufferedCount(id), Seq(label), false, Seq(count), child)
        }
        def assertNullChildFailure(body: => Unit): Unit = {
          val error = intercept[SparkException](body)
          val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
          assert(causes.exists(_.isInstanceOf[NullPointerException]))
        }
        assertNullChildFailure {
          plan().executeCollect()
        }
        val vanilla = plan()
        assertNullChildFailure {
          ColumnarPartialGenerateExec(vanilla, vanilla.child)
            .executeColumnar()
            .mapPartitions(batches => batches.map(_.numRows()))
            .collect()
        }
      }
  }
}
