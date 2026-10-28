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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import java.nio.file.Files
import java.nio.file.Paths

import scala.collection.JavaConverters._

private[expression] class ExpressionEvaluator(
    val plan: ExpressionPlan,
    private[expression] val batchSize: Int = 10240,
    vanillaCollector: Option[InternalRow => Unit] = None,
    nativeCollector: Option[ColumnarBatch => Unit] = None) {
  def scenario: Catalog.CaseDef = plan.scenario
  private def spark: SparkSession = plan.spark
  private def context: Data.Context = plan.context
  def name: String = scenario.id
  def numRows: Long = context.rows
  private[expression] def dataPlan: Data.Plan = plan.dataPlan
  def outputSchema: StructType = plan.outputSchema
  private lazy val jvmExecutor = new JvmExecutor(
    plan,
    vanillaInputs,
    () => shuffleDir,
    vanillaCollector,
    () => caseTask)
  protected lazy val nativeExecutor = withCaseResources {
    new NativeExecutor(plan, nativeInputs, batchSize, () => shuffleDir, nativeCollector)
  }
  def runVanilla(): Unit = jvmExecutor.execute()
  def runNative(): Unit = nativeExecutor.execute()
  private lazy val caseTask = {
    val task = TaskContext.get()
    require(task != null, "Execution resources require a case task scope")
    task
  }
  private def withCaseResources[T](body: => T): T = {
    val current = TaskContext.get()
    TaskContext.setTaskContext(caseTask)
    try body
    finally TaskContext.setTaskContext(current)
  }
  private lazy val inputs = withCaseResources {
    val rows = Data.materializeRows(dataPlan, context, batchSize)
    val partitionRows =
      if (plan.partitions == 1) {
        Seq(rows)
      } else {
        (0 until plan.partitions).map(
          i =>
            rows.slice(
              (rows.length.toLong * i / plan.partitions).toInt,
              (rows.length.toLong * (i + 1) / plan.partitions).toInt))
      }
    val writable = new Array[WritableColumnVector](dataPlan.inputSchema.length)
    val converter = new RowToColumnConverter(dataPlan.inputSchema)
    val vanilla = partitionRows.map(_.grouped(batchSize).map {
      batch =>
        val columns = OnHeapColumnVector.allocateColumns(batch.length, dataPlan.inputSchema)
        columns.indices.foreach(i => writable(i) = columns(i))
        try batch.foreach(row => converter.convert(row, writable))
        catch {
          case error: Throwable =>
            columns.foreach(_.close())
            throw error
        }
        val out = new ColumnarBatch(columns.map(_.asInstanceOf[ColumnVector]), batch.length)
        TaskResources.addRecycler("JVM input batch", 0)(out.close())
        out
    }.toArray)
    val native = partitionRows.map(
      part =>
        Data.materializeBatches(
          part.iterator,
          dataPlan.inputSchema,
          batchSize).toArray)
    (vanilla, native)
  }
  protected def vanillaInputs: Seq[Array[ColumnarBatch]] = inputs._1
  protected def nativeInputs: Seq[Array[ColumnarBatch]] = inputs._2
  def inputRows: Iterator[InternalRow] =
    vanillaInputs.iterator.flatMap(_.iterator).flatMap(_.rowIterator().asScala)
  lazy val nativeInputBatches: Seq[ColumnarBatch] = nativeInputs.flatten
  protected lazy val shuffleDir = withCaseResources {
    val root = Paths.get(spark.sparkContext.getConf.get(
      "spark.gluten.benchmark.shuffleDir",
      System.getProperty("java.io.tmpdir")))
    Files.createDirectories(root)
    val dir = Files.createTempDirectory(root, "expression-benchmark-")
    TaskResources.addRecycler("benchmark directory", 0)(Utils.deleteRecursively(dir.toFile))
    dir
  }
  def metadata: Map[String, String] = nativeExecutor.metadata ++ Map(
    "mode" -> (if (plan.window) "window"
               else if (plan.vanillaExchange.isDefined) "partial-final"
               else if (!plan.aggregate) "scalar" else "complete"),
    "requestedReducers" -> scenario.reducers.getOrElse(1).toString,
    "actualReducers" -> "1",
    "shuffleDirectory" -> (if (plan.nativeExchange.isDefined) shuffleDir.toString else ""),
    "vanilla.serializedBytes" -> jvmExecutor.serializedBytes.toString,
    "inputPartitions" -> plan.partitions.toString,
    "vanillaCodegenStages" -> jvmExecutor.codegenStageCount.toString
  )
}
private[expression] object ExpressionEvaluator {
  val aggregateSqlConf: Seq[(String, String)] = Seq(
    "spark.sql.ansi.enabled" -> "false",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.shuffle.partitions" -> "1",
    "spark.gluten.sql.mergeTwoPhasesAggregate.enabled" -> "false",
    "spark.gluten.sql.columnar.backend.velox.spillStrategy" -> "none"
  )

  def apply(
      spark: SparkSession,
      scenario: Catalog.CaseDef,
      context: Data.Context,
      batchSize: Int = 10240,
      data: Option[Data.Plan] = None,
      vanillaCollector: Option[InternalRow => Unit] = None,
      nativeCollector: Option[InternalRow => Unit] = None): ExpressionEvaluator = {
    val plan =
      new ExpressionPlan(spark, scenario, context, data.getOrElse(Data.compile(scenario)))
    val validation = plan.validate()
    require(validation.ok(), validation.reason())
    val batches: Option[ColumnarBatch => Unit] =
      nativeCollector.map {
        rows =>
          lazy val collect = collectNative(plan.outputSchema)(rows)
          batch => {
            if (!plan.aggregate && !plan.window) {
              val rows = batch.numRows()
              require(
                batch.numCols() == plan.outputSchema.length && rows > 0 &&
                  (plan.generator || rows == batchSize || rows == context.rows % batchSize),
                "Native output batch shape differs"
              )
            }
            collect(batch)
          }
      }
    new ExpressionEvaluator(plan, batchSize, vanillaCollector, batches)

  }

  def collectNative(schema: StructType)(collect: InternalRow => Unit): ColumnarBatch => Unit = {
    val read = UnsafeProjection.create(schema.asNullable)
    read.initialize(0)
    // Arrow output omits Spark timestamp timezone and nested nullability metadata.
    def compatible(actual: DataType, expected: DataType): Boolean = (actual, expected) match {
      case (TimestampNTZType, TimestampType) => true
      case (ArrayType(a, _), ArrayType(e, _)) => compatible(a, e)
      case (MapType(ak, av, _), MapType(ek, ev, _)) => compatible(ak, ek) && compatible(av, ev)
      case (a: StructType, e: StructType) =>
        a.length == e.length && a.zip(e).forall {
          case (x, y) => compatible(x.dataType, y.dataType)
        }
      case _ => actual == expected
    }
    batch =>
      ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), batch)
      require(batch.numCols() == schema.length, "Native output column count differs")
      schema.fields.indices.foreach {
        i =>
          require(
            compatible(batch.column(i).dataType(), schema(i).dataType),
            s"Native output type differs: ${batch.column(i).dataType()} != ${schema(i).dataType}")
      }
      (0 until batch.numRows()).foreach(i => collect(read(batch.getRow(i)).copy()))
  }

}
