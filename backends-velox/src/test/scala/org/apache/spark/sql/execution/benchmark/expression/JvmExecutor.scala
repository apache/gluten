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

import org.apache.gluten.utils.Arm

import org.apache.spark.{Partition, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.nio.file.{Files, Path}

private[expression] class JvmExecutor(
    plan: ExpressionPlan,
    vanillaInputs: Seq[Array[ColumnarBatch]],
    shuffleDir: () => Path,
    collector: Option[InternalRow => Unit],
    caseTask: () => TaskContext) {
  private lazy val compiled = plan.inPlanningScope {
    JvmExecutor.compile(
      (plan.vanillaExchange.toSeq.map(_.child) :+ plan.vanillaPlan),
      shuffleDir,
      collector,
      plan.vanillaExchange.map(_.outputPartitioning).collect { case h: HashPartitioning => h }
    )
  }
  private val metrics = new Array[Long](2)
  def execute(): Unit = {
    require(TaskContext.get() eq caseTask(), "Evaluator must run in its owning case task")
    compiled(vanillaInputs, metrics)
  }
  def serializedBytes: Long = metrics(1)

  private def codegenStages(plan: SparkPlan): Int = plan match {
    case _: RDDScanExec | _: RowToColumnarExec => 0
    case other => (if (other.isInstanceOf[WholeStageCodegenExec]) 1 else 0) +
        other.children.map(codegenStages).sum
  }
  def codegenStageCount: Int = codegenStages(plan.vanillaPlan)
}

/** Drives a physical stage without changing Spark's codegen boundaries. */
private[expression] object JvmExecutor {
  type Execution = (Seq[Array[ColumnarBatch]], Array[Long]) => Unit

  private def runSafe[T](body: => T): T = {
    val owner = TaskContext.get()
    require(owner != null, "JVM execution resources require a case task scope")
    val env = SparkEnv.get
    val memory = new TaskMemoryManager(env.memoryManager, owner.taskAttemptId())
    val context = new TaskContextImpl(
      owner.stageId(),
      owner.stageAttemptNumber(),
      owner.partitionId(),
      owner.taskAttemptId(),
      owner.attemptNumber(),
      owner.numPartitions(),
      memory,
      owner.getLocalProperties,
      env.metricsSystem,
      TaskMetrics.empty,
      owner.cpus()
    )
    TaskContext.setTaskContext(context)
    var failure: Option[Throwable] = None
    try {
      val result = body
      require(
        context.taskMetrics.diskBytesSpilled == 0,
        "JVM execution spilled; memory-only execution required")
      result
    } catch {
      case error: Throwable =>
        failure = Some(error)
        try context.markTaskFailed(error)
        catch { case listenerError: Throwable => error.addSuppressed(listenerError) }
        throw error
    } finally {
      try {
        try context.markTaskCompleted(failure)
        catch {
          case cleanup: Throwable => failure match {
              case Some(error) => error.addSuppressed(cleanup)
              case None => throw cleanup
            }
        }
      } finally {
        try memory.cleanUpAllAllocatedMemory()
        finally TaskContext.setTaskContext(owner)
      }
    }
  }

  def compile(
      stagePlans: Seq[SparkPlan],
      shuffleDir: () => Path,
      collector: Option[InternalRow => Unit],
      partitioning: Option[HashPartitioning]): Execution = {
    def consume(rows: Iterator[UnsafeRow]): Unit = collector match {
      case Some(collect) => rows.foreach(row => collect(row.copy()))
      case None => rows.foreach(
          row =>
            BenchmarkBlackhole.consume(
              false,
              row.getBaseObject,
              row.getBaseOffset,
              row.getSizeInBytes))
    }
    def executeWithShuffle(
        stages: Seq[(Int, Iterator[InternalRow]) => Iterator[UnsafeRow]],
        inputs: Seq[Array[ColumnarBatch]],
        metrics: Array[Long]): Unit = {
      val files = inputs.indices.map(i => shuffleDir().resolve(s"vanilla-$i"))
      val serializer =
        new UnsafeRowSerializer(stagePlans.head.output.size).newInstance()
      val partitionId = partitioning.map {
        h => UnsafeProjection.create(Seq(h.partitionIdExpression), stagePlans.head.output)
      }
      inputs.indices.foreach {
        partition =>
          partitionId.foreach(_.initialize(partition))
          Arm.withResource(
            serializer.serializeStream(Files.newOutputStream(files(partition)))) {
            writer =>
              stages.head(
                partition,
                inputs(partition).iterator
                  .asInstanceOf[Iterator[InternalRow]]).foreach {
                row =>
                  partitionId.foreach {
                    project =>
                      require(project(row).getInt(0) == 0, "Only one shuffle reducer is supported")
                  }
                  writer.writeValue(row)
              }
          }
      }
      metrics(1) = files.map(Files.size).sum
      var reader: DeserializationStream = null
      try {
        val rows = files.iterator.flatMap {
          file =>
            if (reader != null) reader.close()
            reader = serializer.deserializeStream(Files.newInputStream(file))
            reader.asKeyValueIterator.map(_._2.asInstanceOf[InternalRow])
        }
        consume(stages.last(0, rows))
      } finally if (reader != null) reader.close()
    }

    val plans = stagePlans.init :+ (stagePlans.last match {
      case stage: WholeStageCodegenExec if collector.isEmpty =>
        stage.withNewChildren(Seq(ResultTerminal(stage.child)))
      case other => other
    })
    val stages = plans.map(compileStage)
    (inputs, metrics) =>
      runSafe {
        if (stages.length == 1) {
          consume(
            stages.head(0, inputs.iterator.flatMap(_.iterator).asInstanceOf[Iterator[InternalRow]]))
        } else {
          executeWithShuffle(stages, inputs, metrics)
        }
      }
  }

  private def compileStage(
      plan: SparkPlan): (Int, Iterator[InternalRow]) => Iterator[UnsafeRow] = {
    class InputRDD extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {
      @transient var source: Iterator[Any] = Iterator.empty
      override def getPartitions: Array[Partition] = Array(new Partition {
        override val index: Int = 0
      })
      override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
        source.asInstanceOf[Iterator[InternalRow]]
    }
    val input = new InputRDD
    val localPlan = plan.transformDown {
      case boundary @ (_: RowToColumnarExec | _: Exchange | _: RDDScanExec) =>
        LocalStageInput(boundary.output, input, boundary.supportsColumnar)
      case coalesce: CoalesceExec if coalesce.numPartitions == 1 => coalesce.child
    }
    val output = localPlan.execute()
    (partition, rows) => {
      input.source = rows
      localPlan.resetMetrics()
      try output.iterator(
          new Partition {
            override val index: Int = partition
          },
          TaskContext.get()).asInstanceOf[Iterator[UnsafeRow]]
      finally input.source = Iterator.empty
    }
  }

}

private case class LocalStageInput(
    override val output: Seq[Attribute],
    input: RDD[InternalRow],
    override val supportsColumnar: Boolean) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = input
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    input.asInstanceOf[RDD[ColumnarBatch]]
}

private case class ResultTerminal(child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output
  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()
  override protected def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    output.map {
      attribute =>
        BindReferences.bindReference(ConsumeCodegenResult(attribute), child.output)
          .genCode(ctx).code.toString
    }.mkString("\n")
  override protected def doExecute(): RDD[InternalRow] = child.execute()
  override protected def withNewChildInternal(newChild: SparkPlan): ResultTerminal =
    copy(child = newChild)
}

final private[expression] case class ConsumeCodegenResult(child: Expression)
  extends UnaryExpression
  with NonSQLExpression {
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false
  override def foldable: Boolean = false
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("The benchmark terminal requires code generation")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val result = child.genCode(ctx)
    val consume = child.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
          DoubleType | DateType | TimestampType | BinaryType | _: ArrayType | _: MapType |
          _: StructType | _: DecimalType | _: YearMonthIntervalType =>
        code"""
              org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole.consume(
                ${result.isNull}, ${result.value});
            """
      case _: StringType =>
        code"""
              org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole.consume(
                ${result.isNull},
                ${result.isNull} ? null : ${result.value}.getBaseObject(),
                ${result.isNull} ? 0L : ${result.value}.getBaseOffset(),
                ${result.isNull} ? 0 : ${result.value}.numBytes());
            """
      case other => throw new IllegalArgumentException(s"Unsupported metric output: $other")
    }
    ev.copy(
      code = code"""
          ${result.code}
          $consume
        """,
      isNull = FalseLiteral,
      value = TrueLiteral
    )
  }
}
