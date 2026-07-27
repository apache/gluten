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

import org.apache.gluten.backendsapi.velox.VeloxValidatorApi
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{LocalTableScanTransformer, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Velox-backend implementation of LocalTableScanTransformer.
 *
 * Converts a driver-side local collection (Seq[InternalRow]) into columnar batches using Velox's
 * native row-to-columnar conversion (same JNI path as RowToVeloxColumnarExec).
 */
case class VeloxLocalTableScanTransformer(
    outputAttributes: Seq[Attribute],
    @transient rows: Seq[InternalRow],
    // Row-to-columnar conversion preserves data distribution, so we carry through
    // the original partitioning, consistent with RowToVeloxColumnarExec's behavior.
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder]
) extends LocalTableScanTransformer(outputAttributes, outputPartitioning, outputOrdering)
  with Logging {

  @transient override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
  )

  override protected def doValidateInternal(): ValidationResult = {
    for (field <- schema.fields) {
      val reason = VeloxValidatorApi.validateSchema(field.dataType)
      if (reason.isDefined) {
        return ValidationResult.failed(reason.get)
      }
      val arrowReason = validateArrowCompatibility(field.dataType)
      if (arrowReason.isDefined) {
        return ValidationResult.failed(arrowReason.get)
      }
    }

    logDebug(
      s"local_table_scan native validation succeeded: " +
        s"schema=${schema.fields.map(_.dataType.simpleString).mkString(",")}, " +
        s"appId=${sparkContext.applicationId}")

    ValidationResult.succeeded
  }

  /**
   * Validates that data types are compatible with the Arrow ABI export path used by
   * RowToVeloxColumnarExec.toColumnarBatchIterator:
   *   - Map types can trigger "Map data key type should be a non-nullable" in Arrow export
   *   - Interval types are not supported by ArrowWritableColumnVector
   */
  private def validateArrowCompatibility(dataType: DataType): Option[String] = {
    dataType match {
      case _: MapType =>
        Some(s"Map type is not supported in LocalTableScan Arrow export path: $dataType")
      case _: YearMonthIntervalType | _: DayTimeIntervalType | CalendarIntervalType =>
        Some(s"Interval type is not supported in Arrow export: $dataType")
      case struct: StructType =>
        struct.fields.flatMap(f => validateArrowCompatibility(f.dataType)).headOption
      case array: ArrayType =>
        validateArrowCompatibility(array.elementType)
      case _ => None
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    val localSchema = this.schema
    val batchSize = GlutenConfig.get.maxBatchSize
    val batchBytes = VeloxConfig.get.veloxPreferredBatchBytes

    // `rows` is @transient and becomes null if this transformer is deserialized (e.g. an AQE
    // sub-plan shipped across an RPC boundary). Offload is guarded against null rows in
    // VeloxSparkPlanExecApi.isSupportLocalTableScanExec, so reaching execution with null rows
    // indicates an inconsistent plan; fail fast with a clear message rather than a bare NPE.
    if (rows == null) {
      throw new IllegalStateException(
        "VeloxLocalTableScanTransformer.rows is null (deserialized plan cannot be executed " +
          "natively); this plan should not have been offloaded")
    }

    if (rows.isEmpty) {
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      // Materialize rows as UnsafeRow on the driver, then parallelize
      val proj = UnsafeProjection.create(outputAttributes, outputAttributes)
      val unsafeRows = rows.map(r => proj(r).copy()).toArray
      val numSlices = math.min(
        unsafeRows.length,
        SQLConf.get
          .getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM)
          .getOrElse(sparkContext.defaultParallelism))
      val rowRdd = sparkContext.parallelize(unsafeRows.toSeq, numSlices)

      rowRdd.mapPartitions {
        iter =>
          RowToVeloxColumnarExec.toColumnarBatchIterator(
            iter,
            localSchema,
            numInputRows,
            numOutputBatches,
            convertTime,
            batchSize,
            batchBytes)
      }
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    assert(newChildren.isEmpty, "VeloxLocalTableScanTransformer is a leaf node")
    copy(outputAttributes, rows, outputPartitioning, outputOrdering)
  }
}

object VeloxLocalTableScanTransformer {

  def replace(plan: org.apache.spark.sql.execution.LocalTableScanExec): LocalTableScanTransformer =
    VeloxLocalTableScanTransformer(
      plan.output,
      plan.rows,
      plan.outputPartitioning,
      plan.outputOrdering)
}
