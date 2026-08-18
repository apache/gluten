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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RDDScanTransformer, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Velox-backend implementation of RDDScanTransformer.
 *
 * Converts an RDD[InternalRow] into columnar batches using Velox's native row-to-columnar
 * conversion (same JNI path as RowToVeloxColumnarExec).
 */
case class VeloxRDDScanTransformer(
    outputAttributes: Seq[Attribute],
    rdd: RDD[InternalRow],
    name: String,
    // Row-to-columnar conversion preserves data distribution, so we carry through
    // the original partitioning. This differs from CH which uses UnknownPartitioning(0)
    // but is consistent with RowToVeloxColumnarExec's behavior.
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder]
) extends RDDScanTransformer(outputAttributes, outputPartitioning, outputOrdering)
  with Logging {

  override def nodeName: String = name

  @transient override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
  )

  override protected def doValidateInternal(): ValidationResult = {
    if (schema.isEmpty) {
      return ValidationResult.failed("RDDScan with an empty schema is not supported")
    }
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
    ValidationResult.succeeded
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    val localSchema = this.schema
    val batchSize = GlutenConfig.get.maxBatchSize
    val batchBytes = VeloxConfig.get.veloxPreferredBatchBytes
    rdd.mapPartitions {
      iter =>
        if (iter.hasNext) {
          val first = iter.next()
          // A partition is homogeneous by construction: a checkpoint/cache of a Gluten
          // columnar plan yields all BatchCarrierRows, while any other RDD yields all
          // InternalRows. We therefore select the path once, based on the first row, and
          // apply it to the whole partition.
          first match {
            case _: BatchCarrierRow =>
              // RDD already contains columnar batches wrapped as carrier rows
              // (e.g., from df.checkpoint() on a Gluten plan). Unwrap directly.
              // No row conversion happens here, so convertTime is intentionally left at 0;
              // numInputRows is credited with the already-batched row count for observability.
              (Iterator.single(first) ++ iter).flatMap {
                row =>
                  BatchCarrierRow.unwrap(row).map {
                    batch =>
                      numOutputBatches += 1
                      numInputRows += batch.numRows()
                      batch
                  }
              }
            case _ =>
              // Standard InternalRow path - convert via native row-to-columnar.
              val rowIter = Iterator.single(first) ++ iter
              val processedIter = if (schemaHasNonNullableField(localSchema)) {
                // Pre-convert rows to UnsafeRow respecting schema nullability.
                // This matches Spark's codegen behavior: for non-nullable fields, getLong/getInt
                // is called directly without isNullAt check, so null values in non-nullable
                // columns produce the type's default (0 for Long) rather than setting null bits.
                // Without this, UnsafeProjection.create(schema) always uses nullable=true,
                // which would incorrectly propagate nulls for non-nullable fields (SPARK-35912).
                // The guard recurses into nested struct/array/map types so a non-nullable field
                // nested under a nullable top-level column still routes through the projection
                // (which itself handles nested nullability), rather than silently skipping the fix.
                createNullabilityAwareIterator(rowIter, localSchema)
              } else {
                rowIter
              }
              RowToVeloxColumnarExec.toColumnarBatchIterator(
                processedIter,
                localSchema,
                numInputRows,
                numOutputBatches,
                convertTime,
                batchSize,
                batchBytes)
          }
        } else {
          Iterator.empty
        }
    }
  }

  /**
   * Returns true if `dataType` declares any non-nullable field at any nesting level. The top-level
   * routing guard uses this so that a non-nullable field nested inside an otherwise-nullable
   * struct/array/map column still triggers the nullability-aware projection, matching Spark's
   * codegen null->default behavior for non-nullable fields (SPARK-35912).
   *
   * Coercion is exact for (nested) struct fields, which UnsafeProjection writes via typed getters
   * that turn a null into the primitive default. It is best-effort for elements nested inside a
   * `containsNull = false` array or a `valueContainsNull = false` map: the projection copies the
   * array/map payload wholesale and does not rewrite individual element nulls. Such payloads (a
   * null element in a declared non-null collection produced by a raw RDD) are pathological and not
   * expected in practice; the guard still routes them through the projection for consistency.
   */
  private def schemaHasNonNullableField(dataType: DataType): Boolean = dataType match {
    case s: StructType =>
      s.fields.exists(f => !f.nullable || schemaHasNonNullableField(f.dataType))
    case a: ArrayType =>
      !a.containsNull || schemaHasNonNullableField(a.elementType)
    case m: MapType =>
      // Map keys are non-null by Spark semantics (not a data-null risk), so they do not by
      // themselves trigger the guard; only declared non-null values or nested non-nullable
      // fields in the key/value types do.
      !m.valueContainsNull || schemaHasNonNullableField(m.keyType) ||
      schemaHasNonNullableField(m.valueType)
    case _ => false
  }

  /**
   * Creates an iterator that converts InternalRows to UnsafeRows while respecting schema
   * nullability. For non-nullable fields, values are read via typed getters (getLong, getInt, etc.)
   * which return default values (0) for null inputs, matching Spark's WholeStageCodegen behavior.
   */
  private def createNullabilityAwareIterator(
      iter: Iterator[InternalRow],
      schema: StructType): Iterator[InternalRow] = {
    // Create BoundReferences that respect the schema's declared nullability.
    // When nullable=false, the generated code calls getLong/getInt directly without
    // checking isNullAt, so null.asInstanceOf[Long] unboxes to 0.
    val boundRefs = schema.fields.zipWithIndex.map {
      case (field, i) => BoundReference(i, field.dataType, field.nullable)
    }.toSeq
    val projection = UnsafeProjection.create(boundRefs)
    iter.map {
      row =>
        // The projection returns a mutable UnsafeRow that is reused across calls. This is safe
        // for two reasons: (1) toColumnarBatchIterator copies each row's bytes into an ArrowBuf
        // via Platform.copyMemory before advancing the iterator, and (2) its convertToUnsafeRow
        // passes an already-UnsafeRow straight through without re-projecting, so this
        // nullability-aware projection replaces (not adds to) the converter's internal one.
        projection.apply(row)
    }
  }

  /**
   * Additional validation for Arrow export compatibility. The RDDScan path transfers data via Arrow
   * ABI, which has stricter constraints than Velox's type system:
   *   - Map types can trigger "Map data key type should be a non-nullable" in Arrow export
   *   - Interval types are not supported by ArrowWritableColumnVector
   */
  private def validateArrowCompatibility(dataType: DataType): Option[String] = {
    dataType match {
      case _: MapType =>
        Some(s"Map type is not supported in RDDScan Arrow export path: $dataType")
      case _: YearMonthIntervalType | _: DayTimeIntervalType | CalendarIntervalType =>
        Some(s"Interval type is not supported in Arrow export: $dataType")
      case struct: StructType =>
        struct.fields.flatMap(f => validateArrowCompatibility(f.dataType)).headOption
      case array: ArrayType =>
        validateArrowCompatibility(array.elementType)
      case _ => None
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    assert(newChildren.isEmpty, "VeloxRDDScanTransformer is a leaf node")
    copy(outputAttributes, rdd, name, outputPartitioning, outputOrdering)
  }
}

object VeloxRDDScanTransformer {

  def replace(plan: org.apache.spark.sql.execution.RDDScanExec): RDDScanTransformer =
    VeloxRDDScanTransformer(
      plan.output,
      plan.inputRDD,
      plan.nodeName,
      plan.outputPartitioning,
      plan.outputOrdering)
}
