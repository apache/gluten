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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, MapType, StructType, TimestampNTZType}

import java.util.Locale

/**
 * Decides whether a Delta MERGE may write its data files with the native writer. MERGE only goes
 * native for tables whose files the native writer reproduces faithfully, and keeps Delta's writer
 * for everything else. The check runs when the command is planned, against the analysis-time
 * snapshot, and again in [[GlutenOptimisticTransaction.writeFiles]], against the metadata the
 * commit will carry, because the table can change in between.
 */
object GlutenDeltaMergeWriteGate {

  private val mergeScope = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  /** Runs `f` with the current thread marked as running a natively written MERGE. */
  def withMergeScope[T](f: => T): T = {
    val previous = mergeScope.get()
    mergeScope.set(true)
    try {
      f
    } finally {
      mergeScope.set(previous)
    }
  }

  def inMergeScope: Boolean = mergeScope.get()

  /** Returns why MERGE must keep Delta's writer for this table, or None if it may go native. */
  def fallbackReason(
      metadata: Metadata,
      protocol: Protocol,
      writeSchema: StructType,
      conf: SQLConf): Option[String] = {
    val schemas = Seq(metadata.schema, writeSchema)
    if (metadata.columnMappingMode != NoMapping) {
      // Delta requires Parquet field ids in both id and name mode; native files have none.
      Some(s"column mapping mode '${metadata.columnMappingMode.name}'")
    } else if (IcebergCompat.isAnyEnabled(metadata)) {
      Some("IcebergCompat (UniForm) is enabled")
    } else if (protocol.writerFeatures.exists(_.contains("materializePartitionColumns"))) {
      // Data files must then contain the partition columns, which the native writer drops.
      Some("the table supports materializePartitionColumns")
    } else if (
      metadata.partitionSchema.exists(f => f.dataType == FloatType || f.dataType == DoubleType)
    ) {
      Some("FLOAT or DOUBLE partition column")
    } else if (schemas.exists(_.existsRecursively(_ == TimestampNTZType))) {
      Some("TIMESTAMP_NTZ column")
    } else if (!conf.caseSensitiveAnalysis && schemas.exists(hasUpperCaseNestedField(_, false))) {
      // The native writer lower-cases nested field names unless analysis is case sensitive.
      Some("nested field name with upper-case letters")
    } else if (!conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {
      // Without stats an empty native file cannot be told apart and would be committed.
      Some("statistics collection is disabled")
    } else if (conf.maxConcurrentOutputFileWriters > 0) {
      Some("spark.sql.maxConcurrentOutputFileWriters is set")
    } else if (metadata.partitionColumns.isEmpty && conf.maxRecordsPerFile > 0) {
      Some("spark.sql.files.maxRecordsPerFile is set on an unpartitioned table")
    } else {
      None
    }
  }

  private def hasUpperCaseNestedField(dataType: DataType, nested: Boolean): Boolean = {
    dataType match {
      case struct: StructType =>
        struct.fields.exists {
          field =>
            (nested && field.name.toLowerCase(Locale.ROOT) != field.name) ||
            hasUpperCaseNestedField(field.dataType, nested = true)
        }
      case array: ArrayType => hasUpperCaseNestedField(array.elementType, nested = true)
      case map: MapType =>
        hasUpperCaseNestedField(map.keyType, nested = true) ||
        hasUpperCaseNestedField(map.valueType, nested = true)
      case _ => false
    }
  }
}
