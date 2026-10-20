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
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, MapType, StructType}

import java.util.Locale

/**
 * Decides whether a Delta MERGE may write its data files with the native writer. Like every native
 * write, MERGE first passes [[GlutenDeltaNativeWriteGate]] in
 * [[GlutenOptimisticTransaction.writeFiles]], which keeps Delta's writer for files the Velox writer
 * does not reproduce, such as column mapping, IcebergCompat or partition columns in the data files.
 * This gate only adds what MERGE needs on top. It runs when the command is planned, against the
 * analysis-time snapshot, and again in writeFiles, against the metadata the commit will carry,
 * because the table can change in between.
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

  /**
   * Returns why MERGE must keep Delta's writer for this table, or None if it may go native.
   * `protocol` is unused here, since GlutenDeltaNativeWriteGate checks the table features that
   * change the files; OffloadDeltaCommand, which Delta 3.3 shares, passes it.
   */
  def fallbackReason(
      metadata: Metadata,
      protocol: Protocol,
      writeSchema: StructType,
      conf: SQLConf): Option[String] = {
    val schemas = Seq(metadata.schema, writeSchema)
    if (metadata.partitionSchema.exists(f => f.dataType == FloatType || f.dataType == DoubleType)) {
      // Native writes partitioned by these types are not verified yet (#13138).
      Some("FLOAT or DOUBLE partition column")
    } else if (!conf.caseSensitiveAnalysis && schemas.exists(hasUpperCaseNestedField(_, false))) {
      // The native writer lower-cases nested field names unless analysis is case sensitive.
      Some("nested field name with upper-case letters")
    } else if (!conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {
      // Without stats an empty native file cannot be told apart and would be committed.
      Some("statistics collection is disabled")
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
