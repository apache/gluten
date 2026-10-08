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
package org.apache.gluten.connector.write

import org.apache.spark.sql.connector.write.WriterCommitMessage

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.iceberg.{DeleteFile, FileFormat, FileMetadata, Metrics, MetricsConfig, MetricsModes, PartitionSpec, Schema}
import org.apache.iceberg.types.Conversions
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.util.{BinaryUtil, UnicodeUtil}

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

case class IcebergEqualityDeleteCommitMessage(deleteFiles: Array[DeleteFile])
  extends WriterCommitMessage

/** Apply Iceberg's manifest metrics policy to statistics returned by the native writer. */
case class IcebergEqualityDeleteMetrics(schema: Schema, config: MetricsConfig) {
  def apply(metrics: Metrics): Metrics = {
    def mode(id: Integer): MetricsModes.MetricsMode =
      Option(schema.findColumnName(id))
        .map(config.columnMode).getOrElse(MetricsModes.None.get())

    def counts(values: java.util.Map[Integer, java.lang.Long])
        : java.util.Map[Integer, java.lang.Long] =
      if (values == null) null
      else values.asScala.filter { case (id, _) => mode(id) != MetricsModes.None.get() }.asJava

    def bounds(values: java.util.Map[Integer, ByteBuffer], lower: Boolean)
        : java.util.Map[Integer, ByteBuffer] = {
      if (values == null) return null
      values.asScala.flatMap {
        case (id, value) =>
          val bound = mode(id) match {
            case m if m == MetricsModes.None.get() || m == MetricsModes.Counts.get() => None
            case truncate: MetricsModes.Truncate =>
              val t = schema.findType(id)
              t.typeId() match {
                case TypeID.STRING =>
                  val text = Conversions.fromByteBuffer[CharSequence](t, value.duplicate()).toString
                  val truncated = if (lower) {
                    UnicodeUtil.truncateStringMin(text, truncate.length())
                  } else UnicodeUtil.truncateStringMax(text, truncate.length())
                  Option(truncated).map(Conversions.toByteBuffer(t, _))
                case TypeID.BINARY =>
                  Option(if (lower) {
                    BinaryUtil.truncateBinaryMin(value.duplicate(), truncate.length())
                  } else {
                    BinaryUtil.truncateBinaryMax(value.duplicate(), truncate.length())
                  })
                case _ => Some(value)
              }
            case _ => Some(value)
          }
          bound.map(id -> _)
      }.asJava
    }
    new Metrics(
      metrics.recordCount(),
      counts(metrics.columnSizes()),
      counts(metrics.valueCounts()),
      counts(metrics.nullValueCounts()),
      counts(metrics.nanValueCounts()),
      bounds(metrics.lowerBounds(), lower = true),
      bounds(metrics.upperBounds(), lower = false)
    )
  }
}

object IcebergEqualityDeleteCommitMessage {
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromJson(
      messages: Array[String],
      spec: PartitionSpec,
      format: FileFormat,
      equalityFieldIds: Seq[Int],
      metricsPolicy: Option[IcebergEqualityDeleteMetrics] =
        None): IcebergEqualityDeleteCommitMessage = {
    require(equalityFieldIds.nonEmpty, "Equality field IDs cannot be empty")
    require(equalityFieldIds.forall(_ > 0), "Equality field IDs must be positive")
    require(equalityFieldIds.distinct.size == equalityFieldIds.size, "Duplicate equality field IDs")
    val files = messages.map {
      json =>
        val file = mapper.readValue(json, classOf[DataFileJson])
        require(
          file.content == "EQUALITY_DELETES",
          s"Expected equality deletes, received ${file.content}")
        require(
          file.equalityFieldIds != null &&
            file.equalityFieldIds.asScala.map(_.intValue()).toSeq == equalityFieldIds,
          "Native equality field IDs do not match the requested IDs"
        )
        FileMetadata
          .deleteFileBuilder(spec)
          .ofEqualityDeletes(equalityFieldIds: _*)
          .withPath(file.path)
          .withFormat(format)
          .withFileSizeInBytes(file.fileSizeInBytes)
          .withPartition(PartitionDataJson.fromJson(file.partitionDataJson, spec))
          .withMetrics(metricsPolicy.map(
            _(file.metrics.metrics())).getOrElse(file.metrics.metrics()))
          .withSplitOffsets(file.splitOffsets)
          .build()
    }
    IcebergEqualityDeleteCommitMessage(files)
  }
}
