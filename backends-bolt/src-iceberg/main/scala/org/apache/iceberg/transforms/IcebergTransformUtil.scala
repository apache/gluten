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
package org.apache.iceberg.transforms

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.proto.{IcebergPartitionField, TransformType}

import org.apache.iceberg.{PartitionField, PartitionSpec}

object IcebergTransformUtil {

  def convertPartitionField(field: PartitionField, spec: PartitionSpec): IcebergPartitionField = {
    val transform = field.transform()
    val sourceName = spec.schema().asStruct().field(field.sourceId()).name()
    var builder =
      IcebergPartitionField.newBuilder().setName(sourceName).setSourceId(field.sourceId())
    builder = transform match {
      case _: Identity[_] => builder.setTransform(TransformType.IDENTITY)
      case _: Years[_] => builder.setTransform(TransformType.YEAR)
      case _: Months[_] => builder.setTransform(TransformType.MONTH)
      case _: Days[_] => builder.setTransform(TransformType.DAY)
      case _: Hours[_] => builder.setTransform(TransformType.HOUR)
      case b: Bucket[_] => builder.setTransform(TransformType.BUCKET).setParameter(b.numBuckets())
      case t: Truncate[_] => builder.setTransform(TransformType.TRUNCATE).setParameter(t.width)
      case t: Timestamps => builder.setTransform(convertTimestamps(t))
      case d: Dates => builder.setTransform(convertDates(d))
    }
    builder.build()
  }

  private def convertTimestamps(timestamps: Timestamps): TransformType = {
    // We could not match the enum instance because Iceberg 1.5.0 enum is different, and we fall
    // back TimestampNano data type
    timestamps.toString match {
      case "hour" => TransformType.HOUR
      case "day" => TransformType.DAY
      case "month" => TransformType.MONTH
      case "year" => TransformType.YEAR
      case _ => throw new GlutenNotSupportException()
    }
  }

  private def convertDates(dates: Dates): TransformType = {
    // We could not match the enum instance because Iceberg 1.5.0 enum is different, and we fall
    // back TimestampNano data type
    dates match {
      case Dates.DAY => TransformType.DAY
      case Dates.MONTH => TransformType.MONTH
      case Dates.YEAR => TransformType.YEAR
      case _ => throw new GlutenNotSupportException()
    }
  }
}
