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
package org.apache.spark.sql.kafka010

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.rel.{SplitInfo, StreamKafkaSourceBuilder}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.kafka010.KafkaConfigUpdater
import org.apache.spark.sql.connector.read.{InputPartition, Scan}

import scala.collection.JavaConverters._

object GlutenStreamKafkaSourceUtil {
  def getKafkaParams(inputPartition: InputPartition): Map[String, String] = inputPartition match {
    case batch: KafkaBatchInputPartition =>
      // Validate the effective settings Spark would use, including delegation-token
      // authentication. This copies the parameters; split serialization below stays unchanged.
      KafkaConfigUpdater("executor", batch.executorKafkaParams.asScala.toMap)
        .setAuthenticationConfigIfNeeded()
        .build()
        .asScala
        .map { case (key, value) => key -> value.toString }
        .toMap
    case _ =>
      throw new UnsupportedOperationException("Only support kafka KafkaBatchInputPartition.")
  }

  def genSplitInfo(inputPartition: InputPartition): SplitInfo = inputPartition match {
    case batch: KafkaBatchInputPartition =>
      StreamKafkaSourceBuilder.makeStreamKafkaBatch(
        batch.offsetRange.topicPartition.topic(),
        batch.offsetRange.topicPartition.partition(),
        batch.offsetRange.fromOffset,
        batch.offsetRange.untilOffset,
        batch.pollTimeoutMs,
        batch.failOnDataLoss,
        batch.includeHeaders,
        batch.executorKafkaParams
      )
    case _ =>
      throw new UnsupportedOperationException("Only support kafka KafkaBatchInputPartition.")
  }

  def getFileFormat(scan: Scan): ReadFileFormat = scan.getClass.getSimpleName match {
    case "KafkaScan" => ReadFileFormat.KafkaReadFormat
    case _ =>
      throw new GlutenNotSupportException("Only support KafkaScan.")
  }

}
