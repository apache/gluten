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
package org.apache.gluten.extension

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.BatchCDFSchemaEndVersion
import org.apache.spark.sql.delta.commands.cdc.CDCReader

object DeltaCDFRelationHelper {
  def changesToBatchDF(
      relation: CDCReader.DeltaCDFRelation,
      spark: SparkSession): DataFrame = {
    val deltaLog = relation.snapshotWithSchemaMode.snapshot.deltaLog
    val latestVersion = deltaLog.update().version
    val endingVersionForBatchSchema =
      relation.endingVersion.map(v => latestVersion.min(v)).getOrElse(latestVersion)
    val snapshotForBatchSchema = relation.snapshotWithSchemaMode.schemaMode match {
      case BatchCDFSchemaEndVersion => deltaLog.getSnapshotAt(endingVersionForBatchSchema)
      case _ => relation.snapshotWithSchemaMode.snapshot
    }
    val endVersion = relation.endingVersion.getOrElse(latestVersion)

    CDCReader.changesToBatchDF(
      deltaLog,
      relation.startingVersion.get,
      endVersion,
      spark,
      readSchemaSnapshot = Some(snapshotForBatchSchema))
  }
}
