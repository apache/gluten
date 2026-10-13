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
package org.apache.iceberg.spark.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CompactIcebergTable(spark: SparkSession, delegate: SparkTable)
  extends SparkTable(delegate.table(), delegate.snapshotId(), false) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    delegate.newScanBuilder(options) match {
      case builder: SparkScanBuilder if builder.getClass == classOf[SparkScanBuilder] =>
        new ScanBuilder with SupportsPushDownAggregates with SupportsPushDownV2Filters
          with SupportsPushDownRequiredColumns with SupportsReportStatistics {
          override def build(): Scan = CompactIcebergBatchScan.wrap(spark, builder.build(), options)
          override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] =
            builder.pushPredicates(predicates)
          override def pushedPredicates(): Array[Predicate] = builder.pushedPredicates()
          override def pruneColumns(schema: StructType): Unit = builder.pruneColumns(schema)
          override def pushAggregation(aggregation: Aggregation): Boolean =
            builder.pushAggregation(aggregation)
          override def supportCompletePushDown(aggregation: Aggregation): Boolean =
            builder.supportCompletePushDown(aggregation)
          override def readSchema(): StructType = builder.readSchema()
          override def estimateStatistics(): Statistics = builder.estimateStatistics()
        }
      case builder => builder
    }
  }
}
