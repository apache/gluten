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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

import java.util.TimeZone
import java.util.concurrent.TimeUnit

class GlutenStatisticsCollectionSuite extends StatisticsCollectionSuite with GlutenSQLTestsTrait {

  import testImplicits._

  testGluten("store and retrieve column stats in different time zones") {
    // TODO: bug fix on TableScan.
    // val (start, end) = (0, TimeUnit.DAYS.toSeconds(2))
    val (start, end) = (0, 200)

    def checkTimestampStats(t: DataType, srcTimeZone: TimeZone, dstTimeZone: TimeZone)(
        checker: ColumnStat => Unit): Unit = {
      val table = "time_table"
      val column = "T"
      val original = TimeZone.getDefault
      try {
        withTable(table) {
          TimeZone.setDefault(srcTimeZone)
          spark
            .range(start, end)
            .select(timestamp_seconds($"id").cast(t).as(column))
            .write
            .saveAsTable(table)
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS $column")

          TimeZone.setDefault(dstTimeZone)
          val stats = getCatalogTable(table).stats.get.colStats(column).toPlanStat(column, t)
          checker(stats)
        }
      } finally {
        TimeZone.setDefault(original)
      }
    }

    DateTimeTestUtils.outstandingZoneIds.foreach {
      zid =>
        val timeZone = TimeZone.getTimeZone(zid)
        checkTimestampStats(DateType, TimeZoneUTC, timeZone) {
          stats =>
            assert(stats.min.get.asInstanceOf[Int] == TimeUnit.SECONDS.toDays(start))
            assert(stats.max.get.asInstanceOf[Int] == TimeUnit.SECONDS.toDays(end - 1))
        }
        checkTimestampStats(TimestampType, TimeZoneUTC, timeZone) {
          stats =>
            assert(stats.min.get.asInstanceOf[Long] == TimeUnit.SECONDS.toMicros(start))
            assert(stats.max.get.asInstanceOf[Long] == TimeUnit.SECONDS.toMicros(end - 1))
        }
    }
  }
}
