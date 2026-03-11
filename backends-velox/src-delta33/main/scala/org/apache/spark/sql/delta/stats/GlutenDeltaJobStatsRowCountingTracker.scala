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
package org.apache.spark.sql.delta.stats

import org.apache.gluten.execution.{PlaceholderRow, TerminalRow}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}

/**
 * A fallback stats tracker to simply call `newRow` many times when a columnar batch comes. There is
 * no C2R process involved to save performance. Therefore, the delegate stats tracker must not read
 * the row in its `newRow` implementation.
 */
private[stats] class GlutenDeltaJobStatsRowCountingTracker(val delegate: WriteJobStatsTracker)
  extends WriteJobStatsTracker {
  import GlutenDeltaJobStatsRowCountingTracker._

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new GlutenDeltaTaskStatsRowCountingTracker(delegate.newTaskInstance())
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    delegate.processStats(stats, jobCommitTime)
  }
}

private object GlutenDeltaJobStatsRowCountingTracker {
  private class GlutenDeltaTaskStatsRowCountingTracker(delegate: WriteTaskStatsTracker)
    extends WriteTaskStatsTracker {
    override def newPartition(partitionValues: InternalRow): Unit =
      delegate.newPartition(partitionValues)

    override def newFile(filePath: String): Unit = delegate.newFile(filePath)

    override def closeFile(filePath: String): Unit = delegate.closeFile(filePath)

    override def newRow(filePath: String, row: InternalRow): Unit = row match {
      case _: PlaceholderRow =>
      case t: TerminalRow =>
        for (_ <- 0 until t.batch().numRows()) {
          // Here we pass null row to the delegate stats tracker as we assume
          // the delegate stats tracker only counts on row numbers.
          delegate.newRow(filePath, null)
        }
    }

    override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
      delegate.getFinalStats(taskCommitTime)
    }
  }
}
