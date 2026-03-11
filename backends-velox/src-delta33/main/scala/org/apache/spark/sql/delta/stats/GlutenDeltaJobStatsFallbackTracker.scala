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

import org.apache.gluten.execution.{PlaceholderRow, TerminalRow, VeloxColumnarToRowExec}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * A fallback stats tracker where a C2R converter converts all the incoming batches to rows then
 * send to the delegate tracker.
 */
private[stats] class GlutenDeltaJobStatsFallbackTracker(val delegate: WriteJobStatsTracker)
  extends WriteJobStatsTracker {
  import GlutenDeltaJobStatsFallbackTracker._

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new GlutenDeltaTaskStatsFallbackTracker(delegate.newTaskInstance())
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    delegate.processStats(stats, jobCommitTime)
  }
}

private object GlutenDeltaJobStatsFallbackTracker {
  private class GlutenDeltaTaskStatsFallbackTracker(delegate: WriteTaskStatsTracker)
    extends WriteTaskStatsTracker {
    private val c2r = new VeloxColumnarToRowExec.Converter(new SQLMetric("convertTime"))

    override def newPartition(partitionValues: InternalRow): Unit =
      delegate.newPartition(partitionValues)

    override def newFile(filePath: String): Unit = delegate.newFile(filePath)

    override def closeFile(filePath: String): Unit = delegate.closeFile(filePath)

    override def newRow(filePath: String, row: InternalRow): Unit = row match {
      case _: PlaceholderRow =>
      case t: TerminalRow =>
        c2r.toRowIterator(t.batch()).foreach(eachRow => delegate.newRow(filePath, eachRow))
    }

    override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
      delegate.getFinalStats(taskCommitTime)
    }
  }
}
