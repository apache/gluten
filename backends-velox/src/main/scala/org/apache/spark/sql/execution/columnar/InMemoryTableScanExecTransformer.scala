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
package org.apache.spark.sql.execution.columnar

import org.apache.gluten.execution.VeloxColumnarToRowExec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * [[InMemoryTableScanExec]] over a Velox columnar table cache, with read metrics.
 *
 * Spark's [[InMemoryTableScanExec]] only reports `numOutputRows`, and the [[CachedBatchSerializer]]
 * API has no access to the plan node, so the cost of reading a Velox cache is invisible in the SQL
 * UI. When a vanilla Spark operator reads the cache, the Velox-to-row conversion happens inside
 * this node and is reported by the parent operator instead.
 *
 * This node subclasses [[InMemoryTableScanExec]] rather than implementing
 * [[InMemoryTableScanLike]], so code in Spark and Gluten that matches the concrete class (AQE table
 * cache stages, the SQL UI plan graph, `CollectMetricsExec`, `PlanUtil.isGlutenTableCache`) keeps
 * working on every supported Spark version. It re-implements Spark's private
 * `filteredCachedBatches` and the two execute paths to add the metrics; everything else is
 * inherited.
 *
 * Gluten forbids inheriting from case classes because the subclass would be equal to its parent
 * under case-class equality. Spark still matches the concrete [[InMemoryTableScanExec]] class (for
 * example `SparkPlanInfo`, which shows the cached plan in the SQL UI, and `CollectMetricsExec`), so
 * a separate [[InMemoryTableScanLike]] node would lose those features. The rule is suppressed here
 * and equality is handled explicitly by `canEqual` and `equals` below.
 */
@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class InMemoryTableScanExecTransformer(
    attrs: Seq[Attribute],
    preds: Seq[Expression],
    @transient rel: InMemoryRelation)
  extends InMemoryTableScanExec(attrs, preds, rel) {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numCachedBatches" -> SQLMetrics.createMetric(sparkContext, "number of cached batches"),
    "numCachedBatchesRead" ->
      SQLMetrics.createMetric(sparkContext, "number of cached batches read after pruning"),
    "cachedBytesRead" -> SQLMetrics.createSizeMetric(sparkContext, "cached bytes read"),
    "readTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to read cached batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of columnar to row batches"),
    // VeloxColumnarToRowExec.toRowIterator adds milliseconds to convertTime.
    "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time of columnar to row")
  )

  // Case-class equality would make this node equal to the vanilla InMemoryTableScanExec it
  // replaces, and TreeNode.transform keeps the original node when the rule's result is equal to
  // it. Make the two classes unequal.
  override def canEqual(other: Any): Boolean = other.isInstanceOf[InMemoryTableScanExecTransformer]

  override def equals(other: Any): Boolean = other match {
    case o: InMemoryTableScanExecTransformer => super.equals(o)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  /** Same as Spark's private `filteredCachedBatches`, counting batches before and after pruning. */
  private def filteredCachedBatchesWithMetrics(): RDD[CachedBatch] = {
    val numCachedBatches = longMetric("numCachedBatches")
    val numCachedBatchesRead = longMetric("numCachedBatchesRead")
    val cachedBytesRead = longMetric("cachedBytesRead")
    val buffers = relation.cacheBuilder.cachedColumnBuffers.mapPartitionsInternal {
      iter =>
        iter.map {
          batch =>
            numCachedBatches += 1
            batch
        }
    }
    val filtered = if (conf.inMemoryPartitionPruning) {
      val filterFunc = relation.cacheBuilder.serializer.buildFilter(predicates, relation.output)
      buffers.mapPartitionsWithIndexInternal(filterFunc)
    } else {
      buffers
    }
    filtered.mapPartitionsInternal {
      iter =>
        iter.map {
          batch =>
            numCachedBatchesRead += 1
            cachedBytesRead += batch.sizeInBytes
            batch
        }
    }
  }

  private def readColumnar(input: RDD[CachedBatch]): RDD[ColumnarBatch] = {
    val readTime = longMetric("readTime")
    relation.cacheBuilder.serializer
      .convertCachedBatchToColumnarBatch(input, relation.output, attributes, conf)
      .mapPartitionsInternal {
        iter =>
          new Iterator[ColumnarBatch] {
            override def hasNext: Boolean = {
              val start = System.nanoTime()
              try iter.hasNext
              finally readTime += System.nanoTime() - start
            }

            override def next(): ColumnarBatch = {
              val start = System.nanoTime()
              try iter.next()
              finally readTime += System.nanoTime() - start
            }
          }
      }
  }

  private lazy val columnarInputRDDWithMetrics: RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    readColumnar(filteredCachedBatchesWithMetrics()).mapPartitionsInternal {
      iter =>
        iter.map {
          batch =>
            numOutputRows += batch.numRows()
            batch
        }
    }
  }

  private lazy val inputRDDWithMetrics: RDD[InternalRow] = {
    // Keeps Spark's test-only accumulators and its row-path numOutputRows accounting.
    if (enableAccumulatorsForTest) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")
    val accumulatorsEnabled = enableAccumulatorsForTest
    val readPartitionsAcc = readPartitions
    val readBatchesAcc = readBatches
    val withMetrics = filteredCachedBatchesWithMetrics().mapPartitionsInternal {
      iter =>
        if (accumulatorsEnabled && iter.hasNext) {
          readPartitionsAcc.add(1)
        }
        iter.map {
          batch =>
            if (accumulatorsEnabled) {
              readBatchesAcc.add(1)
            }
            numOutputRows += batch.numRows
            batch
        }
    }
    readColumnar(withMetrics).mapPartitionsInternal {
      batches =>
        // Rows are already counted on the cached batches above, as Spark does. Created in the
        // task because an unregistered accumulator cannot be shipped to executors.
        val unusedRows = new SQLMetric("unusedRows")
        VeloxColumnarToRowExec.toRowIterator(batches, unusedRows, numInputBatches, convertTime)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = inputRDDWithMetrics

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = columnarInputRDDWithMetrics
}

object InMemoryTableScanExecTransformer {
  def apply(scan: InMemoryTableScanExec): InMemoryTableScanExecTransformer =
    new InMemoryTableScanExecTransformer(scan.attributes, scan.predicates, scan.relation)
}
