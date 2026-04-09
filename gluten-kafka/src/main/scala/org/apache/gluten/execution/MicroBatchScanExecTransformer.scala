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
package org.apache.gluten.execution

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{ReadRelNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.kafka010.GlutenStreamKafkaSourceUtil
import org.apache.spark.sql.types.StructType

import java.util.Objects

/** Physical plan node for scanning a micro-batch of data from a data source. */
case class MicroBatchScanExecTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    @transient stream: MicroBatchStream,
    @transient start: Offset,
    @transient end: Offset,
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None
) extends BatchScanExecTransformerBase(
    output = output,
    scan = scan,
    runtimeFilters = Seq.empty,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  ) {

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def hashCode(): Int = Objects.hashCode(this.stream)

  override def equals(other: Any): Boolean = other match {
    case other: MicroBatchScanExecTransformer => this.stream == other.stream
    case _ => false
  }

  override lazy val readerFactory: PartitionReaderFactory = stream.createReaderFactory()

  @transient override lazy val inputPartitionsShim: Seq[InputPartition] =
    stream.planInputPartitions(start, end)

  override def scanFilters: Seq[Expression] = Seq.empty

  override def supportPushDownFilters: Boolean = false

  def pushDownFilters: Option[Seq[Expression]] = None

  override def getMetadataColumns(): Seq[AttributeReference] = Seq.empty

  // todo: consider grouped partitions
  override def getPartitions: Seq[Partition] = inputPartitionsShim.zipWithIndex.map {
    case (inputPartition, index) => new SparkDataSourceRDDPartition(index, Seq(inputPartition))
  }

  override def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)] =
    getPartitions.map((_, fileFormat))

  /** Returns the actual schema of this data source scan. */
  override def getDataSchema: StructType = scan.readSchema()

  override def nodeName: String = s"MicroBatchScanExecTransformer(${scan.description()})"

  override lazy val fileFormat: ReadFileFormat = GlutenStreamKafkaSourceUtil.getFileFormat(scan)

  protected[this] def supportsBatchScan(scan: Scan): Boolean = {
    MicroBatchScanExecTransformer.supportsBatchScan(scan)
  }

  override def withOutput(newOutput: Seq[AttributeReference]): BatchScanExecTransformerBase = {
    this.copy(output = newOutput)
  }

  override def getSplitInfosFromPartitions(
      partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    val groupedPartitions = filteredPartitions.flatten
    groupedPartitions.zipWithIndex.map {
      case (p, _) => GlutenStreamKafkaSourceUtil.genSplitInfo(p)
    }
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val ctx = super.doTransform(context)
    ctx.root.asInstanceOf[ReadRelNode].setStreamKafka(true);
    ctx
  }
}

object MicroBatchScanExecTransformer {
  def apply(batch: MicroBatchScanExec): MicroBatchScanExecTransformer = {
    val output = batch.output
      .filter(_.isInstanceOf[AttributeReference])
      .map(_.asInstanceOf[AttributeReference])
      .toSeq
    if (output.size == batch.output.size) {
      new MicroBatchScanExecTransformer(
        output,
        batch.scan,
        batch.stream,
        batch.start,
        batch.end,
        null,
        Option.empty,
        Option.empty)
    } else {
      throw new UnsupportedOperationException(
        s"Unsupported DataSourceV2ScanExecBase: ${batch.output.getClass.getName}")
    }
  }

  def supportsBatchScan(scan: Scan): Boolean = {
    scan.getClass.getName == "org.apache.spark.sql.kafka010.KafkaSourceProvider$KafkaScan"
  }
}
