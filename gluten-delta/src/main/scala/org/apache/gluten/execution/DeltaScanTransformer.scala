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

import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.utils.DeltaDeletionVectorRegistry

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal

case class DeltaScanTransformer(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false,
    override val pushDownFilters: Option[Seq[Expression]] = None)
  extends FileSourceScanExecTransformerBase(
    relation,
    stream,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan
  ) {

  override lazy val fileFormat: ReadFileFormat = ReadFileFormat.ParquetReadFormat

  private lazy val deltaDeletionVectorRegistryId: Option[String] =
    DeltaScanTransformer.registerDeletionVectorsFromFileFormat(relation)

  override protected def doValidateInternal(): ValidationResult = super.doValidateInternal()

  override def getProperties: Map[String, String] = {
    super.getProperties ++ deltaDeletionVectorRegistryId
      .map(DeltaDeletionVectorRegistry.RegistryIdProperty -> _)
      .toMap
  }

  override def doCanonicalize(): DeltaScanTransformer = {
    DeltaScanTransformer(
      relation,
      None,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan,
      pushDownFilters.map(QueryPlan.normalizePredicates(_, output))
    )
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): BasicScanExecTransformer =
    copy(pushDownFilters = Some(filters))
}

object DeltaScanTransformer {
  private val IfContainedFilterType = "IF_CONTAINED"

  private def registerDeletionVectorsFromFileFormat(relation: HadoopFsRelation): Option[String] = {
    registerDeletionVectorsFromBroadcastMap(relation)
      .orElse(registerDeletionVectorsFromPreparedScan(relation))
  }

  private def registerDeletionVectorsFromBroadcastMap(
      relation: HadoopFsRelation): Option[String] = {
    try {
      val format = relation.fileFormat
      val formatClass = format.getClass
      val broadcastDvMap = Option(formatClass.getMethod("broadcastDvMap").invoke(format))
        .collect { case o: Option[_] => o }
        .flatten
        .collect { case b: Broadcast[_] => b.value }
        .collect { case m: scala.collection.Map[_, _] => m }
        .getOrElse(Map.empty)
        .collect { case (uri: java.net.URI, value) => uri -> value }
      if (broadcastDvMap.isEmpty) {
        return None
      }

      val tablePath =
        Option(formatClass.getMethod("tablePath").invoke(format))
          .collect { case o: Option[_] => o }
          .flatten
          .map(_.toString)
          .orElse(relation.location.rootPaths.headOption.map(_.toString))
          .map(new Path(_))
          .orNull
      if (tablePath == null) {
        return None
      }

      val dvStore = new HadoopFileSystemDVStore(relation.sparkSession.sessionState.newHadoopConf())
      val registeredEntries = broadcastDvMap.iterator.flatMap {
        case (uri, dvDescriptorWithFilterType) =>
          try {
            val descriptor = dvDescriptorWithFilterType.getClass
              .getMethod("descriptor")
              .invoke(dvDescriptorWithFilterType)
              .asInstanceOf[DeletionVectorDescriptor]
            val filterType = dvDescriptorWithFilterType.getClass
              .getMethod("filterType")
              .invoke(dvDescriptorWithFilterType)
              .toString
            val payload = StoredBitmap
              .create(descriptor, tablePath)
              .load(dvStore)
              .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
            pathAliases(uri).map {
              _ -> DeltaDeletionVectorRegistry.Entry(descriptor.cardinality, filterType, payload)
            }
          } catch {
            case NonFatal(_) => Seq.empty
          }
      }.toMap

      if (registeredEntries.isEmpty) {
        None
      } else {
        Some(DeltaDeletionVectorRegistry.register(registeredEntries))
      }
    } catch {
      case _: NoSuchMethodException => None
      case NonFatal(_) => None
    }
  }

  private def registerDeletionVectorsFromPreparedScan(
      relation: HadoopFsRelation): Option[String] = {
    relation.location match {
      case preparedIndex: PreparedDeltaFileIndex =>
        val tablePath =
          Option(preparedIndex.path)
            .orElse(relation.location.rootPaths.headOption)
            .orNull
        if (tablePath == null) {
          return None
        }

        val dvStore =
          new HadoopFileSystemDVStore(relation.sparkSession.sessionState.newHadoopConf())
        val preparedFiles = preparedIndex.preparedScan.files
        registerDeletionVectorsFromAddFiles(preparedFiles.iterator, tablePath, dvStore)
      case _ =>
        None
    }
  }

  private def registerDeletionVectorsFromAddFiles(
      files: Iterator[AddFile],
      tablePath: Path,
      dvStore: HadoopFileSystemDVStore): Option[String] = {
    val registeredEntries = files.flatMap {
      addFile =>
        Option(addFile.deletionVector).iterator.flatMap {
          descriptor =>
            try {
              val payload = StoredBitmap
                .create(descriptor, tablePath)
                .load(dvStore)
                .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
              val absolutePath = new Path(tablePath, addFile.path)
              pathAliases(absolutePath.toUri, absolutePath.toString).map {
                _ -> DeltaDeletionVectorRegistry.Entry(
                  descriptor.cardinality,
                  IfContainedFilterType,
                  payload)
              }
            } catch {
              case NonFatal(_) => Seq.empty
            }
        }
    }.toMap

    if (registeredEntries.isEmpty) {
      None
    } else {
      Some(DeltaDeletionVectorRegistry.register(registeredEntries))
    }
  }

  private def pathAliases(uri: java.net.URI, extraAliases: String*): Seq[String] = {
    val decodedExtraAliases = extraAliases.map(percentUnescapePathName)
    (Seq(uri.toASCIIString, uri.getPath, Option(uri.getPath).map(_.stripPrefix("/")).orNull) ++
      extraAliases ++
      decodedExtraAliases ++
      extraAliases.map(_.stripPrefix("/")) ++
      decodedExtraAliases.map(_.stripPrefix("/")))
      .filter(_ != null)
      .map(DeltaDeletionVectorRegistry.normalizePathKey)
      .filter(_.nonEmpty)
      .distinct
  }

  private def percentUnescapePathName(path: String): String = {
    if (path == null || path.isEmpty) {
      return path
    }
    var plaintextEndIdx = path.indexOf('%')
    val length = path.length
    if (plaintextEndIdx == -1 || plaintextEndIdx + 2 >= length) {
      path
    } else {
      val sb = new java.lang.StringBuilder(length)
      var plaintextStartIdx = 0
      while (plaintextEndIdx != -1 && plaintextEndIdx + 2 < length) {
        if (plaintextEndIdx > plaintextStartIdx) sb.append(path, plaintextStartIdx, plaintextEndIdx)
        if (
          java.lang.Character.digit(path.charAt(plaintextEndIdx + 1), 16) != -1 &&
          java.lang.Character.digit(path.charAt(plaintextEndIdx + 2), 16) != -1
        ) {
          sb.append(
            ((java.lang.Character.digit(path.charAt(plaintextEndIdx + 1), 16) << 4) |
              java.lang.Character.digit(path.charAt(plaintextEndIdx + 2), 16)).toChar)
          plaintextStartIdx = plaintextEndIdx + 3
        } else {
          sb.append('%')
          plaintextStartIdx = plaintextEndIdx + 1
        }
        plaintextEndIdx = path.indexOf('%', plaintextStartIdx)
      }
      if (plaintextStartIdx < length) {
        sb.append(path, plaintextStartIdx, length)
      }
      sb.toString
    }
  }

  def apply(scanExec: FileSourceScanExec): DeltaScanTransformer = {
    new DeltaScanTransformer(
      scanExec.relation,
      SparkShimLoader.getSparkShims.getFileSourceScanStream(scanExec),
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
  }

}
