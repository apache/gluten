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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.{DeltaFileReadOptions, RowIndexFilterType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitionedFile

import java.lang.{Long => JLong}
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

private[velox] object DeltaSplitMetadataExtractor {
  type NormalizedDeltaSplitMetadata =
    (Seq[java.util.Map[String, Object]], Seq[DeltaFileReadOptions])

  private val DeltaScanInfoClassName = "org.apache.gluten.delta.DeltaDeletionVectorScanInfo$"
  private val scanInfoMethodsCache = new ConcurrentHashMap[Class[_], ScanInfoMethods]()
  private val deletionVectorInfoMethodsCache =
    new ConcurrentHashMap[Class[_], DeletionVectorInfoMethods]()

  private lazy val deltaScanInfoReflection: Option[DeltaScanInfoReflection] = {
    try {
      // DeltaDeletionVectorScanInfo is compiled only in Delta-enabled source profiles. Keep this
      // bridge isolated and cached; all directly linkable Gluten classes should be referenced
      // without reflection.
      // scalastyle:off classforname
      val moduleClass = Class.forName(DeltaScanInfoClassName)
      // scalastyle:on classforname
      val module = moduleClass.getField("MODULE$").get(null)
      val extractAllMethod = moduleClass.getMethod(
        "extractAllFromJava",
        classOf[SparkSession],
        classOf[Int],
        classOf[java.util.List[_]])
      Some(DeltaScanInfoReflection(module, extractAllMethod))
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException =>
        None
    }
  }

  def normalize(
      partitionColumnCount: Int,
      partitionFiles: Seq[PartitionedFile]): Option[NormalizedDeltaSplitMetadata] = {
    deltaScanInfoReflection.flatMap {
      reflection =>
        val scanInfos = reflection.extractAllFromJava
          .invoke(
            reflection.module,
            activeSparkSession,
            Int.box(partitionColumnCount),
            partitionFiles.asJava)
          .asInstanceOf[java.util.List[_]]
          .asScala
          .toSeq
        val splitMetadata = scanInfos.map(toDeltaSplitMetadata)
        if (splitMetadata.exists(_._2.hasDeletionVector())) {
          Some((splitMetadata.map(_._1), splitMetadata.map(_._2)))
        } else {
          None
        }
    }
  }

  private def toDeltaSplitMetadata(
      scanInfo: Any): (java.util.Map[String, Object], DeltaFileReadOptions) = {
    val scanInfoMethods = methodsForScanInfo(scanInfo.getClass)
    val metadata = scanInfoMethods.normalizedOtherMetadataColumns
      .invoke(scanInfo)
      .asInstanceOf[scala.collection.Map[String, Object]]
      .asJava
    val deletionVectorInfo = scanInfoMethods.deletionVectorInfo.invoke(scanInfo)
    val deletionVectorInfoMethods = methodsForDeletionVectorInfo(deletionVectorInfo.getClass)
    val rowIndexFilterType = deletionVectorInfoMethods.rowIndexFilterType
      .invoke(deletionVectorInfo)
      .toString
    val hasDeletionVector = deletionVectorInfoMethods.hasDeletionVector
      .invoke(deletionVectorInfo)
      .asInstanceOf[Boolean]
    val cardinality = deletionVectorInfoMethods.cardinality
      .invoke(deletionVectorInfo)
      .asInstanceOf[JLong]
      .longValue()
    val serializedDeletionVector = deletionVectorInfoMethods.serializedDeletionVector
      .invoke(deletionVectorInfo)
      .asInstanceOf[Array[Byte]]

    (
      metadata,
      new DeltaFileReadOptions(
        toDeltaRowIndexFilterType(rowIndexFilterType),
        hasDeletionVector,
        cardinality,
        serializedDeletionVector))
  }

  private def toDeltaRowIndexFilterType(rowIndexFilterType: String): RowIndexFilterType = {
    rowIndexFilterType match {
      case "IF_CONTAINED" => RowIndexFilterType.IF_CONTAINED
      case "IF_NOT_CONTAINED" => RowIndexFilterType.IF_NOT_CONTAINED
      case _ => RowIndexFilterType.KEEP_ALL
    }
  }

  private def methodsForScanInfo(scanInfoClass: Class[_]): ScanInfoMethods = {
    val cached = scanInfoMethodsCache.get(scanInfoClass)
    if (cached != null) {
      return cached
    }

    val methods = ScanInfoMethods(
      scanInfoClass.getMethod("normalizedOtherMetadataColumns"),
      scanInfoClass.getMethod("deletionVectorInfo"))
    val previous = scanInfoMethodsCache.putIfAbsent(scanInfoClass, methods)
    if (previous != null) previous else methods
  }

  private def methodsForDeletionVectorInfo(
      deletionVectorInfoClass: Class[_]): DeletionVectorInfoMethods = {
    val cached = deletionVectorInfoMethodsCache.get(deletionVectorInfoClass)
    if (cached != null) {
      return cached
    }

    val methods = DeletionVectorInfoMethods(
      deletionVectorInfoClass.getMethod("rowIndexFilterType"),
      deletionVectorInfoClass.getMethod("hasDeletionVector"),
      deletionVectorInfoClass.getMethod("cardinality"),
      deletionVectorInfoClass.getMethod("serializedDeletionVector")
    )
    val previous = deletionVectorInfoMethodsCache.putIfAbsent(deletionVectorInfoClass, methods)
    if (previous != null) previous else methods
  }

  private def activeSparkSession: SparkSession = {
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse {
        throw new IllegalStateException(
          "Active SparkSession is required to materialize Delta deletion vectors")
      }
  }

  private case class DeltaScanInfoReflection(module: AnyRef, extractAllFromJava: Method)

  private case class ScanInfoMethods(
      normalizedOtherMetadataColumns: Method,
      deletionVectorInfo: Method)

  private case class DeletionVectorInfoMethods(
      rowIndexFilterType: Method,
      hasDeletionVector: Method,
      cardinality: Method,
      serializedDeletionVector: Method)
}
