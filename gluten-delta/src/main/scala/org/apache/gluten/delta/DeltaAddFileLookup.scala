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
package org.apache.gluten.delta

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.fs.Path

import scala.collection.mutable

/** Driver-side AddFile index shared by all FilePartitions in one prepared Delta scan. */
final private[gluten] class DeltaAddFileLookup private (
    addFiles: IndexedSeq[AddFile],
    absolutePaths: IndexedSeq[Path],
    candidateIndexes: Map[(Option[String], String), Vector[Int]],
    val hasDeletionVector: Boolean) {

  def find(file: PartitionedFile): AddFile = {
    val partitionedFilePath = new Path(file.filePath.toString)
    val candidates = mutable.BitSet.empty
    DeltaAddFileLookup.pathVariants(partitionedFilePath).foreach {
      key => candidateIndexes.get(key).foreach(indexes => candidates ++= indexes)
    }

    // BitSet iteration preserves the original AddFile order used by the previous Seq.find lookup.
    candidates.iterator
      .find(index => DeltaAddFileLookup.samePath(partitionedFilePath, absolutePaths(index)))
      .map(addFiles.apply)
      .getOrElse {
        throw new IllegalStateException(
          s"Unable to find Delta AddFile metadata for split ${file.filePath}")
      }
  }
}

private[gluten] object DeltaAddFileLookup {
  val empty: DeltaAddFileLookup =
    new DeltaAddFileLookup(Vector.empty, Vector.empty, Map.empty, hasDeletionVector = false)

  def apply(
      tablePath: Path,
      addFiles: Seq[AddFile],
      hasDeletionVector: Boolean): DeltaAddFileLookup = {
    val indexedAddFiles = addFiles.toIndexedSeq
    val absolutePaths = indexedAddFiles.map {
      addFile => DeltaFileOperations.absolutePath(tablePath.toString, addFile.path)
    }
    val candidateIndexes = mutable.HashMap.empty[
      (Option[String], String),
      mutable.ArrayBuffer[Int]]

    absolutePaths.zipWithIndex.foreach {
      case (absolutePath, index) =>
        pathVariants(absolutePath).foreach {
          key => candidateIndexes.getOrElseUpdate(key, mutable.ArrayBuffer.empty) += index
        }
    }

    new DeltaAddFileLookup(
      indexedAddFiles,
      absolutePaths,
      candidateIndexes.iterator.map { case (key, indexes) => key -> indexes.toVector }.toMap,
      hasDeletionVector)
  }

  private def samePath(left: Path, right: Path): Boolean = {
    pathVariants(left).intersect(pathVariants(right)).nonEmpty
  }

  private def pathVariants(path: Path): Set[(Option[String], String)] = {
    val uri = path.toUri.normalize()
    val authority = Option(uri.getAuthority)
    Seq(uri.getRawPath, uri.getPath)
      .filter(_ != null)
      .flatMap(percentVariants)
      .map(pathValue => authority -> pathValue)
      .toSet
  }

  // SparkPath and DeltaFileOperations can expose literal '%' characters at different URI escaping
  // levels. Compare a bounded set of full-path variants while retaining the URI authority.
  private def percentVariants(path: String): Set[String] = {
    (0 until 4).foldLeft(Set(path)) {
      (variants, _) => variants ++ variants.map(_.replace("%25", "%"))
    }
  }
}
