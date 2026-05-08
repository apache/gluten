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
package org.apache.gluten.utils

import org.apache.gluten.execution.PartitionedFileUtilShim

import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

import org.scalatest.funsuite.AnyFunSuite

class PartitionsUtilSuite extends AnyFunSuite {

  private def makePartitionedFile(path: String, length: Long): PartitionedFile =
    PartitionedFileUtilShim.makePartitionedFileFromPath(path, length)

  private def makeFilePartitions(
      files: Seq[PartitionedFile],
      numPartitions: Int): Seq[FilePartition] = {
    val groupedFiles = files.zipWithIndex.groupBy {
      case (_, idx) => idx % numPartitions
    }

    (0 until numPartitions).map {
      idx =>
        val partitionFiles = groupedFiles
          .getOrElse(idx, Seq.empty)
          .map(_._1)
          .toArray

        FilePartition(idx, partitionFiles)
    }
  }

  test("large files are distributed evenly by size") {
    val files = Seq(
      makePartitionedFile("f1", 100),
      makePartitionedFile("f2", 90),
      makePartitionedFile("f3", 80),
      makePartitionedFile("f4", 70)
    )
    val initialPartitions = makeFilePartitions(files, 2)

    val result = PartitionsUtil.regeneratePartition(initialPartitions, 0.0)

    assert(result.size === 2)

    val sizes = result.map(_.files.map(_.length).sum)
    assert(sizes.forall(_ === 170))
  }

  test("small files are distributed evenly by number of files") {
    val files = (1 to 10).map(i => makePartitionedFile(s"f$i", 10))
    val initialPartitions = makeFilePartitions(files, 5)

    val result = PartitionsUtil.regeneratePartition(initialPartitions, 1.0)

    assert(result.size === 5)
    val counts = result.map(_.files.length)
    assert(counts.forall(_ === 2))
  }

  test("small files should not be placed into one partition") {
    val files = Seq(
      makePartitionedFile("f1", 10),
      makePartitionedFile("f2", 20),
      makePartitionedFile("f3", 30),
      makePartitionedFile("f4", 40),
      makePartitionedFile("f5", 100)
    )
    val initialPartitions = makeFilePartitions(files, 2)

    // Only "f5" is not small file.
    val result = PartitionsUtil.regeneratePartition(initialPartitions, 0.5)

    assert(result.size === 2)
    assert(result.forall(_.files.exists(_.length <= 40)))
  }

  test("zero length files") {
    val files = Seq(
      makePartitionedFile("f1", 0),
      makePartitionedFile("f2", 0)
    )
    val initialPartitions = makeFilePartitions(files, 2)

    val result = PartitionsUtil.regeneratePartition(initialPartitions, 0.0)

    assert(result.size === 2)
    assert(result.count(_.files.nonEmpty) === 2)
  }

  test("empty inputs") {
    val result = PartitionsUtil.regeneratePartition(Seq.empty, 0.5)
    assert(result.size === 0)
  }
}
