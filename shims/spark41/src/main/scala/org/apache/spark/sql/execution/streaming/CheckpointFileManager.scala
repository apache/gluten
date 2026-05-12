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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.execution.streaming.checkpointing.{CheckpointFileManager => Spark41CheckpointFileManager}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path, PathFilter}

import java.io.OutputStream

/**
 * Binary compatibility shim for Delta 4.0, which was compiled against Spark 4.0's
 * CheckpointFileManager package before Spark 4.1 moved it under streaming.checkpointing.
 */
trait CheckpointFileManager {
  def createAtomic(
      path: Path,
      overwriteIfPossible: Boolean): CheckpointFileManager.CancellableFSDataOutputStream

  def open(path: Path): FSDataInputStream

  def list(path: Path, filter: PathFilter): Array[FileStatus]

  def list(path: Path): Array[FileStatus] = {
    list(
      path,
      new PathFilter {
        override def accept(path: Path): Boolean = true
      })
  }

  def mkdirs(path: Path): Unit

  def exists(path: Path): Boolean

  def delete(path: Path): Unit

  def isLocal: Boolean

  def createCheckpointDirectory(): Path
}

object CheckpointFileManager {
  def create(path: Path, hadoopConf: Configuration): CheckpointFileManager = {
    new Spark41CheckpointFileManagerAdapter(
      Spark41CheckpointFileManager.create(path, hadoopConf))
  }

  abstract class CancellableFSDataOutputStream(outputStream: OutputStream)
    extends org.apache.hadoop.fs.FSDataOutputStream(
      outputStream,
      null.asInstanceOf[FileSystem.Statistics]) {
    def cancel(): Unit
  }

  private class Spark41CheckpointFileManagerAdapter(
      delegate: Spark41CheckpointFileManager)
    extends CheckpointFileManager {
    override def createAtomic(
        path: Path,
        overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
      new CancellableFSDataOutputStreamAdapter(delegate.createAtomic(path, overwriteIfPossible))
    }

    override def open(path: Path): FSDataInputStream = delegate.open(path)

    override def list(path: Path, filter: PathFilter): Array[FileStatus] =
      delegate.list(path, filter)

    override def mkdirs(path: Path): Unit = delegate.mkdirs(path)

    override def exists(path: Path): Boolean = delegate.exists(path)

    override def delete(path: Path): Unit = delegate.delete(path)

    override def isLocal: Boolean = delegate.isLocal

    override def createCheckpointDirectory(): Path = delegate.createCheckpointDirectory()
  }

  private class CancellableFSDataOutputStreamAdapter(
      delegate: Spark41CheckpointFileManager.CancellableFSDataOutputStream)
    extends CancellableFSDataOutputStream(delegate) {
    override def close(): Unit = delegate.close()

    override def cancel(): Unit = delegate.cancel()
  }
}
