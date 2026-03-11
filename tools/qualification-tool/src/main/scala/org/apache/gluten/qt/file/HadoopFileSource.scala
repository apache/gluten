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
package org.apache.gluten.qt.file

import org.apache.gluten.qt.{file, QualificationToolConfiguration}

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter, RollingEventLogFilesWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.io.{BufferedSource, Codec, Source}

/**
 * An abstract file source that leverages Hadoop’s {@link FileSystem} to traverse and read event log
 * files. It uses the provided {@link QualificationToolConfiguration} to determine the input paths,
 * date filters, and other settings. <p> Major functionalities include: <ul> <li>Resolving a list of
 * valid event log file statuses by applying date filters, excluding temporary or hidden files, and
 * handling rolling event log directories.</li> <li>Opening and returning a
 * {@link scala.io.BufferedSource} for an event log file.</li> <li>Cleanly closing the underlying
 * {@link FileSystem} resource.</li> </ul>
 *
 * @param conf
 *   the configuration specifying input paths, date filters, and other settings
 */

abstract class HadoopFileSource(conf: QualificationToolConfiguration) {
  protected def inputPathStrs: Array[String] = conf.inputPath.split(",").filter(_.nonEmpty)
  protected def hadoopConf = new Configuration()
  private val fs: FileSystem = FileSystem.get(new Path(inputPathStrs(0)).toUri, hadoopConf)

  def getFileStatuses: Iterator[Seq[FileStatus]] = {
    inputPathStrs.distinct.toIterator.flatMap(ip => getFileStatuses(fs.getFileStatus(new Path(ip))))
  }

  private def getFileStatuses(status: FileStatus): Iterator[Seq[FileStatus]] = {
    fs.listStatus(status.getPath).toIterator.flatMap {
      s =>
        if (
          !s.isDirectory && !s.getPath.getName.startsWith(
            ".") && status.getModificationTime >= conf.dateFilter && !s.getPath.getName.endsWith(
            EventLogFileWriter.IN_PROGRESS)
        ) {
          Iterator(Seq(s))
        } else if (
          RollingEventLogFilesWriter.isEventLogDir(
            s) && status.getModificationTime >= conf.dateFilter
        ) {
          Iterator(EventLogFileReader.apply(fs, s).get.listEventLogFiles)
        } else if (s.isDirectory) {
          fs.listStatus(s.getPath).toIterator.flatMap(getFileStatuses)
        } else
          {
            Iterator[Seq[FileStatus]]()
          }.filter(_.isEmpty)
    }
  }

  def getSource(path: Path): BufferedSource = {
    val inputStream = EventLogFileReader.openEventLog(path, fs)
    Source.fromInputStream(inputStream)(Codec.UTF8)
  }

  def close(): Unit = fs.close()
}

object HadoopFileSource {
  def apply(conf: QualificationToolConfiguration): HadoopFileSource = {
    conf.inputPath match {
      case f if f.startsWith("gs://") => GcsFileSource(conf)
      case f if f.startsWith("file://") => file.LocalFsFileSource(conf)
      case f if f.startsWith("/") => file.LocalFsFileSource(conf)
      case f if f.startsWith("hdfs://") => file.LocalFsFileSource(conf)
    }
  }
}
