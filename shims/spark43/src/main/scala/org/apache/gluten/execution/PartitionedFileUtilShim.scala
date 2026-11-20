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

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionedFile}

import org.apache.hadoop.fs.Path

import java.lang.reflect.Method

object PartitionedFileUtilShim {

  private val clz: Class[_] = PartitionedFileUtil.getClass
  private val module = clz.getField("MODULE$").get(null)

  private lazy val getPartitionedFileMethod: Method = {
    try {
      val m = clz.getDeclaredMethod(
        "getPartitionedFile",
        classOf[FileStatusWithMetadata],
        classOf[InternalRow])
      m.setAccessible(true)
      m
    } catch {
      case _: NoSuchMethodException => null
    }
  }

  private lazy val getPartitionedFileByPathMethod: Method = {
    try {
      val m = clz.getDeclaredMethod(
        "getPartitionedFile",
        classOf[FileStatusWithMetadata],
        classOf[Path],
        classOf[InternalRow])
      m.setAccessible(true)
      m
    } catch {
      case _: NoSuchMethodException => null
    }
  }

  private lazy val getPartitionedFileByPathSizeMethod: Method = {
    try {
      val m = clz.getDeclaredMethod(
        "getPartitionedFile",
        classOf[FileStatusWithMetadata],
        classOf[Path],
        classOf[InternalRow],
        classOf[Long],
        classOf[Long])
      m.setAccessible(true)
      m
    } catch {
      case _: NoSuchMethodException => null
    }
  }

  def getPartitionedFile(
      file: FileStatusWithMetadata,
      partitionValues: InternalRow): PartitionedFile = {
    if (getPartitionedFileMethod != null) {
      getPartitionedFileMethod
        .invoke(module, file, partitionValues)
        .asInstanceOf[PartitionedFile]
    } else if (getPartitionedFileByPathMethod != null) {
      getPartitionedFileByPathMethod
        .invoke(module, file, file.getPath, partitionValues)
        .asInstanceOf[PartitionedFile]
    } else if (getPartitionedFileByPathSizeMethod != null) {
      getPartitionedFileByPathSizeMethod
        .invoke(module, file, file.getPath, partitionValues, 0, file.getLen)
        .asInstanceOf[PartitionedFile]
    } else {
      val params = clz.getDeclaredMethods
        .find(_.getName == "getPartitionedFile")
        .map(_.getGenericParameterTypes.mkString(", "))
      throw new RuntimeException(
        s"getPartitionedFile with $params is not correctly shimmed " +
          "in PartitionedFileUtilShim")
    }
  }

  private lazy val splitFilesMethod: Method = {
    try {
      val m = clz.getDeclaredMethod(
        "splitFiles",
        classOf[SparkSession],
        classOf[FileStatusWithMetadata],
        classOf[Boolean],
        classOf[Long],
        classOf[InternalRow])
      m.setAccessible(true)
      m
    } catch {
      case _: NoSuchMethodException => null
    }
  }

  private lazy val splitFilesByPathMethod: Method = {
    try {
      val m = clz.getDeclaredMethod(
        "splitFiles",
        classOf[FileStatusWithMetadata],
        classOf[Path],
        classOf[Boolean],
        classOf[Long],
        classOf[InternalRow])
      m.setAccessible(true)
      m
    } catch {
      case _: NoSuchMethodException => null
    }
  }

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatusWithMetadata,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (splitFilesMethod != null) {
      splitFilesMethod
        .invoke(
          module,
          sparkSession,
          file,
          java.lang.Boolean.valueOf(isSplitable),
          java.lang.Long.valueOf(maxSplitBytes),
          partitionValues)
        .asInstanceOf[Seq[PartitionedFile]]
    } else if (splitFilesByPathMethod != null) {
      splitFilesByPathMethod
        .invoke(
          module,
          file,
          file.getPath,
          java.lang.Boolean.valueOf(isSplitable),
          java.lang.Long.valueOf(maxSplitBytes),
          partitionValues)
        .asInstanceOf[Seq[PartitionedFile]]
    } else {
      val params = clz.getDeclaredMethods
        .find(_.getName == "splitFiles")
        .map(_.getGenericParameterTypes.mkString(", "))
      throw new RuntimeException(
        s"splitFiles with $params is not correctly shimmed " +
          "in PartitionedFileUtilShim")
    }
  }

  // Helper method to create PartitionedFile from path and length.
  def makePartitionedFileFromPath(path: String, length: Long): PartitionedFile = {
    PartitionedFile(null, SparkPath.fromPathString(path), 0, length, Array.empty)
  }
}
