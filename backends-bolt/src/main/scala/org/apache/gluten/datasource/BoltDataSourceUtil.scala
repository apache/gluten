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
package org.apache.gluten.datasource

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkSchemaUtil

import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.fs.FileStatus

import java.util

object BoltDataSourceUtil {
  def readSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("No input file specified")
    }
    readSchema(files.toList.head)
  }

  def readSchema(file: FileStatus): Option[StructType] = {
    val allocator = ArrowBufferAllocators.contextInstance()
    val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "BoltWriter")
    val datasourceJniWrapper = BoltDataSourceJniWrapper.create(runtime)
    val dsHandle =
      datasourceJniWrapper.init(file.getPath.toString, -1, new util.HashMap[String, String]())
    val cSchema = ArrowSchema.allocateNew(allocator)
    datasourceJniWrapper.inspectSchema(dsHandle, cSchema.memoryAddress())
    try {
      Option(SparkSchemaUtil.fromArrowSchema(ArrowAbiUtil.importToSchema(allocator, cSchema)))
    } finally {
      cSchema.close()
      datasourceJniWrapper.close(dsHandle)
    }
  }
}
