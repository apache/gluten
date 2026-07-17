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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.gluten.execution.BatchCarrierRow
import org.apache.gluten.vectorized.CHColumnVector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, OutputWriter}

import scala.collection.mutable.ArrayBuffer

class FakeRowOutputWriter(datasourceJniWrapper: Option[CHDatasourceJniWrapper], outputPath: String)
  extends OutputWriter {

  protected var addFiles: ArrayBuffer[AddFile] = new ArrayBuffer[AddFile]()

  override def write(row: InternalRow): Unit = {
    BatchCarrierRow.unwrap(row).foreach {
      batch =>
        if (batch.numRows > 0) {
          val col = batch.column(0).asInstanceOf[CHColumnVector]
          datasourceJniWrapper.foreach(_.write(col.getBlockAddress))
        } // else ignore this empty block
    }
  }

  override def close(): Unit = {
    datasourceJniWrapper.foreach(_.close())
  }

  override def path(): String = {
    outputPath
  }
}
