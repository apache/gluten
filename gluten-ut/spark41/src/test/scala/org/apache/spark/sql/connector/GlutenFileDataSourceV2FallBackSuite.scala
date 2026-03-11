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
package org.apache.spark.sql.connector

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

class GlutenFileDataSourceV2FallBackSuite
  extends FileDataSourceV2FallBackSuite
  with GlutenSQLTestsBaseTrait {

  testGluten("Fallback Parquet V2 to V1") {
    Seq("parquet", classOf[ParquetDataSourceV2].getCanonicalName).foreach {
      format =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> format) {
          val commands = ArrayBuffer.empty[(String, LogicalPlan)]
          val exceptions = ArrayBuffer.empty[(String, Exception)]
          val listener = new QueryExecutionListener {
            override def onFailure(
                funcName: String,
                qe: QueryExecution,
                exception: Exception): Unit = {
              exceptions += funcName -> exception
            }

            override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
              commands += funcName -> qe.logical
            }
          }
          spark.listenerManager.register(listener)

          try {
            withTempPath {
              path =>
                val inputData = spark.range(10)
                inputData.write.format(format).save(path.getCanonicalPath)
                sparkContext.listenerBus.waitUntilEmpty()
                assert(commands.length == 1)
                assert(commands.head._1 == "command")
                assert(commands.head._2.isInstanceOf[InsertIntoHadoopFsRelationCommand])
                assert(
                  commands.head._2
                    .asInstanceOf[InsertIntoHadoopFsRelationCommand]
                    .fileFormat
                    .isInstanceOf[ParquetFileFormat])
                val df = spark.read.format(format).load(path.getCanonicalPath)
                checkAnswer(df, inputData.toDF())
                assert(
                  df.queryExecution.executedPlan.exists(
                    _.isInstanceOf[FileSourceScanExecTransformer]))
            }
          } finally {
            spark.listenerManager.unregister(listener)
          }
        }
    }
  }
}
