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
package org.apache.gluten.qt

import org.apache.gluten.qt.execution.{EVENT_FILE_PROCESS_TASK, Executor, FILE_LISTING_TASK, PriorityTask, Progress, RunState, TaskContext}
import org.apache.gluten.qt.file.HadoopFileSource
import org.apache.gluten.qt.support.GlutenApplicationSupportReporter
import org.apache.gluten.qt.writer.{OperatorImpactReportWriter, PrimaryReportWriter}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.util.kvstore.InMemoryStore

object QualificationTool {

  def main(args: Array[String]): Unit =
    try {
      val conf = QualificationToolConfiguration.parseCommandLineArgs(args)
      val sparkConf = new SparkConf(false)
      sparkConf.set("spark.appStateStore.asyncTracking.enable", "false")

      val primaryReportWriter = writer.PrimaryReportWriter(conf)
      val operatorImpactReportWriter = writer.OperatorImpactReportWriter(conf)
      val fileSource = file.HadoopFileSource(conf)
      val executor = execution.Executor(conf)

      executor.submitTask(
        PriorityTask(
          TaskContext(100, "File Listing", FILE_LISTING_TASK),
          () => {
            Executor.updateState(RunState("LISTING FILES - COMPLETED", Progress()))
            fileSource.getFileStatuses.foreach {
              fileStatuses =>
                Executor.incrementState()
                executor.submitTask(
                  PriorityTask(
                    TaskContext(10, fileStatuses.head.getPath.toString, EVENT_FILE_PROCESS_TASK),
                    () => {
                      val replayBus = new ReplayListenerBus()
                      val kvStore = new InMemoryStore()
                      replayBus.addListener(new SQLAppStatusListener(sparkConf, kvStore))
                      replayBus.addListener(new AppStatusListener(sparkConf, kvStore))
                      fileStatuses
                        .map(_.getPath)
                        .foreach {
                          eventPath =>
                            replayBus.replay(fileSource.getSource(eventPath), eventPath.toString)
                        }
                      generateReport(conf, kvStore, primaryReportWriter, operatorImpactReportWriter)
                    }
                  ))
            }
          }
        ))

      executor.waitAndDisplayStatus()
      primaryReportWriter.closeFile()
      operatorImpactReportWriter.closeFile()
      fileSource.close()
      println(s"Report has been written to ${primaryReportWriter.getFileString}")
      println(
        s"Unsupported Operators have been written to ${operatorImpactReportWriter.getFileString}")
    } catch {
      case e: Throwable =>
        e.printStackTrace(System.out)
        throw e
    }

  private def generateReport(
      conf: QualificationToolConfiguration,
      kvStore: InMemoryStore,
      reportWriter: PrimaryReportWriter,
      impactReportWriter: OperatorImpactReportWriter
  ): Unit = {
    val sqlStore = new SQLAppStatusStore(kvStore)
    val appStore = new AppStatusStore(kvStore)
    val applicationReporter = support.GlutenApplicationSupportReporter(conf, sqlStore, appStore)
    val applicationReport = applicationReporter.getReport
    reportWriter.addReport(applicationReport)
    impactReportWriter.addReportBatch(applicationReporter.getOperatorsToImplement)
  }
}
