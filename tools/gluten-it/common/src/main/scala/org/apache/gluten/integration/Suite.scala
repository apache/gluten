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
package org.apache.gluten.integration

import org.apache.gluten.integration.Constants.TYPE_MODIFIER_DECIMAL_AS_DOUBLE
import org.apache.gluten.integration.action.Action
import org.apache.gluten.integration.metrics.MetricMapper
import org.apache.gluten.integration.report.TestReporter

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.ConfUtils.ConfImplicits._
import org.apache.spark.sql.SparkSessionSwitcher

import org.apache.commons.io.output.{NullOutputStream, TeeOutputStream}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, LogManager}

import java.io.{BufferedOutputStream, File, OutputStream, PrintStream}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Scanner

abstract class Suite(
    private val appName: String,
    private val masterUrl: String,
    private val actions: Array[Action],
    private val testConf: SparkConf,
    private val baselineConf: SparkConf,
    private val extraSparkConf: Map[String, String],
    private val logLevel: Level,
    private val errorOnMemLeak: Boolean,
    private val enableUi: Boolean,
    private val enableHsUi: Boolean,
    private val hsUiPort: Int,
    private val disableAqe: Boolean,
    private val disableBhj: Boolean,
    private val disableWscg: Boolean,
    private val enableCbo: Boolean,
    private val shufflePartitions: Int,
    private val scanPartitions: Int,
    private val decimalAsDouble: Boolean,
    private val baselineMetricMapper: MetricMapper,
    private val testMetricMapper: MetricMapper,
    private val reportPath: String) {

  resetLogLevel()

  private val reporter: TestReporter = TestReporter.create()
  private var hsUiBoundPort: Int = -1

  private[integration] val sessionSwitcher: SparkSessionSwitcher =
    new SparkSessionSwitcher(appName, masterUrl, logLevel.toString)

  // define initial configs
  sessionSwitcher.addDefaultConf("spark.sql.sources.useV1SourceList", "")
  sessionSwitcher.addDefaultConf("spark.sql.shuffle.partitions", s"$shufflePartitions")
  sessionSwitcher.addDefaultConf("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
  sessionSwitcher.addDefaultConf("spark.executor.heartbeatInterval", "10s")
  sessionSwitcher.addDefaultConf("spark.worker.timeout", "3600")
  sessionSwitcher.addDefaultConf("spark.executor.metrics.pollingInterval", "0")
  sessionSwitcher.addDefaultConf("spark.network.timeout", "3601s")
  sessionSwitcher.addDefaultConf("spark.sql.broadcastTimeout", "1800")
  sessionSwitcher.addDefaultConf("spark.network.io.preferDirectBufs", "false")
  sessionSwitcher.addDefaultConf("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")

  if (dataSource() == "delta") {
    sessionSwitcher.addDefaultConf(
      "spark.sql.extensions",
      "io.delta.sql.DeltaSparkSessionExtension")
    sessionSwitcher.addDefaultConf(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  if (!enableUi) {
    sessionSwitcher.addDefaultConf("spark.ui.enabled", "false")
  }

  if (enableHsUi) {
    if (!new File(historyWritePath()).exists() && !new File(historyWritePath()).mkdirs()) {
      throw new RuntimeException(
        "Unable to create history directory: " +
          historyWritePath())
    }
    sessionSwitcher.addDefaultConf("spark.eventLog.enabled", "true")
    sessionSwitcher.addDefaultConf("spark.eventLog.dir", historyWritePath())
  }

  if (disableAqe) {
    sessionSwitcher.addDefaultConf("spark.sql.adaptive.enabled", "false")
  }

  if (disableBhj) {
    sessionSwitcher.addDefaultConf("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  if (disableWscg) {
    sessionSwitcher.addDefaultConf("spark.sql.codegen.wholeStage", "false")
  }

  if (enableCbo) {
    sessionSwitcher.addDefaultConf("spark.sql.cbo.enabled", "true")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.planStats.enabled", "true")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.joinReorder.enabled", "true")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.joinReorder.dp.threshold", "12")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.joinReorder.card.weight", "0.7")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.joinReorder.dp.star.filter", "true")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.starSchemaDetection", "true")
    sessionSwitcher.addDefaultConf("spark.sql.cbo.starJoinFTRatio", "0.9")
    sessionSwitcher.addDefaultConf("spark.sql.statistics.histogram.enabled", "true")
    sessionSwitcher.addDefaultConf("spark.sql.statistics.histogram.numBins", "254")
  }

  if (scanPartitions != -1) {
    // Scan partition number.
    sessionSwitcher.addDefaultConf(
      "spark.sql.files.maxPartitionBytes",
      s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.addDefaultConf("spark.sql.files.openCostInBytes", "0")
    sessionSwitcher.addDefaultConf(
      "spark.sql.files.minPartitionNum",
      s"${(scanPartitions - 1).max(1)}")
  }

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  extraSparkConf.toStream.foreach(kv => sessionSwitcher.addExtraConf(kv._1, kv._2))

  private def startHistoryServer(): Int = {
    val hsConf = new SparkConf(false)
    hsConf.setWarningOnOverriding("spark.history.ui.port", s"$hsUiPort")
    hsConf.setWarningOnOverriding("spark.history.fs.logDirectory", historyWritePath())
    HistoryServerHelper.startHistoryServer(hsConf)
  }

  // boot up history server
  if (enableHsUi) {
    hsUiBoundPort = startHistoryServer()
  }

  def run(): Boolean = {
    // Report metadata.
    val formatter =
      DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(ZoneId.systemDefault())
    val formattedTime = formatter.format(Instant.ofEpochMilli(System.currentTimeMillis()))
    reporter.addMetadata("Timestamp", formattedTime)
    reporter.addMetadata("Arguments", Cli.args().mkString(" "))

    // Construct the output streams for writing test reports.
    val fileOut: OutputStream =
      if (!StringUtils.isBlank(reportPath)) {
        try {
          sessionSwitcher.useSession("baseline", "Suite Initialization")
          val conf = sessionSwitcher.spark().sessionState.newHadoopConf()
          val path = new Path(reportPath)
          val fs = path.getFileSystem(conf)
          if (fs.exists(path) && fs.getFileStatus(path).isDirectory) {
            throw new java.io.FileNotFoundException("Is a directory: " + reportPath)
          }
          println(s"Test report will be written to ${path.toString}")
          new BufferedOutputStream(fs.create(path, true)) // overwrite = true
        } catch {
          case e: java.io.FileNotFoundException =>
            throw new RuntimeException(e)
        }
      } else {
        NullOutputStream.NULL_OUTPUT_STREAM
      }
    val combinedOut = new PrintStream(new TeeOutputStream(System.out, fileOut), true)
    val combinedErr = new PrintStream(new TeeOutputStream(System.err, fileOut), true)

    // Execute the suite.
    val succeeded =
      try {
        runActions()
      } catch {
        case t: Exception =>
          t.printStackTrace(reporter.rootAppender.err)
          false
      }
    try {
      if (succeeded) {
        reporter.write(combinedOut)
        combinedOut.flush()
      } else {
        reporter.write(combinedErr)
        combinedErr.flush()
      }
      succeeded
    } finally {
      fileOut.close()
    }
  }

  private def runActions(): Boolean = {
    val succeeded = actions.forall {
      action =>
        resetLogLevel() // to prevent log level from being set by unknown external codes
        action.execute(this)
    }
    succeeded
  }

  def close(): Unit = {
    sessionSwitcher.close()
    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiBoundPort)
      print("> ")
      new Scanner(System.in).nextLine
    }
  }

  def tableCreator(): TableCreator

  final def tableAnalyzer(): TableAnalyzer = {
    if (enableCbo) {
      return tableAnalyzer0()
    }
    TableAnalyzer.noop()
  }

  protected def tableAnalyzer0(): TableAnalyzer

  def getReporter(): TestReporter = {
    reporter
  }

  private[integration] def getBaselineConf(): SparkConf = {
    baselineConf.clone()
  }

  private[integration] def getTestConf(): SparkConf = {
    testConf.clone()
  }

  private[integration] def getBaselineMetricMapper(): MetricMapper = {
    baselineMetricMapper
  }

  private[integration] def getTestMetricMapper(): MetricMapper = {
    testMetricMapper
  }

  private[integration] def typeModifiers(): List[TypeModifier] = {
    if (decimalAsDouble) List(TYPE_MODIFIER_DECIMAL_AS_DOUBLE) else List()
  }

  protected def historyWritePath(): String

  private[integration] def createDataGen(): DataGen

  private[integration] def dataSource(): String

  private[integration] def dataWritePath(): String

  private[integration] def dataScale(): Double

  private[integration] def genPartitionedData(): Boolean

  private[integration] def allQueries(): QuerySet

  private[integration] def desc(): String

  private def resetLogLevel(): Unit = {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel.toString)
    LogManager.getRootLogger.setLevel(logLevel)
  }
}

object Suite {}
