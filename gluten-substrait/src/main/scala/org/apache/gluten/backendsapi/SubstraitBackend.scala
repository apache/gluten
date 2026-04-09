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
package org.apache.gluten.backendsapi

import org.apache.gluten.GlutenBuildInfo
import org.apache.gluten.backend.Backend
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.config.GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED
import org.apache.gluten.events.GlutenBuildInfoEvent
import org.apache.gluten.extension.columnar.LoggedRule
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.softaffinity.SoftAffinityListener
import org.apache.spark.sql.execution.adaptive.GlutenCostEvaluator
import org.apache.spark.sql.execution.ui.{GlutenSQLAppStatusListener, GlutenUIUtils}
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

trait SubstraitBackend extends Backend with Logging {
  import SubstraitBackend._
  private var _sc: Option[SparkContext] = None

  final override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    _sc = Some(sc)
    val conf = pc.conf()

    // Register Gluten listeners
    GlutenSQLAppStatusListener.register(sc)
    if (conf.get(GLUTEN_SOFT_AFFINITY_ENABLED)) {
      SoftAffinityListener.register(sc)
    }

    postBuildInfoEvent(sc)
    setBuildInfoConfig(conf)

    setPredefinedConfigs(conf)

    listenerApi().onDriverStart(sc, pc)
  }

  final override def onDriverShutdown(): Unit = {
    listenerApi().onDriverShutdown()
  }
  final override def onExecutorStart(pc: PluginContext): Unit = {
    listenerApi().onExecutorStart(pc)
  }
  final override def onExecutorShutdown(): Unit = {
    listenerApi().onExecutorShutdown()
  }
  final override def injectRules(injector: Injector): Unit = {
    injector.gluten.legacy.injectRuleWrapper(r => new LoggedRule(r))
    ruleApi().injectRules(injector)
  }

  final override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    _sc.foreach {
      sc =>
        if (GlutenUIUtils.uiEnabled(sc)) {
          GlutenUIUtils.attachUI(sc)
          logInfo("Gluten SQL Tab has been attached.")
        }
    }
  }

  def iteratorApi(): IteratorApi
  def sparkPlanExecApi(): SparkPlanExecApi
  def transformerApi(): TransformerApi
  def validatorApi(): ValidatorApi
  def metricsApi(): MetricsApi
  def listenerApi(): ListenerApi
  def ruleApi(): RuleApi
  def settings(): BackendSettingsApi
}

object SubstraitBackend extends Logging {

  /** Since https://github.com/apache/gluten/pull/2247. */
  private def postBuildInfoEvent(sc: SparkContext): Unit = {
    // export gluten version to property to spark
    System.setProperty("gluten.version", GlutenBuildInfo.VERSION)

    val glutenBuildInfo = new mutable.LinkedHashMap[String, String]()

    glutenBuildInfo.put("Gluten Version", GlutenBuildInfo.VERSION)
    glutenBuildInfo.put("GCC Version", GlutenBuildInfo.GCC_VERSION)
    glutenBuildInfo.put("Java Version", GlutenBuildInfo.JAVA_COMPILE_VERSION)
    glutenBuildInfo.put("Scala Version", GlutenBuildInfo.SCALA_COMPILE_VERSION)
    glutenBuildInfo.put("Spark Version", GlutenBuildInfo.SPARK_COMPILE_VERSION)
    glutenBuildInfo.put("Hadoop Version", GlutenBuildInfo.HADOOP_COMPILE_VERSION)
    glutenBuildInfo.put("Gluten Branch", GlutenBuildInfo.BRANCH)
    glutenBuildInfo.put("Gluten Revision", GlutenBuildInfo.REVISION)
    glutenBuildInfo.put("Gluten Revision Time", GlutenBuildInfo.REVISION_TIME)
    glutenBuildInfo.put("Gluten Build Time", GlutenBuildInfo.BUILD_DATE)
    glutenBuildInfo.put("Gluten Repo URL", GlutenBuildInfo.REPO_URL)

    val loggingInfo = glutenBuildInfo
      .map { case (name, value) => s"$name: $value" }
      .mkString(
        "Gluten build info:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
    if (GlutenUIUtils.uiEnabled(sc)) {
      val event = GlutenBuildInfoEvent(glutenBuildInfo.toMap)
      GlutenUIUtils.postEvent(sc, event)
    }
  }

  private def setBuildInfoConfig(conf: SparkConf): Unit = {
    conf.set("spark.gluten.branch", GlutenBuildInfo.BRANCH)
    conf.set("spark.gluten.revision", GlutenBuildInfo.REVISION)
    conf.set("spark.gluten.revisionTime", GlutenBuildInfo.REVISION_TIME)
    conf.set("spark.gluten.buildTime", GlutenBuildInfo.BUILD_DATE)
  }

  private def setPredefinedConfigs(conf: SparkConf): Unit = {
    // adaptive custom cost evaluator class
    val enableGlutenCostEvaluator = conf.get(GlutenConfig.COST_EVALUATOR_ENABLED)
    if (enableGlutenCostEvaluator) {
      conf.set(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS, classOf[GlutenCostEvaluator].getName)
    }

    // Disable vanilla columnar readers, to prevent columnar-to-columnar conversions.
    // FIXME: Do we still need this trick since
    //  https://github.com/apache/gluten/pull/1931 was merged?
    if (!conf.get(GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED)) {
      // FIXME Hongze 22/12/06
      //  BatchScan.scala in shim was not always loaded by class loader.
      //  The file should be removed and the "ClassCastException" issue caused by
      //  spark.sql.<format>.enableVectorizedReader=true should be fixed in another way.
      //  Before the issue is fixed we force the use of vanilla row reader by using
      //  the following statement.
      conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED, false)
      conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED, false)
      conf.set(SQLConf.CACHE_VECTORIZED_READER_ENABLED, false)
    }
  }
}
