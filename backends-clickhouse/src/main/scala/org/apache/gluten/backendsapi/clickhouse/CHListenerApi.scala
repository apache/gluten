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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.ListenerApi
import org.apache.gluten.config.{GlutenConfig, GlutenCoreConfig}
import org.apache.gluten.execution.CHBroadcastBuildSideCache
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.expression.UDFMappings
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.jni.JniLibLoader
import org.apache.gluten.vectorized.CHNativeExpressionEvaluator

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.listener.CHGlutenSQLAppStatusListener
import org.apache.spark.rpc.{GlutenDriverEndpoint, GlutenExecutorEndpoint}
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules
import org.apache.spark.sql.execution.datasources.v1._
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.sql.utils.ExpressionUtil
import org.apache.spark.util.{SparkDirectoryUtil, SparkShutdownManagerUtil}

import org.apache.commons.lang3.StringUtils

import java.util.TimeZone

class CHListenerApi extends ListenerApi with Logging {

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    GlutenDriverEndpoint.glutenDriverEndpointRef = (new GlutenDriverEndpoint).self
    CHGlutenSQLAppStatusListener.registerListener(sc)
    initialize(pc.conf, isDriver = true)

    val expressionExtensionTransformer = ExpressionUtil.extendedExpressionTransformer(
      pc.conf.get(CHConfig.EXTENDED_EXPRESSION_TRAN_CONF)
    )
    if (expressionExtensionTransformer != null) {
      ExpressionExtensionTrait.registerExpressionExtension(expressionExtensionTransformer)
    }
  }

  override def onDriverShutdown(): Unit = shutdown()

  override def onExecutorStart(pc: PluginContext): Unit = {
    GlutenExecutorEndpoint.executorEndpoint = new GlutenExecutorEndpoint(pc.executorID, pc.conf)
    if (pc.conf().get("spark.master").startsWith("local")) {
      logDebug("Skipping duplicate initializing clickhouse backend on spark local mode")
    } else {
      initialize(pc.conf, isDriver = false)
    }
  }

  override def onExecutorShutdown(): Unit = shutdown()

  private def initialize(conf: SparkConf, isDriver: Boolean): Unit = {
    // Do row / batch type initializations.
    Convention.ensureSparkRowAndBatchTypesRegistered()
    CHBatchType.ensureRegistered()
    CHCarrierRowType.ensureRegistered()
    SparkDirectoryUtil.init(conf)
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH)
    if (StringUtils.isBlank(libPath)) {
      throw new IllegalArgumentException(
        "Please set spark.gluten.sql.columnar.libpath to enable clickhouse backend")
    }
    if (isDriver) {
      JniLibLoader.loadFromPath(libPath)
    } else {
      val executorLibPath = conf.get(GlutenConfig.GLUTEN_EXECUTOR_LIB_PATH.key, libPath)
      JniLibLoader.loadFromPath(executorLibPath)
    }
    CHListenerApi.addShutdownHook
    // Add configs
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._
    conf.setCHConfig(
      "timezone" -> conf.get("spark.sql.session.timeZone", TimeZone.getDefault.getID),
      "local_engine.settings.log_processors_profiles" -> "true")
    conf.setCHSettings("spark_version", SPARK_VERSION)
    if (!conf.contains(RuntimeSettings.ENABLE_MEMORY_SPILL_SCHEDULER.key)) {
      // Enable adaptive memory spill scheduler for native by default
      conf.set(
        RuntimeSettings.ENABLE_MEMORY_SPILL_SCHEDULER.key,
        RuntimeSettings.ENABLE_MEMORY_SPILL_SCHEDULER.defaultValueString)
    }

    // add memory limit for external sort
    if (conf.get(RuntimeSettings.MAX_BYTES_BEFORE_EXTERNAL_SORT) <= 0) {
      if (conf.getBoolean(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, defaultValue = false)) {
        val memSize = conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, 0)
        if (memSize > 0L) {
          val cores = conf.getInt("spark.executor.cores", 1).toLong
          val sortMemLimit = ((memSize / cores) * 0.8).toLong
          logDebug(s"max memory for sorting: $sortMemLimit")
          conf.set(RuntimeSettings.MAX_BYTES_BEFORE_EXTERNAL_SORT, sortMemLimit)
        }
      }
    }

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    CHNativeExpressionEvaluator.initNative(conf.getAll.toMap)

    // inject backend-specific implementations to override spark classes
    GlutenFormatFactory.register(
      new CHParquetWriterInjects,
      new CHOrcWriterInjects,
      new CHMergeTreeWriterInjects)
    GlutenFormatFactory.injectPostRuleFactory(
      session => GlutenWriterColumnarRules.NativeWritePostRule(session))
    GlutenFormatFactory.register(new CHRowSplitter())
  }

  private def shutdown(): Unit = {
    CHBroadcastBuildSideCache.cleanAll()
    CHNativeExpressionEvaluator.finalizeNative()
  }
}

object CHListenerApi {
  var initialized = false

  def addShutdownHook: Unit = {
    if (!initialized) {
      initialized = true
      SparkShutdownManagerUtil.addHookForLibUnloading(
        () => {
          // Due to the changes in the JNI OnUnLoad calling mechanism of the JDK17,
          // it needs to manually call the destroy native function
          // to release ch resources and avoid core dump
          CHNativeExpressionEvaluator.destroyNative()
        })
    }
  }
}
