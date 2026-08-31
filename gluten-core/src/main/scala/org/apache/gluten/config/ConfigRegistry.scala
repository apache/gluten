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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

trait ConfigRegistry {
  private val configEntries =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]().asScala

  private def register(entry: ConfigEntry[_]): Unit = {
    val existing = configEntries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Config entry ${entry.key} already registered!")
  }

  private def registerToSQLConf(entry: ConfigEntry[_], isStatic: Boolean): Unit = {
    (entry.key :: entry.alternatives).foreach(registerToSQLConf(entry, _, isStatic))
  }

  private def registerToSQLConf(entry: ConfigEntry[_], key: String, isStatic: Boolean): Unit = {
    var builder =
      if (isStatic) SQLConf.buildStaticConf(key) else SQLConf.buildConf(key)
    if (entry.doc.nonEmpty) {
      builder = builder.doc(entry.doc)
    }
    if (entry.version.nonEmpty) {
      builder = builder.version(entry.version)
    }
    if (!entry.isPublic) {
      builder = builder.internal()
    }

    val sparkEntry = builder.stringConf.transform {
      value =>
        entry.valueConverter(value)
        value
    }
    entry match {
      // A dynamic default has to stay dynamic in the mirror too, otherwise `spark.conf.get` reports
      // whatever the default resolved to while this conf object was initializing.
      case e: ConfigEntryWithDefaultFunction[_] =>
        sparkEntry.createWithDefaultFunction(() => e.defaultValueString)
      case _ if entry.defaultValue.isDefined =>
        sparkEntry.createWithDefaultString(entry.defaultValueString)
      case _ =>
        sparkEntry.createOptional
    }
  }

  /** Visible for testing. */
  private[config] def allEntries: Seq[ConfigEntry[_]] = {
    configEntries.values.toSeq
  }

  /**
   * Forces this conf object's initialization so that its native conf declarations (via
   * `ConfigBuilder.passToNative`) are in place before native confs are selected.
   *
   * Call this instead of referencing an arbitrary field of the object: a reference to a constant
   * `val` may be folded away by the compiler, leaving the object uninitialized and its native confs
   * silently unregistered, while a no-arg method call always triggers initialization.
   */
  def ensureRegistered(): Unit = {}

  /**
   * Declares a Gluten configuration that is modifiable at any time and usable at any time. When
   * marked with `passToNative`, it is delivered both during native backend initialization and on
   * each native runtime creation.
   */
  protected def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate {
      entry =>
        register(entry)
        registerToSQLConf(entry, isStatic = false)
        ConfigRegistry.registerToAllEntries(entry)
    }
  }

  /**
   * Declares a Gluten configuration that is set while the native backend is initialized and not
   * modifiable afterwards. When marked with `passToNative`, it is delivered once during native
   * backend initialization.
   */
  protected def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key)
      .markStatic()
      .onCreate {
        entry =>
          register(entry)
          registerToSQLConf(entry, isStatic = true)
          ConfigRegistry.registerToAllEntries(entry)
      }
  }

  /**
   * Declares how a configuration owned by Spark / Hadoop rather than by Gluten is delivered to
   * native side, e.g. `spark.sql.orc.compression.codec` or `spark.hadoop.input.read.timeout`.
   *
   * Same contract as [[buildConf]]: modifiable at any time and usable at any time, delivered on
   * both channels. The difference is that nothing is registered as a Gluten config entry or to
   * `SQLConf` - the Spark/Hadoop already did that, and re-registering would conflict with it. Only
   * the native delivery is declared, hence `passToNative` is required.
   *
   * The terminal method states what is delivered when the user did not set the key:
   *
   * {{{
   *   // (A) native has a correct fallback (matches Spark's default or branches on absence).
   *   registerConf(SQLConf.CASE_SENSITIVE.key).booleanConf.passToNative().createOptional
   *
   *   // (B) native has no correct fallback; deliver Spark's own default, resolved per delivery so
   *   // a dynamic or version-dependent default is not restated on the Gluten side.
   *   registerConf(SQLConf.SESSION_LOCAL_TIMEZONE.key)
   *     .stringConf.passToNative().createWithForeignDefault
   *
   *   // (C) Gluten deliberately departs from what both Spark and native would apply.
   *   registerConf(SPARK_S3_PATH_STYLE_ACCESS).booleanConf.passToNative().createWithDefault(true)
   * }}}
   */
  protected def registerConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).markForeign()
  }

  /**
   * Same as [[registerConf]], with the contract of [[buildStaticConf]]: the configuration is set
   * while the native backend is initialized and not modifiable afterwards, so it is delivered once
   * during native backend initialization.
   *
   * {{{
   *   registerStaticConf("spark.sql.orc.compression.codec")
   *     .doc("Consumed by ClickHouse backend initialization.")
   *     .stringConf
   *     .passToNative()
   *     .createWithDefault("snappy")
   * }}}
   */
  protected def registerStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).markForeign().markStatic()
  }

  /** The typed accessor of this conf object. */
  def get: GlutenCoreConfig
}

object ConfigRegistry {
  private val allConfigEntries =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]().asScala

  private def registerToAllEntries(entry: ConfigEntry[_]): Unit = {
    val existing = allConfigEntries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Config entry ${entry.key} already registered!")
  }

  def containsEntry(entry: ConfigEntry[_]): Boolean = {
    allConfigEntries.contains(entry.key)
  }

  def findEntry(key: String): Option[ConfigEntry[_]] = allConfigEntries.get(key)
}
