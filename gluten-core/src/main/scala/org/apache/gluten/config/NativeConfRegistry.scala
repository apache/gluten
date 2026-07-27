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

import scala.collection.JavaConverters._

/**
 * The native scope where a conf is passed to, i.e. when the conf is delivered from JVM to native
 * side.
 *
 *   - RUNTIME: the conf is dynamic. It is passed to native side each time a native runtime instance
 *     is created, e.g. per task pipeline / native memory manager. See
 *     `GlutenConfig.getNativeSessionConf`.
 *   - BACKEND: the conf is static to native backend. It is passed to native side once during native
 *     backend initialization. See `GlutenConfig.getNativeBackendConf`.
 *   - ALL: the conf is passed in both of the above cases.
 */
object NativeScope extends Enumeration {
  type NativeScope = Value
  val RUNTIME, BACKEND, ALL = Value
}

/**
 * A registration of one conf key that should be passed to native side.
 *
 * @param key
 *   the conf key.
 * @param defaultToPass
 *   if it yields a defined value, the default value is passed to native side even when the conf is
 *   not explicitly set by user. Use this when native side relies on the key being always present.
 *   If it yields None, the conf is passed only when it's set by user. Re-evaluated on each
 *   selection rather than snapshotted at registration, since some defaults are dynamic, e.g.
 *   `spark.sql.session.timeZone` defaults to the current JVM default time zone.
 * @param transform
 *   if defined, applied to the user-set value before passing to native. Useful for value
 *   normalization, e.g. converting size strings like "64k" to byte numbers, or upper-casing. Not
 *   applied to `defaultToPass`.
 */
case class NativeConfEntry(
    key: String,
    defaultToPass: () => Option[String] = () => None,
    transform: Option[String => String] = None)

/**
 * A registry for conf keys that should be passed to native side.
 *
 * This registry is the single conf-passing mechanism on JVM side, but it is not the API to use it.
 * Rather than maintaining hard-coded key lists in common conf-passing code, each module (core,
 * backend, connector, etc.) declares its own native confs at their definition, through
 * `ConfigRegistry`'s four builders plus `ConfigBuilder.passToNative`:
 *
 *   - `buildConf` / `buildStaticConf` for Gluten's own configurations;
 *   - `registerConf` / `registerStaticConf` for Spark / Hadoop keys that have no Gluten
 *     `ConfigEntry`, which declare the native delivery only.
 *
 * The native scope follows the conf's mutability, so no caller states it: a modifiable conf
 * (`buildConf` / `registerConf`) goes to both channels, a static one (`buildStaticConf` /
 * `registerStaticConf`) to BACKEND. Registration happens automatically on entry creation, hence
 * [[register]] is meant for `ConfigBuilder` rather than for conf objects.
 *
 * Registrations are naturally modular: a backend's or connector's registrations only take effect
 * when its conf object is loaded, so e.g. Velox-only keys never leak into a ClickHouse deployment.
 *
 * Runtime and backend scopes are tracked separately, so the same key can be registered with
 * different semantics per scope, e.g. filter-only in runtime scope while always-passed with a
 * default value in backend scope.
 */
object NativeConfRegistry {
  import NativeScope._

  private val runtimeEntries =
    new java.util.concurrent.ConcurrentHashMap[String, NativeConfEntry]().asScala
  private val backendEntries =
    new java.util.concurrent.ConcurrentHashMap[String, NativeConfEntry]().asScala

  /**
   * Register a conf key to be passed to native side. Called by `ConfigBuilder` when an entry
   * declaring `passToNative` is created; conf objects declare their native confs through the
   * builders instead of calling this directly.
   *
   * @param defaultToPass
   *   if it yields a defined value, the default value is passed even when the conf is not set by
   *   user. Evaluated lazily on each selection so dynamic defaults stay up to date.
   * @param transform
   *   if defined, applied to the user-set value before passing.
   */
  private[config] def register(
      key: String,
      scope: NativeScope,
      defaultToPass: => Option[String] = None,
      transform: Option[String => String] = None): Unit = {
    val entry = NativeConfEntry(key, () => defaultToPass, transform)
    scope match {
      case RUNTIME => doRegister(runtimeEntries, entry)
      case BACKEND => doRegister(backendEntries, entry)
      case ALL =>
        doRegister(runtimeEntries, entry)
        doRegister(backendEntries, entry)
    }
  }

  private def doRegister(
      entries: scala.collection.concurrent.Map[String, NativeConfEntry],
      entry: NativeConfEntry): Unit = {
    val existing = entries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Native conf ${entry.key} already registered!")
  }

  def isRuntimeKey(key: String): Boolean = runtimeEntries.contains(key)

  def isBackendKey(key: String): Boolean = backendEntries.contains(key)

  /**
   * Select runtime-scoped native confs from the given conf map. Entries with `defaultToPass`
   * defined are always included, using the default value when not set in `conf`.
   */
  def selectRuntimeConf(conf: scala.collection.Map[String, String]): Map[String, String] = {
    select(runtimeEntries, conf)
  }

  /**
   * Select backend(static)-scoped native confs from the given conf map. Entries with
   * `defaultToPass` defined are always included, using the default value when not set in `conf`.
   */
  def selectBackendConf(conf: scala.collection.Map[String, String]): Map[String, String] = {
    select(backendEntries, conf)
  }

  private def select(
      entries: scala.collection.concurrent.Map[String, NativeConfEntry],
      conf: scala.collection.Map[String, String]): Map[String, String] = {
    entries.values.flatMap {
      entry =>
        val value = conf.get(entry.key) match {
          case Some(v) => Some(entry.transform.map(_(v)).getOrElse(v))
          case None => entry.defaultToPass()
        }
        value.map(v => entry.key -> v)
    }.toMap
  }

  /** Visible for testing. */
  private[config] def unregister(key: String): Unit = {
    runtimeEntries.remove(key)
    backendEntries.remove(key)
  }
}
