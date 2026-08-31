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

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
 * A registration of one conf key that should be passed to native side.
 *
 * @param key
 *   the conf key.
 * @param convert
 *   normalizes a value for native side through the conf's own converter, i.e. the one chosen by
 *   `stringConf` / `bytesConf(unit)` / `intConf` / ... plus any `transform`. Both a user-set value
 *   and a resolved default flow through it, so a size conf reaches native as a number rather than
 *   as "64k", and a `timeParserPolicy` conf reaches native upper-cased even when the user wrote it
 *   in lower case.
 * @param declaredDefault
 *   the default to deliver when the user did not set the key, in its already-converted native form,
 *   or `None` to deliver nothing (native's own fallback takes over). Evaluated per delivery rather
 *   than snapshotted here, since a default may be dynamic - `spark.sql.session.timeZone` follows
 *   the JVM default time zone, and `spark.sql.ansi.enabled` follows Spark's own default which
 *   differs between 3.x and 4.x. Which path applies is determined at the declaration site:
 *
 *   - `createOptional`: `None` - nothing is delivered when unset.
 *   - `createWithForeignDefault` (foreign only): resolved from the foreign entry via
 *     `GlutenConfigUtil.resolveForeignDeclaredDefault` at each delivery, then run through
 *     `convert`.
 *   - `createWithDefault(value)`: the stated Gluten value in converted form.
 */
case class NativeConfEntry(
    key: String,
    convert: String => String = identity,
    declaredDefault: () => Option[String] = () => None)

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
 * Registration happens automatically on entry creation, hence [[register]] is meant for
 * `ConfigBuilder` rather than for conf objects.
 *
 * Registrations are naturally modular: a backend's or connector's registrations only take effect
 * when its conf object is loaded, so e.g. Velox-only keys never leak into a ClickHouse deployment.
 *
 * There are two delivery channels, matching the two lifecycle stages of a native backend, and which
 * ones a conf lands on follows its mutability rather than any argument:
 *
 *   - backend: delivered once during native backend initialization. See
 *     `GlutenConfig.getNativeBackendConf`. A static conf (`buildStaticConf` / `registerStaticConf`)
 *     goes here only, since a snapshot taken at init is its value forever.
 *   - runtime: delivered each time a native runtime instance is created, e.g. per task pipeline /
 *     native memory manager. See `GlutenConfig.getNativeSessionConf`. A modifiable conf
 *     (`buildConf` / `registerConf`) goes here *and* to the backend channel, so native observes the
 *     current value wherever it reads the key.
 *
 * When the user did not set a registered key, what is delivered is stated at the declaration site
 * (see `ConfigBuilder.passToNative`): `createOptional` delivers nothing and leaves native's own
 * fallback in charge; `createWithForeignDefault` delivers the foreign-declared default resolved per
 * delivery via `GlutenConfigUtil.resolveForeignDeclaredDefault`; `createWithDefault(value)`
 * delivers the stated value. Delivery is always normalized through the conf's own value converter,
 * so all per-key parsing lives at the declaration site rather than in per-key transforms at
 * delivery.
 */

object NativeConfRegistry extends Logging {

  private val runtimeEntries =
    new java.util.concurrent.ConcurrentHashMap[String, NativeConfEntry]().asScala
  private val backendEntries =
    new java.util.concurrent.ConcurrentHashMap[String, NativeConfEntry]().asScala

  // The backend channel is delivered once, during native backend initialization, so a registration
  // arriving afterwards can never reach it: the conf would show up on the runtime channel only, and
  // native would keep using its own fallback wherever it reads the key at init. Latched on first
  // delivery so a late declaration is reported rather than silently half-applied.
  @volatile private var backendConfDelivered = false

  /**
   * Register a conf key to be passed to native side. Called by `ConfigBuilder` when an entry
   * declaring `passToNative` is created; conf objects declare their native confs through the
   * builders instead of calling this directly.
   *
   * @param isStatic
   *   whether the conf is static to the native backend, i.e. declared by `buildStaticConf` /
   *   `registerStaticConf`. A static conf is delivered on the backend channel only; a modifiable
   *   one on both.
   * @param convert
   *   normalizes a value for native through the conf's own value converter, applied whether the
   *   value comes from the user or from a resolved default.
   * @param declaredDefault
   *   the default declared by the conf itself, in its already-converted native form, evaluated per
   *   delivery so a dynamic default stays up to date. `None` for a `createOptional` conf, which
   *   then falls back to the foreign declaration for a Spark-owned key, or is delivered only when
   *   set.
   */
  private[config] def register(
      key: String,
      isStatic: Boolean,
      convert: String => String = identity,
      declaredDefault: => Option[String] = None): Unit = {
    val entry = NativeConfEntry(key, convert, () => declaredDefault)
    if (!isStatic) {
      doRegister(runtimeEntries, entry)
    }
    doRegisterToBackend(entry)
  }

  private def doRegisterToBackend(entry: NativeConfEntry): Unit = {
    if (backendConfDelivered) {
      // Not fatal: the conf still works on the runtime channel, and failing here would take down a
      // query for a conf object that merely loaded late. But native backend init has already
      // happened, so declare the gap loudly - the usual cause is a conf object that is not declared
      // through `Component.confs()`.
      logWarning(
        s"Native conf ${entry.key} was declared after native backend conf had already been " +
          s"delivered, so it will not reach the backend channel. Declare its conf object through " +
          s"Component.confs() so that it is initialized before native backend initialization.")
    }
    doRegister(backendEntries, entry)
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
   * Select runtime-scoped native confs from the given conf map. A key absent from `conf` is
   * delivered with its declared default, if it has one.
   */
  def selectRuntimeConf(conf: scala.collection.Map[String, String]): Map[String, String] = {
    select(runtimeEntries, conf)
  }

  /**
   * Select backend(static)-scoped native confs from the given conf map. A key absent from `conf` is
   * delivered with its declared default, if it has one.
   *
   * Marks the backend channel as delivered, so that a declaration arriving afterwards - which can
   * no longer reach native backend initialization - is reported rather than silently applied to the
   * runtime channel alone.
   */
  def selectBackendConf(conf: scala.collection.Map[String, String]): Map[String, String] = {
    backendConfDelivered = true
    select(backendEntries, conf)
  }

  private def select(
      entries: scala.collection.concurrent.Map[String, NativeConfEntry],
      conf: scala.collection.Map[String, String]): Map[String, String] = {
    entries.values.flatMap {
      entry =>
        // A user-set value goes through the conf's own converter so that e.g. "32k"
        // reaches native as "32" (KiB), and "legacy" as "LEGACY". When unset, the
        // declared default - already in converted form - is delivered instead.
        conf.get(entry.key).map(v => entry.key -> entry.convert(v))
          .orElse(entry.declaredDefault().map(d => entry.key -> d))
    }.toMap
  }

  /** Visible for testing. */
  private[config] def unregister(key: String): Unit = {
    runtimeEntries.remove(key)
    backendEntries.remove(key)
  }
}
