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

import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.internal.GlutenConfigUtil

import java.util.concurrent.TimeUnit

object BackendType extends Enumeration {
  type BackendType = Value
  val COMMON, VELOX, CLICKHOUSE = Value
}

private[gluten] case class ConfigBuilder(key: String) {
  import ConfigHelpers._

  private[config] var _doc = ""
  private[config] var _version = ""
  private[config] var _backend = BackendType.COMMON
  private[config] var _public = true
  private[config] var _experimental = false
  private[config] var _alternatives = List.empty[String]
  private[config] var _onCreate: Option[ConfigEntry[_] => Unit] = None
  private[config] var _isStatic = false
  private[config] var _passToNative = false
  private[config] var _isForeign = false
  private[config] var _deliverForeignDefault = false

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(s: String): ConfigBuilder = {
    _version = s
    this
  }

  def backend(backend: BackendType.BackendType): ConfigBuilder = {
    _backend = backend
    this
  }

  /**
   * This method marks a config as internal for any of the following reasons:
   *   - Intended exclusively for developers or advanced users
   *   - Allows for flexibility in development and testing without compromising the public API's
   *     stability
   */
  def internal(): ConfigBuilder = {
    _public = false
    this
  }

  def experimental(): ConfigBuilder = {
    _experimental = true
    this
  }

  def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
    _onCreate = Option(callback)
    this
  }

  def withAlternative(key: String): ConfigBuilder = {
    _alternatives = _alternatives :+ key
    this
  }

  /** Marks this config as a static (non-session-mutable) conf. Set by `buildStaticConf`. */
  private[config] def markStatic(): ConfigBuilder = {
    _isStatic = true
    this
  }

  /**
   * Marks this config as owned by Spark / Hadoop rather than by Gluten. Set by `registerConf` /
   * `registerStaticConf`.
   *
   * A foreign config is not registered as a Gluten config entry and not registered to `SQLConf`
   * (Spark or Hadoop already did that); the builder is only used to declare how the key is
   * delivered to native side.
   */
  private[config] def markForeign(): ConfigBuilder = {
    _isForeign = true
    this
  }

  /**
   * Marks this foreign config as delivering the default declared by Spark / Hadoop for the key,
   * resolved freshly at each delivery. Set by `createWithForeignDefault`, which is only meaningful
   * for a foreign config: a Gluten config states its default in `createWithDefault(value)` instead.
   */
  private[config] def markDeliverForeignDefault(): ConfigBuilder = {
    require(
      _isForeign,
      s"Config $key: createWithForeignDefault is only valid for registerConf() / " +
        s"registerStaticConf(), since a Gluten config has no Spark / Hadoop declaration to " +
        s"resolve a default from. Use createWithDefault(value) instead."
    )
    _deliverForeignDefault = true
    this
  }

  /**
   * Marks this config to be passed to native side. A value set by the user is always delivered;
   * what happens when it is not set is stated by the terminal method, which is the whole of the
   * rule:
   *
   *   - `createOptional`: nothing is delivered, leaving native's own fallback in charge. This is
   *     the common case for a foreign key, since native usually declares the same fallback Spark /
   *     Hadoop does, or branches on the key being absent at all.
   *   - `createWithForeignDefault` (foreign only): the default Spark / Hadoop declares for the key
   *     is delivered, resolved freshly at each delivery. Use it when native's fallback is wrong or
   *     missing, and the foreign default is either computed at runtime
   *     (`spark.sql.session.timeZone` follows the JVM default time zone) or has changed across
   *     Spark versions (`spark.sql.ansi.enabled` flipped its default in 4.0). Never restate such a
   *     default on the Gluten side - that is exactly what drifts.
   *   - `createWithDefault(value)`: the stated value is delivered. For a Gluten config this is its
   *     own default; for a foreign one it says Gluten deliberately departs from what both Spark /
   *     Hadoop and native would apply, e.g. `fs.s3a.path.style.access` where native falls back to
   *     `false` and Gluten wants `true`.
   *
   * The config is registered to [[NativeConfRegistry]] on entry creation.
   *
   * Which delivery channels it lands on follows the conf's mutability, so there is no argument:
   *   - `buildConf` / `registerConf`: modifiable at any time and usable at any time. Delivered both
   *     during native backend initialization and on each native runtime creation, so native
   *     observes the current value wherever it reads the key.
   *   - `buildStaticConf` / `registerStaticConf`: set while the native backend is initialized and
   *     not modifiable afterwards. Delivered once during native backend initialization.
   */
  def passToNative(): ConfigBuilder = {
    _passToNative = true
    this
  }

  /**
   * Normalizes a value for native side through the conf's own value converter, i.e. the one chosen
   * by `stringConf` / `bytesConf(unit)` / `intConf` / ... plus any `transform`. A conf therefore
   * states how its value is parsed exactly once, at its declaration, and both a user-set value and
   * a resolved default go through it.
   *
   * This is what makes a size conf reach native as a number rather than as "64k": a
   * `bytesConf(ByteUnit.KiB)` yields the KiB count the foreign entry would yield, and a
   * `bytesConf(ByteUnit.BYTE)` the byte count. A foreign conf declares the same converter Spark /
   * Hadoop declares, so JVM and native agree on the value's meaning and native applies whatever
   * unit conversion it needs on top - `spark.shuffle.file.buffer` is KiB on both sides, and native
   * multiplies by 1024.
   *
   * Falls back to the raw string when the entry has no usable converter (e.g. a fallback entry),
   * since delivering the value unchanged is always better than dropping it.
   */
  private def convertForNative(entry: ConfigEntry[_], raw: String): String = {
    try {
      entry.valueConverter(raw) match {
        // An `OptionalConfigEntry` wraps its converter's result in `Option`.
        case o: Option[_] => o.map(_.toString).getOrElse(raw)
        case null => raw
        case v => v.toString
      }
    } catch {
      // A value Spark or Hadoop would reject is not this mechanism's business to validate: Spark or
      // Hadoop raises on it at its own read site, with its own message. Deliver it unchanged rather
      // than failing conf selection, which runs per task.
      case _: IllegalArgumentException => raw
    }
  }

  private[config] def registerToNative(entry: ConfigEntry[_]): Unit = {
    require(
      !_isForeign || _passToNative,
      s"Config $key: a config declared by registerConf() / registerStaticConf() must be marked " +
        s"with passToNative(), otherwise declaring it has no effect"
    )
    if (!_passToNative) {
      return
    }
    // The channel follows the conf's mutability. A modifiable conf is delivered on both channels so
    // native observes the current value wherever it reads the key; a static conf is set while the
    // native backend is initialized and not modifiable afterwards, so delivering it once there is
    // lossless.
    NativeConfRegistry.register(
      key,
      _isStatic,
      convert = convertForNative(entry, _),
      declaredDefault = declaredDefault(entry))
  }

  /**
   * The default delivered to native for a key the user did not set, or `None` to deliver nothing
   * and leave native's own fallback in charge. Read per delivery rather than snapshotted, so a
   * default that follows JVM or session state keeps delivering its current value.
   *
   * Which of the three the caller gets is stated by the terminal method - see [[passToNative]].
   */
  private def declaredDefault(entry: ConfigEntry[_]): Option[String] = entry match {
    // A fallback entry reports the *target* conf's default as its own, and the target is delivered
    // under its own key. Delivering it here would also contradict the user: with only the target
    // conf set, this key would carry the target's default rather than the value the user chose.
    case _: ConfigEntryFallback[_] | _: ConfigEntryForeignFallback[_] => None
    // `createWithForeignDefault` on a foreign key: take the foreign declaration, resolved
    // now rather than restated on the Gluten side, so the two cannot drift across
    // versions. The foreign default is a raw string ("32k" for
    // `spark.shuffle.file.buffer`), so it goes through this conf's own converter just
    // as a user-set value does.
    case e if _deliverForeignDefault =>
      GlutenConfigUtil.resolveForeignDeclaredDefault(key).map(convertForNative(e, _))
    // `createWithDefault(value)`. Reading the parsed default rather than the raw
    // default string means e.g. a "64MB" bytes conf reaches native as "67108864".
    case e if e.defaultValue.isDefined => e.defaultValue.map(_.toString)
    // `createOptional`: nothing is delivered when the key is not set.
    case _ => None
  }

  def intConf: TypedConfigBuilder[Int] = {
    new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
  }

  def longConf: TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    new TypedConfigBuilder(this, toBoolean(_, key))
  }

  def stringConf: TypedConfigBuilder[String] = {
    new TypedConfigBuilder(this, identity)
  }

  def timeConf(unit: TimeUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, timeFromString(_, unit), timeToString(_, unit))
  }

  def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, byteFromString(_, unit), byteToString(_, unit))
  }

  def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T] = {
    val entry =
      new ConfigEntryFallback[T](
        key,
        _doc,
        _version,
        _backend,
        _public,
        _experimental,
        _alternatives,
        fallback)
    _onCreate.foreach(_(entry))
    registerToNative(entry)
    entry
  }
}

private object ConfigHelpers {
  def toNumber[T](s: String, converter: String => T, key: String, configType: String): T = {
    try {
      converter(s.trim)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key should be $configType, but was $s")
    }
  }

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def timeFromString(str: String, unit: TimeUnit): Long = JavaUtils.timeStringAs(str, unit)

  def timeToString(v: Long, unit: TimeUnit): String = s"${TimeUnit.MILLISECONDS.convert(v, unit)}ms"

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.length() > 0 && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def byteToString(v: Long, unit: ByteUnit): String = s"${unit.convertTo(v, ByteUnit.BYTE)}b"
}

private[gluten] class TypedConfigBuilder[T](
    val parent: ConfigBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfigBuilder, converter: String => T) = {
    this(parent, converter, { v: T => v.toString })
  }

  def transform(fn: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /**
   * Creates an entry that falls back to a foreign configuration when this one is not set, e.g.
   * Gluten's shuffle codec falling back to `spark.io.compression.codec`. The foreign default
   * applies when neither key is set, so this entry never needs a default of its own.
   *
   * The fallback is stated by key and default value rather than as the foreign `ConfigEntry`, which
   * for Spark is `private[spark]` and so cannot appear in a signature outside `org.apache.spark`.
   */
  def fallbackConf(fallbackKey: String, fallbackDefault: String): ConfigEntryForeignFallback[T] = {
    val entry = new ConfigEntryForeignFallback[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      fallbackKey,
      fallbackDefault
    )
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }

  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
    transform {
      v =>
        if (!validator(v)) {
          throw new IllegalArgumentException(s"'$v' in ${parent.key} is invalid. $errorMsg")
        }
        v
    }
  }

  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform {
      v =>
        if (!validValues.contains(v)) {
          throw new IllegalArgumentException(
            s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, " +
              s"but was $v")
        }
        v
    }
  }

  /** See [[ConfigBuilder.passToNative]]. Callable after the value type is chosen. */
  def passToNative(): TypedConfigBuilder[T] = {
    parent.passToNative()
    this
  }

  def createOptional: OptionalConfigEntry[T] = {
    val entry = new OptionalConfigEntry[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter)
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }

  /**
   * Declares the config to deliver the default Spark / Hadoop declares for the key when the user
   * did not set it, resolved freshly at each delivery. Only valid for a foreign config declared via
   * `registerConf` / `registerStaticConf`, since a Gluten config has no separate owner to consult -
   * it uses [[createWithDefault(default:T)*]] instead.
   */
  def createWithForeignDefault: OptionalConfigEntry[T] = {
    parent.markDeliverForeignDefault()
    createOptional
  }

  def createWithDefault(default: T): ConfigEntry[T] = {
    assert(default != null, "Use createOptional.")
    default match {
      case str: String => createWithDefaultString(str)
      case _ =>
        val transformedDefault = converter(stringConverter(default))
        val entry = new ConfigEntryWithDefault[T](
          parent.key,
          parent._doc,
          parent._version,
          parent._backend,
          parent._public,
          parent._experimental,
          parent._alternatives,
          converter,
          stringConverter,
          transformedDefault
        )
        parent._onCreate.foreach(_(entry))
        parent.registerToNative(entry)
        entry
    }
  }

  def createWithDefaultString(default: String): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultString[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      default
    )
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }

  /**
   * Creates an entry whose default value is computed on each read rather than fixed here, mirroring
   * Spark's `createWithDefaultFunction`. Use it when the default depends on JVM or session state,
   * e.g. a time zone conf defaulting to the current JVM default time zone. Combined with
   * `passToNative`, native receives the value resolved at delivery time.
   */
  def createWithDefaultFunction(defaultFunc: () => T): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultFunction[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      defaultFunc
    )
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }
}
