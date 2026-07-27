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

import org.apache.gluten.config.BackendType.BackendType

import org.apache.spark.sql.internal.GlutenConfigProvider

/**
 * An entry contains all meta information for a configuration.
 *
 * The code is similar to Spark's relevant config code but extended for Gluten's use, like adding
 * backend type, etc.
 *
 * @tparam T
 *   the value type
 */
trait ConfigEntry[T] {

  /** The key for the configuration. */
  def key: String

  /** The documentation for the configuration. */
  def doc: String

  /** The gluten version when the configuration was released. */
  def version: String

  /** The backend type of the configuration. */
  def backend: BackendType.BackendType

  /**
   * If this configuration is public to the user. If it's `false`, this configuration is only used
   * internally and we should not expose it to users.
   */
  def isPublic: Boolean

  def isExperimental: Boolean

  /** the alternative keys for the configuration. */
  def alternatives: List[String]

  /**
   * How to convert a string to the value. It should throw an exception if the string does not have
   * the required format.
   */
  def valueConverter: String => T

  /** How to convert a value to a string that the user can use it as a valid string value. */
  def stringConverter: T => String

  /** Read the configuration from the given GlutenConfigProvider. */
  def readFrom(conf: GlutenConfigProvider): T

  /** The default value of the configuration. */
  def defaultValue: Option[T]

  /** The string representation of the default value. */
  def defaultValueString: String

  final protected def readString(provider: GlutenConfigProvider): Option[String] = {
    alternatives.foldLeft(provider.get(key))((res, nextKey) => res.orElse(provider.get(nextKey)))
  }

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version)"
  }
}

private[gluten] class OptionalConfigEntry[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String)
  extends ConfigEntry[Option[T]] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => Option[T] = s => Option(_valueConverter(s))

  override def stringConverter: Option[T] => String = v => v.map(_stringConverter).orNull

  override def readFrom(conf: GlutenConfigProvider): Option[T] =
    readString(conf).map(_valueConverter)

  override def defaultValue: Option[Option[T]] = None

  override def defaultValueString: String = ConfigEntry.UNDEFINED
}

private[gluten] class ConfigEntryWithDefault[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultVal: T)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(_defaultVal)
  }

  override def defaultValue: Option[T] = Option(_defaultVal)

  override def defaultValueString: String = stringConverter(_defaultVal)
}

private[gluten] class ConfigEntryWithDefaultString[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultVal: String)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    val value = readString(conf).getOrElse(_defaultVal)
    valueConverter(value)
  }

  override def defaultValue: Option[T] = Some(valueConverter(_defaultVal))

  override def defaultValueString: String = _defaultVal
}

/**
 * A config entry whose default value is computed on each read rather than fixed at declaration,
 * mirroring Spark's `createWithDefaultFunction`. Use it when the default depends on JVM or session
 * state, e.g. `spark.sql.session.timeZone` defaults to the current JVM default time zone.
 */
private[gluten] class ConfigEntryWithDefaultFunction[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultFunction: () => T)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(_defaultFunction())
  }

  override def defaultValue: Option[T] = Some(_defaultFunction())

  override def defaultValueString: String = stringConverter(_defaultFunction())
}

private[gluten] class ConfigEntryFallback[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    fallback: ConfigEntry[T])
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = fallback.valueConverter

  override def stringConverter: T => String = fallback.stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(fallback.readFrom(conf))
  }

  override def defaultValue: Option[T] = fallback.defaultValue

  override def defaultValueString: String = fallback.defaultValueString
}

/**
 * A config entry that falls back to a config owned by Spark rather than by Gluten, e.g. Gluten's
 * shuffle codec falling back to `spark.io.compression.codec`.
 *
 * The fallback is stated by key and default value rather than as Spark's own `ConfigEntry`, which
 * is `private[spark]` and so cannot appear in a signature here. It is read through
 * [[GlutenConfigProvider]] so that both keys are looked up in the same conf source, and its default
 * value applies when neither key is set.
 */
private[gluten] class ConfigEntrySparkFallback[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _fallbackKey: String,
    _fallbackDefault: String)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  /** The key of the Spark configuration this entry falls back to. */
  def fallbackKey: String = _fallbackKey

  override def readFrom(conf: GlutenConfigProvider): T = readWithSource(conf)._1

  /**
   * Reads the value and tells whether it came from this entry's own key rather than from the Spark
   * configuration it falls back to. A caller that treats the two differently - e.g. validating an
   * explicitly set value against a stricter set of allowed values - should use this rather than
   * looking the key up a second time, so the value and its origin cannot disagree.
   */
  def readWithSource(conf: GlutenConfigProvider): (T, Boolean) = {
    readString(conf) match {
      case Some(value) => (valueConverter(value), true)
      case None =>
        (conf.get(_fallbackKey).map(valueConverter).getOrElse(defaultValue.get), false)
    }
  }

  override def defaultValue: Option[T] = Some(valueConverter(defaultValueString))

  override def defaultValueString: String = _fallbackDefault
}

object ConfigEntry {
  val UNDEFINED = "<undefined>"
}
