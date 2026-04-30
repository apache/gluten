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

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

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

  private val TIME_STRING_PATTERN = Pattern.compile("(-?[0-9]+)([a-z]+)?")

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
    entry
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
    entry
  }
}
