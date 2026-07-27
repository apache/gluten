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
package org.apache.spark.sql.internal

import org.apache.gluten.config.ConfigEntry

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigEntry => SparkConfigEntry, OptionalConfigEntry}

object SparkConfigUtil {

  implicit class RichSparkConf(val conf: SparkConf) {
    def get[T](entry: SparkConfigEntry[T]): T = {
      SparkConfigUtil.get(conf, entry)
    }

    def get[T](entry: ConfigEntry[T]): T = {
      SparkConfigUtil.get(conf, entry)
    }

    def set[T](entry: SparkConfigEntry[T], value: T): SparkConf = {
      SparkConfigUtil.set(conf, entry, value)
    }

    def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
      SparkConfigUtil.set(conf, entry, value)
    }

    def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
      SparkConfigUtil.set(conf, entry, value)
    }
  }

  def get[T](conf: SparkConf, entry: SparkConfigEntry[T]): T = {
    conf.get(entry)
  }

  def get[T](conf: SparkConf, entry: ConfigEntry[T]): T = {
    // Read through the entry itself, so that an entry which resolves more than its own key - e.g.
    // one falling back to a Spark conf - is honoured rather than short-circuited to its default.
    entry.readFrom(new SparkConfProvider(conf))
  }

  def get[T](conf: java.util.Map[String, String], entry: SparkConfigEntry[T]): T = {
    Option(conf.get(entry.key))
      .map(entry.valueConverter)
      .getOrElse(entry.defaultValue.getOrElse(None).asInstanceOf[T])
  }

  def get[T](conf: java.util.Map[String, String], entry: ConfigEntry[T]): T = {
    entry.readFrom(new JavaMapProvider(conf))
  }

  def set[T](conf: SparkConf, entry: SparkConfigEntry[T], value: T): SparkConf = {
    conf.set(entry, value)
  }

  def set[T](conf: SparkConf, entry: OptionalConfigEntry[T], value: T): SparkConf = {
    conf.set(entry, value)
  }

  def set[T](conf: SparkConf, entry: ConfigEntry[T], value: T): SparkConf = {
    value match {
      case Some(v) => conf.set(entry.key, v.toString)
      case None | null => conf.set(entry.key, null)
      case _ => conf.set(entry.key, value.toString)
    }
  }
}
