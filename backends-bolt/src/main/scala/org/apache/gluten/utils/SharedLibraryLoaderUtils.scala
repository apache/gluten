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
package org.apache.gluten.utils

import org.apache.gluten.config.GlutenConfig._
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.jni.BoltJniLibLoader
import org.apache.gluten.spi.SharedLibraryLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SparkConfigUtil._

import java.io.FileInputStream
import java.util.{Properties, ServiceLoader}

import scala.collection.JavaConverters._

object SharedLibraryLoaderUtils {
  private def isMacOS: Boolean = {
    val osName = System.getProperty("os.name")
    osName.startsWith("Mac OS X") || osName.startsWith("macOS")
  }

  private def stripQuotes(s: String): String = {
    if (s == null) {
      null
    } else {
      s.stripPrefix("\"").stripSuffix("\"")
    }
  }

  def load(conf: SparkConf, jni: BoltJniLibLoader): Unit = {
    val shouldLoad = conf.get(GLUTEN_LOAD_LIB_FROM_JAR)
    if (!shouldLoad) {
      return
    }

    val (osName, osVersion) = conf.get(GLUTEN_LOAD_LIB_OS) match {
      case Some(os) =>
        (
          os,
          conf
            .get(GLUTEN_LOAD_LIB_OS_VERSION)
            .getOrElse(
              throw new GlutenException(
                s"${GLUTEN_LOAD_LIB_OS_VERSION.key} must be specified when specifies the " +
                  s"${GLUTEN_LOAD_LIB_OS.key}")))
      case None if isMacOS =>
        (System.getProperty("os.name"), System.getProperty("os.version"))
      case None =>
        val props = new Properties()
        val in = new FileInputStream("/etc/os-release")
        props.load(in)
        (stripQuotes(props.getProperty("NAME")), stripQuotes(props.getProperty("VERSION")))
    }

    val loaders = ServiceLoader
      .load(classOf[SharedLibraryLoader])
      .asScala
      .filter(loader => loader.accepts(osName, osVersion))
      .toSeq

    if (loaders.isEmpty) {
      throw new GlutenException(
        s"Cannot find SharedLibraryLoader for $osName $osVersion, please" +
          "check whether your custom SharedLibraryLoader is implemented and loadable.")
    }

    if (loaders.size > 1) {
      throw new GlutenException(
        s"Found more than one SharedLibraryLoader for $osName $osVersion:" +
          s" ${loaders.mkString(",")}, " +
          "please check whether your custom SharedLibraryLoader is implemented correctly.")
    }

    val loader = loaders.head
    try {
      loader.loadLib(jni)
    } catch {
      case e: Throwable =>
        throw new GlutenException(
          s"Failed to load shared libraries for $osName $osVersion using $loader",
          e)
    }
  }
}
