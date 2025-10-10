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

import org.apache.gluten.config.GlutenConfig.{GLUTEN_LOAD_LIB_FROM_JAR, GLUTEN_LOAD_LIB_OS, GLUTEN_LOAD_LIB_OS_VERSION}

import org.apache.spark.SparkConf

import org.scalatest.funsuite.AnyFunSuite

class SharedLibraryLoaderUtilsSuite extends AnyFunSuite {

  test("Load SharedLibraryLoader with SPI") {
    val sparkConf = new SparkConf()
      .set(GLUTEN_LOAD_LIB_FROM_JAR.key, "true")
      .set(GLUTEN_LOAD_LIB_OS_VERSION.key, "1.0")
      .set(GLUTEN_LOAD_LIB_OS.key, "My OS")

    SharedLibraryLoaderUtils.load(sparkConf, null)
    assert(MySharedLibraryLoader.LOADED.get())
  }
}
