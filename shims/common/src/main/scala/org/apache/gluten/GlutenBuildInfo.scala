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
package org.apache.gluten

import java.util.Properties

import scala.util.Try

/** Since https://github.com/apache/gluten/pull/1973. */
object GlutenBuildInfo {
  private val buildFile = "gluten-build-info.properties"
  private val buildFileStream =
    Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)

  if (buildFileStream == null) {
    throw new RuntimeException(s"Can not load the core build file: $buildFile")
  }

  private val unknown = "<unknown>"
  private val props = new Properties()

  try {
    props.load(buildFileStream)
  } finally {
    Try(buildFileStream.close())
  }

  val VERSION: String = props.getProperty("gluten_version", unknown)
  val GCC_VERSION: String = props.getProperty("gcc_version", unknown)
  val JAVA_COMPILE_VERSION: String = props.getProperty("java_version", unknown)
  val SCALA_COMPILE_VERSION: String = props.getProperty("scala_version", unknown)
  val SPARK_COMPILE_VERSION: String = props.getProperty("spark_version", unknown)
  val HADOOP_COMPILE_VERSION: String = props.getProperty("hadoop_version", unknown)
  val BRANCH: String = props.getProperty("branch", unknown)
  val REVISION: String = props.getProperty("revision", unknown)
  val REVISION_TIME: String = props.getProperty("revision_time", unknown)
  val BUILD_DATE: String = props.getProperty("date", unknown)
  val REPO_URL: String = props.getProperty("url", unknown)
  val VELOX_BRANCH: String = props.getProperty("velox_branch", unknown)
  val VELOX_REVISION: String = props.getProperty("velox_revision", unknown)
  val VELOX_REVISION_TIME: String = props.getProperty("velox_revision_time", unknown)
  val CH_BRANCH: String = props.getProperty("ch_branch", unknown)
  val CH_COMMIT: String = props.getProperty("ch_commit", unknown)
}
