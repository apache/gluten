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

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths

class AllVeloxConfiguration extends AnyFunSuite {
  private val glutenHome: String =
    AllGlutenConfiguration.getCodeSourceLocation(this.getClass).split("backends-velox")(0)
  private val markdown =
    Paths.get(glutenHome, "docs", "backends", "velox", "velox-configuration.md").toAbsolutePath

  test("Broadcast build input streaming defaults to disabled and forces a single build thread") {
    val conf = new SQLConf
    val veloxConf = new VeloxConfig(conf)
    val dataSize = veloxConf.veloxBroadcastHashTableBuildTargetBytes * 4
    assert(!veloxConf.broadcastBuildInputStreamingEnabled)
    assert(veloxConf.broadcastHashTableBuildThreads(0) == 1)
    assert(veloxConf.broadcastHashTableBuildThreads(dataSize) == 4)
    conf.setConfString(VeloxConfig.VELOX_BROADCAST_BUILD_INPUT_STREAMING_ENABLED.key, "true")
    assert(veloxConf.broadcastHashTableBuildThreads(0) == 1)
    assert(veloxConf.broadcastHashTableBuildThreads(dataSize) == 1)
    conf.setConfString(VeloxConfig.VELOX_BROADCAST_BUILD_INPUT_STREAMING_ENABLED.key, "false")
    assert(veloxConf.broadcastHashTableBuildThreads(dataSize) == 4)
  }

  test("Check velox backend configs") {
    val builder = MarkdownBuilder(getClass.getName)

    builder ++=
      s"""
         |---
         |layout: page
         |title: Configuration
         |nav_order: 16
         |---
         |
         |"""

    builder ++=
      s"""
         |## Gluten Velox backend configurations
         |
         |${AllGlutenConfiguration.configTypeLegend}
         |
         | Key | Type | Default | Description
         | --- | --- | --- | ---
         |"""

    VeloxConfig.allEntries
      .filter(_.isPublic)
      .filter(!_.isExperimental)
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(
            s"${entry.key}",
            AllGlutenConfiguration.configModifiability(entry.key),
            s"$dft",
            s"${entry.doc}")
            .mkString("|")
      }

    builder ++=
      s"""
         |## Gluten Velox backend *experimental* configurations
         |
         |${AllGlutenConfiguration.configTypeLegend}
         |
         | Key | Type | Default | Description
         | --- | --- | --- | ---
         |"""

    VeloxConfig.allEntries
      .filter(_.isPublic)
      .filter(_.isExperimental)
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(
            s"${entry.key}",
            AllGlutenConfiguration.configModifiability(entry.key),
            s"$dft",
            s"${entry.doc}")
            .mkString("|")
      }

    AllGlutenConfiguration.verifyOrRegenerateGoldenFile(
      markdown,
      builder.toMarkdown,
      "dev/gen-all-config-docs.sh")
  }

  test("Velox resize batches copyRanges is enabled by default") {
    assert(
      VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_COPY_RANGES_ENABLED.defaultValue.contains(true))
  }
}
