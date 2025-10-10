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

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths

class AllBoltConfiguration extends AnyFunSuite {
  private val glutenHome: String =
    AllGlutenConfiguration.getCodeSourceLocation(this.getClass).split("backends-bolt")(0)
  private val markdown = Paths.get(glutenHome, "docs", "bolt-configuration.md").toAbsolutePath

  test("Check bolt backend configs") {
    val builder = MarkdownBuilder()

    builder ++=
      s"""
         |---
         |layout: page
         |title: Configuration
         |nav_order: 16
         |---
         |
         |"""

    builder.generationHint(getClass.getName)

    builder ++=
      s"""
         |## Gluten Bolt backend configurations
         |
         | Key | Default | Description
         | --- | --- | ---
         |"""

    BoltConfig.allEntries
      .filter(_.isPublic)
      .filter(!_.isExperimental)
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(s"${entry.key}", s"$dft", s"${entry.doc}")
            .mkString("|")
      }

    builder ++=
      s"""
         |## Gluten Bolt backend *experimental* configurations
         |
         | Key | Default | Description
         | --- | --- | ---
         |"""

    BoltConfig.allEntries
      .filter(_.isPublic)
      .filter(_.isExperimental)
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(s"${entry.key}", s"$dft", s"${entry.doc}")
            .mkString("|")
      }

    AllGlutenConfiguration.verifyOrRegenerateGoldenFile(
      markdown,
      builder.toMarkdown,
      "dev/gen_all_config_docs.sh")
  }
}
