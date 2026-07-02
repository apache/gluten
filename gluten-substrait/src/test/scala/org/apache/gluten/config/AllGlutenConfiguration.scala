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

import org.scalactic.Prettifier
import org.scalactic.source.Position
import org.scalatest.Assertions._
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * End-to-end test cases for configuration doc file The golden result file is
 * "docs/Configuration.md".
 *
 * This mechanism for generating configuration files is borrowed from Apache kyuubi project.
 *
 * To run the entire test suite:
 * {{{
 *   GLUTEN_UPDATE=0 dev/gen-all-config-docs.sh
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   dev/gen-all-config-docs.sh
 * }}}
 */
class AllGlutenConfiguration extends AnyFunSuite {
  private val glutenHome: String =
    AllGlutenConfiguration.getCodeSourceLocation(this.getClass).split("gluten-substrait")(0)
  private val markdown = Paths.get(glutenHome, "docs", "Configuration.md").toAbsolutePath

  test("Check gluten configs") {
    val builder = MarkdownBuilder(getClass.getName)

    builder ++=
      s"""
         |---
         |layout: page
         |title: Configuration
         |nav_order: 15
         |---
         |
         |"""

    //
    builder ++=
      s"""
         |## Spark base configurations for Gluten plugin
         |
         | Key | Modifiability | Recommend Setting | Description
         | --- | --- | --- | ---
         |"""

    // scalastyle:off
    builder += Seq(
      "spark.plugins",
      AllGlutenConfiguration.configModifiability("spark.plugins"),
      "org.apache.gluten.GlutenPlugin",
      "To load Gluten's components by Spark's plug-in loader."
    ).mkString("|")
    builder += Seq(
      "spark.memory.offHeap.enabled",
      AllGlutenConfiguration.configModifiability("spark.memory.offHeap.enabled"),
      "true",
      "Gluten use off-heap memory for certain operations."
    ).mkString("|")
    builder += Seq(
      "spark.memory.offHeap.size",
      AllGlutenConfiguration.configModifiability("spark.memory.offHeap.size"),
      "30G",
      "The absolute amount of memory which can be used for off-heap allocation, in bytes unless otherwise specified.<br /> Note: Gluten Plugin will leverage this setting to allocate memory space for native usage even offHeap is disabled. <br /> The value is based on your system and it is recommended to set it larger if you are facing Out of Memory issue in Gluten Plugin."
    ).mkString("|")
    builder += Seq(
      "spark.shuffle.manager",
      AllGlutenConfiguration.configModifiability("spark.shuffle.manager"),
      "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
      "To turn on Gluten Columnar Shuffle Plugin."
    ).mkString("|")
    builder += Seq(
      "spark.driver.extraClassPath",
      AllGlutenConfiguration.configModifiability("spark.driver.extraClassPath"),
      "/path/to/gluten_jar_file",
      "Gluten Plugin jar file to prepend to the classpath of the driver."
    ).mkString("|")
    builder += Seq(
      "spark.executor.extraClassPath",
      AllGlutenConfiguration.configModifiability("spark.executor.extraClassPath"),
      "/path/to/gluten_jar_file",
      "Gluten Plugin jar file to prepend to the classpath of executors."
    ).mkString("|")
    // scalastyle:on

    builder ++=
      s"""
         |## Gluten configurations
         |
         | Key | Modifiability | Default | Description
         | --- | --- | --- | ---
         |"""

    val allEntries = GlutenConfig.allEntries ++ GlutenCoreConfig.allEntries

    allEntries
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
         |## Gluten *experimental* configurations
         |
         | Key | Modifiability | Default | Description
         | --- | --- | --- | ---
         |"""

    allEntries
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
}

object AllGlutenConfiguration {
  private val Anchor = Character.toString(0x2693)
  private val CounterclockwiseArrows = new String(Character.toChars(0x1f504))

  val staticConfigLabel: String = s"$Anchor Static"
  val dynamicConfigLabel: String = s"$CounterclockwiseArrows Dynamic"

  def configModifiability(key: String): String = {
    if (SQLConf.get.isModifiable(key)) {
      dynamicConfigLabel
    } else {
      staticConfigLabel
    }
  }

  def isRegenerateGoldenFiles: Boolean = sys.env.get("GLUTEN_UPDATE").contains("1")

  def getCodeSourceLocation[T](clazz: Class[T]): String = {
    clazz.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
  }

  /**
   * Verify the golden file content when GLUTEN_UPDATE env is not equals to 1, or regenerate the
   * golden file content when GLUTEN_UPDATE env is equals to 1.
   *
   * @param path
   *   the path of file
   * @param lines
   *   the expected lines for validation or regeneration
   * @param regenScript
   *   the script for regeneration, used for hints when verification failed
   */
  def verifyOrRegenerateGoldenFile(
      path: Path,
      lines: Iterable[String],
      regenScript: String): Unit = {
    if (isRegenerateGoldenFiles) {
      Files.write(
        path,
        lines.asJava,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      assertFileContent(path, lines.toSeq, regenScript)
    }
  }

  /**
   * Assert the file content is equal to the expected lines. If not, throws assertion error with the
   * given regeneration hint.
   * @param expectedLines
   *   expected lines
   * @param path
   *   source file path
   * @param regenScript
   *   regeneration script
   */
  def assertFileContent(
      path: Path,
      expectedLines: Seq[String],
      regenScript: String)(implicit
      prettifier: Prettifier,
      pos: Position): Unit = {
    val fileSource = Source.fromFile(path.toUri, StandardCharsets.UTF_8.name())
    try {
      val fileLines = fileSource.getLines().toSeq
      val regenerationHint = s"The file ($path) is out of date. " + {
        if (regenScript != null && regenScript.nonEmpty) {
          s" Please regenerate it by running `${regenScript.stripMargin}`. "
        } else ""
      }
      val fileLineCount = fileLines.length
      withClue(s"Line number is not expected. $regenerationHint") {
        assertResult(expectedLines.size)(fileLineCount)(prettifier, pos)
      }
      fileLines.zipWithIndex
        .zip(expectedLines)
        .foreach {
          case ((lineInFile, lineIndex), expectedLine) =>
            val lineNum = lineIndex + 1
            withClue(s"Line $lineNum is not expected. $regenerationHint") {
              assertResult(expectedLine)(lineInFile)(prettifier, pos)
            }
        }
    } finally {
      fileSource.close()
    }
  }
}
