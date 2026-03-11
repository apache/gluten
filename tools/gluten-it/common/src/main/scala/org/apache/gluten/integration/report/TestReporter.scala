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
package org.apache.gluten.integration.report

import java.io.{ByteArrayOutputStream, OutputStream, PrintStream, PrintWriter}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import scala.collection.mutable

trait TestReporter {
  def addMetadata(key: String, value: String): Unit
  def rootAppender(): TestReporter.Appender
  def actionAppender(actionName: String): TestReporter.Appender
  def write(out: OutputStream): Unit
}

object TestReporter {
  trait Appender {
    def out: PrintStream
    def err: PrintStream
  }

  private class AppenderImpl extends Appender {
    val outStream = new ByteArrayOutputStream()
    val errStream = new ByteArrayOutputStream()

    override val out: PrintStream = new PrintStream(outStream)
    override val err: PrintStream = new PrintStream(errStream)
  }

  def create(): TestReporter = {
    new Impl()
  }

  private class Impl() extends TestReporter {
    private val rootAppenderName = "__ROOT__"
    private val metadataMap = mutable.LinkedHashMap[String, String]()
    private val appenderMap = mutable.LinkedHashMap[String, AppenderImpl]()

    override def addMetadata(key: String, value: String): Unit = {
      metadataMap += key -> value
    }

    override def rootAppender(): Appender = {
      appenderMap.getOrElseUpdate(rootAppenderName, new AppenderImpl)
    }

    override def actionAppender(actionName: String): Appender = {
      require(actionName != rootAppenderName)
      appenderMap.getOrElseUpdate(actionName, new AppenderImpl)
    }

    override def write(out: OutputStream): Unit = {
      val writer = new PrintWriter(out)

      def line(): Unit =
        writer.println("========================================")

      def subLine(): Unit =
        writer.println("----------------------------------------")

      def printStreamBlock(label: String, content: String, indent: String): Unit = {
        if (content.nonEmpty) {
          writer.println(s"$indent$label:")
          subLine()
          content.linesIterator.foreach(l => writer.println(s"$indent  $l"))
          writer.println()
        }
      }

      line()
      writer.println("              TEST REPORT               ")
      line()
      metadataMap.foreach {
        case (k, v) =>
          writer.println(s"$k : $v")
      }
      writer.println()

      // ---- ROOT (suite-level) ----
      appenderMap.get(rootAppenderName).foreach {
        root =>
          val stdout = root.outStream.toString("UTF-8").trim
          val stderr = root.errStream.toString("UTF-8").trim

          writer.println("SUITE OUTPUT")
          subLine()
          writer.println()

          printStreamBlock("STDOUT", stdout, "")
          printStreamBlock("STDERR", stderr, "")
      }

      // ---- ACTIONS ----
      val actions =
        appenderMap.iterator.filterNot(_._1 == rootAppenderName)

      if (actions.nonEmpty) {
        writer.println("ACTIONS")
        subLine()
        writer.println()

        actions.foreach {
          case (name, appender) =>
            val stdout = appender.outStream.toString("UTF-8").trim
            val stderr = appender.errStream.toString("UTF-8").trim

            writer.println(s"[ $name ]")
            writer.println()

            if (stdout.isEmpty && stderr.isEmpty) {
              writer.println("  (no output)")
              writer.println()
            } else {
              printStreamBlock("STDOUT", stdout, "  ")
              printStreamBlock("STDERR", stderr, "  ")
            }
        }
      }

      line()
      writer.flush()
    }
  }
}
