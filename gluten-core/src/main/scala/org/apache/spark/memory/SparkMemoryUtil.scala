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
package org.apache.spark.memory

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.memory.memtarget._
import org.apache.gluten.memory.memtarget.spark.{RegularMemoryConsumer, TreeMemoryConsumer}
import org.apache.gluten.proto.MemoryUsageStats

import org.apache.spark.SparkEnv
import org.apache.spark.util.Utils

import com.google.common.base.Preconditions
import org.apache.commons.lang3.StringUtils

import java.util

import scala.annotation.nowarn
import scala.collection.JavaConverters._

object SparkMemoryUtil {
  private val mmClazz = classOf[MemoryManager]
  private val smpField = mmClazz.getDeclaredField("offHeapStorageMemoryPool")
  private val empField = mmClazz.getDeclaredField("offHeapExecutionMemoryPool")
  private val onHeapSmpField = mmClazz.getDeclaredField("onHeapStorageMemoryPool")
  private val onHeapEmpField = mmClazz.getDeclaredField("onHeapExecutionMemoryPool")
  Seq(smpField, empField, onHeapSmpField, onHeapEmpField).foreach(_.setAccessible(true))

  private val tmmClazz = classOf[TaskMemoryManager]
  private val consumersField = tmmClazz.getDeclaredField("consumers")
  private val taskIdField = tmmClazz.getDeclaredField("taskAttemptId")
  consumersField.setAccessible(true)
  taskIdField.setAccessible(true)

  def bytesToString(size: Long): String = {
    Utils.bytesToString(size)
  }

  // We assume storage memory can be fully transferred to execution memory so far
  def getCurrentAvailableOffHeapMemory: Long = {
    val env = SparkEnv.get
    val mm = env.memoryManager
    // With dynamic off-heap sizing enabled, Gluten's global reservations are
    // charged to the ON-heap pools (see GlobalOffHeapMemoryTarget), so the
    // available figure must be read from the same pools to stay meaningful;
    // reading the off-heap pools would ignore every reservation.
    val dynamicSizingEnabled =
      env.conf.getBoolean(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, false)
    val (smp, emp) =
      if (dynamicSizingEnabled) {
        (
          onHeapSmpField.get(mm).asInstanceOf[StorageMemoryPool],
          onHeapEmpField.get(mm).asInstanceOf[ExecutionMemoryPool])
      } else {
        (
          smpField.get(mm).asInstanceOf[StorageMemoryPool],
          empField.get(mm).asInstanceOf[ExecutionMemoryPool])
      }
    smp.memoryFree + emp.memoryFree
  }

  def dumpMemoryManagerStats(tmm: TaskMemoryManager): String = {
    val stats = tmm.synchronized {
      val consumers = consumersField.get(tmm).asInstanceOf[util.HashSet[MemoryConsumer]]

      // create stats map
      val statsMap = new util.HashMap[String, MemoryUsageStats]()
      consumers.asScala.foreach {
        case mt: KnownNameAndStats =>
          statsMap.put(mt.name(), mt.stats())
        case mc =>
          statsMap.put(
            mc.toString,
            MemoryUsageStats
              .newBuilder()
              .setCurrent(mc.getUsed)
              .setPeak(-1L)
              .build())
      }
      Preconditions.checkState(statsMap.size() == consumers.size())

      // add root
      new KnownNameAndStats {
        override def name(): String = s"Task.${taskIdField.get(tmm)}"

        override def stats(): MemoryUsageStats = MemoryUsageStats
          .newBuilder()
          .setCurrent(tmm.getMemoryConsumptionForThisTask)
          .setPeak(-1L)
          .putAllChildren(statsMap)
          .build()
      }
    }

    prettyPrintStats("Memory consumer stats: ", stats)
  }

  def dumpMemoryTargetStats(target: MemoryTarget): String = {
    target.accept(new MemoryTargetVisitor[String] {
      override def visit(overAcquire: OverAcquire): String = {
        overAcquire.getTarget.accept(this)
      }

      @nowarn
      override def visit(regularMemoryConsumer: RegularMemoryConsumer): String = {
        collectFromTaskMemoryManager(regularMemoryConsumer.getTaskMemoryManager)
      }

      override def visit(throwOnOomMemoryTarget: ThrowOnOomMemoryTarget): String = {
        throwOnOomMemoryTarget.target().accept(this)
      }

      override def visit(treeMemoryConsumer: TreeMemoryConsumer): String = {
        collectFromTaskMemoryManager(treeMemoryConsumer.getTaskMemoryManager)
      }

      override def visit(node: TreeMemoryConsumer.Node): String = {
        node.parent().accept(this) // walk up to find the one bound with task memory manager
      }

      private def collectFromTaskMemoryManager(tmm: TaskMemoryManager): String = {
        dumpMemoryManagerStats(tmm)
      }

      override def visit(loggingMemoryTarget: LoggingMemoryTarget): String = {
        loggingMemoryTarget.delegated().accept(this)
      }

      override def visit(noopMemoryTarget: NoopMemoryTarget): String = {
        prettyPrintStats("No-op memory target stats: ", noopMemoryTarget)
      }

      override def visit(
          dynamicOffHeapSizingMemoryTarget: DynamicOffHeapSizingMemoryTarget): String = {
        prettyPrintStats(
          "Dynamic off-heap sizing memory target stats: ",
          dynamicOffHeapSizingMemoryTarget)
        dynamicOffHeapSizingMemoryTarget.target().accept(this)
      }

      override def visit(retryOnOomMemoryTarget: RetryOnOomMemoryTarget): String = {
        retryOnOomMemoryTarget.target().accept(this)
      }

      override def visit(globalOffHeapMemoryTarget: GlobalOffHeapMemoryTarget): String = {
        prettyPrintStats("Global off-heap target stats: ", globalOffHeapMemoryTarget)
      }
    })
  }

  def prettyPrintStats(title: String, stats: KnownNameAndStats): String = {
    def asPrintable(name: String, mus: MemoryUsageStats): PrintableMemoryUsageStats = {
      def sortStats(stats: Seq[PrintableMemoryUsageStats]) = {
        stats.sortBy(_.used.getOrElse(Long.MinValue))(Ordering.Long.reverse)
      }

      PrintableMemoryUsageStats(
        name,
        Some(mus.getCurrent),
        mus.getPeak match {
          case -1L => None
          case v => Some(v)
        },
        sortStats(
          mus.getChildrenMap
            .entrySet()
            .asScala
            .toList
            .map(entry => asPrintable(entry.getKey, entry.getValue)))
      )
    }

    prettyPrintToString(title, asPrintable(stats.name(), stats.stats()))
  }

  private def prettyPrintToString(title: String, stats: PrintableMemoryUsageStats): String = {

    def getBytes(bytes: Option[Long]): String = {
      bytes.map(Utils.bytesToString).getOrElse("N/A")
    }

    def getFullName(name: String, prefix: String): String = {
      "%s%s:".format(prefix, name)
    }

    val sb = new StringBuilder()
    sb.append(title)

    // determine padding widths
    var nameWidth = 0
    var usedWidth = 0
    var peakWidth = 0
    def addPaddingSingleLevel(stats: PrintableMemoryUsageStats, extraWidth: Integer): Unit = {
      nameWidth = Math.max(nameWidth, getFullName(stats.name, "").length + extraWidth)
      usedWidth = Math.max(usedWidth, getBytes(stats.used).length)
      peakWidth = Math.max(peakWidth, getBytes(stats.peak).length)
      stats.children.foreach(addPaddingSingleLevel(_, extraWidth + 3)) // e.g. "\- "
    }
    addPaddingSingleLevel(stats, 1) // take the leading '\t' into account

    // print
    def printSingleLevel(
        stats: PrintableMemoryUsageStats,
        treePrefix: String,
        treeChildrenPrefix: String): Unit = {
      sb.append(System.lineSeparator())

      val name = getFullName(stats.name, treePrefix)
      sb.append(
        s"%s Current used bytes: %s, peak bytes: %s"
          .format(
            StringUtils.rightPad(name, nameWidth, ' '),
            StringUtils.leftPad(String.valueOf(getBytes(stats.used)), usedWidth, ' '),
            StringUtils.leftPad(String.valueOf(getBytes(stats.peak)), peakWidth, ' ')
          ))

      stats.children.zipWithIndex.foreach {
        case (child, i) =>
          if (i != stats.children.size - 1) {
            printSingleLevel(child, treeChildrenPrefix + "+- ", treeChildrenPrefix + "|  ")
          } else {
            printSingleLevel(child, treeChildrenPrefix + "\\- ", treeChildrenPrefix + "   ")
          }
      }
    }

    printSingleLevel(
      stats,
      "\t",
      "\t"
    ) // top level is indented with one tab (align with exception stack trace)

    // return
    sb.toString()
  }

  private case class PrintableMemoryUsageStats(
      name: String,
      used: Option[Long],
      peak: Option[Long],
      children: Iterable[PrintableMemoryUsageStats])
}
