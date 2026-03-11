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
package org.apache.gluten.memory

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.listener.ReservationListeners
import org.apache.gluten.memory.memtarget.{KnownNameAndStats, MemoryTarget, Spiller, Spillers}
import org.apache.gluten.proto.MemoryUsageStats
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.sql.internal.{GlutenConfigUtil, SQLConf}
import org.apache.spark.task.{TaskResource, TaskResources}

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable

trait NativeMemoryManager {
  def addSpiller(spiller: Spiller): Unit
  def hold(): Unit
  def getHandle(): Long
}

object NativeMemoryManager {
  private class Impl(backendName: String, name: String)
    extends NativeMemoryManager
    with TaskResource {
    private val LOGGER = LoggerFactory.getLogger(classOf[NativeMemoryManager])
    private val spillers = Spillers.appendable()
    private val mutableStats: mutable.Map[String, MemoryUsageStatsBuilder] = mutable.Map()
    private val rl = ReservationListeners.create(name, spillers, mutableStats.asJava)
    private val handle = NativeMemoryManagerJniWrapper.create(
      backendName,
      rl,
      ConfigUtil.serialize(
        GlutenConfig
          .getNativeSessionConf(backendName, GlutenConfigUtil.parseConfig(SQLConf.get.getAllConfs))
          .asJava)
    )
    spillers.append(new Spiller() {
      override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = phase match {
        case Spiller.Phase.SHRINK => // Only respond for shrinking.
          val shrunk = NativeMemoryManagerJniWrapper.shrink(handle, size)
          LOGGER.info(s"NativeMemoryManager: Shrunk $shrunk / $size bytes of data.")
          shrunk
        case _ => 0L
      }
    })
    mutableStats += "single" -> new MemoryUsageStatsBuilder {
      override def toStats: MemoryUsageStats = collectUsage()
    }

    private def collectUsage() = {
      MemoryUsageStats.parseFrom(NativeMemoryManagerJniWrapper.collectUsage(handle))
    }

    private val released: AtomicBoolean = new AtomicBoolean(false)

    override def addSpiller(spiller: Spiller): Unit = spillers.append(spiller)
    override def hold(): Unit = NativeMemoryManagerJniWrapper.hold(handle)
    override def getHandle(): Long = handle
    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Memory manager instance already released: $handle, ${resourceName()}, ${priority()}")
      }

      def dump(): String = {
        SparkMemoryUtil.prettyPrintStats(
          s"[${resourceName()}]",
          new KnownNameAndStats() {
            override def name: String = resourceName()
            override def stats: MemoryUsageStats = collectUsage()
          })
      }

      if (LOGGER.isDebugEnabled) {
        LOGGER.debug("About to release memory manager, " + dump())
      }

      NativeMemoryManagerJniWrapper.release(handle)

      if (rl.getUsedBytes != 0) {
        LOGGER.warn(
          String.format(
            "%s Reservation listener %s still reserved non-zero bytes, which may cause memory" +
              " leak, size: %s.",
            name,
            rl.toString,
            SparkMemoryUtil.bytesToString(rl.getUsedBytes)
          ))
      }
    }
    override def priority(): Int = {
      // Memory managers should be released after all runtimes are released.
      // So set the priority lower than runtime resources.
      10
    }
    override def resourceName(): String = "nmm"
  }

  def apply(backendName: String, name: String): NativeMemoryManager = {
    TaskResources.addAnonymousResource(new Impl(backendName, name))
  }
}
