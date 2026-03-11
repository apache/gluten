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
package org.apache.gluten.softaffinity

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.hash.ConsistentHash
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.softaffinity.strategy.{ConsistentHashSoftAffinityStrategy, ExecutorNode}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.datasources.FilePartition

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.util.Random

abstract class AffinityManager extends LogLevelUtil with Logging {

  private val resourceRWLock = new ReentrantReadWriteLock(true)

  lazy val softAffinityReplicationNum: Int =
    GlutenConfig.GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM.defaultValue.get

  lazy val minOnTargetHosts: Int =
    GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS.defaultValue.get

  lazy val usingSoftAffinity: Boolean = true

  lazy val detectDuplicateReading: Boolean = true

  lazy val duplicateReadingMaxCacheItems: Int =
    GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_MAX_CACHE_ITEMS.defaultValue.get

  // (execId, host) list
  private val idForExecutors = new mutable.ListBuffer[(String, String)]()
  var sortedIdForExecutors = new mutable.ListBuffer[(String, String)]()
  // host list
  val nodesExecutorsMap = new mutable.HashMap[String, mutable.HashSet[String]]()

  protected val totalRegisteredExecutors = new AtomicInteger(0)

  // rdd id -> patition id, file path, start, length
  val rddPartitionInfoMap: LoadingCache[Integer, Array[(Int, String, Long, Long)]] =
    CacheBuilder
      .newBuilder()
      .maximumSize(duplicateReadingMaxCacheItems)
      .build(new CacheLoader[Integer, Array[(Int, String, Long, Long)]] {
        override def load(id: Integer): Array[(Int, String, Long, Long)] = {
          Array.empty[(Int, String, Long, Long)]
        }
      })
  // stage id -> execution id + rdd ids: job start / execution end
  val stageInfoMap: LoadingCache[Integer, Array[Int]] =
    CacheBuilder
      .newBuilder()
      .maximumSize(duplicateReadingMaxCacheItems)
      .build(new CacheLoader[Integer, Array[Int]] {
        override def load(id: Integer): Array[Int] = {
          Array.empty[Int]
        }
      })
  // final result: partition composed key("path1_start_length,path2_start_length") --> array_host
  val duplicateReadingInfos: LoadingCache[String, Array[(String, String)]] =
    CacheBuilder
      .newBuilder()
      .maximumSize(duplicateReadingMaxCacheItems)
      .build(new CacheLoader[String, Array[(String, String)]] {
        override def load(name: String): Array[(String, String)] = {
          Array.empty[(String, String)]
        }
      })

  private val hashRing = new ConsistentHash[ExecutorNode](softAffinityReplicationNum)
  private val softAffinityStrategy = new ConsistentHashSoftAffinityStrategy(hashRing)

  private val rand = new Random(System.currentTimeMillis)

  def totalExecutors(): Int = totalRegisteredExecutors.intValue()

  def handleExecutorAdded(execHostId: (String, String)): Unit = {
    resourceRWLock.writeLock().lock()
    try {
      // first, check whether the execId exists
      if (
        !idForExecutors.exists(
          exec => {
            exec._1.equals(execHostId._1)
          })
      ) {
        val executorsSet =
          nodesExecutorsMap.getOrElseUpdate(execHostId._2, new mutable.HashSet[String]())
        executorsSet.add(execHostId._1)
        idForExecutors += execHostId
        sortedIdForExecutors = idForExecutors.sortBy(_._2)
        hashRing.addNode(ExecutorNode(execHostId._1, execHostId._2))
        totalRegisteredExecutors.addAndGet(1)
      }
      logOnLevel(
        GlutenConfig.get.softAffinityLogLevel,
        s"After adding executor ${execHostId._1} on host ${execHostId._2}, " +
          s"idForExecutors is ${idForExecutors.mkString(",")}, " +
          s"sortedIdForExecutors is ${sortedIdForExecutors.mkString(",")}, " +
          s"nodesExecutorsMap is ${nodesExecutorsMap.keySet.mkString(",")}, " +
          s"actual executors count is ${totalRegisteredExecutors.intValue()}."
      )
    } finally {
      resourceRWLock.writeLock().unlock()
    }
  }

  def handleExecutorRemoved(execId: String): Unit = {
    resourceRWLock.writeLock().lock()
    try {
      val execIdx = idForExecutors.indexWhere(
        execHost => {
          execHost._1.equals(execId)
        })
      if (execIdx != -1) {
        val findedExecId = idForExecutors(execIdx)
        idForExecutors.remove(execIdx)
        val nodeExecs = nodesExecutorsMap(findedExecId._2)
        nodeExecs -= findedExecId._1
        if (nodeExecs.isEmpty) {
          // there is no executor on this host, remove
          nodesExecutorsMap.remove(findedExecId._2)
        }
        sortedIdForExecutors = idForExecutors.sortBy(_._2)
        hashRing.removeNode(ExecutorNode(execId, findedExecId._2))
        totalRegisteredExecutors.addAndGet(-1)
      }
      logOnLevel(
        GlutenConfig.get.softAffinityLogLevel,
        s"After removing executor $execId, " +
          s"idForExecutors is ${idForExecutors.mkString(",")}, " +
          s"sortedIdForExecutors is ${sortedIdForExecutors.mkString(",")}, " +
          s"nodesExecutorsMap is ${nodesExecutorsMap.keySet.mkString(",")}, " +
          s"actual executors count is ${totalRegisteredExecutors.intValue()}."
      )
    } finally {
      resourceRWLock.writeLock().unlock()
    }
  }

  def updateStageMap(event: SparkListenerStageSubmitted): Unit = {
    if (!detectDuplicateReading) {
      return
    }
    val info = event.stageInfo
    val rddIds = info.rddInfos.map(_.id).toArray
    stageInfoMap.put(info.stageId, rddIds)
  }

  def updateHostMap(event: SparkListenerTaskEnd): Unit = {
    if (!detectDuplicateReading) {
      return
    }
    event.reason match {
      case org.apache.spark.Success =>
        val stageId = event.stageId
        val rddInfo = stageInfoMap.getIfPresent(stageId)
        if (rddInfo != null) {
          rddInfo.foreach {
            rddId =>
              val partitions = rddPartitionInfoMap.getIfPresent(rddId)
              if (partitions != null) {
                val key = partitions
                  .filter(p => p._1 == event.taskInfo.partitionId)
                  .map(pInfo => s"${pInfo._2}_${pInfo._3}_${pInfo._4}")
                  .sortBy(p => p)
                  .mkString(",")
                val value = Array(((event.taskInfo.executorId, event.taskInfo.host)))
                val originalValues = duplicateReadingInfos.get(key)
                val values = if (originalValues.contains(value(0))) {
                  originalValues
                } else {
                  (originalValues ++ value)
                }
                logOnLevel(
                  GlutenConfig.get.softAffinityLogLevel,
                  s"update host for $key: ${values.mkString(",")}")
                duplicateReadingInfos.put(key, values)
              }
          }
        }
      case _ =>
    }
  }

  def cleanMiddleStatusMap(event: SparkListenerStageCompleted): Unit = {
    clearPartitionMap(event.stageInfo.rddInfos.map(_.id))
    clearStageMap(event.stageInfo.stageId)
  }

  def clearPartitionMap(rddIds: Seq[Int]): Unit = {
    rddIds.foreach(id => rddPartitionInfoMap.invalidate(id))
  }

  def clearStageMap(id: Int): Unit = {
    stageInfoMap.invalidate(id)
  }

  def checkTargetHosts(hosts: Array[String]): Boolean = {
    resourceRWLock.readLock().lock()
    try {
      if (hosts.length < 1) {
        // there is no host locality
        false
      } else if (nodesExecutorsMap.size < 1) {
        true
      } else {
        // when the replication num of hdfs is less than 'minOnTargetHosts'
        val minHostsNum = Math.min(minOnTargetHosts, hosts.length)
        // there are how many the same hosts
        nodesExecutorsMap.keys.toArray.intersect(hosts).length >= minHostsNum
      }
    } finally {
      resourceRWLock.readLock().unlock()
    }
  }

  def askExecutors(file: String): Array[(String, String)] = {
    resourceRWLock.readLock().lock()
    try {
      if (nodesExecutorsMap.size < 1) {
        Array.empty
      } else {
        softAffinityStrategy.allocateExecs(file, softAffinityReplicationNum)
      }
    } finally {
      resourceRWLock.readLock().unlock()
    }
  }

  def askExecutors(f: FilePartition): Array[(String, String)] = {
    resourceRWLock.readLock().lock()
    try {
      if (sortedIdForExecutors.size < 1) {
        Array.empty
      } else {
        val result = getDuplicateReadingLocation(f)
        result.filter(r => sortedIdForExecutors.exists(s => s._1 == r._1)).toArray
      }
    } finally {
      resourceRWLock.readLock().unlock()
    }
  }

  def getDuplicateReadingLocation(f: FilePartition): Seq[(String, String)] = {
    val hosts = mutable.ListBuffer.empty[(String, String)]
    val key = f.files
      .map(file => s"${file.filePath}_${file.start}_${file.length}")
      .sortBy(p => p)
      .mkString(",")
    val host = duplicateReadingInfos.get(key)
    if (!host.isEmpty) {
      hosts ++= host
    }

    if (!hosts.isEmpty) {
      rand.shuffle(hosts)
      logOnLevel(
        GlutenConfig.get.softAffinityLogLevel,
        s"get host for $f: ${hosts.distinct.mkString(",")}")
    }
    hosts.distinct.toSeq
  }

  def updatePartitionMap(f: FilePartition, rddId: Int): Unit = {
    if (!detectDuplicateReading) {
      return
    }

    val paths =
      f.files.map(file => (f.index, file.filePath.toString, file.start, file.length)).toArray
    val key = rddId
    var values = rddPartitionInfoMap.getIfPresent(key)
    values = if (values != null) {
      values ++ paths
    } else {
      paths
    }
    rddPartitionInfoMap.put(key, values)
  }
}

object SoftAffinityManager extends AffinityManager {
  override lazy val usingSoftAffinity: Boolean = SparkEnv.get.conf.getBoolean(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED.key,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED.defaultValue.get
  )

  override lazy val softAffinityReplicationNum: Int = SparkEnv.get.conf.getInt(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM.key,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM.defaultValue.get)

  override lazy val minOnTargetHosts: Int = SparkEnv.get.conf.getInt(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS.key,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS.defaultValue.get
  )

  override lazy val detectDuplicateReading: Boolean = SparkEnv.get.conf.getBoolean(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_DETECT_ENABLED.key,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_DETECT_ENABLED.defaultValue.get
  )

  override lazy val duplicateReadingMaxCacheItems: Int = SparkEnv.get.conf.getInt(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_MAX_CACHE_ITEMS.key,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_MAX_CACHE_ITEMS.defaultValue.get
  )
}
