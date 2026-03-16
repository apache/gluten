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
package org.apache.spark.rpc

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.GlutenRpcMessages._

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}

import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

/**
 * The gluten driver endpoint is responsible for communicating with the executor. Executor will
 * register with the driver when it starts.
 */
class GlutenDriverEndpoint extends IsolatedRpcEndpoint with Logging {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  protected val totalRegisteredExecutors = new AtomicInteger(0)

  private val driverEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpoint(GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME, this)

  // TODO(yuan): get thread cnt from spark context
  override def threadCount(): Int = 1
  override def receive: PartialFunction[Any, Unit] = {
    case GlutenOnExecutionStart(executionId) =>
      if (executionId == null) {
        logWarning(s"Execution Id is null. Resources maybe not clean after execution end.")
      }

    case GlutenOnExecutionEnd(executionId) =>
      logWarning(s"Execution Id is $executionId end.")

      GlutenDriverEndpoint.executionResourceRelation.invalidate(executionId)

    case GlutenExecutorRemoved(executorId) =>
      GlutenDriverEndpoint.executorDataMap.remove(executorId)
      totalRegisteredExecutors.addAndGet(-1)
      logTrace(s"Executor endpoint ref $executorId is removed.")

    case e =>
      logError(s"Received unexpected message. $e")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case GlutenRegisterExecutor(executorId, executorRef) =>
      if (GlutenDriverEndpoint.executorDataMap.contains(executorId)) {
        context.sendFailure(new IllegalStateException(s"Duplicate executor ID: $executorId"))
      } else {
        // If the executor's rpc env is not listening for incoming connections, `hostPort`
        // will be null, and the client connection should be used to contact the executor.
        val executorAddress = if (executorRef.address != null) {
          executorRef.address
        } else {
          context.senderAddress
        }
        logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")

        totalRegisteredExecutors.addAndGet(1)
        val data = new ExecutorData(executorRef)
        // This must be synchronized because variables mutated
        // in this block are read when requesting executors
        GlutenDriverEndpoint.this.synchronized {
          GlutenDriverEndpoint.executorDataMap.put(executorId, data)
        }
        logTrace(s"Executor size ${GlutenDriverEndpoint.executorDataMap.size()}")
        // Note: some tests expect the reply to come after we put the executor in the map
        context.reply(true)
      }

  }

  override def onStart(): Unit = {
    logInfo(s"Initialized GlutenDriverEndpoint, address: ${driverEndpoint.address.toString()}.")
  }
}

object GlutenDriverEndpoint extends Logging with RemovalListener[String, util.Set[String]] {
  private lazy val executionResourceExpiredTime = SparkEnv.get.conf.getLong(
    GlutenConfig.GLUTEN_RESOURCE_RELATION_EXPIRED_TIME.key,
    GlutenConfig.GLUTEN_RESOURCE_RELATION_EXPIRED_TIME.defaultValue.get
  )

  var glutenDriverEndpointRef: RpcEndpointRef = _

  // keep executorRef on memory
  val executorDataMap = new ConcurrentHashMap[String, ExecutorData]

  // If spark.scheduler.listenerbus.eventqueue.capacity is set too small,
  //   the listener may lose messages.
  // We set a maximum expiration time of 1 day by default
  // key: executionId, value: resourceIds
  private val executionResourceRelation: Cache[String, util.Set[String]] =
    Caffeine.newBuilder
      .expireAfterAccess(executionResourceExpiredTime, TimeUnit.SECONDS)
      .removalListener(this)
      .build[String, util.Set[String]]()

  def collectResources(executionId: String, resourceId: String): Unit = {
    val resources = executionResourceRelation
      .get(executionId, (_: String) => new util.HashSet[String]())
    resources.add(resourceId)
  }

  def invalidateResourceRelation(executionId: String): Unit = {
    executionResourceRelation.invalidate(executionId)
  }

  override def onRemoval(key: String, value: util.Set[String], cause: RemovalCause): Unit = {
    executorDataMap.forEach(
      (_, executor) => executor.executorEndpointRef.send(GlutenCleanExecutionResource(key, value)))
  }
}

class ExecutorData(val executorEndpointRef: RpcEndpointRef) {}
