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

import org.apache.gluten.execution.VeloxBroadcastBuildSideCache

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rpc.GlutenRpcMessages._
import org.apache.spark.util.ThreadUtils

import scala.util.{Failure, Success}

/** Gluten executor endpoint. */
class GlutenExecutorEndpoint(val executorId: String, val conf: SparkConf)
  extends IsolatedRpcEndpoint
  with Logging {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  private val driverHost = conf.get(config.DRIVER_HOST_ADDRESS.key, "localhost")
  private val driverPort = conf.getInt(config.DRIVER_PORT.key, 7077)
  private val rpcAddress = RpcAddress(driverHost, driverPort)
  private val driverUrl =
    RpcEndpointAddress(rpcAddress, GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME).toString

  @volatile var driverEndpointRef: RpcEndpointRef = null

  rpcEnv.setupEndpoint(GlutenRpcConstants.GLUTEN_EXECUTOR_ENDPOINT_NAME, this)
  // TODO(yuan): get thread cnt from spark context
  override def threadCount(): Int = 1
  override def onStart(): Unit = {
    rpcEnv
      .asyncSetupEndpointRefByURI(driverUrl)
      .flatMap {
        ref =>
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          driverEndpointRef = ref
          ref.ask[Boolean](GlutenRegisterExecutor(executorId, self))
      }(ThreadUtils.sameThread)
      .onComplete {
        case Success(_) => logTrace("Register GlutenExecutor listener success.")
        case Failure(e) => logError("Register GlutenExecutor listener error.", e)
      }(ThreadUtils.sameThread)
    logInfo("Initialized GlutenExecutorEndpoint.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case GlutenCleanExecutionResource(executionId, hashIds) =>
      if (executionId != null) {
        hashIds.forEach(
          resource_id => VeloxBroadcastBuildSideCache.invalidateBroadcastHashtable(resource_id))
      }

    case e =>
      logError(s"Received unexpected message. $e")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case e =>
      logInfo(s"Received message. $e")
  }
}
object GlutenExecutorEndpoint {
  var executorEndpoint: GlutenExecutorEndpoint = _
}
