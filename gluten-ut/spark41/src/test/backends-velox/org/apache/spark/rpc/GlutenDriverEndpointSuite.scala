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

import org.apache.spark.{SparkEnv, SparkFunSuite}

import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{mock, verify, when}

class GlutenDriverEndpointSuite extends SparkFunSuite {
  test("onStart can run before setupEndpoint returns") {
    val previousEnv = SparkEnv.get
    val sparkEnv = mock(classOf[SparkEnv])
    val rpcEnv = mock(classOf[RpcEnv])
    val endpointRef = mock(classOf[RpcEndpointRef])
    when(sparkEnv.rpcEnv).thenReturn(rpcEnv)
    when(endpointRef.address).thenReturn(RpcAddress("localhost", 12345))
    when(rpcEnv.setupEndpoint(
      eqTo(GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME),
      any[RpcEndpoint]))
      .thenAnswer {
        invocation =>
          val endpoint = invocation.getArgument[RpcEndpoint](1)
          when(rpcEnv.endpointRef(endpoint)).thenReturn(endpointRef)
          // Spark registers self before onStart, which may precede setupEndpoint returning.
          endpoint.onStart()
          endpointRef
      }

    try {
      SparkEnv.set(sparkEnv)
      val endpoint = new GlutenDriverEndpoint
      assert(endpoint.self eq endpointRef)
      verify(endpointRef).address
    } finally {
      SparkEnv.set(previousEnv)
    }
  }
}
