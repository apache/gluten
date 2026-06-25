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
package org.apache.gluten.init;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.config.GlutenConfig;
import org.apache.gluten.memory.listener.ReservationListener;
import org.apache.gluten.utils.ConfigUtil;

import org.apache.spark.util.SparkShutdownManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.runtime.BoxedUnit;

// Initialize native backend before calling any native methods from Java side.
public final class NativeBackendInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(NativeBackendInitializer.class);
  private static final Map<String, NativeBackendInitializer> instances = new ConcurrentHashMap<>();

  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final String backendName;

  private NativeBackendInitializer(String backendName) {
    this.backendName = backendName;
  }

  public static NativeBackendInitializer forBackend(String backendName) {
    return instances.computeIfAbsent(backendName, k -> new NativeBackendInitializer(backendName));
  }

  // Spark DriverPlugin/ExecutorPlugin will only invoke NativeBackendInitializer#initializeBackend
  // method once in its init method.
  // In cluster mode, NativeBackendInitializer#initializeBackend only will be invoked in different
  // JVM.
  // In local mode, NativeBackendInitializer#initializeBackend will be invoked twice in same
  // thread, driver first then executor, initialized flag ensure only invoke initializeBackend once,
  // so there are no race condition here.
  public void initialize(ReservationListener rl, scala.collection.Map<String, String> conf) {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("Already initialized");
    }
    initialize0(rl, conf);
    SparkShutdownManagerUtil.addHookForLibUnloading(
        () -> {
          shutdown();
          return BoxedUnit.UNIT;
        });
  }

  private void initialize0(ReservationListener rl, scala.collection.Map<String, String> conf) {
    try {
      Map<String, String> nativeConfMap = GlutenConfig.getNativeBackendConf(backendName, conf);
      // Get the customer config from SparkConf for each backend.
      BackendsApiManager.getTransformerApiInstance()
          .postProcessNativeConfig(
              nativeConfMap, GlutenConfig.prefixOf(BackendsApiManager.getBackendName()));
      initialize(rl, ConfigUtil.serialize(nativeConfMap));
    } catch (Exception e) {
      LOG.error("Failed to call native backend's initialize method", e);
      throw e;
    }
  }

  private native void initialize(ReservationListener rl, byte[] configPlan);

  private native void shutdown();
}
