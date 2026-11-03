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
package org.apache.spark.shuffle.gluten.celeborn;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for the {@code FailedShuffleCleaner} ownership of {@link CelebornShuffleManager}.
 *
 * <p>The test is hermetic: {@link LifecycleManager} construction and the {@link ShuffleClient}
 * factory are mocked, so no {@code RpcEnv}, no {@code ApplicationHeartbeater} and no RPC against a
 * Celeborn master is involved. Only the cleaner itself is a real Celeborn object, because its
 * executor is the resource whose lifecycle is under test.
 */
public class CelebornShuffleManagerTest {

  private static final String STAGE_RERUN_KEY = "spark.celeborn.client.spark.stageRerun.enabled";
  private static final String CLEAN_FAILED_SHUFFLE_KEY =
      "spark.celeborn.client.spark.fetch.cleanFailedShuffle";
  private static final String CLEAN_FAILED_SHUFFLE_INTERVAL_KEY =
      "spark.celeborn.client.spark.fetch.cleanFailedShuffleInterval";

  @Test
  public void testManagerRetainsFailedShuffleCleanerAndStopShutsDownItsExecutor() throws Exception {
    FailedShuffleCleanerTestSupport.assumeSupported();

    // SparkConf(false) ignores ambient spark.* system properties, so the manager only sees the
    // settings below.
    SparkConf sparkConf =
        new SparkConf(false)
            .set(STAGE_RERUN_KEY, "true")
            .set(CLEAN_FAILED_SHUFFLE_KEY, "true")
            // Long enough that the periodic cleanup never fires while the test runs.
            .set(CLEAN_FAILED_SHUFFLE_INTERVAL_KEY, "1h")
            .set("spark.sql.adaptive.localShuffleReader.enabled", "false");

    SparkEnv previousEnv = SparkEnv.get();
    SparkEnv env = mock(SparkEnv.class);
    when(env.executorId()).thenReturn("driver");
    when(env.mapOutputTracker()).thenReturn(mock(MapOutputTrackerMaster.class));
    SparkEnv.set(env);
    try (MockedStatic<ShuffleClient> shuffleClients = mockStatic(ShuffleClient.class);
        MockedStatic<SparkUtils> sparkUtils = mockStatic(SparkUtils.class, sparkUtilsAnswer());
        MockedConstruction<LifecycleManager> lifecycleManagers =
            mockConstruction(
                LifecycleManager.class,
                context -> withSettings().defaultAnswer(lifecycleManagerAnswer(context)),
                (lifecycleManager, context) -> {})) {
      CelebornShuffleManager manager = new CelebornShuffleManager(sparkConf);
      ExecutorService cleanerExecutor;
      try {
        initializeLifecycleManager(manager, "gluten-celeborn-test-app");
        assertEquals(
            "The driver side manager should create exactly one LifecycleManager",
            1,
            lifecycleManagers.constructed().size());

        Object cleaner = failedShuffleCleanerOf(manager);
        assertNotNull(
            "CelebornShuffleManager should retain the FailedShuffleCleaner created during"
                + " LifecycleManager initialization",
            cleaner);
        assertEquals(CelebornUtils.FAILED_SHUFFLE_CLEANER_NAME, cleaner.getClass().getName());

        cleanerExecutor = FailedShuffleCleanerTestSupport.executorOf(cleaner);
        assertNotNull("FailedShuffleCleaner should own a scheduled executor", cleanerExecutor);
        assertFalse(
            "FailedShuffleCleaner executor should be running before the manager is stopped",
            cleanerExecutor.isShutdown());
      } finally {
        manager.stop();
      }

      assertTrue(
          "CelebornShuffleManager.stop() should shut down the cleaner executor",
          cleanerExecutor.isShutdown());
      assertTrue(
          "cleaner executor should terminate once the manager is stopped",
          cleanerExecutor.awaitTermination(10, TimeUnit.SECONDS));
      assertNull(
          "CelebornShuffleManager.stop() should release the cleaner reference",
          failedShuffleCleanerOf(manager));
    } finally {
      SparkEnv.set(previousEnv);
    }
  }

  /**
   * Keeps the real Celeborn {@code SparkUtils} behavior, except for {@code addSparkListener}, which
   * dereferences {@code SparkContext.getActive()} without a null check and would therefore require
   * a live SparkContext. Registering the listener is unrelated to the cleaner ownership under test.
   */
  private static Answer<Object> sparkUtilsAnswer() {
    return invocation -> {
      if ("addSparkListener".equals(invocation.getMethod().getName())) {
        return null;
      }
      return invocation.callRealMethod();
    };
  }

  /**
   * Drives the production initialization path, which is where the cleaner has to be retained. It is
   * reached from {@code registerShuffle} in production, which additionally needs a live
   * SparkContext, so the private method is invoked directly instead.
   */
  private static void initializeLifecycleManager(CelebornShuffleManager manager, String appId)
      throws ReflectiveOperationException {
    Method method =
        CelebornShuffleManager.class.getDeclaredMethod("initializeLifecycleManager", String.class);
    method.setAccessible(true);
    try {
      method.invoke(manager, appId);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw e;
    }
  }

  private static Object failedShuffleCleanerOf(CelebornShuffleManager manager)
      throws ReflectiveOperationException {
    Field field = CelebornShuffleManager.class.getDeclaredField("failedShuffleCleaner");
    field.setAccessible(true);
    return field.get(manager);
  }

  /**
   * Answers only what the production initialization path reads from the LifecycleManager: the conf
   * the manager passed to the constructor, and the counters it increments. Everything else keeps
   * the Mockito defaults, so no real LifecycleManager behavior is involved. Return types are
   * matched instead of method names to stay tolerant to Celeborn version differences.
   */
  private static Answer<Object> lifecycleManagerAnswer(MockedConstruction.Context context) {
    CelebornConf celebornConf = (CelebornConf) context.arguments().get(1);
    return invocation -> {
      Class<?> returnType = invocation.getMethod().getReturnType();
      if (CelebornConf.class.equals(returnType)) {
        return celebornConf;
      }
      if (LongAdder.class.equals(returnType)) {
        return new LongAdder();
      }
      return Answers.RETURNS_DEFAULTS.answer(invocation);
    };
  }
}
