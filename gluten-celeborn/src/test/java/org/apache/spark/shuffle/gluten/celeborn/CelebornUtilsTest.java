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
import org.apache.celeborn.common.CelebornConf;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for the Celeborn reflection compatibility layer in {@link CelebornUtils}. The manager side
 * ownership of the cleaner is covered by {@link CelebornShuffleManagerTest}.
 */
public class CelebornUtilsTest {

  /** Mirrors classes like {@code FailedShuffleCleaner} that only expose a parameterized ctor. */
  public static class ParameterizedOnly {
    public final String value;

    public ParameterizedOnly(String value) {
      this.value = value;
    }
  }

  /** Mirrors classes usable through both the no-arg and the parameterized ctor. */
  public static class NoArgAndParameterized {
    public String value;

    public NoArgAndParameterized() {
      this.value = "no-arg";
    }

    public NoArgAndParameterized(String value) {
      this.value = value;
    }
  }

  /** Only accepts a primitive parameter, which can never be fed with a {@code null} arg. */
  public static class PrimitiveParameter {
    public PrimitiveParameter(int value) {}
  }

  /** Accepts any reference type, so a {@code String} arg must be assignable. */
  public static class SupertypeParameter {
    public final Object value;

    public SupertypeParameter(Object value) {
      this.value = value;
    }
  }

  @Test
  public void testCreateInstanceWithParameterizedConstructor() {
    Object instance = CelebornUtils.createInstance(ParameterizedOnly.class.getName(), "arg");
    assertTrue(instance instanceof ParameterizedOnly);
    assertEquals("arg", ((ParameterizedOnly) instance).value);
  }

  @Test
  public void testCreateInstancePicksConstructorMatchingArguments() {
    Object noArg = CelebornUtils.createInstance(NoArgAndParameterized.class.getName());
    assertEquals("no-arg", ((NoArgAndParameterized) noArg).value);

    Object withArg = CelebornUtils.createInstance(NoArgAndParameterized.class.getName(), "arg");
    assertEquals("arg", ((NoArgAndParameterized) withArg).value);
  }

  @Test
  public void testCreateInstanceAcceptsSubtypeArgument() {
    Object instance = CelebornUtils.createInstance(SupertypeParameter.class.getName(), "arg");
    assertTrue(instance instanceof SupertypeParameter);
    assertEquals("arg", ((SupertypeParameter) instance).value);
  }

  @Test
  public void testCreateInstanceAcceptsNullForReferenceParameter() {
    Object instance =
        CelebornUtils.createInstance(SupertypeParameter.class.getName(), (Object) null);
    assertTrue(instance instanceof SupertypeParameter);
    assertNull(((SupertypeParameter) instance).value);
  }

  @Test
  public void testCreateInstanceRejectsNullForPrimitiveParameter() {
    try {
      CelebornUtils.createInstance(PrimitiveParameter.class.getName(), (Object) null);
      fail("Expected RuntimeException since no constructor accepts null for a primitive");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof NoSuchMethodException);
    }
  }

  @Test
  public void testCreateInstanceThrowsWhenNoConstructorMatches() {
    try {
      CelebornUtils.createInstance(PrimitiveParameter.class.getName(), "not-an-int");
      fail("Expected RuntimeException since no constructor matches the argument type");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof NoSuchMethodException);
    }
  }

  @Test
  public void testCreateInstanceReturnsNullWhenClassIsAbsent() {
    assertNull(CelebornUtils.createInstance("org.apache.celeborn.NonexistentClass"));
  }

  @Test
  public void testFetchCleanFailedShuffleReturnsCleanerAndStopShutsDownItsExecutor()
      throws Exception {
    FailedShuffleCleanerTestSupport.assumeSupported();

    CelebornConf conf = hermeticConf();
    conf.set("celeborn.client.spark.fetch.cleanFailedShuffle", "true");
    // A mocked LifecycleManager keeps the test hermetic: no RpcEnv, no ApplicationHeartbeater and
    // therefore no RPC against an ambient Celeborn master.
    LifecycleManager lifecycleManager = mock(LifecycleManager.class);
    when(lifecycleManager.conf()).thenReturn(conf);

    Object cleaner = CelebornUtils.fetchCleanFailedShuffle(lifecycleManager, conf);
    ExecutorService cleanerExecutor = null;
    try {
      assertNotNull("FailedShuffleCleaner instance should be returned to the caller", cleaner);
      assertEquals(CelebornUtils.FAILED_SHUFFLE_CLEANER_NAME, cleaner.getClass().getName());
      // The returned instance is the very one wired into the LifecycleManager callbacks.
      verify(lifecycleManager).registerValidateCelebornShuffleIdForCleanCallback(any());
      verify(lifecycleManager).registerUnregisterShuffleCallback(any());

      cleanerExecutor = FailedShuffleCleanerTestSupport.executorOf(cleaner);
      assertNotNull("FailedShuffleCleaner should own a scheduled executor", cleanerExecutor);
      assertFalse(
          "FailedShuffleCleaner executor should be running before stop",
          cleanerExecutor.isShutdown());
    } finally {
      if (cleaner != null) {
        CelebornUtils.stopFailedShuffleCleaner(cleaner);
      }
    }

    assertTrue(
        "stopFailedShuffleCleaner should shut down the cleaner executor",
        cleanerExecutor.isShutdown());
    assertTrue(
        "cleaner executor should terminate once the cleaner is stopped",
        cleanerExecutor.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testFetchCleanFailedShuffleDisabledCreatesNoCleaner() {
    FailedShuffleCleanerTestSupport.assumeSupported();

    CelebornConf conf = hermeticConf();
    conf.set("celeborn.client.spark.fetch.cleanFailedShuffle", "false");
    LifecycleManager lifecycleManager = mock(LifecycleManager.class);

    assertNull(CelebornUtils.fetchCleanFailedShuffle(lifecycleManager, conf));
    verifyNoInteractions(lifecycleManager);
  }

  @Test
  public void testStageRerunDisabledReturnsNull() {
    assertNull(CelebornUtils.stageRerun(false, null, hermeticConf()));
  }

  /**
   * Builds a {@link CelebornConf} that ignores ambient {@code celeborn.*} settings coming from
   * system properties, so the assertions below cannot be influenced by the local environment.
   */
  private static CelebornConf hermeticConf() {
    return new CelebornConf(false);
  }
}
