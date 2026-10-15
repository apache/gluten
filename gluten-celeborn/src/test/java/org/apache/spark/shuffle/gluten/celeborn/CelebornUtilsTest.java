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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the Celeborn reflection compatibility layer in {@link CelebornUtils}. */
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
  public void testFetchCleanFailedShuffleReturnsCleanerInstance() {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.spark.fetch.cleanFailedShuffle", "true");
    LifecycleManager lifecycleManager = null;
    try {
      lifecycleManager = new LifecycleManager("gluten-celeborn-test", conf);
      Object cleaner = CelebornUtils.fetchCleanFailedShuffle(lifecycleManager, conf);
      assertNotNull("FailedShuffleCleaner instance should be returned to the caller", cleaner);
      assertEquals(CelebornUtils.FAILED_SHUFFLE_CLEANER_NAME, cleaner.getClass().getName());
      CelebornUtils.stopFailedShuffleCleaner(cleaner);
    } finally {
      if (lifecycleManager != null) {
        lifecycleManager.stop();
      }
    }
  }

  @Test
  public void testStageRerunDisabledReturnsNull() {
    assertNull(CelebornUtils.stageRerun(false, null, new CelebornConf()));
  }
}
