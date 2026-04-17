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
package org.apache.gluten.jni;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;
import org.apache.gluten.test.VeloxBackendTestBase;
import org.apache.gluten.vectorized.ColumnarBatchInIterator;

import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for SIGSEGV in CPUThreadPool threads during HDFS scan.
 *
 * <p>Root cause: JniColumnarBatchIterator destructor called DetachCurrentThread(), which poisoned
 * libhdfs.so's TLS-cached JNIEnv*. The next HDFS call on the same thread used the stale env,
 * causing SIGSEGV in jni_NewStringUTF.
 *
 * <p>This test reproduces the exact crash: on a native std::thread (simulating CPUThreadPool), it
 * saves the JNIEnv* (like libhdfs caches in TLS), destroys a real JniColumnarBatchIterator, then
 * reuses the saved env for a JNI call. With the buggy code, this triggers SIGSEGV and the JVM
 * crashes. With the fix, it works normally.
 */
public class JniThreadDetachTest extends VeloxBackendTestBase {

  /**
   * Native helper in JniTestHelper.cc. Spawns a std::thread and reproduces:
   *
   * <ol>
   *   <li>Attach thread, save env (simulates libhdfs TLS cache)
   *   <li>Create/destroy real JniColumnarBatchIterator (destructor under test)
   *   <li>Reuse saved env for FindClass (simulates libhdfs's next HDFS call)
   * </ol>
   *
   * With the fix: returns true. With the bug: SIGSEGV crashes the JVM at step 3.
   */
  private static native boolean nativeTestIteratorDestructorKeepsThreadAttached(
      long runtimeHandle, Object jColumnarBatchItr);

  @Test
  public void testIteratorDestructorDoesNotDetachThread() {
    AtomicBoolean result = new AtomicBoolean(false);
    AtomicReference<Throwable> thrown = new AtomicReference<>(null);

    TaskResources$.MODULE$.runUnsafe(
        () -> {
          try {
            String backendName = BackendsApiManager.getBackendName();
            Runtime runtime = Runtimes.contextInstance(backendName, "JniThreadDetachTest");
            long runtimeHandle = runtime.getHandle();

            Iterator<ColumnarBatch> emptyIter = Collections.emptyIterator();
            ColumnarBatchInIterator batchItr = new ColumnarBatchInIterator(backendName, emptyIter);

            boolean ok = nativeTestIteratorDestructorKeepsThreadAttached(runtimeHandle, batchItr);
            result.set(ok);
          } catch (Throwable t) {
            thrown.set(t);
          }
          return null;
        });

    if (thrown.get() != null) {
      Assert.fail(
          "Test setup failed (exception in TaskResources scope): " + thrown.get().getMessage());
    }
    Assert.assertTrue(
        "JNI call on native thread failed after JniColumnarBatchIterator destructor.",
        result.get());
  }

  /**
   * Native helper in JniTestHelper.cc. Creates a JniAwareThreadFactory, runs a task on it, destroys
   * the executor, then verifies the thread was properly attached and JNI calls succeeded (no crash,
   * executor destroyed cleanly).
   *
   * <p>With the fix: returns true. With a regression (e.g. detach removed): JavaThread objects
   * accumulate silently — this test catches the crash case and documents the correct lifecycle.
   */
  private static native boolean nativeTestSpillThreadDetachesCleanly();

  @Test
  public void testSpillThreadDetachesCleanly() {
    boolean ok = nativeTestSpillThreadDetachesCleanly();
    Assert.assertTrue("Spill thread JNI lifecycle test failed.", ok);
  }
}
