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

import org.apache.gluten.exception.GlutenException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Tests the core JNI exception contract without starting Spark or a backend. */
public final class JavaExceptionJniTest {
  private static boolean libraryLoaded;

  public interface Callback {
    void run() throws Throwable;
  }

  private static final class StructuredException extends RuntimeException {
    private final String errorClass;
    private final Map<String, String> parameters;

    private StructuredException(String errorClass, Map<String, String> parameters) {
      this.errorClass = errorClass;
      this.parameters = parameters;
    }
  }

  private static native int roundTrip(Callback callback, boolean nested);

  private static native void roundTripWithFailedCapture(Callback callback, Throwable failure);

  private static native void roundTripOnWorker(
      Callback callback, Callback nextCallback, Throwable captureFailure);

  private static native int roundTripWithPendingException(
      Callback callback, boolean nested, Throwable pending);

  private static native int roundTripWithFailedRestore(
      Callback callback, boolean nested, Throwable failure);

  private static native void throwNativeException();

  private static native void close();

  private static native boolean entryActive();

  @BeforeClass
  public static void loadLibrary() {
    String directory = System.getProperty("gluten.jni.test.lib.dir");
    Assert.assertNotNull(
        "Enable the jni-test Maven profile to configure the native bridge", directory);
    System.load(
        new File(directory, System.mapLibraryName("java_exception_jni_test")).getAbsolutePath());
    libraryLoaded = true;
  }

  @AfterClass
  public static void releaseReferences() {
    if (libraryLoaded) {
      close();
    }
  }

  @Test
  public void direct() {
    preservesThrowable(false);
  }

  @Test
  public void nested() {
    preservesThrowable(true);
  }

  @Test
  public void directRetainsAlreadyPendingException() {
    retainsAlreadyPendingException(false);
  }

  @Test
  public void nestedRetainsAlreadyPendingException() {
    retainsAlreadyPendingException(true);
  }

  private static void retainsAlreadyPendingException(boolean nested) {
    ArithmeticException original = new ArithmeticException("original callback failure");
    IllegalStateException newer = new IllegalStateException("newer pending exception");
    try {
      roundTripWithPendingException(
          () -> {
            throw original;
          },
          nested,
          newer);
      Assert.fail("Expected the newer pending Java exception");
    } catch (RuntimeException actual) {
      Assert.assertSame(newer, actual);
    }
    preservesThrowable(nested);
  }

  @Test
  public void failedDirectRestorationFallsBack() {
    failedRestorationFallsBack(false);
  }

  @Test
  public void failedNestedRestorationFallsBack() {
    failedRestorationFallsBack(true);
  }

  private static void failedRestorationFallsBack(boolean nested) {
    ArithmeticException original = new ArithmeticException("original callback failure");
    try {
      roundTripWithFailedRestore(
          () -> {
            throw original;
          },
          nested,
          null);
      Assert.fail("Failed restoration must not return without a Java exception");
    } catch (GlutenException actual) {
      Assert.assertSame(GlutenException.class, actual.getClass());
      Assert.assertNull(actual.getCause());
      if (nested) {
        Assert.assertEquals("native wrapper", actual.getMessage());
      } else {
        Assert.assertTrue(actual.getMessage().contains("original callback failure"));
      }
    }
    preservesThrowable(nested);
  }

  @Test
  public void failedDirectRestorationRetainsPendingError() {
    failedRestorationRetainsPendingError(false);
  }

  @Test
  public void failedNestedRestorationRetainsPendingError() {
    failedRestorationRetainsPendingError(true);
  }

  private static void failedRestorationRetainsPendingError(boolean nested) {
    OutOfMemoryError newer = new OutOfMemoryError("injected restoration failure");
    try {
      roundTripWithFailedRestore(
          () -> {
            throw new ArithmeticException("original callback failure");
          },
          nested,
          newer);
      Assert.fail("Expected the newer pending Java exception");
    } catch (OutOfMemoryError actual) {
      Assert.assertSame(newer, actual);
    }
    preservesThrowable(nested);
  }

  private static void preservesThrowable(boolean nested) {
    preservesThrowable(callback -> roundTrip(callback, nested));
  }

  private static void preservesThrowable(Consumer<Callback> invoke) {
    StructuredException original =
        new StructuredException("TEST_ERROR", Collections.singletonMap("value", "original"));
    Throwable cause = new IllegalStateException("original cause");
    Throwable suppressed = new IllegalArgumentException("original suppressed exception");
    original.initCause(cause);
    original.addSuppressed(suppressed);
    StackTraceElement[] stack = original.getStackTrace();
    try {
      invoke.accept(
          () -> {
            throw original;
          });
      Assert.fail("Expected the original exception");
    } catch (StructuredException actual) {
      Assert.assertSame(original, actual);
      Assert.assertSame(original.getClass(), actual.getClass());
      Assert.assertSame(cause, actual.getCause());
      Assert.assertEquals(1, actual.getSuppressed().length);
      Assert.assertSame(suppressed, actual.getSuppressed()[0]);
      Assert.assertArrayEquals(stack, actual.getStackTrace());
      Assert.assertEquals("TEST_ERROR", actual.errorClass);
      Assert.assertEquals(Collections.singletonMap("value", "original"), actual.parameters);
    }
  }

  @Test
  public void failedCaptureRetainsPendingErrorAtJavaEntry() {
    OutOfMemoryError newer = new OutOfMemoryError("injected global reference failure");
    try {
      roundTripWithFailedCapture(
          () -> {
            throw new ArithmeticException("original callback failure");
          },
          newer);
      Assert.fail("Expected the newer pending error at the Java return boundary");
    } catch (OutOfMemoryError actual) {
      Assert.assertSame(newer, actual);
    }
  }

  @Test
  public void failedCaptureDoesNotPoisonReusedWorker() {
    AtomicReference<Thread> worker = new AtomicReference<>();
    int[] calls = {0};
    try {
      roundTripOnWorker(
          () -> {
            worker.set(Thread.currentThread());
            throw new ArithmeticException("original worker failure");
          },
          () -> {
            Assert.assertSame(worker.get(), Thread.currentThread());
            calls[0]++;
          },
          new OutOfMemoryError("injected global reference failure"));
      Assert.fail("Expected the native capture failure");
    } catch (GlutenException actual) {
      Assert.assertEquals(
          "Unable to create global reference for Java exception", actual.getMessage());
    }
    Assert.assertEquals(1, calls[0]);
    Assert.assertNotSame(Thread.currentThread(), worker.get());
  }

  @Test
  public void capturedWorkerThrowableSurvivesReuse() {
    AtomicReference<Thread> worker = new AtomicReference<>();
    int[] calls = {0};
    preservesThrowable(
        callback ->
            roundTripOnWorker(
                () -> {
                  worker.set(Thread.currentThread());
                  callback.run();
                },
                () -> {
                  Assert.assertSame(worker.get(), Thread.currentThread());
                  calls[0]++;
                },
                null));
    Assert.assertEquals(1, calls[0]);
    Assert.assertNotSame(Thread.currentThread(), worker.get());
  }

  @Test
  public void nestedJavaReentryRestoresScope() {
    Assert.assertFalse(entryActive());
    roundTrip(
        () -> {
          Assert.assertTrue(entryActive());
          normalReturn();
          Assert.assertTrue(entryActive());
          nativeOnly();
          Assert.assertTrue(entryActive());
          failedCaptureRetainsPendingErrorAtJavaEntry();
          Assert.assertTrue(entryActive());
        },
        false);
    Assert.assertFalse(entryActive());
    preservesThrowable(false);
    Assert.assertFalse(entryActive());
  }

  @Test
  public void nativeOnly() {
    try {
      throwNativeException();
      Assert.fail("Expected GlutenException");
    } catch (GlutenException actual) {
      Assert.assertSame(GlutenException.class, actual.getClass());
      Assert.assertNull(actual.getCause());
      Assert.assertEquals("native-only failure", actual.getMessage());
    }
  }

  @Test
  public void normalReturn() {
    for (boolean nested : new boolean[] {false, true}) {
      int[] calls = {0};
      int result = roundTrip(() -> calls[0]++, nested);
      Assert.assertEquals(42, result);
      Assert.assertEquals(1, calls[0]);
    }
  }
}
