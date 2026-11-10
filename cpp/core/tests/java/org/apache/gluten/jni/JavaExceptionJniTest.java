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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/** Lightweight JVM cases run by the native CTest suite, without Spark or a backend. */
public final class JavaExceptionJniTest {
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

  private static native void throwNativeException();

  private static native void close();

  private static void assertSame(Object expected, Object actual, String description) {
    if (actual != expected) {
      throw new AssertionError(description + " changed");
    }
  }

  private static void preservesThrowable(boolean nested) {
    StructuredException original =
        new StructuredException("TEST_ERROR", Collections.singletonMap("value", "original"));
    Throwable cause = new IllegalStateException("original cause");
    Throwable suppressed = new IllegalArgumentException("original suppressed exception");
    original.initCause(cause);
    original.addSuppressed(suppressed);
    StackTraceElement[] stack = original.getStackTrace();
    try {
      roundTrip(
          () -> {
            throw original;
          },
          nested);
      throw new AssertionError("Expected the original exception");
    } catch (StructuredException actual) {
      assertSame(original, actual, "Throwable identity");
      assertSame(original.getClass(), actual.getClass(), "Exception class");
      assertSame(cause, actual.getCause(), "Cause");
      if (actual.getSuppressed().length != 1) {
        throw new AssertionError("Suppressed exception count changed");
      }
      assertSame(suppressed, actual.getSuppressed()[0], "Suppressed exception");
      if (!Arrays.equals(stack, actual.getStackTrace())) {
        throw new AssertionError("Java stack trace changed");
      }
      if (!"TEST_ERROR".equals(actual.errorClass)
          || !Collections.singletonMap("value", "original").equals(actual.parameters)) {
        throw new AssertionError("Structured error metadata changed");
      }
    }
  }

  private static void preservesNativeFallback() {
    try {
      throwNativeException();
      throw new AssertionError("Expected GlutenException");
    } catch (GlutenException actual) {
      assertSame(GlutenException.class, actual.getClass(), "Native fallback class");
      assertSame(null, actual.getCause(), "Native fallback cause");
      if (!"native-only failure".equals(actual.getMessage())) {
        throw new AssertionError("Native fallback message changed");
      }
    }
  }

  private static void returnsNormally() {
    for (boolean nested : new boolean[] {false, true}) {
      int[] calls = {0};
      int result = roundTrip(() -> calls[0]++, nested);
      if (result != 42 || calls[0] != 1) {
        throw new AssertionError("Successful JNI callback did not return normally");
      }
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("Expected the native library path and test case");
    }
    Runnable test;
    switch (args[1]) {
      case "direct":
        test = () -> preservesThrowable(false);
        break;
      case "nested":
        test = () -> preservesThrowable(true);
        break;
      case "nativeOnly":
        test = JavaExceptionJniTest::preservesNativeFallback;
        break;
      case "normalReturn":
        test = JavaExceptionJniTest::returnsNormally;
        break;
      default:
        throw new IllegalArgumentException("Unknown test case: " + args[1]);
    }
    System.load(args[0]);
    try {
      test.run();
    } finally {
      close();
    }
  }
}
