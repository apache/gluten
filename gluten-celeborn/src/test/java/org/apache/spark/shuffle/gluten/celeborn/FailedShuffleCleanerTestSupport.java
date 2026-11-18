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

import org.apache.celeborn.common.CelebornConf;
import org.junit.Assume;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;

/** Shared helpers for the {@code FailedShuffleCleaner} lifecycle assertions. */
final class FailedShuffleCleanerTestSupport {

  private FailedShuffleCleanerTestSupport() {}

  /**
   * Keeps the cleaner assertions strict on Celeborn clients that ship {@code FailedShuffleCleaner},
   * and skips them on older clients where {@link CelebornUtils#fetchCleanFailedShuffle} is a no-op
   * by design.
   */
  static void assumeSupported() {
    try {
      CelebornConf.class.getDeclaredMethod("clientFetchCleanFailedShuffle");
      Class.forName(CelebornUtils.FAILED_SHUFFLE_CLEANER_NAME);
    } catch (NoSuchMethodException | ClassNotFoundException e) {
      Assume.assumeNoException("Celeborn client in use does not support FailedShuffleCleaner", e);
    }
  }

  /**
   * Returns the executor owned by the cleaner, which is the resource that leaked before the fix.
   * The field is looked up by type instead of by name to stay tolerant to Celeborn renames.
   */
  static ExecutorService executorOf(Object cleaner) throws ReflectiveOperationException {
    for (Field field : cleaner.getClass().getDeclaredFields()) {
      if (ExecutorService.class.isAssignableFrom(field.getType())) {
        field.setAccessible(true);
        return (ExecutorService) field.get(cleaner);
      }
    }
    throw new AssertionError("No ExecutorService field found on " + cleaner.getClass().getName());
  }
}
