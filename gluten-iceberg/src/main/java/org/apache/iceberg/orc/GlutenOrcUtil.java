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
package org.apache.iceberg.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class GlutenOrcUtil {
  private GlutenOrcUtil() {}

  /**
   * Returns whether the ORC footer of {@code file} carries Iceberg field IDs.
   *
   * <p>The Iceberg Spark runtime jar relocates ORC, while plain Iceberg jars do not. The ORC
   * classes are therefore resolved from the signatures of the Iceberg build on the classpath
   * instead of being linked against one of the two packages.
   */
  public static boolean hasFieldIds(InputFile file) throws IOException {
    try {
      Method newFileReader =
          ORC.class.getDeclaredMethod("newFileReader", InputFile.class, Configuration.class);
      newFileReader.setAccessible(true);
      Method getSchema = newFileReader.getReturnType().getMethod("getSchema");
      Method hasIds = ORCSchemaUtil.class.getDeclaredMethod("hasIds", getSchema.getReturnType());
      hasIds.setAccessible(true);

      try (AutoCloseable reader =
          (AutoCloseable) invoke(newFileReader, null, file, new Configuration())) {
        return (Boolean) invoke(hasIds, null, invoke(getSchema, reader));
      }
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Cannot read ORC field IDs with this Iceberg build", e);
    }
  }

  private static Object invoke(Method method, Object target, Object... args) throws Exception {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
  }
}
