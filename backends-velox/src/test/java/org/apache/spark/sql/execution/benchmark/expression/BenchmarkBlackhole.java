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
package org.apache.spark.sql.execution.benchmark.expression;

import java.lang.management.ManagementFactory;
import java.util.List;

public final class BenchmarkBlackhole {
  private static final String UNLOCK = "-XX:+UnlockExperimentalVMOptions";
  private static final String COMMAND =
      "-XX:CompileCommand=blackhole,"
          + "org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole::consume";

  private BenchmarkBlackhole() {}

  public static void requireEnabled(boolean required) {
    if (!required) return;
    String vmName = System.getProperty("java.vm.name");
    if (!vmName.contains("OpenJDK") && !vmName.contains("HotSpot")) {
      throw new IllegalStateException("This benchmark requires a supported HotSpot JVM");
    }
    List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    if (!arguments.contains(UNLOCK) || !arguments.contains(COMMAND)) {
      throw new IllegalStateException(
          "Compiler blackhole requires JVM arguments: " + UNLOCK + " " + COMMAND);
    }
  }

  // HotSpot recognizes every overload through the mandatory CompileCommand.
  public static void consume(boolean isNull, boolean value) {}

  public static void consume(boolean isNull, byte value) {}

  public static void consume(boolean isNull, short value) {}

  public static void consume(boolean isNull, int value) {}

  public static void consume(boolean isNull, long value) {}

  public static void consume(boolean isNull, float value) {}

  public static void consume(boolean isNull, double value) {}

  public static void consume(boolean isNull, byte[] value) {}

  // ArrayData, MapData, InternalRow and Decimal; strings use the representation overload below.
  public static void consume(boolean isNull, Object value) {}

  public static void consume(boolean isNull, Object base, long offset, int size) {}
}
