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
package org.apache.gluten.integration;

import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassVisitor;
import org.apache.xbean.asm9.Handle;
import org.apache.xbean.asm9.MethodVisitor;
import org.apache.xbean.asm9.Opcodes;
import org.apache.xbean.asm9.Type;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.PostDiscoveryFilter;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/** Select query/write tests before execution, without maintaining an upstream test-name list. */
public class IcebergQueryTestFilter implements PostDiscoveryFilter {
  private final int shard;
  private final int shards;

  public IcebergQueryTestFilter() {
    this(
        Integer.parseInt(System.getProperty("iceberg.upstream.shard", "0")),
        Integer.parseInt(System.getProperty("iceberg.upstream.shards", "1")));
  }

  IcebergQueryTestFilter(int shard, int shards) {
    if (shards < 1 || shard < 0 || shard >= shards) {
      throw new IllegalArgumentException("Invalid Iceberg shard " + shard + "/" + shards);
    }
    this.shard = shard;
    this.shards = shards;
  }

  static int shardFor(String className, String methodName, int shards) {
    // Split expensive suites while limiting repeated class fixtures to at most four runners.
    // Parameterized invocations stay together, so their identities and parameters are unchanged.
    int parts = Math.min(4, Integer.lowestOneBit(shards));
    return (int) (checksum(className) % (shards / parts) * parts + checksum(methodName) % parts);
  }

  private static long checksum(String value) {
    CRC32 crc = new CRC32();
    crc.update(value.getBytes(StandardCharsets.UTF_8));
    return crc.getValue();
  }

  private static final Pattern QUERY =
      Pattern.compile(
          "(?is)^\\s*(SELECT\\s+|WITH\\s+\\w+.*\\bAS\\s*\\(|INSERT\\s+(INTO|OVERWRITE)\\b"
              + "|UPDATE\\s+\\S+\\s+SET\\b|DELETE\\s+FROM\\b|MERGE\\s+INTO\\b"
              + "|EXPLAIN\\s+)|\\bAS\\s+SELECT\\b");
  private final Map<String, ClassCalls> classes = new HashMap<>();
  private static final Pattern PROCEDURE =
      Pattern.compile("(?is)^\\s*CALL\\s+(?:[^()]*\\.)?([a-z_][a-z_0-9]*)\\s*\\(");

  @Override
  public FilterResult apply(TestDescriptor descriptor) {
    if (!(descriptor.getSource().orElse(null) instanceof MethodSource)) {
      return FilterResult.included("Test container");
    }
    MethodSource source = (MethodSource) descriptor.getSource().get();
    if (!source.getClassName().startsWith("org.apache.iceberg.")) {
      return FilterResult.included("Gluten observer checks");
    }
    Class<?> concrete = source.getJavaClass();
    TestDescriptor ancestor = descriptor;
    while (ancestor != null) {
      if (ancestor.getSource().orElse(null) instanceof ClassSource) {
        concrete = ((ClassSource) ancestor.getSource().get()).getJavaClass();
        break;
      }
      ancestor = ancestor.getParent().orElse(null);
    }
    if (shardFor(concrete.getName(), source.getMethodName(), shards) != shard) {
      return FilterResult.excluded("Assigned to another Iceberg shard");
    }
    return FilterResult.includedIf(
        applicable(concrete, source.getJavaMethod()),
        () -> "Spark query or write",
        () -> "No Spark query/write in the test or its helpers");
  }

  boolean applicable(Class<?> concrete, Method method) {
    return inspect(concrete, method, false);
  }

  boolean expectsException(Class<?> concrete, Method method) {
    return inspect(concrete, method, true);
  }

  private boolean inspect(Class<?> concrete, Method method, boolean exceptions) {
    Set<String> hierarchy = new HashSet<>();
    for (Class<?> type = concrete; type != null; type = type.getSuperclass()) {
      hierarchy.add(Type.getInternalName(type));
    }
    return query(
        Type.getInternalName(method.getDeclaringClass()),
        method.getName() + Type.getMethodDescriptor(method),
        Type.getInternalName(concrete),
        hierarchy,
        new HashSet<>(),
        new Evidence(),
        true,
        exceptions);
  }

  private boolean query(
      String owner,
      String signature,
      String concrete,
      Set<String> hierarchy,
      Set<String> visited,
      Evidence evidence,
      boolean virtual,
      boolean exceptions) {
    if (virtual && hierarchy.contains(owner) && !signature.startsWith("<")) {
      owner = concrete;
    }
    MethodCalls method = null;
    while (owner != null) {
      ClassCalls type = read(owner);
      method = type.methods.get(signature);
      if (method != null) {
        break;
      }
      owner = type.parent;
    }
    if (method == null) {
      return false;
    }
    if (!visited.add(owner + "." + signature)) {
      return false;
    }
    evidence.sql |= method.sql;
    evidence.query |= method.query;
    evidence.action |= method.action;
    evidence.execute |= method.execute;
    evidence.expectedException |= method.expectedException;
    evidence.implementations.addAll(method.implementations);
    evidence.procedures.addAll(method.procedures);
    if (exceptions
        ? evidence.expectedException
        : evidence.action || (evidence.sql && evidence.query)) {
      return true;
    }
    for (String[] call : method.calls) {
      if (query(
          call[0],
          call[1],
          concrete,
          hierarchy,
          visited,
          evidence,
          Boolean.parseBoolean(call[2]),
          exceptions)) {
        return true;
      }
    }
    if (!exceptions && evidence.execute) {
      for (String implementation : new ArrayList<>(evidence.implementations)) {
        for (String entry : read(implementation).methods.keySet()) {
          if (entry.startsWith("execute(")
              && query(
                  implementation, entry, concrete, hierarchy, visited, evidence, false, false)) {
            return true;
          }
        }
      }
    }
    if (!exceptions && evidence.sql) {
      for (String[] procedure : new ArrayList<>(evidence.procedures)) {
        if (query(
            procedure[0], procedure[1], concrete, hierarchy, visited, evidence, false, false)) {
          return true;
        }
      }
    }
    return false;
  }

  private ClassCalls read(String owner) {
    ClassCalls cached = classes.get(owner);
    if (cached != null) {
      return cached;
    }
    ClassCalls type = new ClassCalls();
    classes.put(owner, type);
    // Follow upstream helpers, including inherited tests. Only Spark execution APIs and SQL
    // call sites qualify: a call to a standalone Java reader/writer is not a Spark query.
    if (!owner.startsWith("org/apache/iceberg/")
        && !owner.startsWith("org/apache/gluten/integration/")) {
      return type;
    }
    try (InputStream stream = getClass().getClassLoader().getResourceAsStream(owner + ".class")) {
      if (stream == null) {
        throw new IllegalStateException("Cannot inspect upstream test helper " + owner);
      }
      new ClassReader(stream)
          .accept(
              new ClassVisitor(Opcodes.ASM9) {
                @Override
                public void visit(int v, int a, String n, String s, String parent, String[] i) {
                  type.parent = parent;
                }

                @Override
                public MethodVisitor visitMethod(
                    int access, String name, String descriptor, String signature, String[] errors) {
                  MethodCalls calls = new MethodCalls();
                  type.methods.put(name + descriptor, calls);
                  return new MethodVisitor(Opcodes.ASM9) {
                    @Override
                    public void visitLdcInsn(Object value) {
                      if (value instanceof String && QUERY.matcher((String) value).find()) {
                        calls.query = true;
                      }
                      if (value instanceof String) {
                        Matcher procedure = PROCEDURE.matcher((String) value);
                        if (procedure.find()) {
                          StringBuilder target =
                              new StringBuilder("org/apache/iceberg/spark/procedures/");
                          for (String word :
                              procedure.group(1).toLowerCase(Locale.ROOT).split("_")) {
                            if (!word.isEmpty()) {
                              target
                                  .append(Character.toUpperCase(word.charAt(0)))
                                  .append(word.substring(1));
                            }
                          }
                          target.append("Procedure");
                          String procedureClass = target.toString();
                          if (getClass().getClassLoader().getResource(procedureClass + ".class")
                              != null) {
                            for (String entry : read(procedureClass).methods.keySet()) {
                              if (entry.startsWith("call(")) {
                                calls.procedures.add(new String[] {procedureClass, entry});
                              }
                            }
                          }
                        }
                      }
                    }

                    @Override
                    public void visitMethodInsn(
                        int opcode, String target, String method, String desc, boolean itf) {
                      calls.expectedException |=
                          (target.startsWith("org/junit/") && method.startsWith("assertThrows"))
                              || (target.startsWith("org/assertj/")
                                  && (method.equals("isThrownBy")
                                      || method.equals("assertThatThrownBy")
                                      || method.startsWith("catchThrowable")));
                      // sql()/scalarSql() are generic helpers used by both DDL and data queries.
                      // Query SQL is recognized at their call sites; collecting a DDL result
                      // inside those helpers must not turn catalog-only tests into query tests.
                      boolean sqlHelper = name.equals("sql") || name.equals("scalarSql");
                      if (!sqlHelper && sparkAction(target, method)) {
                        calls.action = true;
                      }
                      calls.sql |=
                          target.equals("org/apache/spark/sql/SparkSession")
                              && method.equals("sql");
                      calls.execute |=
                          target.startsWith("org/apache/iceberg/actions/")
                              && method.equals("execute");
                      if (target.startsWith("org/apache/iceberg/")
                          || target.startsWith("org/apache/gluten/integration/")) {
                        calls.calls.add(
                            new String[] {
                              target,
                              method + desc,
                              Boolean.toString(
                                  opcode == Opcodes.INVOKEVIRTUAL
                                      || opcode == Opcodes.INVOKEINTERFACE)
                            });
                      }
                    }

                    @Override
                    public void visitTypeInsn(int opcode, String target) {
                      if (opcode == Opcodes.NEW
                          && target.startsWith("org/apache/iceberg/spark/actions/")) {
                        calls.implementations.add(target);
                      }
                    }

                    @Override
                    public void visitInvokeDynamicInsn(
                        String method, String desc, Handle bootstrap, Object... arguments) {
                      for (Object argument : arguments) {
                        if (argument instanceof String) {
                          visitLdcInsn(argument);
                        }
                        if (argument instanceof Handle) {
                          Handle handle = (Handle) argument;
                          calls.calls.add(
                              new String[] {
                                handle.getOwner(),
                                handle.getName() + handle.getDesc(),
                                Boolean.toString(
                                    handle.getTag() == Opcodes.H_INVOKEVIRTUAL
                                        || handle.getTag() == Opcodes.H_INVOKEINTERFACE)
                              });
                        }
                      }
                    }
                  };
                }
              },
              ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      return type;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot inspect upstream test helper " + owner, e);
    }
  }

  private static boolean sparkAction(String owner, String method) {
    if (owner.equals("org/apache/spark/sql/DataFrameWriter")
        || owner.equals("org/apache/spark/sql/DataFrameWriterV2")
        || owner.equals("org/apache/spark/sql/CreateTableWriter")) {
      return Set.of(
              "save",
              "saveAsTable",
              "insertInto",
              "append",
              "overwrite",
              "overwritePartitions",
              "create",
              "replace",
              "createOrReplace",
              "parquet",
              "orc",
              "json",
              "csv",
              "text",
              "jdbc")
          .contains(method);
    }
    if (owner.equals("org/apache/spark/sql/streaming/DataStreamWriter")) {
      return method.equals("start") || method.equals("toTable");
    }
    if (owner.equals("org/apache/spark/sql/streaming/StreamingQuery")) {
      return method.equals("processAllAvailable") || method.equals("awaitTermination");
    }
    if (owner.equals("org/apache/spark/sql/DataFrameReader")
        || owner.equals("org/apache/spark/sql/streaming/DataStreamReader")) {
      return Set.of("load", "table", "parquet", "orc", "json", "csv", "text", "textFile", "jdbc")
          .contains(method);
    }
    return owner.equals("org/apache/spark/sql/Dataset")
        && Set.of(
                "collect",
                "collectAsList",
                "count",
                "show",
                "take",
                "takeAsList",
                "head",
                "first",
                "foreach",
                "foreachPartition",
                "toLocalIterator",
                "isEmpty",
                "rdd",
                "toJavaRDD")
            .contains(method);
  }

  private static class ClassCalls {
    private String parent;
    private final Map<String, MethodCalls> methods = new HashMap<>();
  }

  private static class MethodCalls {
    private boolean expectedException;
    private boolean query;
    private boolean sql;
    private boolean action;
    private boolean execute;
    private final Set<String> implementations = new HashSet<>();
    private final List<String[]> procedures = new ArrayList<>();
    private final List<String[]> calls = new ArrayList<>();
  }

  private static class Evidence {
    private boolean expectedException;
    private boolean query;
    private boolean sql;
    private boolean action;
    private boolean execute;
    private final Set<String> implementations = new HashSet<>();
    private final List<String[]> procedures = new ArrayList<>();
  }
}
