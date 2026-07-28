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
package org.apache.gluten.table.runtime.stream.common;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.sun.jna.Library;
import com.sun.jna.Native;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GlutenStreamingTestBase extends StreamingTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(GlutenStreamingTestBase.class);
  private static final String EXECUTION_PLAN_PREIFX = "== Physical Execution Plan ==";
  private static final long timeoutMS = 30000;

  // dup2 fd=1 onto a file: Velox print sink writes to std::cout, which bypasses System.setOut and
  // goes straight to the process's fd=1.
  private interface CLibrary extends Library {
    int dup(int oldfd);

    int dup2(int oldfd, int newfd);

    int open(String path, int flags, int mode);

    int close(int fd);
  }

  private static final CLibrary C_LIB = Native.load("c", CLibrary.class);
  private static final int O_WRONLY = 1;
  private static final int O_CREAT = 0100;
  private static final int O_TRUNC = 01000;

  @BeforeAll
  public static void setup() throws Exception {
    LOG.info("GlutenStreamingTestBase setup");
    Velox4jEnvironment.initializeOnce();
  }

  /*
   * schema is in format of "a int, b bigint, c string"
   */
  protected void createSimpleBoundedValuesTable(String tableName, String schema, List<Row> rows) {
    String myTableDataId = TestValuesTableFactory.registerData(rows);
    String sourceTable =
        "CREATE TABLE "
            + tableName
            + "(\n"
            + schema
            + "\n"
            + ") WITH (\n"
            + " 'connector' = 'values',\n"
            + " 'bounded' = 'true',\n"
            + String.format(" 'data-id' = '%s',\n", myTableDataId)
            + " 'nested-projection-supported' = 'true'\n"
            + ")";
    tEnv().executeSql(sourceTable);
  }

  protected void createPrintSinkTable(String tableName, ResolvedSchema schema) {
    List<Column> cols = schema.getColumns();
    StringBuilder schemaBuilder = new StringBuilder();
    for (int i = 0; i < cols.size(); ++i) {
      Column col = cols.get(i);
      schemaBuilder.append(col.getName()).append(" ");
      if (col.getDataType().getLogicalType() instanceof TimestampType) {
        String typeName = col.getDataType().toString().replace("*ROWTIME*", "");
        schemaBuilder.append(typeName);
      } else {
        schemaBuilder.append(col.getDataType().toString());
      }
      if (i != cols.size() - 1) {
        schemaBuilder.append(",");
      }
    }
    String sinkTable =
        "CREATE TABLE "
            + tableName
            + "(\n"
            + schemaBuilder.toString()
            + "\n"
            + ") WITH (\n"
            + " 'connector' = 'print')";
    tEnv().executeSql(sinkTable);
  }

  // Return the execution plan represented by StreamEexcNode
  protected String explainExecutionPlan(String query) {
    Table table = tEnv().sqlQuery(query);
    String plainPlans = table.explain(ExplainDetail.JSON_EXECUTION_PLAN);
    int index = plainPlans.indexOf(EXECUTION_PLAN_PREIFX);
    if (index != -1) {
      return plainPlans.substring(index + EXECUTION_PLAN_PREIFX.length());
    } else {
      return "";
    }
  }

  protected void runAndCheck(String query, List<String> expected) {
    String printResultDirPath = System.getProperty("user.dir") + "/log/";
    new File(printResultDirPath).mkdirs();
    String printResultFilePath = printResultDirPath + "taskmanager.out";
    File printResultFile = new File(printResultFilePath);
    if (printResultFile.exists()) {
      printResultFile.delete();
    }

    int savedStdout = C_LIB.dup(1);
    int fileFd = C_LIB.open(printResultFilePath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fileFd < 0) {
      C_LIB.close(savedStdout);
      throw new FlinkRuntimeException("Failed to open " + printResultFilePath);
    }
    C_LIB.dup2(fileFd, 1);
    try {
      Table table = tEnv().sqlQuery(query);
      createPrintSinkTable("printT", table.getResolvedSchema());
      String newQuery = String.format("insert into %s %s", "printT", query);
      TableResult tableResult = tEnv().executeSql(newQuery);
      assertTrue(tableResult.getJobClient().isPresent());
      JobClient jobClient = tableResult.getJobClient().get();
      try {
        long startTime = System.currentTimeMillis();
        while (printResultFile.length() == 0) {
          if (System.currentTimeMillis() - startTime > timeoutMS) {
            break;
          }
          Thread.sleep(10);
        }
        long fileSize = -1L;
        startTime = System.currentTimeMillis();
        while (printResultFile.length() > fileSize) {
          if (System.currentTimeMillis() - startTime > timeoutMS) {
            break;
          }
          fileSize = printResultFile.length();
          Thread.sleep(3000);
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new FlinkRuntimeException(ie);
      } finally {
        jobClient.cancel();
      }
    } finally {
      C_LIB.dup2(savedStdout, 1);
      C_LIB.close(fileFd);
      C_LIB.close(savedStdout);
    }

    try {
      List<String> result = new ArrayList<>();
      try (FileReader fr = new FileReader(printResultFile);
          BufferedReader br = new BufferedReader(fr)) {
        String line = null;
        while ((line = br.readLine()) != null) {
          result.add(line);
        }
      }
      assertThat(result).isEqualTo(expected);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    } finally {
      tEnv().executeSql("drop table if exists printT");
    }
  }
}
