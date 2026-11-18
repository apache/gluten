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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.sun.jna.Library;
import com.sun.jna.Native;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Native streaming execution tests for the temporal functions: CURRENT_TIMESTAMP, NOW(),
 * LOCALTIMESTAMP, LOCALTIME — same base as the other stream tests (bounded values source, print
 * sink). The GFV print sink renders TIMESTAMP_LTZ(3)/TIMESTAMP(3) as ISO local strings (no zone
 * suffix) and LOCALTIME as the bare INTEGER milliseconds-of-day carrier, so the assertions parse
 * those shapes. Every assertion is wall-clock bounded (never a fixed value) plus the per-record
 * consistency invariants.
 */
class TemporalFunctionsTest extends GlutenStreamingTestBase {

  private static final long TIMEOUT_MS = 30000;
  private static final long WINDOW_EPSILON_MS = 2000;
  private static final long MILLIS_PER_DAY = 86_400_000L;

  // dup2 fd=1 onto a file: Velox print sink writes to std::cout, which bypasses System.setOut
  // and goes straight to the process's fd=1.
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

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  // Runs the query through the print sink (same mechanism as runAndCheck) and returns the
  // collected output lines instead of asserting against a fixed list — the temporal values are
  // wall-clock dependent.
  private List<String> runAndCollect(String query) throws Exception {
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
      assertThat(tableResult.getJobClient().isPresent()).isTrue();
      JobClient jobClient = tableResult.getJobClient().get();
      try {
        long startTime = System.currentTimeMillis();
        while (printResultFile.length() == 0) {
          if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
            break;
          }
          Thread.sleep(10);
        }
        long fileSize = -1L;
        startTime = System.currentTimeMillis();
        while (printResultFile.length() > fileSize) {
          if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
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

    List<String> result = new ArrayList<>();
    try (FileReader fr = new FileReader(printResultFile);
        BufferedReader br = new BufferedReader(fr)) {
      String line;
      while ((line = br.readLine()) != null) {
        result.add(line);
      }
    }
    tEnv().executeSql("drop table if exists printT");
    return result;
  }

  private static String[] fieldsOf(String line) {
    int begin = line.indexOf('[');
    int end = line.lastIndexOf(']');
    return line.substring(begin + 1, end).split(", ");
  }

  // The printed TIMESTAMP_LTZ(3) value is the session-zone wall clock; parse
  // it back to epoch millis.
  private static long epochOfPrintedTimestamp(String printed, ZoneId sessionTz) {
    return LocalDateTime.parse(printed).atZone(sessionTz).toInstant().toEpochMilli();
  }

  // The printed TIMESTAMP(3) value is a naive local wall clock; its "epoch"
  // is the clock face read as UTC.
  private static long epochOfPrintedNaiveTimestamp(String printed) {
    return LocalDateTime.parse(printed).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
  }

  private static long millisOfDayOfPrintedNaiveTimestamp(String printed) {
    LocalDateTime wall = LocalDateTime.parse(printed);
    return wall.getHour() * 3_600_000L
        + wall.getMinute() * 60_000L
        + wall.getSecond() * 1_000L
        + (wall.getNano() / 1_000_000L);
  }

  private static List<Row> sevenRows() {
    List<Row> rows = new ArrayList<>();
    for (int i = 1; i <= 7; i++) {
      rows.add(Row.of(i));
    }
    return rows;
  }

  // E1: UTC session — per-record bounded wall-clock window, same-row
  // CURRENT_TIMESTAMP == NOW (string equality at ms precision), LOCALTIME
  // is the INTEGER carrier in [0, 86'400'000) and equals exactly the
  // milliseconds-of-day derived from the same row's LOCALTIMESTAMP.
  @Test
  void testTemporalSameRowConsistencyUtc() throws Exception {
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    createSimpleBoundedValuesTable("tblTemporalUtc", "id int", sevenRows());
    String query =
        "select id, CURRENT_TIMESTAMP as cts, NOW() as nws, LOCALTIMESTAMP as lts, LOCALTIME as ltime from tblTemporalUtc";

    long t0 = System.currentTimeMillis();
    List<String> lines = runAndCollect(query);
    long t1 = System.currentTimeMillis();

    assertThat(lines).hasSize(7);
    for (String line : lines) {
      String[] fields = fieldsOf(line);
      assertThat(fields).hasSize(5);
      long ct = epochOfPrintedTimestamp(fields[1], ZoneId.of("UTC"));
      long nw = epochOfPrintedTimestamp(fields[2], ZoneId.of("UTC"));
      long lt = epochOfPrintedNaiveTimestamp(fields[3]);
      long localTime = Long.parseLong(fields[4]);

      // Bounded wall-clock window (RC-07): never a fixed value.
      assertThat(ct).isBetween(t0 - WINDOW_EPSILON_MS, t1 + WINDOW_EPSILON_MS);
      assertThat(lt).isBetween(t0 - WINDOW_EPSILON_MS, t1 + WINDOW_EPSILON_MS);

      // Same-row consistency (RC-01): one shared instant per record.
      assertThat(fields[1]).isEqualTo(fields[2]);
      assertThat(ct).isEqualTo(lt);

      // LOCALTIME carrier (RC-02 domain): INTEGER in [0, 86'400'000) and
      // exactly the ms-of-day of the same row's LOCALTIMESTAMP.
      assertThat(localTime).isBetween(0L, MILLIS_PER_DAY - 1);
      assertThat(localTime).isEqualTo(millisOfDayOfPrintedNaiveTimestamp(fields[3]));
    }
  }

  // E2: Asia/Shanghai session — same invariants with the +8h session
  // offset: LOCALTIMESTAMP leads CURRENT_TIMESTAMP by the zone offset, and
  // LOCALTIME is the ms-of-day of the Shanghai clock face.
  @Test
  void testTemporalSameRowConsistencyShanghai() throws Exception {
    ZoneId shanghai = ZoneId.of("Asia/Shanghai");
    tEnv().getConfig().setLocalTimeZone(shanghai);
    createSimpleBoundedValuesTable("tblTemporalSh", "id int", sevenRows());
    String query =
        "select id, CURRENT_TIMESTAMP as cts, NOW() as nws, LOCALTIMESTAMP as lts, LOCALTIME as ltime from tblTemporalSh";

    long t0 = System.currentTimeMillis();
    List<String> lines = runAndCollect(query);
    long t1 = System.currentTimeMillis();

    assertThat(lines).hasSize(7);
    long offsetMillis = 8L * 3600 * 1000;
    for (String line : lines) {
      String[] fields = fieldsOf(line);
      assertThat(fields).hasSize(5);
      // The print sink renders TIMESTAMP_LTZ(3) on the UTC clock face
      // regardless of the session zone (known behavior, consistent with
      // native Flink), so parse it as a naive UTC clock.
      long ct = epochOfPrintedNaiveTimestamp(fields[1]);
      long lt = epochOfPrintedNaiveTimestamp(fields[3]);
      long localTime = Long.parseLong(fields[4]);

      assertThat(ct).isBetween(t0 - WINDOW_EPSILON_MS, t1 + WINDOW_EPSILON_MS);

      // Same-row alias equality.
      assertThat(fields[1]).isEqualTo(fields[2]);

      // LOCALTIMESTAMP = shared instant shifted by the session offset.
      assertThat(lt - ct)
          .isBetween(offsetMillis - WINDOW_EPSILON_MS, offsetMillis + WINDOW_EPSILON_MS);

      // LOCALTIME is the +8h clock face's ms-of-day.
      assertThat(localTime).isBetween(0L, MILLIS_PER_DAY - 1);
      assertThat(localTime).isEqualTo(millisOfDayOfPrintedNaiveTimestamp(fields[3]));
    }
  }

  // E3: the same-row equality survives the filter path — the predicate
  // comparing two cast renderings of the shared instant is always true, so
  // all rows pass.
  @Test
  void testTemporalFilterSameRowEquality() throws Exception {
    tEnv().getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    createSimpleBoundedValuesTable("tblTemporalFilter", "id int", sevenRows());
    String query =
        "select id from tblTemporalFilter"
            + " where CAST(CURRENT_TIMESTAMP AS STRING) = CAST(NOW() AS STRING)";
    runAndCheck(query, List.of("+I[1]", "+I[2]", "+I[3]", "+I[4]", "+I[5]", "+I[6]", "+I[7]"));
  }
}
