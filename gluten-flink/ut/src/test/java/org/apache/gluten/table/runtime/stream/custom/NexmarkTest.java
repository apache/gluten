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

import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NexmarkTest {

  private static final Logger LOG = LoggerFactory.getLogger(NexmarkTest.class);
  private static final String NEXMARK_RESOURCE_DIR = "nexmark";

  private static final Map<String, String> NEXMARK_VARIABLES =
      new HashMap<String, String>() {
        {
          put("TPS", "10");
          put("EVENTS_NUM", "100");
          put("PERSON_PROPORTION", "1");
          put("AUCTION_PROPORTION", "3");
          put("BID_PROPORTION", "46");
          put("NEXMARK_TABLE", "datagen");
        }
      };

  private static final int KAFKA_PORT = 19092;
  private static String topicName = "nexmark";

  @RegisterExtension
  public static final SharedKafkaTestResource kafkaInstance =
      new SharedKafkaTestResource()
          .withBrokers(1)
          .registerListener(new PlainListener().onPorts(KAFKA_PORT));

  private static final Map<String, String> KAFKA_VARIABLES =
      new HashMap<>() {
        {
          put("BOOTSTRAP_SERVERS", "localhost:" + KAFKA_PORT);
          put("NEXMARK_TABLE", "kafka");
        }
      };

  private static final List<String> VIEWS = List.of("person", "auction", "bid", "B");
  private static final List<String> FUNCTIONS = List.of("count_char");

  private static StreamTableEnvironment tEnv;

  @BeforeAll
  static void setup() {
    LOG.info("NexmarkTest setup");
    Velox4jEnvironment.initializeOnce();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    tEnv = StreamTableEnvironment.create(env, settings);
  }

  @Test
  void testNexmarkSourceSqlDoesNotPushDownWatermark() {
    String createNexmarkSource = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/ddl_gen.sql");
    createNexmarkSource = replaceVariables(createNexmarkSource, NEXMARK_VARIABLES);
    try {
      tEnv.executeSql(createNexmarkSource);
      String explain = tEnv.explainSql("SELECT * FROM datagen");

      assertThat(explain).contains("WatermarkAssigner");
      List<String> tableSourceScanLines =
          Arrays.stream(explain.split("\\R"))
              .filter(line -> line.contains("TableSourceScan"))
              .collect(Collectors.toList());
      assertThat(tableSourceScanLines).isNotEmpty();
      assertThat(tableSourceScanLines).noneMatch(line -> line.contains("watermark=["));
    } finally {
      tEnv.executeSql("drop table if exists datagen");
    }
  }

  @Test
  void testKafkaSourceSqlPushesDownWatermark() {
    String createKafkaSource = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/ddl_kafka.sql");
    createKafkaSource = replaceVariables(createKafkaSource, KAFKA_VARIABLES);
    try {
      tEnv.executeSql(createKafkaSource);
      String explain = tEnv.explainSql("SELECT * FROM kafka");

      List<String> tableSourceScanLines =
          Arrays.stream(explain.split("\\R"))
              .filter(line -> line.contains("TableSourceScan"))
              .collect(Collectors.toList());
      assertThat(tableSourceScanLines).isNotEmpty();
      assertThat(tableSourceScanLines).anyMatch(line -> line.contains("watermark=["));
    } finally {
      tEnv.executeSql("drop table if exists kafka");
    }
  }

  @Test
  void testAllNexmarkSourceQueries()
      throws ExecutionException, InterruptedException, TimeoutException {
    try {
      setupNexmarkEnvironment(tEnv, "ddl_gen.sql", NEXMARK_VARIABLES);
      List<String> queryFiles = getQueries();
      assertThat(queryFiles).isNotEmpty();
      LOG.warn("Found {} Nexmark query files: {}", queryFiles.size(), queryFiles);

      for (String queryFile : queryFiles) {
        LOG.warn("Executing nextmark query from file: {}", queryFile);
        executeQuery(tEnv, queryFile, false);
      }
    } finally {
      clearEnvironment(tEnv);
    }
  }

  @Test
  void testAllKafkaSourceQueries()
      throws ExecutionException, InterruptedException, TimeoutException {
    try {
      kafkaInstance.getKafkaTestUtils().createTopic(topicName, 1, (short) 1);
      setupNexmarkEnvironment(tEnv, "ddl_kafka.sql", KAFKA_VARIABLES);
      List<String> queryFiles = getQueries();
      assertThat(queryFiles).isNotEmpty();
      LOG.warn("Found {} Nexmark query files: {}", queryFiles.size(), queryFiles);

      for (String queryFile : queryFiles) {
        LOG.warn("Executing kafka query from file:{}", queryFile);
        if (!"q10_orc.sql".equals(queryFile)) {
          executeQuery(tEnv, queryFile, true);
        }
      }
    } finally {
      clearEnvironment(tEnv);
    }
  }

  private static void setupNexmarkEnvironment(
      StreamTableEnvironment tEnv, String sourceFileName, Map<String, String> variables) {
    String createNexmarkSource = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/" + sourceFileName);
    createNexmarkSource = replaceVariables(createNexmarkSource, variables);
    tEnv.executeSql(createNexmarkSource);

    String createTableView = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/ddl_views.sql");
    String[] sqlTableView = createTableView.split(";");
    for (String sql : sqlTableView) {
      sql = replaceVariables(sql, variables);
      String trimmedSql = sql.trim();
      if (!trimmedSql.isEmpty()) {
        tEnv.executeSql(trimmedSql);
      }
    }
  }

  private static String replaceVariables(String sql, Map<String, String> variables) {
    String result = sql;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      result = result.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }

  private static void clearEnvironment(StreamTableEnvironment tEnv) {
    for (int i = 0; i <= 22; ++i) {
      String tableName = "nexmark_q" + i;
      String sql = String.format("drop table if exists %s", tableName);
      tEnv.executeSql(sql);
    }
    tEnv.executeSql("drop table if exists nexmark_q10_orc");
    for (String view : VIEWS) {
      String dropTemporaryViewSql = String.format("drop temporary view if exists %s", view);
      tEnv.executeSql(dropTemporaryViewSql);
      String sql = String.format("drop view if exists %s", view);
      tEnv.executeSql(sql);
    }
    for (String func : FUNCTIONS) {
      String sql = String.format("drop function if exists %s", func);
      tEnv.executeSql(sql);
    }
    tEnv.executeSql("drop table if exists datagen");
    tEnv.executeSql("drop table if exists kafka");
  }

  private void executeQuery(StreamTableEnvironment tEnv, String queryFileName, boolean kafkaSource)
      throws ExecutionException, InterruptedException, TimeoutException {
    if ("q10_orc.sql".equals(queryFileName) && !kafkaSource) {
      executeQ10OrcBatchQuery();
      return;
    }

    String queryContent = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/" + queryFileName);
    if ("q10_orc.sql".equals(queryFileName)) {
      cleanQ10OrcOutput();
    }

    String[] sqlStatements = queryContent.split(";");
    assertThat(sqlStatements.length).isGreaterThanOrEqualTo(2);

    for (int i = 0; i < sqlStatements.length - 2; i++) {
      // For some query tests like q12 q13 q14, the first two of the three statements create tables
      // or views. For others, there are only two statements, with the first one creating a table.
      String createResultTable = sqlStatements[i].trim();
      if (!createResultTable.isEmpty()) {
        TableResult createResult = tEnv.executeSql(createResultTable);
        assertFalse(createResult.getJobClient().isPresent());
      }
    }

    String insertQuery = sqlStatements[sqlStatements.length - 2].trim();
    if (!insertQuery.isEmpty()) {
      TableResult insertResult = tEnv.executeSql(insertQuery);
      if (kafkaSource) {
        assertThat(checkJobRunningStatus(insertResult, 30000) == true);
      } else {
        waitForJobCompletion(insertResult, 30000);
        if ("q10_orc.sql".equals(queryFileName)) {
          verifyQ10OrcOutput();
        }
      }
    }
    assertTrue(sqlStatements[sqlStatements.length - 1].trim().isEmpty());
  }

  private void executeQ10OrcBatchQuery()
      throws ExecutionException, InterruptedException, TimeoutException {
    cleanQ10OrcOutput();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamTableEnvironment batchTEnv = StreamTableEnvironment.create(env, settings);
    try {
      createQ10OrcBidView(batchTEnv);
      String queryContent = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/q10_orc.sql");
      String[] sqlStatements = queryContent.split(";");
      assertThat(sqlStatements.length).isEqualTo(3);

      TableResult createResult = batchTEnv.executeSql(sqlStatements[0].trim());
      assertFalse(createResult.getJobClient().isPresent());

      TableResult insertResult = batchTEnv.executeSql(sqlStatements[1].trim());
      waitForJobCompletion(insertResult, 30000);
      verifyQ10OrcOutput();
      assertTrue(sqlStatements[2].trim().isEmpty());
    } finally {
      clearEnvironment(batchTEnv);
    }
  }

  private static void createQ10OrcBidView(StreamTableEnvironment tEnv) {
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bid AS "
            + "SELECT "
            + "CAST(1 AS BIGINT) AS auction, "
            + "CAST(2 AS BIGINT) AS bidder, "
            + "CAST(100 AS BIGINT) AS price, "
            + "CAST('channel' AS STRING) AS channel, "
            + "CAST('url' AS STRING) AS url, "
            + "TIMESTAMP '2026-07-21 07:30:00' AS `dateTime`, "
            + "CAST('extra' AS STRING) AS extra");
  }

  private void cleanQ10OrcOutput() {
    Path outputDir = Paths.get("/tmp/data/output/bid_orc");
    if (!Files.exists(outputDir)) {
      return;
    }
    try (java.util.stream.Stream<Path> files = Files.walk(outputDir)) {
      files
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to delete " + path, e);
                }
              });
    } catch (IOException e) {
      throw new RuntimeException("Failed to clean Q10 ORC output directory", e);
    }
  }

  private void verifyQ10OrcOutput() throws InterruptedException {
    Path outputDir = Paths.get("/tmp/data/output/bid_orc");
    assertTrue("Q10 ORC output directory should exist", Files.exists(outputDir));

    List<Path> partFiles = waitForFinalQ10OrcPartFiles(outputDir);
    long rowCount = 0L;
    for (Path partFile : partFiles) {
      try {
        rowCount += readAndVerifyQ10OrcFile(partFile);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read Q10 ORC output file " + partFile, e);
      }
    }
    assertThat(rowCount).isGreaterThan(0L);
  }

  private List<Path> waitForFinalQ10OrcPartFiles(Path outputDir) throws InterruptedException {
    long deadlineMillis = System.currentTimeMillis() + 30000L;
    List<Path> regularFiles = List.of();
    while (System.currentTimeMillis() < deadlineMillis) {
      try (java.util.stream.Stream<Path> files = Files.walk(outputDir)) {
        regularFiles = files.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException("Failed to inspect Q10 ORC output", e);
      }

      boolean hasInProgress =
          regularFiles.stream().anyMatch(path -> path.toString().contains(".inprogress"));
      List<Path> partFiles =
          regularFiles.stream()
              .filter(path -> path.getFileName().toString().startsWith("part-"))
              .collect(Collectors.toList());
      if (!hasInProgress && !partFiles.isEmpty()) {
        return partFiles;
      }
      Thread.sleep(1000L);
    }

    assertThat(regularFiles).allMatch(path -> !path.toString().contains(".inprogress"));
    List<Path> partFiles =
        regularFiles.stream()
            .filter(path -> path.getFileName().toString().startsWith("part-"))
            .collect(Collectors.toList());
    assertThat(partFiles).isNotEmpty();
    return partFiles;
  }

  private long readAndVerifyQ10OrcFile(Path partFile) throws IOException {
    Reader reader =
        OrcFile.createReader(
            new org.apache.hadoop.fs.Path(partFile.toUri()),
            OrcFile.readerOptions(new Configuration()));
    TypeDescription schema = reader.getSchema();
    assertThat(schema.getCategory()).isEqualTo(TypeDescription.Category.STRUCT);
    assertThat(schema.getFieldNames())
        .containsExactly("auction", "bidder", "price", "dateTime", "extra");

    long rowCount = 0L;
    try (RecordReader rows = reader.rows()) {
      VectorizedRowBatch batch = schema.createRowBatch();
      while (rows.nextBatch(batch)) {
        rowCount += batch.size;
      }
    }
    assertThat(rowCount).isEqualTo(reader.getNumberOfRows());
    return rowCount;
  }

  private void waitForJobCompletion(TableResult result, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    assertTrue(result.getJobClient().isPresent());
    result.getJobClient().get().getJobExecutionResult().get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  private boolean checkJobRunningStatus(TableResult result, long timeoutMs)
      throws InterruptedException {
    long startTime = System.currentTimeMillis();
    assertTrue(result.getJobClient().isPresent());
    JobClient jobClient = result.getJobClient().get();
    while (System.currentTimeMillis() < startTime + timeoutMs) {
      if (jobClient.getJobStatus().complete(JobStatus.RUNNING)) {
        jobClient.cancel();
        return true;
      } else {
        Thread.sleep(1000);
      }
    }
    LOG.warn("Job not running in " + timeoutMs + " millseconds.");
    jobClient.cancel();
    return false;
  }

  private List<String> getQueries() {
    URL resourceUrl = getClass().getClassLoader().getResource(NEXMARK_RESOURCE_DIR);

    try {
      Path resourcePath = Paths.get(resourceUrl.toURI());
      List<String> queryFiles = new ArrayList<>();

      try (DirectoryStream<Path> stream = Files.newDirectoryStream(resourcePath, "q*.sql")) {
        for (Path entry : stream) {
          queryFiles.add(entry.getFileName().toString());
        }
      }

      String queryFilter = System.getProperty("nexmark.queries");
      if (queryFilter != null && !queryFilter.trim().isEmpty()) {
        List<String> selectedQueries =
            Arrays.stream(queryFilter.split(","))
                .map(String::trim)
                .filter(query -> !query.isEmpty())
                .collect(Collectors.toList());
        queryFiles.retainAll(selectedQueries);
      }

      return queryFiles.stream().sorted().collect(Collectors.toList());

    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException("Failed to discover query files", e);
    }
  }

  private static String readSqlFromFile(String fileName) {
    try {
      URL resource = NexmarkTest.class.getClassLoader().getResource(fileName);
      if (resource == null) {
        throw new RuntimeException("SQL file not found: " + fileName);
      }
      return new String(Files.readAllBytes(Paths.get(resource.toURI())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to read SQL file: " + fileName, e);
    }
  }
}
