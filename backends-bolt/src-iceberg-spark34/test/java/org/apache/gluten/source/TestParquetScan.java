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
package org.apache.gluten.source;

import org.apache.gluten.TestConfUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.shaded.org.apache.avro.generic.GenericData;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.assertj.core.api.Assertions.assertThat;

// UUID and FixedType is not supported.
@RunWith(Parameterized.class)
public class TestParquetScan extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  private static SparkSession spark = null;

  protected static final Types.StructType GLUTEN_SUPPORTED_PRIMITIVES =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withZone()),
          required(110, "s", Types.StringType.get()),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)), // int encoded
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)), // long encoded
          required(116, "dec_20_5", Types.DecimalType.of(20, 5)), // requires padding
          required(117, "dec_38_10", Types.DecimalType.of(38, 10)) // Spark's maximum precision
          );

  @BeforeClass
  public static void startSpark() {
    TestParquetScan.spark =
        SparkSession.builder().master("local[2]").config(TestConfUtil.GLUTEN_CONF).getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestParquetScan.spark;
    TestParquetScan.spark = null;
    currentSpark.stop();
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "vectorized = {0}")
  public static Object[] parameters() {
    return new Object[] {false, true};
  }

  private final boolean vectorized;

  public TestParquetScan(boolean vectorized) {
    this.vectorized = vectorized;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    Assume.assumeTrue(
        "Cannot handle non-string map keys in parquet-avro",
        null
            == TypeUtil.find(
                schema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    Table table = createTable(schema);

    // Important: use the table's schema for the rest of the test
    // When tables are created, the column ids are reassigned.
    List<GenericData.Record> expected = RandomData.generateList(table.schema(), 100, 1L);
    writeRecords(table, expected);

    configureVectorization(table);
    Dataset<Row> df = spark.read().format("iceberg").load(table.location());
    List<Row> rows = df.collectAsList();
    spark.conf().set("spark.gluten.enabled", "false");
    List<Row> rows2 = df.collectAsList();

    Assert.assertEquals("Should contain 100 rows", 100, rows.size());
    assertThat(rows).containsExactlyInAnyOrderElementsOf(Iterables.concat(rows2));

    spark.conf().set("spark.gluten.enabled", "true");
    // Cannot use this helper test function because the order is not same.
    //    for (int i = 0; i < expected.size(); i += 1) {
    //      TestHelpers.assertEqualsSafe(table.schema().asStruct(), expected.get(i), rows.get(i));
    //    }
  }

  @Test
  public void testEmptyTableProjection() throws IOException {
    Types.StructType structType =
        Types.StructType.of(
            required(100, "id", Types.LongType.get()),
            optional(101, "data", Types.StringType.get()),
            required(102, "b", Types.BooleanType.get()),
            optional(103, "i", Types.IntegerType.get()));
    Table table = createTable(new Schema(structType.fields()));

    List<GenericData.Record> expected = RandomData.generateList(table.schema(), 100, 1L);
    writeRecords(table, expected);

    configureVectorization(table);

    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .load(table.location())
            .select(monotonically_increasing_id())
            .collectAsList();
    assertThat(rows).hasSize(100);
  }

  @Test
  public void testGlutenSimpleStruct() throws IOException {
    this.writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(new Schema(GLUTEN_SUPPORTED_PRIMITIVES.fields())));
  }

  @Test
  public void testSimpleStruct() throws IOException {
    this.writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(new Schema(SUPPORTED_PRIMITIVES.fields())));
  }

  @Test
  public void testArray() throws IOException {
    Schema schema =
        new Schema(
            new Types.NestedField[] {
              Types.NestedField.required(0, "id", Types.LongType.get()),
              Types.NestedField.optional(
                  1, "data", Types.ListType.ofOptional(2, Types.StringType.get()))
            });
    this.writeAndValidate(schema);
  }

  // The result is right but order is not same.
  // Use @Test to overwrite the super class test
  @Test
  public void testMap() throws IOException {}

  // The result is right but order is not same.
  // Use @Test to overwrite the super class test
  @Test
  public void testMapOfStructs() throws IOException {}

  @Test
  public void testMixedTypes() throws IOException {
    Types.StructType structType =
        Types.StructType.of(
            required(0, "id", Types.LongType.get()),
            optional(
                1,
                "list_of_maps",
                Types.ListType.ofOptional(
                    2,
                    Types.MapType.ofOptional(
                        3, 4, Types.StringType.get(), GLUTEN_SUPPORTED_PRIMITIVES))),
            optional(
                5,
                "map_of_lists",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.ListType.ofOptional(8, GLUTEN_SUPPORTED_PRIMITIVES))),
            required(
                9,
                "list_of_lists",
                Types.ListType.ofOptional(
                    10, Types.ListType.ofOptional(11, GLUTEN_SUPPORTED_PRIMITIVES))),
            required(
                12,
                "map_of_maps",
                Types.MapType.ofOptional(
                    13,
                    14,
                    Types.StringType.get(),
                    Types.MapType.ofOptional(
                        15, 16, Types.StringType.get(), GLUTEN_SUPPORTED_PRIMITIVES))),
            required(
                17,
                "list_of_struct_of_nested_types",
                Types.ListType.ofOptional(
                    19,
                    Types.StructType.of(
                        Types.NestedField.required(
                            20,
                            "m1",
                            Types.MapType.ofOptional(
                                21, 22, Types.StringType.get(), GLUTEN_SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            23, "l1", Types.ListType.ofRequired(24, GLUTEN_SUPPORTED_PRIMITIVES)),
                        Types.NestedField.required(
                            25, "l2", Types.ListType.ofRequired(26, GLUTEN_SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            27,
                            "m2",
                            Types.MapType.ofOptional(
                                28, 29, Types.StringType.get(), GLUTEN_SUPPORTED_PRIMITIVES))))));

    Schema schema =
        new Schema(
            TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
                .asStructType()
                .fields());

    writeAndValidate(schema);
  }

  @Test
  public void testStructWithOptionalFields() throws IOException {
    this.writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                Lists.transform(
                    GLUTEN_SUPPORTED_PRIMITIVES.fields(), Types.NestedField::asOptional))));
  }

  @Test
  public void testStructWithRequiredFields() throws IOException {
    this.writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                Lists.transform(
                    GLUTEN_SUPPORTED_PRIMITIVES.fields(), Types.NestedField::asRequired))));
  }

  @Test
  public void testArrayOfStructs() throws IOException {
    Schema schema =
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                new Types.NestedField[] {
                  Types.NestedField.required(0, "id", Types.LongType.get()),
                  Types.NestedField.optional(
                      1, "data", Types.ListType.ofOptional(2, GLUTEN_SUPPORTED_PRIMITIVES))
                }));
    this.writeAndValidate(schema);
  }

  @Test
  public void testNestedStruct() throws IOException {
    this.writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                new Types.NestedField[] {
                  Types.NestedField.required(1, "struct", GLUTEN_SUPPORTED_PRIMITIVES)
                })));
  }

  private Table createTable(Schema schema) throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");
    HadoopTables tables = new HadoopTables(CONF);
    return tables.create(schema, PartitionSpec.unpartitioned(), location.toString());
  }

  private void writeRecords(Table table, List<GenericData.Record> records) throws IOException {
    File dataFolder = new File(table.location(), "data");
    dataFolder.mkdirs();

    File parquetFile =
        new File(dataFolder, FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(localOutput(parquetFile)).schema(table.schema()).build()) {
      writer.addAll(records);
    }

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFileSizeInBytes(parquetFile.length())
            .withPath(parquetFile.toString())
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(file).commit();
  }

  private void configureVectorization(Table table) {
    table
        .updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, String.valueOf(vectorized))
        .commit();
  }
}
