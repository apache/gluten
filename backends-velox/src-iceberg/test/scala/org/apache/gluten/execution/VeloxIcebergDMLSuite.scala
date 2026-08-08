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
package org.apache.gluten.execution

import org.apache.spark.sql.Row

class VeloxIcebergCopyOnWriteSuite extends VeloxIcebergTestBase {

  test("copy-on-write delete") {
    withTable("iceberg_cow_delete") {
      spark.sql("""
                  |CREATE TABLE iceberg_cow_delete (
                  |  id INT,
                  |  name STRING,
                  |  p STRING
                  |) USING iceberg
                  |TBLPROPERTIES (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |)
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO iceberg_cow_delete
                  |VALUES (1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2'),
                  |       (4, 'a4', 'p1'), (5, 'a5', 'p2'), (6, 'a6', 'p1')
                  |""".stripMargin)

      val delete = spark.sql("DELETE FROM iceberg_cow_delete WHERE name = 'a1'")

      checkCommandPlan[VeloxIcebergReplaceDataExec](delete)
      checkAnswer(
        spark.sql("SELECT * FROM iceberg_cow_delete ORDER BY id"),
        Seq(
          Row(2, "a2", "p1"),
          Row(3, "a3", "p2"),
          Row(4, "a4", "p1"),
          Row(5, "a5", "p2"),
          Row(6, "a6", "p1")))
    }
  }

  test("copy-on-write update after schema evolution") {
    withTable("iceberg_cow_update_evolved") {
      spark.sql("""
                  |CREATE TABLE iceberg_cow_update_evolved (
                  |  id INT,
                  |  name STRING,
                  |  age INT
                  |) USING iceberg
                  |TBLPROPERTIES (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |)
                  |""".stripMargin)

      spark.sql("""
                  |ALTER TABLE iceberg_cow_update_evolved
                  |ADD COLUMNS (salary DECIMAL(10, 2))
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO iceberg_cow_update_evolved VALUES
                  |  (1, 'Name1', 23, 3400.00),
                  |  (2, 'Name2', 30, 5500.00),
                  |  (3, 'Name3', 35, 6500.00)
                  |""".stripMargin)

      val update = spark.sql("""
                               |UPDATE iceberg_cow_update_evolved
                               |SET name = 'Name4'
                               |WHERE id = 1
                               |""".stripMargin)

      checkCommandPlan[VeloxIcebergReplaceDataExec](update)
      checkAnswer(
        spark.sql("""
                    |SELECT id, name, age, salary
                    |FROM iceberg_cow_update_evolved
                    |ORDER BY id
                    |""".stripMargin),
        Seq(
          Row(1, "Name4", 23, new java.math.BigDecimal("3400.00")),
          Row(2, "Name2", 30, new java.math.BigDecimal("5500.00")),
          Row(3, "Name3", 35, new java.math.BigDecimal("6500.00"))
        )
      )
    }
  }
}
