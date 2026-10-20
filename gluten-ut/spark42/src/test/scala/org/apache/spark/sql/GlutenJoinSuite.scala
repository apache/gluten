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
package org.apache.spark.sql

class GlutenJoinSuite extends JoinSuite with GlutenSQLTestsTrait {

  override def testNameBlackList: Seq[String] = Seq(
    // Below tests are to verify operators, just skip.
    "join operator selection",
    "broadcasted hash join operator selection",
    "broadcasted hash outer join operator selection",
    "broadcasted existence join operator selection",
    "SPARK-28323: PythonUDF should be able to use in join condition",
    "SPARK-28345: PythonUDF predicate should be able to pushdown to join",
    "cross join with broadcast",
    "test SortMergeJoin output ordering",
    "SPARK-22445 Respect stream-side child's needCopyResult in BroadcastHashJoin",
    "SPARK-32330: Preserve shuffled hash join build side partitioning",
    "SPARK-32383: Preserve hash join (BHJ and SHJ) stream side ordering",
    "SPARK-32399: Full outer shuffled hash join",
    "SPARK-32649: Optimize BHJ/SHJ inner/semi join with empty hashed relation",
    "SPARK-34593: Preserve broadcast nested loop join partitioning and ordering",
    "SPARK-35984: Config to force applying shuffled hash join",
    "test SortMergeJoin (with spill)",
    "SPARK-36612: Support left outer join build left or right" +
      " outer join build right in shuffled hash join",
    // NaN is not supported currently, just skip.
    "NaN and -0.0 in join keys"
  )

  testGluten("test case sensitive for BHJ") {
    spark.sql("create table t_bhj(a int, b int, C int) using parquet")
    spark.sql("insert overwrite t_bhj select id as a, (id+1) as b, (id+2) as c from range(3)")
    val sql =
      """
        |select /*+ BROADCAST(t1) */ t0.a, t0.b
        |from t_bhj as t0 join t_bhj as t1 on t0.a = t1.a and t0.b = t1.b and t0.c = t1.c
        |group by t0.a, t0.b
        |order by t0.a, t0.b
        |""".stripMargin
    checkAnswer(spark.sql(sql), Seq(Row(0, 1), Row(1, 2), Row(2, 3)))
  }
}
