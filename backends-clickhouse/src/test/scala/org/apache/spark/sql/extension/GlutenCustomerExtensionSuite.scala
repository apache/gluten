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
package org.apache.spark.sql.extension

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BasicScanExecTransformer, FileSourceScanExecTransformerBase}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

class GlutenCustomerExtensionSuite extends SharedSparkSession {
  // These configs only take effect on ClickHouse backend.
  private val ExtendedColumnarTransformRulesKey =
    "spark.gluten.sql.columnar.extended.columnar.transform.rules"
  private val ExtendedColumnarPostRulesKey =
    "spark.gluten.sql.columnar.extended.columnar.post.rules"

  override def sparkConf: SparkConf = {
    super.sparkConf
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set(
        ExtendedColumnarTransformRulesKey,
        "org.apache.spark.sql" +
          ".extension.CustomerColumnarPreRules")
      .set(ExtendedColumnarPostRulesKey, "")
  }

  test("customer column rules") {
    withSQLConf((GlutenConfig.GLUTEN_ENABLED.key, "false")) {
      sql("create table my_parquet(id int) using parquet")
      sql("insert into my_parquet values (1)")
      sql("insert into my_parquet values (2)")
    }
    withSQLConf((GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")) {
      val df = sql("select * from my_parquet")
      val testFileSourceScanExecTransformer = df.queryExecution.executedPlan.collect {
        case f: TestFileSourceScanExecTransformer => f
      }
      assert(testFileSourceScanExecTransformer.nonEmpty)
      assert(testFileSourceScanExecTransformer.head.nodeNamePrefix.equals("TestFile"))
    }
  }
}

/** Test for customer column rules */
case class TestFileSourceScanExecTransformer(
    @transient override val relation: HadoopFsRelation,
    @transient stream: Option[SparkDataStream],
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false,
    override val pushDownFilters: Option[Seq[Expression]] = None)
  extends FileSourceScanExecTransformerBase(
    relation,
    stream,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  override def getPartitions: Seq[Partition] =
    BackendsApiManager.getTransformerApiInstance
      .genPartitionSeq(
        relation,
        requiredSchema,
        selectedPartitions,
        output,
        bucketedScan,
        optionalBucketSet,
        optionalNumCoalescedBuckets,
        disableBucketedScan)

  override def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)] =
    getPartitions.map((_, fileFormat))

  override val nodeNamePrefix: String = "TestFile"

  override def withNewPushdownFilters(filters: Seq[Expression]): BasicScanExecTransformer =
    copy(pushDownFilters = Some(filters))
}

case class CustomerColumnarPreRules(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case fileSourceScan: FileSourceScanExec =>
      val transformer = new TestFileSourceScanExecTransformer(
        fileSourceScan.relation,
        SparkShimLoader.getSparkShims.getFileSourceScanStream(fileSourceScan),
        fileSourceScan.output,
        fileSourceScan.requiredSchema,
        fileSourceScan.partitionFilters,
        fileSourceScan.optionalBucketSet,
        fileSourceScan.optionalNumCoalescedBuckets,
        fileSourceScan.dataFilters,
        fileSourceScan.tableIdentifier,
        fileSourceScan.disableBucketedScan
      )
      if (transformer.doValidate().ok()) {
        transformer
      } else {
        plan
      }
  }
}
