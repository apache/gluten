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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.GlutenBuildInfo._
import org.apache.gluten.backendsapi._
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.expression.WindowFunctionsBuilder
import org.apache.gluten.extension.columnar.cost.{LegacyCoster, LongCoster}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionFunc}
import org.apache.gluten.substrait.rel.LocalFilesNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.hadoop.conf.Configuration

import java.util.Locale

import scala.util.control.Breaks.{break, breakable}

class CHBackend extends SubstraitBackend {
  import CHBackend._
  override def name(): String = CHConfig.BACKEND_NAME
  override def info(): Map[String, String] = {
    Map(
      "ch_branch" -> CH_BRANCH,
      "ch_revision" -> CH_COMMIT
    )
  }
  override def iteratorApi(): IteratorApi = new CHIteratorApi
  override def sparkPlanExecApi(): SparkPlanExecApi = new CHSparkPlanExecApi
  override def transformerApi(): TransformerApi = new CHTransformerApi
  override def validatorApi(): ValidatorApi = new CHValidatorApi
  override def metricsApi(): MetricsApi = new CHMetricsApi
  override def listenerApi(): ListenerApi = new CHListenerApi
  override def ruleApi(): RuleApi = new CHRuleApi
  override def settings(): BackendSettingsApi = CHBackendSettings
  override def convFuncOverride(): ConventionFunc.Override = new ConvFunc()
  override def costers(): Seq[LongCoster] = Seq(LegacyCoster)
}

object CHBackend {
  private class ConvFunc() extends ConventionFunc.Override {
    override def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
      case a: AdaptiveSparkPlanExec if a.supportsColumnar =>
        CHBatchType
    }
  }
}

object CHBackendSettings extends BackendSettingsApi with Logging {
  override def primaryBatchType: Convention.BatchType = CHBatchType

  val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD = "spark.gluten.sql.columnar.separate.scan.rdd.for.ch"
  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT = "false"

  // experimental: when the files count per partition exceeds this threshold,
  // it will put the files into one partition.
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD: String =
    CHConfig.prefixOf("files.per.partition.threshold")
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT = "-1"

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE: String =
    CHConfig.prefixOf("customized.shuffle.codec.enable")
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT = false
  lazy val useCustomizedShuffleCodec: Boolean = SparkEnv.get.conf.getBoolean(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT
  )

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE: String =
    CHConfig.prefixOf("customized.buffer.size")
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT = 4096
  lazy val customizeBufferSize: Int = SparkEnv.get.conf.getInt(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT
  )

  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME: String =
    CHConfig.prefixOf("broadcast.cache.expired.time")
  // unit: SECONDS, default 1 day
  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT: Int = 86400

  private val GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC: Set[String] = Set("lz4", "zstd", "snappy")

  // The algorithm for hash partition of the shuffle
  val GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM: String =
    CHConfig.prefixOf("shuffle.hash.algorithm")
  // valid values are: cityHash64 or sparkMurmurHash3_32
  private val GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT = "sparkMurmurHash3_32"
  def shuffleHashAlgorithm: String = {
    val algorithm = SparkEnv.get.conf.get(
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM,
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT
    )
    if (!algorithm.equals("cityHash64") && !algorithm.equals("sparkMurmurHash3_32")) {
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT
    } else {
      algorithm
    }
  }

  private val GLUTEN_CLICKHOUSE_AFFINITY_MODE: String = CHConfig.prefixOf("affinity.mode")
  val SOFT: String = "soft"
  val FORCE: String = "force"
  private val GLUTEN_CLICKHOUSE_AFFINITY_MODE_DEFAULT = SOFT

  private val GLUTEN_MAX_BLOCK_SIZE: String = CHConfig.runtimeSettings("max_block_size")
  // Same as default value in clickhouse
  private val GLUTEN_MAX_BLOCK_SIZE_DEFAULT = 65409
  private val GLUTEN_MAX_SHUFFLE_READ_BYTES: String =
    CHConfig.runtimeConfig("max_source_concatenate_bytes")
  private val GLUTEN_MAX_SHUFFLE_READ_BYTES_DEFAULT = GLUTEN_MAX_BLOCK_SIZE_DEFAULT * 256

  val GLUTEN_AQE_PROPAGATEEMPTY: String = CHConfig.prefixOf("aqe.propagate.empty.relation")

  val GLUTEN_CLICKHOUSE_DELTA_SCAN_CACHE_SIZE: String = CHConfig.prefixOf("deltascan.cache.size")
  val GLUTEN_CLICKHOUSE_ADDFILES_TO_MTPS_CACHE_SIZE: String =
    CHConfig.prefixOf("addfiles.to.mtps.cache.size")
  val GLUTEN_CLICKHOUSE_TABLE_PATH_TO_MTPS_CACHE_SIZE: String =
    CHConfig.prefixOf("table.path.to.mtps.cache.size")

  val GLUTEN_CLICKHOUSE_DELTA_METADATA_OPTIMIZE: String =
    CHConfig.prefixOf("delta.metadata.optimize")
  val GLUTEN_CLICKHOUSE_DELTA_METADATA_OPTIMIZE_DEFAULT_VALUE: String = "true"

  val GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT: String =
    CHConfig.prefixOf("convert.left.anti_semi.to.right")
  val GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT_DEFAULT_VALUE: String = "false"

  val GLUTEN_ENABLE_COALESCE_AGGREGATION_UNION: String =
    CHConfig.prefixOf("enable.coalesce.aggregation.union")
  val GLUTEN_ENABLE_COALESCE_PROJECT_UNION: String =
    CHConfig.prefixOf("enable.coalesce.project.union")
  val GLUTEN_JOIN_AGGREGATE_TO_AGGREGATE_UNION: String =
    CHConfig.prefixOf("join.aggregate.to.aggregate.union")
  val GLUTEN_ELIMINATE_DEDUPLICATE_AGGREGATE_WITH_ANY_JOIN: String =
    CHConfig.prefixOf("eliminate_deduplicate_aggregate_with_any_join")

  // If the partition keys are high cardinality, the aggregation method is slower.
  val GLUTEN_ENABLE_WINDOW_GROUP_LIMIT_TO_AGGREGATE: String =
    CHConfig.prefixOf("runtime_settings.enable_window_group_limit_to_aggregate")

  def affinityMode: String = {
    SparkEnv.get.conf
      .get(
        CHBackendSettings.GLUTEN_CLICKHOUSE_AFFINITY_MODE,
        CHBackendSettings.GLUTEN_CLICKHOUSE_AFFINITY_MODE_DEFAULT
      )
      .toLowerCase(Locale.getDefault)
  }

  override def validateScanExec(
      format: ReadFileFormat,
      fields: Array[StructField],
      dataSchema: StructType,
      rootPaths: Seq[String],
      properties: Map[String, String],
      hadoopConf: Configuration,
      partitionFileFormats: Set[ReadFileFormat]): ValidationResult = {

    // Validate if all types are supported.
    def hasComplexType: Boolean = {
      // Collect unsupported types.
      val unsupportedDataTypes = fields.map(_.dataType).collect {
        case _: MapType => "MapType"
        case _: StructType => "StructType"
        case _: ArrayType => "ArrayType"
      }
      for (unsupportedDataType <- unsupportedDataTypes) {
        // scalastyle:off println
        println(
          s"Validation failed for ${this.getClass.toString}" +
            s" due to: data type $unsupportedDataType. in file schema. ")
        // scalastyle:on println
      }
      !unsupportedDataTypes.isEmpty
    }

    def checkFormat(format: ReadFileFormat): ValidationResult =
      format match {
        case ParquetReadFormat => ValidationResult.succeeded
        case OrcReadFormat => ValidationResult.succeeded
        case MergeTreeReadFormat => ValidationResult.succeeded
        case TextReadFormat =>
          if (!hasComplexType) {
            ValidationResult.succeeded
          } else {
            ValidationResult.failed("Has complex type.")
          }
        case JsonReadFormat => ValidationResult.succeeded
        case KafkaReadFormat => ValidationResult.succeeded
        case _ => ValidationResult.failed(s"Unsupported file format $format")
      }

    val distinctFileFormats = partitionFileFormats + format
    distinctFileFormats
      .collectFirst {
        case format if !checkFormat(format).ok() => checkFormat(format)
      }
      .getOrElse(ValidationResult.succeeded)
  }

  override def getSubstraitReadFileFormatV1(
      fileFormat: FileFormat): LocalFilesNode.ReadFileFormat = {
    fileFormat.getClass.getSimpleName match {
      case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DeltaParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DeltaMergeTreeFileFormat" => ReadFileFormat.MergeTreeReadFormat
      case "CSVFileFormat" => ReadFileFormat.TextReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  override def getSubstraitReadFileFormatV2(scan: Scan): LocalFilesNode.ReadFileFormat = {
    scan.getClass.getSimpleName match {
      case "OrcScan" => ReadFileFormat.OrcReadFormat
      case "ParquetScan" => ReadFileFormat.ParquetReadFormat
      case "ClickHouseScan" => ReadFileFormat.MergeTreeReadFormat
      case "KafkaScan" => ReadFileFormat.KafkaReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  override def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      isPartitionedTable: Boolean,
      options: Map[String, String]): ValidationResult = {

    def validateCompressionCodec(): Option[String] = {
      // FIXME: verify Support compression codec
      val compressionCodec = WriteFilesExecTransformer.getCompressionCodec(options)
      None
    }

    def validateFileFormat(): Option[String] = {
      format match {
        case _: ParquetFileFormat => None
        case _: OrcFileFormat => None
        case f: FileFormat => Some(s"Not support FileFormat: ${f.getClass.getSimpleName}")
      }
    }

    // Validate if all types are supported.
    def validateDateTypes(): Option[String] = {
      None
    }

    def validateFieldMetadata(): Option[String] = {
      // copy CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
      val CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING"
      fields
        .find(_.metadata != Metadata.empty)
        .filterNot(_.metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY))
        .map {
          filed =>
            s"StructField contain the metadata information: $filed, metadata: ${filed.metadata}"
        }
    }
    def validateWriteFilesOptions(): Option[String] = {
      val maxRecordsPerFile = options
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(SQLConf.get.maxRecordsPerFile)
      if (maxRecordsPerFile > 0) {
        Some("Unsupported native write: maxRecordsPerFile not supported.")
      } else {
        None
      }
    }

    validateCompressionCodec()
      .orElse(validateFileFormat())
      .orElse(validateFieldMetadata())
      .orElse(validateDateTypes())
      .orElse(validateWriteFilesOptions()) match {
      case Some(reason) => ValidationResult.failed(reason)
      case _ => ValidationResult.succeeded
    }
  }

  override def supportSortExec(): Boolean = {
    GlutenConfig.get.enableColumnarSort
  }

  override def supportSortMergeJoinExec(): Boolean = {
    GlutenConfig.get.enableColumnarSortMergeJoin
  }

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var allSupported = true
    breakable {
      windowFunctions.foreach(
        func => {
          val aliasExpr = func.asInstanceOf[Alias]
          val wExpression = WindowFunctionsBuilder.extractWindowExpression(aliasExpr.child)

          def checkLagOrLead(third: Expression): Unit = {
            third match {
              case _: Literal =>
                allSupported = allSupported
              case _ =>
                logInfo("Not support lag/lead function with default value not literal null")
                allSupported = false
                break
            }
          }

          wExpression.windowFunction match {
            case _: RowNumber | _: AggregateExpression | _: Rank | _: DenseRank | _: PercentRank |
                _: NTile =>
              allSupported = allSupported
            case l: Lag =>
              checkLagOrLead(l.third)
            case l: Lead =>
              checkLagOrLead(l.third)
            case _ =>
              logDebug(s"Not support window function: ${wExpression.getClass}")
              allSupported = false
              break
          }
        })
    }
    allSupported
  }

  override def supportStructType(): Boolean = true

  override def structFieldToLowerCase(): Boolean = false

  override def supportExpandExec(): Boolean = true

  override def excludeScanExecFromCollapsedStage(): Boolean =
    SQLConf.get
      .getConfString(GLUTEN_CLICKHOUSE_SEP_SCAN_RDD, GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT)
      .toBoolean

  override def shuffleSupportedCodec(): Set[String] = GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC
  override def needOutputSchemaForPlan(): Boolean = true

  override def transformCheckOverflow: Boolean = false

  override def requireBloomFilterAggMightContainJointFallback(): Boolean = false

  def maxShuffleReadRows(): Long = {
    SparkEnv.get.conf
      .getLong(GLUTEN_MAX_BLOCK_SIZE, GLUTEN_MAX_BLOCK_SIZE_DEFAULT)
  }

  def maxShuffleReadBytes(): Long = {
    SparkEnv.get.conf
      .getLong(GLUTEN_MAX_SHUFFLE_READ_BYTES, GLUTEN_MAX_SHUFFLE_READ_BYTES_DEFAULT)
  }

  // Move the pre-prejection for a aggregation ahead of the expand node
  // for example, select a, b, sum(c+d) from t group by a, b with cube
  def enablePushdownPreProjectionAheadExpand(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.prefixOf("enable_pushdown_preprojection_ahead_expand"),
      defaultValue = true
    )
  }

  // It try to move the expand node after the pre-aggregate node. That is to make the plan from
  //  expand -> pre-aggregate -> shuffle -> final-aggregate
  // to
  //  pre-aggregate -> expand -> shuffle -> final-aggregate
  // It could reduce the overhead of pre-aggregate node.
  def enableLazyAggregateExpand(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.runtimeConfig("enable_lazy_aggregate_expand"),
      defaultValue = true
    )
  }

  def enablePreProjectionForJoinConditions(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.runtimeConfig("enable_pre_projection_for_join_conditions"),
      defaultValue = true
    )
  }

  def enableReplaceFromJsonWithGetJsonObject(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.runtimeConfig("enable_replace_from_json_with_get_json_object"),
      defaultValue = true
    )
  }

  def eliminateDeduplicateAggregateWithAnyJoin(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.runtimeConfig("eliminate_deduplicate_aggregate_with_any_join"),
      defaultValue = true
    )
  }

  def enableSimplifySum(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.runtimeConfig("enable_simplify_sum"),
      defaultValue = true
    )
  }

  def enableAggregateIfToFilter(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConfig.runtimeConfig("enable_aggregate_if_to_filter"),
      defaultValue = false
    )
  }

  override def enableNativeWriteFiles(): Boolean = {
    GlutenConfig.get.enableNativeWriter.getOrElse(false)
  }

  override def supportCartesianProductExec(): Boolean = true

  override def supportCartesianProductExecWithCondition(): Boolean = false

  override def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnLeft(t)) {
        true
      } else {
        t match {
          case LeftAnti | LeftSemi
              if (SQLConf.get
                .getConfString(
                  GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT,
                  GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT_DEFAULT_VALUE)
                .toBoolean) =>
            true
          case LeftOuter => true
          case _ => false
        }
      }
  }

  override def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnRight(t)) {
        true
      } else {
        t match {
          case RightOuter => true
          case _ => false
        }
      }
  }

  override def supportWindowGroupLimitExec(rankLikeFunction: Expression): Boolean = true
}
