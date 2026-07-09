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

#include <jni.h>

#include <bolt/connectors/hive/PartitionIdGenerator.h>
#include <bolt/exec/OperatorUtils.h>
#include <bolt/vector/ComplexVector.h>
#include <glog/logging.h>
#include <jni/JniCommon.h>

#include <exception>
#include "BoltInputIterator.h"
#include "JniUdf.h"
#include "bolt/common/base/BloomFilter.h"
#include "bolt/common/file/FileSystems.h"
#include "compute/BoltBackend.h"
#include "compute/BoltRuntime.h"
#include "compute/Runtime.h"
#include "config/GlutenConfig.h"
#include "jni/JniError.h"
#include "jni/JniFileSystem.h"
#include "jni/JniWrapper.h"
#include "memory/BoltColumnarBatch.h"
#include "memory/BoltGlutenMemoryManager.h"
#include "memory/BoltMemoryManager.h"
#include "memory/OnHeapUsageGetter.h"
#include "shuffle/BoltShuffleReaderWrapper.h"
#include "shuffle/BoltShuffleWriterWrapper.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "shuffle_reader_info.pb.h"
#include "shuffle_writer_info.pb.h"
#include "substrait/SubstraitToBoltPlanValidator.h"
#include "utils/BoltBatchResizer.h"
#include "utils/ObjectStore.h"

#ifdef GLUTEN_ENABLE_GPU
#include "cudf/CudfPlanValidator.h"
#endif

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "IcebergNestedField.pb.h"
#endif

using namespace gluten;
using namespace bytedance;

static jclass shuffleReaderMetricsClass;
static jmethodID shuffleReaderMetricsSetDecompressTime;
static jmethodID shuffleReaderMetricsSetDeserializeTime;

static jclass splitResultClass;
static jmethodID splitResultConstructor;

namespace {
jclass infoCls;
jmethodID infoClsInitMethod;

jclass blockStripesClass;
jmethodID blockStripesConstructor;

} // namespace

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void*) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }

  jint jniVersion = JNI_OnLoad_Base(vm, nullptr);
  if (jniVersion == JNI_ERR) {
    return JNI_ERR;
  }

  initBoltJniFileSystem(env);
  initBoltJniUDF(env);
  registerBoltInputIteratorFactory();
  gluten::OnHeapMemUsedHookSetter::init(vm);

  infoCls = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/validate/NativePlanValidationInfo;");
  infoClsInitMethod = getMethodIdOrError(env, infoCls, "<init>", "(ILjava/lang/String;)V");

  blockStripesClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;");
  blockStripesConstructor = getMethodIdOrError(env, blockStripesClass, "<init>", "(J[J[II[[B)V");

  shuffleReaderMetricsClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/shuffle/BoltShuffleReaderMetrics;");
  shuffleReaderMetricsSetDecompressTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDecompressTime", "(J)V");
  shuffleReaderMetricsSetDeserializeTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDeserializeTime", "(J)V");

  splitResultClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/shuffle/BoltSplitResult;");
  splitResultConstructor = getMethodIdOrError(env, splitResultClass, "<init>", "(JJJJJJJJJJJJJJJJJ[J[J)V");

  DLOG(INFO) << "Loaded Bolt backend.";

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void*) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);

  env->DeleteGlobalRef(splitResultClass);
  env->DeleteGlobalRef(shuffleReaderMetricsClass);
  env->DeleteGlobalRef(blockStripesClass);
  env->DeleteGlobalRef(infoCls);

  finalizeBoltJniUDF(env);
  finalizeBoltJniFileSystem(env);

  JNI_OnUnload_Base(vm, nullptr);

  google::ShutdownGoogleLogging();
}

JNIEXPORT void JNICALL Java_org_apache_gluten_init_NativeBackendInitializer_initialize( // NOLINT
    JNIEnv* env,
    jclass,
    jobject jListener,
    jbyteArray conf) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }
  auto safeArray = getByteArrayElementsSafe(env, conf);
  // Create a global allocation listener that reserves global off-heap memory from Java-side GlobalOffHeapMemory utility
  // class.
  std::unique_ptr<AllocationListener> listener = std::make_unique<SparkAllocationListener>(vm, jListener);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  BoltBackend::create(std::move(listener), sparkConf);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_init_NativeBackendInitializer_shutdown( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  BoltBackend::get()->tearDown();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_udf_UdfJniWrapper_registerFunctionSignatures( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  jniRegisterFunctionSignatures(env);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeValidateWithFailureReason( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArray) {
  JNI_METHOD_START
  const auto ctx = getRuntime(env, wrapper);
  const auto safeArray = getByteArrayElementsSafe(env, planArray);
  const auto planData = safeArray.elems();
  const auto planSize = env->GetArrayLength(planArray);
  const auto runtime = dynamic_cast<BoltRuntime*>(ctx);
  if (runtime->debugModeEnabled()) {
    try {
      const auto jsonPlan = substraitFromPbToJson("Plan", planData, planSize);
      LOG(INFO) << std::string(50, '#') << " received substrait::Plan: for validation";
      LOG(INFO) << jsonPlan;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan for validation to JSON: " << e.what();
    }
  }

  const auto pool = defaultLeafBoltMemoryPool().get();
  SubstraitToBoltPlanValidator planValidator(pool, runtime->getConfMap());
  ::substrait::Plan subPlan;
  parseProtobuf(planData, planSize, &subPlan);

  try {
    const auto isSupported = planValidator.validate(subPlan);
    const auto logs = planValidator.getValidateLog();
    std::string concatLog;
    for (int i = 0; i < logs.size(); i++) {
      concatLog += logs[i] + "@";
    }
    return env->NewObject(infoCls, infoClsInitMethod, isSupported, env->NewStringUTF(concatLog.c_str()));
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Failed to validate substrait plan because " << e.what();
    return env->NewObject(infoCls, infoClsInitMethod, false, env->NewStringUTF(""));
  }
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_BoltColumnarBatchJniWrapper_from( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<BoltRuntime*>(ctx);

  auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
  auto newBatch = BoltColumnarBatch::from(runtime->memoryManager()->getLeafMemoryPool().get(), batch);
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_BoltColumnarBatchJniWrapper_compose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray batchHandles) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<BoltRuntime*>(ctx);

  int handleCount = env->GetArrayLength(batchHandles);
  auto safeArray = getLongArrayElementsSafe(env, batchHandles);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (int i = 0; i < handleCount; ++i) {
    int64_t handle = safeArray.elems()[i];
    auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
    batches.push_back(batch);
  }
  auto newBatch = BoltColumnarBatch::compose(runtime->memoryManager()->getLeafMemoryPool().get(), std::move(batches));
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_BoltColumnarBatchJniWrapper_repeatedThenCompose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong repeatedBatchHandle,
    jlong nonRepeatedBatchHandle,
    jintArray rowId2RowNums) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<BoltRuntime*>(ctx);

  int rowId2RowNumsSize = env->GetArrayLength(rowId2RowNums);
  auto safeRowId2RowNumsArray = getIntArrayElementsSafe(env, rowId2RowNums);

  auto boltPool = runtime->memoryManager()->getLeafMemoryPool();
  vector_size_t rowNums = 0;
  for (int i = 0; i < rowId2RowNumsSize; ++i) {
    rowNums += safeRowId2RowNumsArray.elems()[i];
  }

  // Create a indices vector.
  // The indices will be used to create a dictionary vector for the first batch.
  auto repeatedIndices = AlignedBuffer::allocate<vector_size_t>(rowNums, boltPool.get(), 0);
  auto* rawRepeatedIndices = repeatedIndices->asMutable<vector_size_t>();
  int lastRowIndexEnd = 0;
  for (int i = 0; i < rowId2RowNumsSize; ++i) {
    auto rowNum = safeRowId2RowNumsArray.elems()[i];
    std::fill(rawRepeatedIndices + lastRowIndexEnd, rawRepeatedIndices + lastRowIndexEnd + rowNum, i);
    lastRowIndexEnd += rowNum;
  }

  auto repeatedBatch = ObjectStore::retrieve<ColumnarBatch>(repeatedBatchHandle);
  auto nonRepeatedBatch = ObjectStore::retrieve<ColumnarBatch>(nonRepeatedBatchHandle);
  GLUTEN_CHECK(rowNums == nonRepeatedBatch->numRows(), "Row numbers after repeated do not match the expected size");

  // wrap repeatedBatch's rowVector in dictionary vector.
  auto vb = std::dynamic_pointer_cast<BoltColumnarBatch>(repeatedBatch);
  auto rowVector = vb->getRowVector();
  std::vector<VectorPtr> outputs(rowVector->childrenSize());
  for (int i = 0; i < outputs.size(); i++) {
    outputs[i] = BaseVector::wrapInDictionary(nullptr /*nulls*/, repeatedIndices, rowNums, rowVector->childAt(i));
  }
  auto newRowVector =
      std::make_shared<RowVector>(boltPool.get(), rowVector->type(), BufferPtr(nullptr), rowNums, std::move(outputs));
  repeatedBatch = std::make_shared<BoltColumnarBatch>(std::move(newRowVector));
  auto newBatch = BoltColumnarBatch::compose(boltPool.get(), {std::move(repeatedBatch), std::move(nonRepeatedBatch)});
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_empty( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint capacity) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto filter = std::make_shared<bolt::BloomFilter<std::allocator<uint64_t>>>();
  filter->reset(capacity);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  return ctx->saveObject(filter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray data) {
  JNI_METHOD_START
  auto safeArray = getByteArrayElementsSafe(env, data);
  auto ctx = getRuntime(env, wrapper);
  auto filter = std::make_shared<bolt::BloomFilter<std::allocator<uint64_t>>>();
  uint8_t* serialized = safeArray.elems();
  filter->merge(reinterpret_cast<char*>(serialized));
  return ctx->saveObject(filter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_insertLong( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong item) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<bolt::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  filter->insert(folly::hasher<int64_t>()(item));
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_mightContainLong( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong item) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<bolt::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  bool out = filter->mayContain(folly::hasher<int64_t>()(item));
  return out;
  JNI_METHOD_END(false)
}

JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_mightContainLongOnSerializedBloom( // NOLINT
    JNIEnv* env,
    jclass,
    jlong address,
    jlong item) {
  JNI_METHOD_START

  auto bloomMask = [](uint64_t hashCode) -> uint64_t {
    return (1L << (hashCode & 63)) | (1L << ((hashCode >> 6) & 63)) | (1L << ((hashCode >> 12) & 63)) |
        (1L << ((hashCode >> 18) & 63));
  };

  auto bloomIndex = [](uint32_t bloomSize, uint64_t hashCode) -> uint32_t {
    return ((hashCode >> 24) & (bloomSize - 1));
  };

  auto test = [&](const uint64_t* bloom, int32_t bloomSize, uint64_t hashCode) -> bool {
    auto mask = bloomMask(hashCode);
    auto index = bloomIndex(bloomSize, hashCode);
    return mask == (bloom[index] & mask);
  };

  auto mayContain = [&](const char* serializedBloom, uint64_t value) -> bool {
    static constexpr int8_t kBloomFilterV1 = 1;
    common::InputByteStream stream(serializedBloom);
    const auto version = stream.read<int8_t>();
    BOLT_USER_CHECK_EQ(kBloomFilterV1, version);
    const auto size = stream.read<int32_t>();
    BOLT_USER_CHECK_GT(size, 0);
    const uint64_t* bloomBits = reinterpret_cast<const uint64_t*>(serializedBloom + stream.offset());
    return test(bloomBits, size, value);
  };

  bool out = mayContain(reinterpret_cast<const char*>(address), folly::hasher<int64_t>()(item));
  return out;
  JNI_METHOD_END(false)
}

namespace {
static std::vector<char> serialize(BloomFilter<std::allocator<uint64_t>>* bf) {
  uint32_t size = bf->serializedSize();
  std::vector<char> buffer;
  buffer.reserve(size);
  char* data = buffer.data();
  bf->serialize(data);
  return buffer;
}
} // namespace

JNIEXPORT void JNICALL Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_mergeFrom( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong other) {
  JNI_METHOD_START
  auto to = ObjectStore::retrieve<bolt::BloomFilter<std::allocator<uint64_t>>>(handle);
  auto from = ObjectStore::retrieve<bolt::BloomFilter<std::allocator<uint64_t>>>(other);
  GLUTEN_CHECK(to->isSet(), "Bloom-filter is not initialized");
  GLUTEN_CHECK(from->isSet(), "Bloom-filter is not initialized");
  std::vector<char> serialized = serialize(from.get());
  to->merge(serialized.data());
  JNI_METHOD_END()
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_utils_BoltBloomFilterJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<bolt::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  std::vector<char> buffer = serialize(filter.get());
  auto size = buffer.capacity();
  jbyteArray out = env->NewByteArray(size);
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer.data()));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_BoltBatchResizerJniWrapper_create( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint minOutputBatchSize,
    jint maxOutputBatchSize,
    jobject jIter) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto pool = dynamic_cast<BoltMemoryManager*>(ctx->memoryManager())->getLeafMemoryPool();
  auto iter = makeJniColumnarBatchIterator(env, jIter, ctx);
  auto appender = std::make_shared<ResultIterator>(
      std::make_unique<BoltBatchResizer>(pool.get(), minOutputBatchSize, maxOutputBatchSize, std::move(iter)));
  return ctx->saveObject(appender);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_utils_BoltFileSystemValidationJniWrapper_allSupportedByRegisteredFileSystems( // NOLINT
    JNIEnv* env,
    jclass,
    jobjectArray stringArray) {
  JNI_METHOD_START
  // TODO sync bolt and uncomment it (https://github.com/apache/incubator-gluten/pull/6672)
  // int size = env->GetArrayLength(stringArray);
  // for (int i = 0; i < size; i++) {
  //   jstring string = (jstring)(env->GetObjectArrayElement(stringArray, i));
  //   std::string path = jStringToCString(env, string);
  //   if (!bolt::filesystems::isPathSupportedByRegisteredFileSystems(path)) {
  //     return false;
  //   }
  // }
  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_datasource_BoltDataSourceJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring filePath,
    jlong cSchema,
    jbyteArray options,
    jstring encryptionAlgoStr,
    jobjectArray encryptionOptionKeys,
    jobjectArray encryptionOptionValues) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto runtime = dynamic_cast<BoltRuntime*>(ctx);

  ObjectHandle handle = kInvalidObjectHandle;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    handle = ctx->saveObject(runtime->createDataSource(jStringToCString(env, filePath), nullptr));
  } else {
    auto safeArray = gluten::getByteArrayElementsSafe(env, options);
    auto datasourceOptions = gluten::parseConfMap(env, safeArray.elems(), safeArray.length());
    auto& sparkConf = ctx->getConfMap();
    datasourceOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    handle = ctx->saveObject(runtime->createDataSource(jStringToCString(env, filePath), schema));

    auto encryptionAlgo = jStringToCString(env, encryptionAlgoStr);
    if (encryptionAlgo != "") {
      auto encryptionKeys = ToStringVector(env, encryptionOptionKeys);
      auto encryptionValues = FromByteArrToStringVector(env, encryptionOptionValues);
      for (unsigned int j = 0; j < encryptionKeys.size(); ++j) {
        datasourceOptions[encryptionKeys[j]] = encryptionValues[j];
      }
      datasourceOptions["algorithm"] = encryptionAlgo;
    }

    auto datasource = ObjectStore::retrieve<BoltDataSource>(handle);
    datasource->init(datasourceOptions);
  }

  return handle;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_BoltDataSourceJniWrapper_inspectSchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong cSchema) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<BoltDataSource>(dsHandle);
  datasource->inspectSchema(reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_BoltDataSourceJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<BoltDataSource>(dsHandle);
  datasource->close();
  ObjectStore::release(dsHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_BoltDataSourceJniWrapper_writeBatch( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong batchHandle) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<BoltDataSource>(dsHandle);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  datasource->write(batch);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_datasource_BoltDataSourceJniWrapper_splitBlockByPartitionAndBucket( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jintArray partitionColIndices,
    jboolean hasBucket) {
  JNI_METHOD_START

  GLUTEN_CHECK(!hasBucket, "Bucketing not supported by splitBlockByPartitionAndBucket");

  const auto ctx = gluten::getRuntime(env, wrapper);
  const auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);

  auto partitionKeyArray = gluten::getIntArrayElementsSafe(env, partitionColIndices);
  int numPartitionKeys = partitionKeyArray.length();
  std::vector<uint32_t> partitionColIndicesVec;
  for (int i = 0; i < numPartitionKeys; ++i) {
    const auto partitionColumnIndex = partitionKeyArray.elems()[i];
    GLUTEN_CHECK(partitionColumnIndex < batch->numColumns(), "Partition column index overflow");
    partitionColIndicesVec.emplace_back(partitionColumnIndex);
  }

  std::vector<int32_t> dataColIndicesVec;
  for (int i = 0; i < batch->numColumns(); ++i) {
    if (std::find(partitionColIndicesVec.begin(), partitionColIndicesVec.end(), i) == partitionColIndicesVec.end()) {
      // The column is not a partition column. Add it to the data column vector.
      dataColIndicesVec.emplace_back(i);
    }
  }

  auto pool = dynamic_cast<BoltMemoryManager*>(ctx->memoryManager())->getLeafMemoryPool();
  const auto boltBatch = BoltColumnarBatch::from(pool.get(), batch);
  const auto inputRowVector = boltBatch->getRowVector();
  const auto numRows = inputRowVector->size();

  connector::hive::PartitionIdGenerator idGen{
      asRowType(inputRowVector->type()), partitionColIndicesVec, 128, pool.get(), true};
  raw_vector<uint64_t> partitionIds{};
  idGen.run(inputRowVector, partitionIds);
  GLUTEN_CHECK(partitionIds.size() == numRows, "Mismatched number of partition ids");
  const auto numPartitions = static_cast<int32_t>(idGen.numPartitions());

  std::vector<vector_size_t> partitionSizes(numPartitions);
  std::vector<BufferPtr> partitionRows(numPartitions);
  std::vector<vector_size_t*> rawPartitionRows(numPartitions);
  std::fill(partitionSizes.begin(), partitionSizes.end(), 0);

  for (auto row = 0; row < numRows; ++row) {
    const auto partitionId = partitionIds[row];
    ++partitionSizes[partitionId];
  }

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    partitionRows[partitionId] = allocateIndices(partitionSizes[partitionId], pool.get());
    rawPartitionRows[partitionId] = partitionRows[partitionId]->asMutable<vector_size_t>();
  }

  std::vector<vector_size_t> partitionNextRowOffset(numPartitions);
  std::fill(partitionNextRowOffset.begin(), partitionNextRowOffset.end(), 0);
  for (auto row = 0; row < numRows; ++row) {
    const auto partitionId = partitionIds[row];
    rawPartitionRows[partitionId][partitionNextRowOffset[partitionId]] = row;
    ++partitionNextRowOffset[partitionId];
  }

  jobjectArray partitionHeadingRowBytesArray = env->NewObjectArray(numPartitions, env->FindClass("[B"), nullptr);
  std::vector<jlong> partitionBatchHandles(numPartitions);

  auto execWrap = [](vector_size_t size, BufferPtr mapping, const RowVectorPtr& vector) -> RowVectorPtr {
    if (!mapping) {
      return vector;
    }

    std::vector<VectorPtr> wrappedChildren;
    const auto& childVectors = vector->children();
    wrappedChildren.reserve(childVectors.size());
    for (auto& child : childVectors) {
      wrappedChildren.emplace_back(exec::wrapChild(size, mapping, child));
    }
    return std::make_shared<RowVector>(vector->pool(), asRowType(vector->type()), nullptr, size, wrappedChildren);
  };

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    const vector_size_t partitionSize = partitionSizes[partitionId];
    if (partitionSize == 0) {
      continue;
    }

    const RowVectorPtr rowVector = partitionSize == inputRowVector->size()
        ? inputRowVector
        : execWrap(partitionSize, partitionRows[partitionId], inputRowVector);

    const std::shared_ptr<BoltColumnarBatch> partitionBatch = std::make_shared<BoltColumnarBatch>(rowVector);
    const std::shared_ptr<BoltColumnarBatch> partitionBatchWithoutPartitionColumns =
        partitionBatch->select(pool.get(), dataColIndicesVec);
    partitionBatchHandles[partitionId] = ctx->saveObject(partitionBatchWithoutPartitionColumns);
    const auto headingRow = partitionBatch->toUnsafeRow(0);
    const auto headingRowBytes = headingRow.data();
    const auto headingRowNumBytes = headingRow.size();

    jbyteArray jHeadingRowBytes = env->NewByteArray(headingRowNumBytes);
    env->SetByteArrayRegion(jHeadingRowBytes, 0, headingRowNumBytes, reinterpret_cast<const jbyte*>(headingRowBytes));
    env->SetObjectArrayElement(partitionHeadingRowBytesArray, partitionId, jHeadingRowBytes);
  }

  jlongArray partitionBatchArray = env->NewLongArray(numPartitions);
  env->SetLongArrayRegion(partitionBatchArray, 0, numPartitions, partitionBatchHandles.data());

  jobject blockStripes = env->NewObject(
      blockStripesClass,
      blockStripesConstructor,
      batchHandle,
      partitionBatchArray,
      nullptr,
      batch->numColumns(),
      partitionHeadingRowBytesArray);
  return blockStripes;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_BoltColumnarBatchJniWrapper_slice( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong boltBatchHandle,
    jint offset,
    jint limit) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(boltBatchHandle);

  auto numRows = batch->numRows();
  if (limit >= numRows) {
    return boltBatchHandle;
  }

  auto boltBatch = std::dynamic_pointer_cast<BoltColumnarBatch>(batch);
  BOLT_CHECK_NOT_NULL(boltBatch, "Expected BoltColumnarBatch but got a different type.");

  auto rowVector = boltBatch->getRowVector();
  auto prunedVector = rowVector->slice(offset, limit);

  auto prunedRowVector = std::dynamic_pointer_cast<bytedance::bolt::RowVector>(prunedVector);
  BOLT_CHECK_NOT_NULL(prunedRowVector, "Expected RowVector but got a different type.");

  auto prunedBatch = std::make_shared<BoltColumnarBatch>(prunedRowVector);

  jlong prunedHandle = ctx->saveObject(prunedBatch);
  return prunedHandle;

  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_monitor_BoltMemoryProfiler_start( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
#ifdef ENABLE_JEMALLOC_STATS
  bool active = true;
  mallctl("prof.active", NULL, NULL, &active, sizeof(bool));
#endif
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_monitor_BoltMemoryProfiler_dump( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
#ifdef ENABLE_JEMALLOC_STATS
  mallctl("prof.dump", NULL, NULL, NULL, 0);
#endif
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_monitor_BoltMemoryProfiler_stop( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
#ifdef ENABLE_JEMALLOC_STATS
  bool active = false;
  mallctl("prof.active", NULL, NULL, &active, sizeof(bool));
#endif
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_CelebornPartitionWriterJniWrapper_createPartitionWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint compressionBufferSize,
    jint pushBufferMaxSize,
    jlong sortBufferMaxSize,
    jobject partitionPusher) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }

  const auto ctx = getRuntime(env, wrapper);

  jclass celebornPartitionPusherClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
  jmethodID celebornPushPartitionDataMethod =
      getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
  std::shared_ptr<JavaRssClient> celebornClient =
      std::make_shared<JavaRssClient>(vm, partitionPusher, celebornPushPartitionDataMethod);

  auto partitionWriterOptions = std::make_shared<RssPartitionWriterOptions>(
      compressionBufferSize,
      pushBufferMaxSize > 0 ? pushBufferMaxSize : kDefaultPushMemoryThreshold,
      sortBufferMaxSize > 0 ? sortBufferMaxSize : kDefaultSortBufferThreshold);

  auto partitionWriter = std::make_shared<RssPartitionWriter>(
      numPartitions,
      createCompressionCodec(
          getCompressionType(env, codecJstr), getCodecBackend(env, codecBackendJstr), compressionLevel),
      ctx->memoryManager(),
      partitionWriterOptions,
      celebornClient);

  return ctx->saveObject(partitionWriter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_UnifflePartitionWriterJniWrapper_createPartitionWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint compressionBufferSize,
    jint pushBufferMaxSize,
    jlong sortBufferMaxSize,
    jobject partitionPusher) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }

  const auto ctx = getRuntime(env, wrapper);

  jclass unifflePartitionPusherClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/writer/PartitionPusher;");
  jmethodID unifflePushPartitionDataMethod =
      getMethodIdOrError(env, unifflePartitionPusherClass, "pushPartitionData", "(I[BI)I");
  std::shared_ptr<JavaRssClient> uniffleClient =
      std::make_shared<JavaRssClient>(vm, partitionPusher, unifflePushPartitionDataMethod);

  auto partitionWriterOptions = std::make_shared<RssPartitionWriterOptions>(
      compressionBufferSize,
      pushBufferMaxSize > 0 ? pushBufferMaxSize : kDefaultPushMemoryThreshold,
      sortBufferMaxSize > 0 ? sortBufferMaxSize : kDefaultSortBufferThreshold);

  auto partitionWriter = std::make_shared<RssPartitionWriter>(
      numPartitions,
      createCompressionCodec(
          getCompressionType(env, codecJstr), getCodecBackend(env, codecBackendJstr), compressionLevel),
      ctx->memoryManager(),
      partitionWriterOptions,
      uniffleClient);

  return ctx->saveObject(partitionWriter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_config_ConfigJniWrapper_isEnhancedFeaturesEnabled( // NOLINT
    JNIEnv* env,
    jclass) {
#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
  return true;
#else
  return false;
#endif
}

#ifdef GLUTEN_ENABLE_GPU
JNIEXPORT jboolean JNICALL Java_org_apache_gluten_cudf_BoltCudfPlanValidatorJniWrapper_validate( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray planArr) {
  JNI_METHOD_START
  auto safePlanArray = getByteArrayElementsSafe(env, planArr);
  auto planSize = env->GetArrayLength(planArr);
  ::substrait::Plan substraitPlan;
  parseProtobuf(safePlanArray.elems(), planSize, &substraitPlan);
  // get the task and driver, validate the plan, if return all operator except table scan is offloaded, validate true.
  return CudfPlanValidator::validate(substraitPlan);
  JNI_METHOD_END(false)
}
#endif

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
JNIEXPORT jlong JNICALL Java_org_apache_gluten_execution_IcebergWriteJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jint format,
    jstring directory,
    jstring codecJstr,
    jbyteArray partition,
    jbyteArray fieldBytes) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<BoltRuntime*>(ctx);
  auto backendConf = BoltBackend::get()->getBackendConf()->rawConfigs();
  auto sparkConf = ctx->getConfMap();
  sparkConf.merge(backendConf);
  auto safeArray = gluten::getByteArrayElementsSafe(env, partition);
  auto arrowSchema = reinterpret_cast<struct ArrowSchema*>(cSchema);
  auto rowType = asRowType(importFromArrow(*arrowSchema));
  ArrowSchemaRelease(arrowSchema);
  auto spec = parseIcebergPartitionSpec(safeArray.elems(), safeArray.length(), rowType);
  auto safeArrayField = gluten::getByteArrayElementsSafe(env, fieldBytes);
  gluten::IcebergNestedField protoField;
  gluten::parseProtobuf(safeArrayField.elems(), safeArrayField.length(), &protoField);
  return ctx->saveObject(runtime->createIcebergWriter(
      rowType,
      format,
      jStringToCString(env, directory),
      bytedance::bolt::common::stringToCompressionKind(jStringToCString(env, codecJstr)),
      spec,
      protoField,
      sparkConf));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_execution_IcebergWriteJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong writerHandle,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  auto writer = ObjectStore::retrieve<IcebergWriter>(writerHandle);
  writer->write(*(std::dynamic_pointer_cast<BoltColumnarBatch>(batch)));
  JNI_METHOD_END()
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_gluten_execution_IcebergWriteJniWrapper_commit( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong writerHandle) {
  JNI_METHOD_START
  auto writer = ObjectStore::retrieve<IcebergWriter>(writerHandle);
  auto commitMessages = writer->commit();
  jobjectArray ret =
      env->NewObjectArray(commitMessages.size(), env->FindClass("java/lang/String"), env->NewStringUTF(""));
  for (auto i = 0; i < commitMessages.size(); i++) {
    env->SetObjectArrayElement(ret, i, env->NewStringUTF(commitMessages[i].data()));
  }
  return ret;

  JNI_METHOD_END(nullptr)
}
#endif

// Shuffle
JNIEXPORT jlong JNICALL Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_createShuffleWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray shuffleWriterInfoProto,
    jlong firstBatchHandle,
    jobject partitionPusher) {
  JNI_METHOD_START
  auto ctx = dynamic_cast<BoltRuntime*>(gluten::getRuntime(env, wrapper));
  auto batch = ObjectStore::retrieve<ColumnarBatch>(firstBatchHandle);

  ShuffleWriterInfo shuffleWriterInfo;
  shuffleWriterInfo.ParseFromArray(
      (void*)env->GetByteArrayElements(shuffleWriterInfoProto, nullptr), env->GetArrayLength(shuffleWriterInfoProto));

  std::shared_ptr<RssClient> rssClient = nullptr;
  if (partitionPusher != nullptr) {
    jclass celebornPartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celebornPushPartitionDataMethod =
        getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw gluten::GlutenException("Unable to get JavaVM instance");
    }
    rssClient = std::make_shared<JavaRssClient>(vm, partitionPusher, celebornPushPartitionDataMethod);
  }

  if (gluten::BoltGlutenMemoryManager::enabled()) {
    shuffleWriterInfo.set_mem_limit(
        gluten::BoltGlutenMemoryManager::getMinimumFreeMemoryForTask(shuffleWriterInfo.task_attempt_id()));
  }

  return ctx->saveObject(ctx->createShuffleWriter(shuffleWriterInfo, rssClient, batch));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_reclaim( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jlong size) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriterBase>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }
  int64_t evictedSize;
  gluten::arrowAssertOkOrThrow(
      shuffleWriter->reclaimFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return (jlong)evictedSize;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jint numRows,
    jlong batchHandle,
    jlong memLimit) {
  JNI_METHOD_START
  if (gluten::BoltGlutenMemoryManager::enabled()) {
    // When enable BoltMemoryManager, SparkMemoryUtil.getCurrentAvailableOffHeapMemory() will not
    // change during task running.
    memLimit = gluten::BoltGlutenMemoryManager::getAvailableMemoryPerTask();
  }

  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriterBase>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }

  // The column batch maybe BoltColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  gluten::arrowAssertOkOrThrow(shuffleWriter->split(batch, memLimit), "Native split: shuffle writer split failed");
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_stop( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriterBase>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }

  gluten::arrowAssertOkOrThrow(shuffleWriter->stop(), "Native shuffle write: ShuffleWriter stop failed");

  const auto& partitionLengths = shuffleWriter->partitionLengths();
  auto partitionLengthArr = env->NewLongArray(partitionLengths.size());
  auto src = reinterpret_cast<const jlong*>(partitionLengths.data());
  env->SetLongArrayRegion(partitionLengthArr, 0, partitionLengths.size(), src);

  const auto& rawPartitionLengths = shuffleWriter->rawPartitionLengths();
  auto rawPartitionLengthArr = env->NewLongArray(rawPartitionLengths.size());
  auto rawSrc = reinterpret_cast<const jlong*>(rawPartitionLengths.data());
  env->SetLongArrayRegion(rawPartitionLengthArr, 0, rawPartitionLengths.size(), rawSrc);

  jobject splitResult = env->NewObject(
      splitResultClass,
      splitResultConstructor,
      0L,
      shuffleWriter->totalWriteTime(),
      shuffleWriter->totalEvictTime(),
      shuffleWriter->totalCompressTime(),
      shuffleWriter->totalBytesWritten(),
      shuffleWriter->totalBytesEvicted(),
      shuffleWriter->maxPartitionBufferSize(),
      shuffleWriter->avgPeallocSize(),
      shuffleWriter->useV2(),
      shuffleWriter->rowVectorModeCompress(),
      shuffleWriter->combinedVectorNumber(),
      shuffleWriter->combineVectorTimes(),
      shuffleWriter->combineVectorCost(),
      shuffleWriter->useRowBased(),
      shuffleWriter->totalConvertTime(),
      shuffleWriter->totalFlattenTime(),
      shuffleWriter->totalComputePidTime(),
      partitionLengthArr,
      rawPartitionLengthArr);

  return splitResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  ObjectStore::release(shuffleWriterHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_addShuffleWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle,
    jbyteArray shuffleWriterInfoProto,
    jobject celebornPusher) {
  JNI_METHOD_START
  auto ctx = dynamic_cast<BoltRuntime*>(gluten::getRuntime(env, wrapper));
  auto iterator = ObjectStore::retrieve<gluten::ResultIterator>(iterHandle);
  auto wholeStageIterator = dynamic_cast<gluten::WholeStageResultIterator*>(iterator->getInputIter());
  GLUTEN_CHECK(wholeStageIterator != nullptr, "WholeStageResultIterator is null");

  std::shared_ptr<RssClient> rssClient = nullptr;
  if (celebornPusher != nullptr) {
    // Celeborn client use Celeborn Java API, so we need to pass the client to the C++ side.
    GLUTEN_CHECK(celebornPusher != nullptr, "Celeborn pusher cannot be null");
    jclass celebornPartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celebornPushPartitionDataMethod =
        getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw gluten::GlutenException("Unable to get JavaVM instance");
    }
    rssClient = std::make_shared<JavaRssClient>(vm, celebornPusher, celebornPushPartitionDataMethod);
  }

  ShuffleWriterInfo shuffleWriterInfo;
  shuffleWriterInfo.ParseFromArray(
      (void*)env->GetByteArrayElements(shuffleWriterInfoProto, nullptr), env->GetArrayLength(shuffleWriterInfoProto));
  auto options = BoltShuffleWriterWrapper::getOptionsFromInfo(shuffleWriterInfo, rssClient);
  wholeStageIterator->addShuffleWriter(
      options, [ctx](const bytedance::bolt::shuffle::sparksql::ShuffleWriterMetrics& metrics) {
        ctx->setShuffleWriterResult(BoltShuffleWriterWrapper::getResultFromMetrics(metrics));
      });
  JNI_METHOD_END()
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_gluten_shuffle_BoltShuffleWriterJniWrapper_getShuffleWriterResult( // NOLINT
    JNIEnv* env,
    jobject wrapper) {
  JNI_METHOD_START
  auto ctx = dynamic_cast<BoltRuntime*>(gluten::getRuntime(env, wrapper));

  auto shuffleWriterResult = ctx->getShuffleWriterResult();
  GLUTEN_CHECK(shuffleWriterResult.has_value(), "Shuffle writer metrics is not set");

  auto shuffleResultStr = shuffleWriterResult.value().SerializeAsString();
  jbyteArray result = env->NewByteArray(shuffleResultStr.size());
  env->SetByteArrayRegion(result, 0, shuffleResultStr.size(), reinterpret_cast<const jbyte*>(shuffleResultStr.c_str()));
  return result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_shuffle_BoltShuffleReaderJniWrapper_make( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jbyteArray shuffleReaderInfoProto) {
  JNI_METHOD_START
  auto ctx = dynamic_cast<BoltRuntime*>(gluten::getRuntime(env, wrapper));

  ShuffleReaderInfo shuffleReaderInfo;

  shuffleReaderInfo.ParseFromArray(
      (void*)env->GetByteArrayElements(shuffleReaderInfoProto, nullptr), env->GetArrayLength(shuffleReaderInfoProto));

  // TODO: Add coalesce option and maximum coalesced size.
  std::shared_ptr<arrow::Schema> schema =
      gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));

  return ctx->saveObject(ctx->createShuffleReader(schema, shuffleReaderInfo));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_shuffle_BoltShuffleReaderJniWrapper_read( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject jStreamReader) {
  JNI_METHOD_START
  auto ctx = dynamic_cast<BoltRuntime*>(gluten::getRuntime(env, wrapper));
  auto reader = std::dynamic_pointer_cast<gluten::BoltShuffleReaderWrapper>(
      ObjectStore::retrieve<ShuffleReaderBase>(shuffleReaderHandle));
  auto streamReader = gluten::makeShuffleStreamReader(env, jStreamReader);
  auto outItr = reader->readStream(streamReader);
  return ctx->saveObject(outItr);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_shuffle_BoltShuffleReaderJniWrapper_populateMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject metrics) {
  JNI_METHOD_START
  auto reader = ObjectStore::retrieve<ShuffleReaderBase>(shuffleReaderHandle);
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDeserializeTime, reader->getDeserializeTime());

  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_shuffle_BoltShuffleReaderJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle) {
  JNI_METHOD_START
  auto reader = ObjectStore::retrieve<ShuffleReaderBase>(shuffleReaderHandle);
  GLUTEN_THROW_NOT_OK(reader->close());
  ObjectStore::release(shuffleReaderHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
