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
package org.apache.spark.shuffle.gluten.celeborn;

import org.apache.gluten.config.GlutenConfig;
import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.shuffle.NeedCustomColumnarBatchSerializer;
import org.apache.gluten.shuffle.SupportsColumnarShuffle;

import com.google.common.base.Preconditions;
import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.spark.*;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.celeborn.*;
import org.apache.spark.shuffle.sort.ColumnarShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class CelebornShuffleManager
    implements ShuffleManager, SupportsColumnarShuffle, NeedCustomColumnarBatchSerializer {

  private static final Logger logger = LoggerFactory.getLogger(CelebornShuffleManager.class);

  private static final String GLUTEN_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.sort.ColumnarShuffleManager";

  private static final String VANILLA_CELEBORN_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.celeborn.SparkShuffleManager";

  private static final String LOCAL_SHUFFLE_READER_KEY =
      "spark.sql.adaptive.localShuffleReader.enabled";

  private static final String CELEBORN_COMPRESSION_CODEC_KEY =
      CelebornConf.SHUFFLE_COMPRESSION_CODEC().key();

  private static final String SPARK_CELEBORN_COMPRESSION_CODEC_KEY =
      "spark." + CELEBORN_COMPRESSION_CODEC_KEY;

  private static final CelebornShuffleWriterFactory writerFactory;

  private static final CelebornColumnarBatchSerializerFactory columnarBatchSerializerFactory;

  static {
    final Iterator<CelebornShuffleWriterFactory> shuffleWriterFactoryIterator =
        ServiceLoader.load(CelebornShuffleWriterFactory.class).iterator();
    Preconditions.checkState(
        shuffleWriterFactoryIterator.hasNext(), "No Celeborn shuffle writer factory found");
    writerFactory = shuffleWriterFactoryIterator.next();
    Preconditions.checkState(
        !shuffleWriterFactoryIterator.hasNext(),
        "Multiple factories found for Celeborn shuffle writer");

    final Iterator<CelebornColumnarBatchSerializerFactory> serializerFactoryIterator =
        ServiceLoader.load(CelebornColumnarBatchSerializerFactory.class).iterator();

    Preconditions.checkState(
        serializerFactoryIterator.hasNext(),
        "No factory found for Celeborn columnar batch serializer");
    columnarBatchSerializerFactory = serializerFactoryIterator.next();
    Preconditions.checkState(
        !serializerFactoryIterator.hasNext(),
        "Multiple factories found for Celeborn columnar batch serializer");
  }

  private final SparkConf conf;
  private final CelebornConf celebornConf;
  private final SparkConf rowBasedConf;
  private final CelebornConf rowBasedCelebornConf;
  // either be "{appId}_{appAttemptId}" or "{appId}"
  private String appUniqueId;

  private LifecycleManager lifecycleManager;
  private ShuffleClient shuffleClient;
  private volatile ColumnarShuffleManager _columnarShuffleManager;
  private volatile SparkShuffleManager _vanillaCelebornShuffleManager;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> columnarShuffleIds =
      ConcurrentHashMap.newKeySet();
  private final CelebornShuffleFallbackPolicyRunner fallbackPolicyRunner;

  private final String celebornDefaultCodec;

  // for Celeborn 0.4.0
  private final Object shuffleIdTracker;

  // for Celeborn 0.6.0
  private final boolean stageRerunEnabled;

  private Object failedShuffleCleaner = null;

  public CelebornShuffleManager(SparkConf conf) {
    if (conf.getBoolean(LOCAL_SHUFFLE_READER_KEY, true)) {
      logger.warn(
          "Detected {} (default is true) is enabled, it's highly recommended to disable it when "
              + "use Celeborn as Remote Shuffle Service to avoid performance degradation.",
          LOCAL_SHUFFLE_READER_KEY);
    }
    this.conf = conf;
    this.celebornConf = SparkUtils.fromSparkConf(conf);
    this.fallbackPolicyRunner = new CelebornShuffleFallbackPolicyRunner(celebornConf);

    this.shuffleIdTracker =
        CelebornUtils.createInstance(CelebornUtils.EXECUTOR_SHUFFLE_ID_TRACKER_NAME);

    this.stageRerunEnabled = CelebornUtils.getStageRerunEnabled(celebornConf);

    this.celebornDefaultCodec = CelebornConf.SHUFFLE_COMPRESSION_CODEC().defaultValueString();

    this.rowBasedConf = conf.clone();
    this.rowBasedCelebornConf = celebornConf.clone();
    if ("none"
        .equalsIgnoreCase(conf.get(SPARK_CELEBORN_COMPRESSION_CODEC_KEY, celebornDefaultCodec))) {
      rowBasedConf.set(SPARK_CELEBORN_COMPRESSION_CODEC_KEY, celebornDefaultCodec);
      rowBasedCelebornConf.set(CELEBORN_COMPRESSION_CODEC_KEY, celebornDefaultCodec);
    }
  }

  private boolean isDriver() {
    return "driver".equals(SparkEnv.get().executorId());
  }

  private ColumnarShuffleManager columnarShuffleManager() {
    if (_columnarShuffleManager == null) {
      synchronized (this) {
        if (_columnarShuffleManager == null) {
          _columnarShuffleManager =
              SparkUtils.instantiateClass(GLUTEN_SHUFFLE_MANAGER_NAME, conf, isDriver());
        }
      }
    }
    return _columnarShuffleManager;
  }

  private SparkShuffleManager vanillaCelebornShuffleManager() {
    if (_vanillaCelebornShuffleManager == null) {
      synchronized (this) {
        if (_vanillaCelebornShuffleManager == null) {
          _vanillaCelebornShuffleManager =
              SparkUtils.instantiateClass(
                  VANILLA_CELEBORN_SHUFFLE_MANAGER_NAME, rowBasedConf, isDriver());
        }
      }
    }
    return _vanillaCelebornShuffleManager;
  }

  private void initializeLifecycleManager(String appId) {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver() && lifecycleManager == null) {
      synchronized (this) {
        if (lifecycleManager == null) {
          appUniqueId = CelebornUtils.getAppUniqueId(appId, celebornConf);
          lifecycleManager = new LifecycleManager(appUniqueId, celebornConf);

          // for Celeborn 0.6.0
          CelebornUtils.incrementApplicationCount(lifecycleManager);
          CelebornUtils.registerCancelShuffleCallback(lifecycleManager);
          CelebornUtils.stageRerun(
              stageRerunEnabled, lifecycleManager, celebornConf, failedShuffleCleaner);

          shuffleClient =
              CelebornUtils.getShuffleClient(
                  appUniqueId,
                  lifecycleManager.getHost(),
                  lifecycleManager.getPort(),
                  celebornConf,
                  lifecycleManager.getUserIdentifier(),
                  Boolean.TRUE,
                  null);
        }
      }
    }
  }

  private <K, V, C> ShuffleHandle registerCelebornShuffleHandle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    // for Celeborn 0.4.0
    CelebornUtils.registerAppShuffleDeterminate(lifecycleManager, shuffleId, dependency);
    return CelebornUtils.getCelebornShuffleHandle(
        appUniqueId,
        lifecycleManager.getHost(),
        lifecycleManager.getPort(),
        lifecycleManager.getUserIdentifier(),
        shuffleId,
        stageRerunEnabled,
        dependency.rdd().getNumPartitions(),
        dependency);
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    String appId = SparkUtils.appUniqueId(dependency.rdd().context());
    initializeLifecycleManager(appId);

    // Note: generate app unique id at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    if (dependency instanceof ColumnarShuffleDependency) {
      CelebornUtils.incrementShuffleCount(lifecycleManager);
      if (CelebornUtils.applyFallbackPolicies(fallbackPolicyRunner, lifecycleManager, dependency)) {
        if (GlutenConfig.get().enableCelebornFallback()) {
          logger.warn("Fallback to ColumnarShuffleManager!");
          columnarShuffleIds.add(shuffleId);
          return columnarShuffleManager().registerShuffle(shuffleId, dependency);
        } else {
          throw new GlutenException(
              "The Celeborn service(Master: " + celebornConf.masterHost() + ") is unavailable");
        }
      } else {
        return registerCelebornShuffleHandle(shuffleId, dependency);
      }
    }
    // If the input shuffle dependency is not columnar, then it's a row-based shuffle.
    // We should fallback to use vanilla celeborn shuffle manager, so that people can use
    // dra normally.
    return registerCelebornShuffleHandle(shuffleId, dependency);
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    if (columnarShuffleIds.remove(shuffleId)) {
      return columnarShuffleManager().unregisterShuffle(shuffleId);
    }
    return CelebornUtils.unregisterShuffle(
        lifecycleManager,
        shuffleClient,
        shuffleIdTracker,
        shuffleId,
        appUniqueId,
        stageRerunEnabled,
        isDriver());
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return columnarShuffleManager().shuffleBlockResolver();
  }

  @Override
  public void stop() {
    if (shuffleClient != null) {
      shuffleClient.shutdown();
      ShuffleClient.reset();
      shuffleClient = null;
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
      lifecycleManager = null;
    }
    if (_columnarShuffleManager != null) {
      _columnarShuffleManager.stop();
      _columnarShuffleManager = null;
    }
    if (_vanillaCelebornShuffleManager != null) {
      _vanillaCelebornShuffleManager.stop();
      _vanillaCelebornShuffleManager = null;
    }
    if (failedShuffleCleaner != null) {
      CelebornUtils.stopFailedShuffleCleaner(failedShuffleCleaner);
      failedShuffleCleaner = null;
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    try {
      if (handle instanceof CelebornShuffleHandle) {
        byte[] extension;
        try {
          Field field = CelebornShuffleHandle.class.getDeclaredField("extension");
          field.setAccessible(true);
          extension = (byte[]) field.get(handle);

        } catch (NoSuchFieldException e) {
          extension = null;
        }
        @SuppressWarnings("unchecked")
        CelebornShuffleHandle<K, V, V> h = ((CelebornShuffleHandle<K, V, V>) handle);

        CelebornConf writerConf = celebornConf;
        if (!(h.dependency() instanceof ColumnarShuffleDependency)) {
          writerConf = rowBasedCelebornConf;
        }

        shuffleClient =
            CelebornUtils.getShuffleClient(
                h.appUniqueId(),
                h.lifecycleManagerHost(),
                h.lifecycleManagerPort(),
                writerConf,
                h.userIdentifier(),
                false,
                extension);

        try {
          Field field;
          try {
            // for Celeborn 0.6.0
            field = CelebornShuffleHandle.class.getDeclaredField("stageRerunEnabled");
          } catch (NoSuchFieldException e) {
            // for Celeborn 0.5.2
            field = CelebornShuffleHandle.class.getDeclaredField("throwsFetchFailure");
          }
          field.setAccessible(true);
          boolean stageRerunEnabled = (boolean) field.get(handle);
          if (stageRerunEnabled) {
            Method addFailureListenerMethod =
                SparkUtils.class.getMethod(
                    "addFailureListenerIfBarrierTask",
                    ShuffleClient.class,
                    TaskContext.class,
                    CelebornShuffleHandle.class);
            addFailureListenerMethod.invoke(null, shuffleClient, context, h);
          }
        } catch (NoSuchFieldException | NoSuchMethodException ignored) {
        }

        int shuffleId;

        // for Celeborn 0.4.0
        try {
          Method celebornShuffleIdMethod =
              SparkUtils.class.getMethod(
                  "celebornShuffleId",
                  ShuffleClient.class,
                  CelebornShuffleHandle.class,
                  TaskContext.class,
                  Boolean.class);
          shuffleId = (int) celebornShuffleIdMethod.invoke(null, shuffleClient, h, context, true);

          Method trackMethod =
              CelebornUtils.getClassOrDefault(CelebornUtils.EXECUTOR_SHUFFLE_ID_TRACKER_NAME)
                  .getMethod("track", int.class, int.class);
          trackMethod.invoke(shuffleIdTracker, h.shuffleId(), shuffleId);

        } catch (NoSuchMethodException e) {
          shuffleId = h.dependency().shuffleId();
        }

        if (h.dependency() instanceof ColumnarShuffleDependency) {
          // columnar-based shuffle
          return writerFactory.createShuffleWriterInstance(
              shuffleId, h, context, celebornConf, shuffleClient, metrics);
        } else {
          // row-based shuffle
          return vanillaCelebornShuffleManager().getWriter(handle, mapId, context, metrics);
        }
      } else {
        return columnarShuffleManager().getWriter(handle, mapId, context, metrics);
      }
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      @SuppressWarnings("unchecked")
      CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
      CelebornConf readerConf = celebornConf;
      if (!(h.dependency() instanceof ColumnarShuffleDependency)) {
        readerConf = rowBasedCelebornConf;
      }
      return CelebornUtils.getCelebornShuffleReader(
          h,
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          readerConf,
          metrics,
          shuffleIdTracker);
    }
    return columnarShuffleManager()
        .getReader(
            handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics);
  }

  @Override
  public String columnarBatchSerializerClass() {
    return columnarBatchSerializerFactory.columnarBatchSerializerClass();
  }
}
