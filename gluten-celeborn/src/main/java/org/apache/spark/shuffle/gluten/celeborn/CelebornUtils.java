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

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.DeterministicLevel;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.celeborn.CelebornShuffleFallbackPolicyRunner;
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle;
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class CelebornUtils {

  private static final Logger logger = LoggerFactory.getLogger(CelebornUtils.class);

  public static final String EXECUTOR_SHUFFLE_ID_TRACKER_NAME =
      "org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker";
  public static final String SHUFFLE_FETCH_FAILURE_REPORT_TASK_CLEAN_LISTENER_NAME =
      "org.apache.spark.shuffle.celeborn.ShuffleFetchFailureReportTaskCleanListener";
  public static final String FAILED_SHUFFLE_CLEANER_NAME =
      "org.apache.celeborn.spark.FailedShuffleCleaner";
  public static final String GET_REDUCER_FILE_GROUP_RESPONSE_NAME =
      "org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse";

  public static boolean unregisterShuffle(
      LifecycleManager lifecycleManager,
      ShuffleClient shuffleClient,
      Object shuffleIdTracker,
      int appShuffleId,
      String appUniqueId,
      boolean stageRerunEnabled,
      boolean isDriver) {
    try {
      try {
        try {
          // for Celeborn 0.4.1
          if (lifecycleManager != null) {
            Method unregisterAppShuffle =
                lifecycleManager
                    .getClass()
                    .getMethod("unregisterAppShuffle", int.class, boolean.class);
            unregisterAppShuffle.invoke(lifecycleManager, appShuffleId, stageRerunEnabled);
          }
        } catch (NoSuchMethodException ex) {
          // for Celeborn 0.4.0
          Method unregisterAppShuffle =
              lifecycleManager.getClass().getMethod("unregisterAppShuffle", int.class);
          unregisterAppShuffle.invoke(lifecycleManager, appShuffleId);
        }
        if (shuffleClient != null) {
          Method unregisterAppShuffleId =
              Class.forName(EXECUTOR_SHUFFLE_ID_TRACKER_NAME)
                  .getMethod("unregisterAppShuffleId", ShuffleClient.class, int.class);
          unregisterAppShuffleId.invoke(shuffleIdTracker, shuffleClient, appShuffleId);
        }
        return true;
      } catch (NoSuchMethodException | ClassNotFoundException ex) {
        try {
          if (lifecycleManager != null) {
            Method unregisterShuffleMethod =
                lifecycleManager.getClass().getMethod("unregisterShuffle", int.class);
            unregisterShuffleMethod.invoke(lifecycleManager, appShuffleId);
          }
          if (shuffleClient != null) {
            Method cleanupShuffleMethod =
                shuffleClient.getClass().getMethod("cleanupShuffle", int.class);
            cleanupShuffleMethod.invoke(shuffleClient, appShuffleId);
          }
          return true;
        } catch (NoSuchMethodException ex1) {
          // for Celeborn 0.3.0 and 0.3.1
          if (appUniqueId == null) {
            return true;
          }

          if (shuffleClient == null) {
            return false;
          }
          Method unregisterShuffleMethod =
              shuffleClient.getClass().getMethod("unregisterShuffle", int.class, boolean.class);
          Object result = unregisterShuffleMethod.invoke(shuffleClient, appShuffleId, isDriver);
          return (Boolean) result;
        }
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static ShuffleClient getShuffleClient(
      String appUniqueId,
      String lifecycleManagerHost,
      Integer lifecycleManagerPort,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      Boolean isDriver,
      byte[] extension) {
    try {
      try {
        try {
          Method method =
              // for Celeborn 0.4.0
              ShuffleClient.class.getDeclaredMethod(
                  "get",
                  String.class,
                  String.class,
                  int.class,
                  CelebornConf.class,
                  UserIdentifier.class,
                  byte[].class);
          return (ShuffleClient)
              method.invoke(
                  null,
                  appUniqueId,
                  lifecycleManagerHost,
                  lifecycleManagerPort,
                  conf,
                  userIdentifier,
                  extension);
        } catch (NoSuchMethodException noMethod) {
          Method method =
              // for Celeborn 0.3.1 and above, see CELEBORN-804
              ShuffleClient.class.getDeclaredMethod(
                  "get",
                  String.class,
                  String.class,
                  int.class,
                  CelebornConf.class,
                  UserIdentifier.class);
          return (ShuffleClient)
              method.invoke(
                  null,
                  appUniqueId,
                  lifecycleManagerHost,
                  lifecycleManagerPort,
                  conf,
                  userIdentifier);
        }
      } catch (NoSuchMethodException noMethod) {
        Method method =
            // for Celeborn 0.3.0, see CELEBORN-798
            ShuffleClient.class.getDeclaredMethod(
                "get",
                String.class,
                String.class,
                int.class,
                CelebornConf.class,
                UserIdentifier.class,
                boolean.class);
        return (ShuffleClient)
            method.invoke(
                null,
                appUniqueId,
                lifecycleManagerHost,
                lifecycleManagerPort,
                conf,
                userIdentifier,
                isDriver);
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static Object createInstance(String className, Object... args) {
    try {
      try {
        Class<?> clazz = Class.forName(className);

        Constructor<?> constructor = clazz.getConstructor();

        return constructor.newInstance(args);

      } catch (ClassNotFoundException e) {
        return null;
      }
    } catch (Exception rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static CelebornShuffleHandle getCelebornShuffleHandle(
      String appUniqueId,
      String lifecycleManagerHost,
      int lifecycleManagerPort,
      UserIdentifier userIdentifier,
      int shuffleId,
      boolean stageRerunEnabled,
      int numMappers,
      ShuffleDependency dependency) {
    try {
      try {
        Constructor<CelebornShuffleHandle> constructor =
            CelebornShuffleHandle.class.getConstructor(
                String.class,
                String.class,
                int.class,
                UserIdentifier.class,
                int.class,
                boolean.class,
                int.class,
                ShuffleDependency.class);

        return constructor.newInstance(
            appUniqueId,
            lifecycleManagerHost,
            lifecycleManagerPort,
            userIdentifier,
            shuffleId,
            stageRerunEnabled,
            numMappers,
            dependency);
      } catch (NoSuchMethodException noMethod) {
        Constructor<CelebornShuffleHandle> constructor =
            CelebornShuffleHandle.class.getConstructor(
                String.class,
                String.class,
                int.class,
                UserIdentifier.class,
                int.class,
                int.class,
                ShuffleDependency.class);

        return constructor.newInstance(
            appUniqueId,
            lifecycleManagerHost,
            lifecycleManagerPort,
            userIdentifier,
            shuffleId,
            numMappers,
            dependency);
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static CelebornShuffleReader getCelebornShuffleReader(
      CelebornShuffleHandle handle,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex,
      TaskContext context,
      CelebornConf conf,
      ShuffleReadMetricsReporter metrics,
      Object shuffleIdTracker) {
    try {
      try {
        // for Celeborn 0.4.0
        Constructor<CelebornShuffleReader> constructor =
            CelebornShuffleReader.class.getConstructor(
                CelebornShuffleHandle.class,
                int.class,
                int.class,
                int.class,
                int.class,
                TaskContext.class,
                CelebornConf.class,
                ShuffleReadMetricsReporter.class,
                getClassOrDefault(EXECUTOR_SHUFFLE_ID_TRACKER_NAME));

        return constructor.newInstance(
            handle,
            startPartition,
            endPartition,
            startMapIndex,
            endMapIndex,
            context,
            conf,
            metrics,
            shuffleIdTracker);
      } catch (NoSuchMethodException noMethod) {
        // for celeborn 0.3.x
        Constructor<CelebornShuffleReader> constructor =
            CelebornShuffleReader.class.getConstructor(
                CelebornShuffleHandle.class,
                int.class,
                int.class,
                int.class,
                int.class,
                TaskContext.class,
                CelebornConf.class,
                ShuffleReadMetricsReporter.class);

        return constructor.newInstance(
            handle,
            startPartition,
            endPartition,
            startMapIndex,
            endMapIndex,
            context,
            conf,
            metrics);
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static Class<?> getClassOrDefault(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      return Object.class;
    }
  }

  public static boolean getStageRerunEnabled(CelebornConf celebornConf) {
    try {
      Method clientStageRerunEnabledMethod;
      try {
        // for Celeborn 0.6.0
        clientStageRerunEnabledMethod =
            celebornConf.getClass().getDeclaredMethod("clientStageRerunEnabled");
      } catch (NoSuchMethodException e) {
        // for Celeborn 0.4.0
        clientStageRerunEnabledMethod =
            celebornConf.getClass().getDeclaredMethod("clientFetchThrowsFetchFailure");
      }
      return (Boolean) clientStageRerunEnabledMethod.invoke(celebornConf);
    } catch (NoSuchMethodException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void stageRerun(
      boolean stageRerunEnabled,
      LifecycleManager lifecycleManager,
      CelebornConf celebornConf,
      Object failedShuffleCleaner) {
    if (stageRerunEnabled) {
      MapOutputTrackerMaster mapOutputTracker =
          (MapOutputTrackerMaster) SparkEnv.get().mapOutputTracker();

      // for Celeborn 0.6.0
      registerReportTaskShuffleFetchFailurePreCheck(lifecycleManager);

      registerShuffleTrackerCallback(lifecycleManager, mapOutputTracker);

      // for Celeborn 0.6.0
      registerCelebornSkewShuffleCheckCallback(lifecycleManager, celebornConf);
      fetchCleanFailedShuffle(lifecycleManager, celebornConf, failedShuffleCleaner);
      reducerFileGroupBroadcast(lifecycleManager, celebornConf);
    }
  }

  public static void registerShuffleTrackerCallback(
      LifecycleManager lifecycleManager, MapOutputTrackerMaster mapOutputTracker) {
    try {
      Method registerShuffleTrackerCallbackMethod =
          lifecycleManager
              .getClass()
              .getDeclaredMethod("registerShuffleTrackerCallback", Consumer.class);

      Consumer<Integer> consumer =
          shuffleId -> {
            try {
              Method unregisterAllMapOutputMethod =
                  SparkUtils.class.getMethod(
                      "unregisterAllMapOutput", MapOutputTrackerMaster.class, int.class);
              unregisterAllMapOutputMethod.invoke(null, mapOutputTracker, shuffleId);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };

      registerShuffleTrackerCallbackMethod.invoke(lifecycleManager, consumer);
    } catch (NoSuchMethodException e) {
      logger.debug("Executing the initializeLifecycleManager of celeborn-0.3.x");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerAppShuffleDeterminate(
      LifecycleManager lifecycleManager, int shuffleId, ShuffleDependency dependency) {
    try {
      Method registerAppShuffleDeterminateMethod =
          lifecycleManager
              .getClass()
              .getDeclaredMethod("registerAppShuffleDeterminate", Integer.TYPE, Boolean.TYPE);

      registerAppShuffleDeterminateMethod.invoke(
          lifecycleManager,
          shuffleId,
          dependency.rdd().getOutputDeterministicLevel() != DeterministicLevel.INDETERMINATE());

    } catch (NoSuchMethodException e) {
      logger.debug("Executing the registerShuffle of celeborn-0.3.x");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String getAppUniqueId(String appId, CelebornConf celebornConf) {
    try {
      // for Celeborn 0.6.0
      Method appUniqueIdWithUUIDSuffixMethod =
          celebornConf.getClass().getDeclaredMethod("appUniqueIdWithUUIDSuffix", String.class);
      return (String) appUniqueIdWithUUIDSuffixMethod.invoke(celebornConf, appId);
    } catch (NoSuchMethodException e) {
      return appId;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void incrementApplicationCount(LifecycleManager lifecycleManager) {
    try {
      // for Celeborn 0.6.0
      Method applicationCountMethod =
          lifecycleManager.getClass().getDeclaredMethod("applicationCount");
      ((LongAdder) applicationCountMethod.invoke(lifecycleManager)).increment();
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerCancelShuffleCallback(LifecycleManager lifecycleManager) {
    try {
      // for Celeborn 0.6.0
      Method registerCancelShuffleCallbackMethod =
          lifecycleManager
              .getClass()
              .getDeclaredMethod("registerCancelShuffleCallback", BiConsumer.class);
      BiConsumer<Integer, String> consumer =
          (shuffleId, reason) -> {
            try {
              Method cancelShuffleMethod =
                  SparkUtils.class.getMethod("cancelShuffle", int.class, String.class);
              cancelShuffleMethod.invoke(null, shuffleId, reason);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
      registerCancelShuffleCallbackMethod.invoke(lifecycleManager, consumer);
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerReportTaskShuffleFetchFailurePreCheck(
      LifecycleManager lifecycleManager) {
    try {
      // for Celeborn 0.6.0
      Method registerReportTaskShuffleFetchFailurePreCheckMethod =
          lifecycleManager
              .getClass()
              .getDeclaredMethod("registerReportTaskShuffleFetchFailurePreCheck", Function.class);
      Function<Long, Boolean> function =
          taskId -> {
            try {
              Method shouldReportShuffleFetchFailureMethod =
                  SparkUtils.class.getMethod("shouldReportShuffleFetchFailure", long.class);
              return (Boolean) shouldReportShuffleFetchFailureMethod.invoke(null, taskId);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
      registerReportTaskShuffleFetchFailurePreCheckMethod.invoke(lifecycleManager, function);
      Method addSparkListenerMethod =
          SparkUtils.class.getMethod("addSparkListener", SparkListener.class);
      addSparkListenerMethod.invoke(
          null, createInstance(SHUFFLE_FETCH_FAILURE_REPORT_TASK_CLEAN_LISTENER_NAME));
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerCelebornSkewShuffleCheckCallback(
      LifecycleManager lifecycleManager, CelebornConf celebornConf) {
    try {
      // for Celeborn 0.6.0
      Method clientAdaptiveOptimizeSkewedPartitionReadEnabledMethod =
          celebornConf
              .getClass()
              .getDeclaredMethod("clientAdaptiveOptimizeSkewedPartitionReadEnabled");
      if ((Boolean) clientAdaptiveOptimizeSkewedPartitionReadEnabledMethod.invoke(celebornConf)) {
        Method registerCelebornSkewShuffleCheckCallbackMethod =
            lifecycleManager
                .getClass()
                .getDeclaredMethod("registerCelebornSkewShuffleCheckCallback", Function.class);
        Function<Integer, Boolean> function =
            appShuffleId -> {
              try {
                Method isCelebornSkewShuffleOrChildShuffleMethod =
                    SparkUtils.class.getMethod("isCelebornSkewShuffleOrChildShuffle", int.class);
                return (Boolean)
                    isCelebornSkewShuffleOrChildShuffleMethod.invoke(null, appShuffleId);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };
        registerCelebornSkewShuffleCheckCallbackMethod.invoke(lifecycleManager, function);
      }
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void fetchCleanFailedShuffle(
      LifecycleManager lifecycleManager, CelebornConf celebornConf, Object failedShuffleCleaner) {
    try {
      // for Celeborn 0.6.0
      Method clientFetchCleanFailedShuffleMethod =
          celebornConf.getClass().getDeclaredMethod("clientFetchCleanFailedShuffle");
      if ((Boolean) clientFetchCleanFailedShuffleMethod.invoke(celebornConf)) {
        failedShuffleCleaner = createInstance(FAILED_SHUFFLE_CLEANER_NAME, lifecycleManager);
        Method registerValidateCelebornShuffleIdForCleanCallbackMethod =
            lifecycleManager
                .getClass()
                .getDeclaredMethod(
                    "registerValidateCelebornShuffleIdForCleanCallback", Consumer.class);
        Object shuffleCleaner = failedShuffleCleaner;
        Consumer<String> addShuffleIdToBeCleanedConsumer =
            appShuffleIdentifier -> {
              try {
                Method addShuffleIdToBeCleanedMethod =
                    shuffleCleaner
                        .getClass()
                        .getDeclaredMethod("addShuffleIdToBeCleaned", String.class);
                addShuffleIdToBeCleanedMethod.invoke(shuffleCleaner, appShuffleIdentifier);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };
        registerValidateCelebornShuffleIdForCleanCallbackMethod.invoke(
            lifecycleManager, addShuffleIdToBeCleanedConsumer);
        Method registerUnregisterShuffleCallbackMethod =
            lifecycleManager
                .getClass()
                .getDeclaredMethod("registerUnregisterShuffleCallback", Consumer.class);
        Consumer<Integer> removeCleanedShuffleIdConsumer =
            celebornShuffleId -> {
              try {
                Method removeCleanedShuffleIdMethod =
                    shuffleCleaner
                        .getClass()
                        .getDeclaredMethod("removeCleanedShuffleId", int.class);
                removeCleanedShuffleIdMethod.invoke(shuffleCleaner, celebornShuffleId);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };
        registerUnregisterShuffleCallbackMethod.invoke(
            lifecycleManager, removeCleanedShuffleIdConsumer);
      }
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void reducerFileGroupBroadcast(
      LifecycleManager lifecycleManager, CelebornConf celebornConf) {
    try {
      // for Celeborn 0.6.0
      Method getReducerFileGroupBroadcastEnabledMethod =
          celebornConf.getClass().getDeclaredMethod("getReducerFileGroupBroadcastEnabled");
      if ((Boolean) getReducerFileGroupBroadcastEnabledMethod.invoke(celebornConf)) {
        Method registerBroadcastGetReducerFileGroupResponseCallbackMethod =
            lifecycleManager
                .getClass()
                .getDeclaredMethod(
                    "registerBroadcastGetReducerFileGroupResponseCallback", BiFunction.class);
        BiFunction<Integer, Object, byte[]> function =
            (shuffleId, getReducerFileGroupResponse) -> {
              try {
                Method serializeGetReducerFileGroupResponseMethod =
                    SparkUtils.class.getMethod(
                        "serializeGetReducerFileGroupResponse",
                        Integer.class,
                        getClassOrDefault(GET_REDUCER_FILE_GROUP_RESPONSE_NAME));
                return (byte[])
                    serializeGetReducerFileGroupResponseMethod.invoke(
                        null, shuffleId, getReducerFileGroupResponse);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };
        registerBroadcastGetReducerFileGroupResponseCallbackMethod.invoke(
            lifecycleManager, function);
        Method registerInvalidatedBroadcastCallbackMethod =
            lifecycleManager
                .getClass()
                .getDeclaredMethod("registerInvalidatedBroadcastCallback", Consumer.class);
        Consumer<Integer> consumer =
            shuffleId -> {
              try {
                Method invalidateSerializedGetReducerFileGroupResponseMethod =
                    SparkUtils.class.getMethod(
                        "invalidateSerializedGetReducerFileGroupResponse", Integer.class);
                invalidateSerializedGetReducerFileGroupResponseMethod.invoke(null, shuffleId);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };
        registerInvalidatedBroadcastCallbackMethod.invoke(lifecycleManager, consumer);
      }
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void incrementShuffleCount(LifecycleManager lifecycleManager) {
    try {
      // for Celeborn 0.6.0
      Method shuffleCountMethod = lifecycleManager.getClass().getDeclaredMethod("shuffleCount");
      ((LongAdder) shuffleCountMethod.invoke(lifecycleManager)).increment();
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean applyFallbackPolicies(
      CelebornShuffleFallbackPolicyRunner fallbackPolicyRunner,
      LifecycleManager lifecycleManager,
      ShuffleDependency<?, ?, ?> dependency) {
    try {
      try {
        // for Celeborn 0.6.0
        Method applyFallbackPoliciesMethod =
            fallbackPolicyRunner
                .getClass()
                .getDeclaredMethod(
                    "applyFallbackPolicies", ShuffleDependency.class, LifecycleManager.class);
        return (Boolean)
            applyFallbackPoliciesMethod.invoke(fallbackPolicyRunner, dependency, lifecycleManager);
      } catch (NoSuchMethodException e) {
        Method applyAllFallbackPolicyMethod =
            fallbackPolicyRunner
                .getClass()
                .getDeclaredMethod("applyAllFallbackPolicy", LifecycleManager.class, int.class);
        return (Boolean)
            applyAllFallbackPolicyMethod.invoke(
                fallbackPolicyRunner, lifecycleManager, dependency.partitioner().numPartitions());
      }
    } catch (NoSuchMethodException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void stopFailedShuffleCleaner(Object failedShuffleCleaner) {
    try {
      try {
        // for Celeborn 0.6.1
        Method stopMethod = failedShuffleCleaner.getClass().getDeclaredMethod("stop");
        stopMethod.invoke(failedShuffleCleaner);
      } catch (NoSuchMethodException e) {
        // for Celeborn 0.6.0
        Method resetMethod = failedShuffleCleaner.getClass().getDeclaredMethod("reset");
        resetMethod.invoke(failedShuffleCleaner);
      }
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void prepareForMergeData(
      ShuffleClient shuffleClient, int shuffleId, int mapId, int attemptId) {
    try {
      Method prepareMethod =
          shuffleClient
              .getClass()
              .getDeclaredMethod("prepareForMergeData", Integer.TYPE, Integer.TYPE, Integer.TYPE);
      prepareMethod.invoke(shuffleClient, shuffleId, mapId, attemptId);
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void mapperEnd(
      ShuffleClient shuffleClient,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions) {
    try {
      try {
        // for Celeborn 0.6.3
        Method mapperEndMethod =
            shuffleClient
                .getClass()
                .getDeclaredMethod(
                    "mapperEnd",
                    Integer.TYPE,
                    Integer.TYPE,
                    Integer.TYPE,
                    Integer.TYPE,
                    Integer.TYPE);
        mapperEndMethod.invoke(
            shuffleClient, shuffleId, mapId, attemptId, numMappers, numPartitions);
      } catch (NoSuchMethodException e) {
        Method mapperEndMethod =
            shuffleClient
                .getClass()
                .getDeclaredMethod(
                    "mapperEnd", Integer.TYPE, Integer.TYPE, Integer.TYPE, Integer.TYPE);
        mapperEndMethod.invoke(shuffleClient, shuffleId, mapId, attemptId, numMappers);
      }
    } catch (NoSuchMethodException ignored) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
