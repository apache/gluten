# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux
set -o pipefail

GLUTEN_JAR=/PATH_TO_GLUTEN_HOME/package/target/<gluten-jar>
DELTA_JARS=/PATHS_TO_DELTA_JARS
SPARK_HOME=/PATH_TO_SPARK_HOME/

cat tpcds_delta.scala | ${SPARK_HOME}/bin/spark-shell \
  --master yarn --deploy-mode client \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR}:${DELTA_JARS} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR}:${DELTA_JARS} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --num-executors 3 \
  --executor-cores 3 \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.driver.maxResultSize=2g

  # If there are some "*.so" libs dependencies issues on some specific Distros,
  # try to enable spark.gluten.loadLibFromJar and build your own gluten-thirdparty-lib Jar.
  # e.g.
  #   --conf spark.gluten.loadLibFromJar=true \
  #   --jars /PATH_TO_GLUTEN_HOME/package/target/thirdparty-lib/gluten-thirdparty-lib-ubuntu-22.04-x86_64.jar,
  #          /PATH_TO_GLUTEN_HOME/package/target/gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04_x86_64-1.x.x-SNAPSHOT.jar
