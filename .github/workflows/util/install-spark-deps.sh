#!/usr/bin/env bash
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

# Download Spark resources, required by some Spark UTs. The resource path should be set
# for spark.test.home in mvn test.
#
# This file can be:
# Sourced to use functions: source install-spark-deps.sh; install_hadoop; setup_hdfs

set -e

# Install Hadoop binary
function install_hadoop() {
  echo "Installing Hadoop..."
  
  apt-get update -y
  apt-get install -y curl tar gzip
  
  local HADOOP_VERSION=3.3.6
  curl -fsSL -o hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
  tar -xzf hadoop.tgz --no-same-owner --no-same-permissions
  rm -f hadoop.tgz

  export HADOOP_HOME="$PWD/hadoop-${HADOOP_VERSION}"
  export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
  export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH"

  if [ -n "$GITHUB_ENV" ]; then
    echo "HADOOP_HOME=$HADOOP_HOME" >> $GITHUB_ENV
    echo "LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH" >> $GITHUB_ENV
    echo "$HADOOP_HOME/bin" >> $GITHUB_PATH
  fi
}

# Setup HDFS namenode and datanode
function setup_hdfs() {
  export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

  cat > "$HADOOP_CONF_DIR/core-site.xml" <<'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

  cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<'EOF'
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.rpc-address</name><value>localhost:9000</value></property>
  <property><name>dfs.namenode.http-address</name><value>localhost:9870</value></property>
  <property><name>dfs.datanode.address</name><value>localhost:9866</value></property>
  <property><name>dfs.datanode.http.address</name><value>localhost:9864</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
EOF

  HDFS_TMP="${RUNNER_TEMP:-/tmp}/hdfs"
  mkdir -p "$HDFS_TMP/nn" "$HDFS_TMP/dn"

  perl -0777 -i -pe 's#</configuration>#  <property>\n    <name>dfs.namenode.name.dir</name>\n    <value>file:'"$HDFS_TMP"'/nn</value>\n  </property>\n  <property>\n    <name>dfs.datanode.data.dir</name>\n    <value>file:'"$HDFS_TMP"'/dn</value>\n  </property>\n</configuration>#s' \
    "$HADOOP_CONF_DIR/hdfs-site.xml"

  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR" >> "$GITHUB_ENV"
    echo "HADOOP_HOME=$HADOOP_HOME" >> "$GITHUB_ENV"
  fi

  "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
  "$HADOOP_HOME/sbin/hadoop-daemon.sh" start namenode
  "$HADOOP_HOME/sbin/hadoop-daemon.sh" start datanode

  for i in {1..60}; do
    "$HADOOP_HOME/bin/hdfs" dfs -ls / >/dev/null 2>&1 && break
    sleep 1
  done

  "$HADOOP_HOME/bin/hdfs" dfs -ls /
}

function install_minio {
  echo "Installing MinIO..."

  apt-get update -y
  apt-get install -y curl

  curl -fsSL -o /usr/local/bin/minio https://dl.min.io/server/minio/release/linux-amd64/minio
  chmod +x /usr/local/bin/minio

  curl -fsSL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc
  chmod +x /usr/local/bin/mc

  echo "MinIO installed successfully"
}

function setup_minio {
  local spark_version="${1:-3.5}"
  local spark_version_short=$(echo "${spark_version}" | cut -d '.' -f 1,2 | tr -d '.')

  case "$spark_version" in
    3.3) hadoop_aws_version="3.3.2"; aws_sdk_artifact="aws-java-sdk-bundle"; aws_sdk_version="1.12.262" ;;
    3.4|3.5*) hadoop_aws_version="3.3.4"; aws_sdk_artifact="aws-java-sdk-bundle"; aws_sdk_version="1.12.262" ;;
    4.0) hadoop_aws_version="3.4.0"; aws_sdk_artifact="bundle"; aws_sdk_version="2.25.11" ;;
    4.1) hadoop_aws_version="3.4.1"; aws_sdk_artifact="bundle"; aws_sdk_version="2.25.11" ;;
    *) hadoop_aws_version="3.3.4"; aws_sdk_artifact="aws-java-sdk-bundle"; aws_sdk_version="1.12.262" ;;
  esac

  local spark_jars_dir="${GITHUB_WORKSPACE:-$PWD}/tools/gluten-it/package/target/lib"
  mkdir -p "$spark_jars_dir"

  wget -nv https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${hadoop_aws_version}/hadoop-aws-${hadoop_aws_version}.jar -P "$spark_jars_dir" || return 1

  if [ "$aws_sdk_artifact" == "aws-java-sdk-bundle" ]; then
    wget -nv https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${aws_sdk_version}/aws-java-sdk-bundle-${aws_sdk_version}.jar -P "$spark_jars_dir" || return 1
  else
    wget -nv https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${aws_sdk_version}/bundle-${aws_sdk_version}.jar -P "$spark_jars_dir" || return 1
  fi

  export MINIO_DATA_DIR="${RUNNER_TEMP:-/tmp}/minio-data"
  mkdir -p "$MINIO_DATA_DIR"
  export MINIO_ROOT_USER=admin
  export MINIO_ROOT_PASSWORD=admin123

  nohup minio server --address ":9100" --console-address ":9101" "$MINIO_DATA_DIR" > /tmp/minio.log 2>&1 &

  for i in {1..60}; do
    curl -sSf http://localhost:9100/minio/health/ready >/dev/null 2>&1 && break
    sleep 1
  done

  if ! curl -sSf http://localhost:9100/minio/health/ready >/dev/null 2>&1; then
    echo "MinIO failed to start"
    cat /tmp/minio.log || true
    exit 1
  fi

  mc alias set s3local http://localhost:9100 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
  mc mb -p s3local/gluten-it || true
}
