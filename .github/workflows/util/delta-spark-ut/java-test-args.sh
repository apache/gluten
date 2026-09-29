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

#
# Shared JVM options for running delta-io/delta's `spark` ScalaTest suite against
# the Gluten Velox bundle. SOURCE this file (don't execute it) before invoking
# `sbt spark/test`, in CI (run-delta-tests.sh) and locally:
#
#     source .github/workflows/util/delta-spark-ut/java-test-args.sh
#     ./build/sbt ... spark/test        # in the Delta clone
#
# Why these are needed: JDK 17 + Gluten/Arrow/Netty requires extra --add-opens and
# the `io.netty.tryReflectionSetAccessible` property; otherwise the forked test JVM
# fails with "sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not
# available" as soon as Gluten's bundled Arrow allocator initializes Netty direct
# buffers. Delta's own `Test / javaOptions` (project/CrossSparkVersions.scala
# `java17TestSettings`) sets the base add-opens but NOT the Netty property. This set
# mirrors `extraJavaTestArgs` in Gluten's root pom.xml.
#
# Exported via JAVA_TOOL_OPTIONS (not sbt's .jvmopts/.sbtopts, which only configure
# the sbt LAUNCHER JVM) so the flags reach BOTH the launcher and the forked test JVM
# -- the forked child inherits the parent env.
#
# NOTE: no -Xmx here on purpose. JAVA_TOOL_OPTIONS is processed BEFORE the JVM
# command line, so Delta's own -Xmx (build.sbt) would win; run-delta-tests.sh bumps
# the forked-test-JVM heap via `set spark / Test / javaOptions ++= ...` instead.

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+${JAVA_TOOL_OPTIONS} }\
-XX:+IgnoreUnrecognizedVMOptions \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
-Djdk.reflect.useDirectMethodHandle=false \
-Dio.netty.tryReflectionSetAccessible=true \
-Dfile.encoding=UTF-8"
