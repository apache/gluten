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
package org.apache.gluten.memory.arrow.alloc

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Arrays
import java.util.concurrent.TimeUnit

/**
 * Reproduces the `NoSuchFieldError: chunkSize` that PySpark's Arrow paths (createDataFrame /
 * toPandas with `spark.sql.execution.arrow.pyspark.enabled`) hit with Gluten on a Spark 3.5
 * distribution whose Netty was upgraded to 4.1.118 or later.
 *
 * Spark 3.5 ships arrow-memory-netty 12.0.1, whose allocator reads a Netty internal field that
 * Netty 4.1.118 removed. The test classpath has Gluten's newer Arrow and Spark's older Netty, which
 * are compatible, so the failure cannot show up in-process. Each case therefore runs
 * [[NettyCompatProbe]] in a child JVM whose classpath mirrors a deployment: Gluten's Arrow first
 * (as with spark.{driver,executor}.extraClassPath), then Spark's arrow-memory-netty and the chosen
 * Netty. The allocation manager type is left to Arrow's classpath detection, as in production. The
 * jars come from maven-dependency-plugin, see this module's pom.xml.
 *
 * The cases on new Netty assert the failure, documenting the bug. A fix that makes Spark's
 * `RootAllocator` work there should turn them into success assertions.
 */
class ArrowNettyCompatSuite extends AnyFunSuite {
  private val compatDir = new File("target/netty-compat")
  private val spark35ArrowNetty = "arrow-memory-netty-12.0.1"
  private val timeoutSeconds = 120L

  private case class ProbeResult(exitCode: Int, line: String, output: String)

  test("Spark 3.5 arrow-memory-netty works with Netty 4.1.96") {
    // Control case: proves the child JVM setup itself is sound.
    val result = runProbe("netty-4.1.96.Final")
    assert(result.exitCode == 0, s"Probe failed:\n${result.output}")
    assert(result.line.contains(" OK "), s"Unexpected probe output:\n${result.output}")
  }

  test("Spark 3.5 arrow-memory-netty fails with Netty 4.1.118") {
    assertFailedWithChunkSize(runProbe("netty-4.1.118.Final"))
  }

  test("Spark 3.5 arrow-memory-netty fails with Netty 4.2.12") {
    assertFailedWithChunkSize(runProbe("netty-4.2.12.Final"))
  }

  private def assertFailedWithChunkSize(result: ProbeResult): Unit = {
    assert(result.exitCode == 1, s"Expected the probe to fail, output:\n${result.output}")
    assert(
      result.line.contains("NoSuchFieldError") && result.line.contains("chunkSize"),
      s"Expected NoSuchFieldError: chunkSize, output:\n${result.output}")
  }

  private def runProbe(nettyDir: String): ProbeResult = {
    // Spark 3.5 distributions pair Gluten's bundled arrow-memory-core 15.x with Spark's
    // arrow-memory-netty 12.0.1. Spark 4.x builds use Spark's own Arrow (>= 18), whose netty
    // module works with new Netty, so this scenario does not exist there.
    val arrowMajor = arrowCoreMajorVersion
    assume(
      arrowMajor > 0 && arrowMajor < 16,
      s"Scenario only applies when Gluten bundles Arrow < 16, found $arrowMajor")
    assert(
      new File(compatDir, spark35ArrowNetty).isDirectory,
      s"Missing ${compatDir.getAbsolutePath}; run the build through Maven so that " +
        "maven-dependency-plugin copies the compat jars"
    )

    val classpath =
      // Gluten first: the probe, the Scala library it needs and Gluten's arrow-memory-core.
      Seq(
        codeSource(NettyCompatProbe.getClass),
        codeSource(classOf[Option[_]]),
        codeSource(classOf[RootAllocator])) ++
        // Then what the Spark distribution provides.
        jarsIn(new File(compatDir, spark35ArrowNetty)) ++
        jarsIn(new File(compatDir, nettyDir)) :+
        codeSource(classOf[Logger])

    val command = Seq(
      new File(System.getProperty("java.home"), "bin/java").getPath,
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-cp",
      classpath.mkString(File.pathSeparator),
      NettyCompatProbe.getClass.getName.stripSuffix("$")
    )
    val builder = new ProcessBuilder(Arrays.asList(command: _*)).redirectErrorStream(true)
    // Leave the allocation manager type to classpath detection, as in production.
    builder.environment().remove("ARROW_ALLOCATION_MANAGER_TYPE")
    val process = builder.start()
    val output = new String(readAll(process), StandardCharsets.UTF_8)
    assert(
      process.waitFor(timeoutSeconds, TimeUnit.SECONDS),
      s"Probe did not finish within ${timeoutSeconds}s")
    val line =
      output.split("\\R").find(_.startsWith(NettyCompatProbe.ResultPrefix)).getOrElse("")
    ProbeResult(process.exitValue(), line, s"Classpath: $classpath\n$output")
  }

  private def readAll(process: Process): Array[Byte] = {
    val in = process.getInputStream
    try {
      Iterator.continually(in.read()).takeWhile(_ != -1).map(_.toByte).toArray
    } finally {
      in.close()
    }
  }

  private def jarsIn(dir: File): Seq[String] = {
    val jars =
      Option(dir.listFiles()).getOrElse(Array.empty[File]).filter(_.getName.endsWith(".jar"))
    assert(jars.nonEmpty, s"No jars in ${dir.getAbsolutePath}")
    jars.map(_.getAbsolutePath).sorted.toSeq
  }

  private def codeSource(clazz: Class[_]): String =
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI).getAbsolutePath

  private def arrowCoreMajorVersion: Int =
    "arrow-memory-core-(\\d+)\\.".r
      .findFirstMatchIn(codeSource(classOf[RootAllocator]))
      .map(_.group(1).toInt)
      .getOrElse(-1)
}
