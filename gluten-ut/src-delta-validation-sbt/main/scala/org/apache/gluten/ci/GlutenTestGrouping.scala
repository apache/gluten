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
package org.apache.gluten.ci

import sbt._
import sbt.Keys._
import sbt.testing.{AnnotatedFingerprint, Event, NestedSuiteSelector, NestedTestSelector, SubclassFingerprint, SuiteSelector, TestSelector}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.{Base64, UUID}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object GlutenTestGrouping {
  val results = LocalProject("spark") / Test / testListeners +=
    new ExecutionResults(file(sys.env("GLUTEN_VALIDATION_DIR")))

  private class ExecutionResults(directory: File) extends TestReportListener {
    private case class Group(name: String) {
      val events: ArrayBuffer[Event] = ArrayBuffer.empty
    }

    private val active = new InheritableThreadLocal[Group]()

    private def encoded(value: String): String = {
      val bytes = value.getBytes(UTF_8)
      require(new String(bytes, UTF_8) == value, "Invalid UTF-8 test identity")
      Base64.getEncoder.encodeToString(bytes)
    }

    private def guarded(body: => Unit): Unit = {
      try {
        body
      } catch {
        case NonFatal(error) =>
          IO.write(directory / s"execution-error-${UUID.randomUUID()}.txt", error.toString)
          throw error
      }
    }

    private def current: Group =
      Option(active.get()).getOrElse(sys.error("Missing execution-results group"))

    override def startGroup(name: String): Unit = guarded {
      require(active.get() == null, s"Overlapping execution-results groups: $name")
      active.set(Group(name))
    }

    override def testEvent(event: TestEvent): Unit = guarded {
      current.events ++= event.detail
    }

    private def eventLine(group: Group, event: Event): String = {
      val fingerprint = event.fingerprint match {
        case value: AnnotatedFingerprint => "annotation:" + value.annotationName
        case value: SubclassFingerprint => "subclass:" + value.superclassName
        case other => sys.error(s"Unsupported execution-results fingerprint: $other")
      }
      val (kind, suite, test) = event.selector match {
        case value: TestSelector => ("test", group.name, value.testName())
        case value: NestedTestSelector => ("test", value.suiteId(), value.testName())
        case _: SuiteSelector => ("suite", group.name, "")
        case value: NestedSuiteSelector => ("suite", value.suiteId(), "")
        case other => sys.error(s"Unsupported execution-results selector: $other")
      }
      Seq(kind, event.status.toString, encoded(suite), encoded(test), encoded(fingerprint))
        .mkString("\t")
    }

    private def finish(name: String, result: TestResult, aborted: Boolean): Unit = guarded {
      val group = current
      try {
        require(group.name == name, s"Mismatched execution-results group: $name")
        val events = group.events.map(event => eventLine(group, event))
        if (aborted) {
          events += Seq("suite", "Error", encoded(name), "", "").mkString("\t")
        }
        // JUnit attribute normalization and sbt's display-name rewriting are intentionally avoided.
        val lines = Seq("version\t1\t" + encoded(name)) ++ events ++
          Seq(s"complete\t$result\t${events.size}")
        Files.write(
          (directory / s"execution-${UUID.randomUUID()}.tsv").toPath,
          lines.mkString("", "\n", "\n").getBytes(UTF_8))
      } finally {
        active.remove()
      }
    }

    override def endGroup(name: String, result: TestResult): Unit =
      finish(name, result, aborted = false)

    override def endGroup(name: String, error: Throwable): Unit =
      finish(name, TestResult.Error, aborted = true)
  }

  val setting = LocalProject("spark") / Test / testGrouping := {
    val groups = (LocalProject("spark") / Test / testGrouping).value
    val definitions = (LocalProject("spark") / Test / definedTests).value
    val classpath = (LocalProject("spark") / Test / fullClasspath).value.files
    val root = (LocalRootProject / baseDirectory).value
    val output = file(sys.env("GLUTEN_VALIDATION_DIR"))
    val canaryName = "org.apache.spark.sql.gluten.GlutenActivationSuite"
    val canary = definitions.find(_.name == canaryName).getOrElse(
      sys.error("Gluten activation canary is missing from compiled tests"))
    require(groups.nonEmpty, "No Delta test forks were scheduled")
    require(
      groups.forall(_.runPolicy.isInstanceOf[Tests.SubProcess]),
      "Gluten validation requires forked test groups; set NUM_SHARDS, SHARD_ID and TEST_PARALLELISM_COUNT"
    )
    val factories = IO.readLines(root / "gluten-validation-factories.txt")
      .map(_.trim).filter(line => line.nonEmpty && !line.startsWith("#")).toSet
    val sparkFixtures = Set(
      "org.apache.spark.sql.test.SharedSparkSession",
      "org.apache.spark.sql.test.SharedSparkSessionBase",
      "org.apache.spark.sql.hive.test.TestHiveSingleton",
      "org.apache.spark.sql.delta.test.DeltaSQLCommandTest",
      "org.apache.spark.sql.delta.test.DeltaHiveTest",
      "org.apache.spark.sql.delta.DeltaSQLCommandJavaTest",
      "org.apache.spark.sql.SessionQueryTestBase",
      "org.apache.spark.LocalSparkContext",
      "org.apache.spark.sql.gluten.GlutenCheckedTest"
    )

    // Inspect compiled inheritance independently of the runtime hooks, without class initialization.
    val loader = new java.net.URLClassLoader(
      classpath.map(_.toURI.toURL).toArray,
      ClassLoader.getPlatformClassLoader)
    def hierarchy(clazz: Class[_]): Set[String] = {
      val pending = scala.collection.mutable.Stack[Class[_]](clazz)
      val names = scala.collection.mutable.Set.empty[String]
      while (pending.nonEmpty) {
        val current = pending.pop()
        if (current != null && names.add(current.getName)) {
          pending.push(current.getSuperclass)
          current.getInterfaces.foreach(parent => pending.push(parent))
        }
      }
      names.toSet
    }
    try {
      val scheduled = (groups.flatMap(_.tests).filterNot(_.name == canaryName) :+ canary)
        .groupBy(_.name).values.map(_.head).toSeq.sortBy(_.name)
      val rows = scheduled.map {
        test =>
          val module = test.fingerprint match {
            case fingerprint: sbt.testing.SubclassFingerprint => fingerprint.isModule
            case fingerprint: sbt.testing.AnnotatedFingerprint => fingerprint.isModule
            case other => sys.error(s"Unsupported test fingerprint: $other")
          }
          val clazz = Class.forName(test.name + (if (module) "$" else ""), false, loader)
          val parents = hierarchy(clazz)
          val role =
            if (test.name == canaryName) "canary"
            else if (factories(test.name) || parents.exists(sparkFixtures)) "required"
            else if (test.name.startsWith("org.apache.spark.sql.gluten.")) "self-test"
            else "uncovered"
          s"$role\t${test.name}"
      }
      IO.write(output / "expected-suites.tsv", rows.mkString("", "\n", "\n"))
      IO.write(output / "expected-forks.txt", groups.size.toString + "\n")
    } finally {
      loader.close()
    }
    groups.map(group => group.copy(tests = canary +: group.tests.filterNot(_.name == canaryName)))
  }
}
