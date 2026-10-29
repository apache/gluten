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

object GlutenTestGrouping {
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
            else if (test.name.startsWith("org.apache.spark.sql.gluten.")) "self-test"
            else if (factories(test.name) || parents.exists(sparkFixtures)) "required"
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
