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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

import java.io.{ByteArrayOutputStream, IOException, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.collection.mutable.ArrayBuffer

class ProfilerSuite extends SparkFunSuite {
  private val cpuStart = "start,event=cpu,interval=1ms,cstack=dwarf,threads"

  private def fixture(event: String = "cpu", response: String => String = _ => "")(
      f: (Profiler, ArrayBuffer[String], Path) => Unit): Unit = {
    val root = Files.createTempDirectory("profiler space,comma ")
    val commands = ArrayBuffer.empty[String]
    try {
      val profiler = org.mockito.Mockito.spy(
        new Profiler(ProfilerOptions(root, event, root.resolve("not-created"))))
      org.mockito.Mockito
        .doReturn(
          (command: String) => {
            commands += command
            response(command)
          },
          Array.empty[Object]: _*)
        .when(profiler)
        .execute
      f(profiler, commands, root.resolve("profile space,comma.collapsed"))
      assert(!Files.exists(profiler.options.output))
    } finally Utils.deleteRecursively(root.toFile)
  }

  test("profile root is created and reported once on demand without loading the profiler") {
    val bytes = new ByteArrayOutputStream
    val errors = new PrintStream(bytes, true, UTF_8.name())
    val previous = System.err
    try {
      System.setErr(errors)
      fixture() {
        (profiler, commands, _) =>
          assert(!Files.exists(profiler.options.output))
          assert(bytes.size() == 0)
          val root = profiler.profileRoot
          assert(root.getParent == profiler.options.output && Files.isDirectory(root))
          assert(root.getFileName.toString.startsWith("profile-"))
          assert(profiler.profileRoot == root && commands.isEmpty)
          assert(
            bytes.toString(UTF_8.name()).linesIterator.toSeq ==
              Seq(s"Expression benchmark profiles: ${root.toAbsolutePath.normalize()}"))
          Utils.deleteRecursively(profiler.options.output.toFile)
      }
    } finally {
      System.setErr(previous)
      errors.close()
    }
  }

  test("unstarted stop and dump never touch an external profile or create output") {
    fixture() {
      (profiler, commands, path) =>
        profiler.stop()
        profiler.dump(path)
        assert(commands.isEmpty && !profiler.owned && !Files.exists(path))
    }
  }

  test("only a successful stop and dump permits the next session to start") {
    fixture() {
      (profiler, commands, path) =>
        profiler.start()
        intercept[IllegalArgumentException](profiler.start())
        profiler.dump(path)
        assert(profiler.owned && commands.toSeq == Seq(cpuStart))
        profiler.stop()
        profiler.stop()
        intercept[IllegalArgumentException](profiler.start())
        assert(!profiler.owned && !Files.exists(path))
        profiler.dump(path)
        profiler.dump(path)
        profiler.stop()
        assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
        assert(Files.size(path) == 0)
        profiler.start()
        profiler.stop()
        profiler.dump(path.resolveSibling("second.collapsed"))
        assert(commands.toSeq == Seq.fill(2)(Seq(cpuStart, "stop", "collapsed,total")).flatten)
    }
  }

  test("allocation commands write total counts in UTF-8 to a final path with spaces and commas") {
    val collapsed = "worker;sample_λ 524288\n" // scalastyle:ignore nonascii
    fixture("alloc", command => if (command == "collapsed,total") collapsed else "") {
      (profiler, commands, path) =>
        profiler.start()
        profiler.stop()
        profiler.dump(path)
        assert(
          commands.toSeq == Seq("start,event=alloc,alloc=512k,threads", "stop", "collapsed,total"))
        assert(new String(Files.readAllBytes(path), UTF_8) == collapsed)
    }
  }

  Seq(cpuStart, "stop", "collapsed,total").foreach {
    failedCommand =>
      Seq(false, true).foreach {
        interrupted =>
          test(s"command failure seals ownership without retries: $failedCommand / $interrupted") {
            val failure =
              if (interrupted) new InterruptedException(failedCommand)
              else new IllegalStateException(failedCommand)
            fixture(response = command => if (command == failedCommand) throw failure else "") {
              (profiler, commands, path) =>
                try {
                  val thrown = intercept[Exception] {
                    profiler.start()
                    profiler.stop()
                    profiler.dump(path)
                  }
                  assert(thrown eq failure)
                  assert(thrown.getSuppressed.isEmpty)
                  assert(Thread.currentThread().isInterrupted == interrupted)
                  val completed = commands.toVector
                  profiler.stop()
                  profiler.dump(path)
                  intercept[IllegalArgumentException](profiler.start())
                  assert(commands.toVector == completed && commands.last == failedCommand)
                  assert(!profiler.owned && !Files.exists(path))
                } finally Thread.interrupted()
            }
          }
      }
  }

  Seq(false, true).foreach {
    existingFile =>
      test(s"write failure is terminal and existing output is never overwritten: $existingFile") {
        fixture(response = _ => "new samples") {
          (profiler, commands, path) =>
            profiler.start()
            profiler.stop()
            if (existingFile) Files.write(path, "original".getBytes(UTF_8))
            else Files.createDirectory(path)
            intercept[IOException](profiler.dump(path))
            profiler.stop()
            profiler.dump(path)
            intercept[IllegalArgumentException](profiler.start())
            assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
            if (existingFile) assert(new String(Files.readAllBytes(path), UTF_8) == "original")
        }
      }
  }

  Seq("", "stop", "collapsed,total").foreach {
    failedCommand =>
      test(s"cleanup clears and restores the interrupt flag even on failure: $failedCommand") {
        fixture(response = command => {
          assert(!Thread.currentThread().isInterrupted)
          if (command == failedCommand) throw new IllegalStateException(command)
          ""
        }) {
          (profiler, commands, path) =>
            profiler.start()
            Thread.currentThread().interrupt()
            try {
              def finish(): Unit = {
                profiler.stop()
                profiler.dump(path)
              }
              if (failedCommand.isEmpty) finish()
              else intercept[IllegalStateException](finish())
              assert(Thread.currentThread().isInterrupted)
              profiler.stop()
              profiler.dump(path)
              assert(commands.count(_ == "stop") == 1)
            } finally Thread.interrupted()
        }
      }
  }
}
