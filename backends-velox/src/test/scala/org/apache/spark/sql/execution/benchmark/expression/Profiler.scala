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

import one.profiler.AsyncProfiler

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}

/** One run's controller for sequential engine sessions of the process-wide profiler. */
class Profiler(val options: ProfilerOptions) {
  lazy val profileRoot: Path = {
    Files.createDirectories(options.output)
    val root = Files.createTempDirectory(options.output, "profile-")
    // scalastyle:off println
    System.err.println(s"Expression benchmark profiles: ${root.toAbsolutePath.normalize()}")
    // scalastyle:on println
    root
  }
  lazy val execute: String => String = {
    val library = options.home.resolve("lib").resolve(System.mapLibraryName("asyncProfiler"))
    AsyncProfiler.getInstance(library.toAbsolutePath.toString).execute _
  }
  private var state = "new"
  def owned: Boolean = state == "active"

  def start(): Unit = {
    require(state == "new", s"Cannot start profiler in state: $state")
    val arguments =
      if (options.event == "cpu") "event=cpu,interval=1ms,cstack=dwarf,threads"
      else "event=alloc,alloc=512k,threads"
    // A failed command must not authorize cleanup of another process-wide profiler session.
    state = "failed"
    try execute(s"start,$arguments")
    catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw e
    }
    state = "active"
  }

  def stop(): Unit = if (owned) {
    state = "failed"
    withoutInterrupt {
      execute("stop")
      state = "stopped"
    }
  }

  /** The final file path; a successful dump permits the next engine to start with reset. */
  def dump(path: Path): Unit = if (state == "stopped") {
    // Seal before executing or writing: either failure must not retry this session.
    state = "failed"
    withoutInterrupt {
      Files.write(path, execute("collapsed,total").getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
      // Empty output is a valid no-samples result, especially for allocation profiles.
      state = "new"
    }
  }

  private def withoutInterrupt(f: => Unit): Unit = {
    val wasInterrupted = Thread.interrupted()
    try f
    catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw e
    } finally {
      if (wasInterrupted) Thread.currentThread().interrupt()
    }
  }
}
