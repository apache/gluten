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
package org.apache.spark.sql.execution

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.io.{FileNameSpec, HadoopMapReduceCommitProtocol}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.WriteJobDescription
import org.apache.spark.util.{SerializableConfiguration, SparkShimVersionUtil, Utils}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.util.UUID

class SparkWriteFilesCommitProtocolSuite extends AnyFunSuite {
  private val uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

  Seq(
    None,
    Some("custom"),
    Some(""),
    Some(".hidden name"),
    Some(".."),
    Some("nested/name"),
    Some("../outside"),
    Some("/absolute")).foreach {
    basename =>
      test(s"output basename $basename follows the loaded Spark commit protocol") {
        val directory = Utils.createTempDir()
        val previousContext = TaskContext.get()
        val context = TaskContext.empty()
        TaskContext.setTaskContext(context)
        try {
          val job = Job.getInstance(new Configuration())
          job.setOutputFormatClass(classOf[TextOutputFormat[Object, Object]])
          FileOutputFormat.setOutputPath(job, new Path(directory.toURI))
          basename.foreach(job.getConfiguration.set("mapreduce.output.basename", _))
          val description = new WriteJobDescription(
            UUID.randomUUID().toString,
            new SerializableConfiguration(job.getConfiguration),
            null,
            Seq.empty,
            Seq.empty,
            Seq.empty,
            None,
            directory.toString,
            Map.empty,
            0L,
            "UTC",
            Seq.empty
          )
          val spark = new HadoopMapReduceCommitProtocol(
            UUID.randomUUID().toString,
            directory.toString)
          val bolt = new SparkWriteFilesCommitProtocol("202610220000", description, spark)
          bolt.setupTask()
          try {
            Seq(FileNameSpec("", ".parquet"), FileNameSpec("bucket_", "_00003.c000.parquet"))
              .foreach {
                spec =>
                  // Use Spark's real public path API as the oracle, including its handling of
                  // separators and dot segments. UUIDs differ because Bolt allocates one per file.
                  val expected = spark.newTaskTempFile(bolt.taskAttemptContext, None, spec)
                  val actual = new Path(bolt.newTaskAttemptTempPath(), bolt.getFilename(spec))
                  assert(actual.toString.replaceAll(uuidPattern, "UUID") ==
                    expected.replaceAll(uuidPattern, "UUID"))
              }
          } finally {
            bolt.abortTask(bolt.newTaskAttemptTempPath())
          }
        } finally {
          if (previousContext == null) TaskContext.unset()
          else TaskContext.setTaskContext(previousContext)
          Utils.deleteRecursively(directory)
        }
      }
  }

  for (operation <- Seq("commit", "abort")) {
    test(s"$operation failure preserves the cause and Spark version's write error contract") {
      val previousContext = TaskContext.get()
      TaskContext.setTaskContext(TaskContext.empty())
      try {
        val outputPath = "/tmp/bolt-write-error-contract"
        val cause = new IOException("injected commit protocol failure")
        val description = new WriteJobDescription(
          UUID.randomUUID().toString,
          new SerializableConfiguration(new Configuration()),
          null,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          None,
          outputPath,
          Map.empty,
          0L,
          "UTC",
          Seq.empty
        )
        val committer = new HadoopMapReduceCommitProtocol("failure", outputPath) {
          override def commitTask(context: TaskAttemptContext): TaskCommitMessage = throw cause
          override def abortTask(context: TaskAttemptContext): Unit = throw cause
        }
        val bolt = new SparkWriteFilesCommitProtocol("202610300000", description, committer)
        val error = intercept[Exception] {
          if (operation == "commit") bolt.commitTask() else bolt.abortTask(outputPath)
        }
        if (SparkShimVersionUtil.sparkMajorMinorVersion()._1 >= 4) {
          assert(error.isInstanceOf[SparkException])
          assert(error.getCause eq cause)
          assert(error.getMessage.contains(outputPath))
        } else {
          assert(error eq cause)
        }
      } finally {
        if (previousContext == null) TaskContext.unset()
        else TaskContext.setTaskContext(previousContext)
      }
    }
  }
}
