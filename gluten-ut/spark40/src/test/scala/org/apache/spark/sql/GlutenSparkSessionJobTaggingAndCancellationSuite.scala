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
package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class GlutenSparkSessionJobTaggingAndCancellationSuite
  extends SparkSessionJobTaggingAndCancellationSuite
  with GlutenTestSetWithSystemPropertyTrait {

  // Override with testGluten because the original test uses ExecutionContext.global whose
  // ForkJoinPool reuses threads created before addTag("one"). InheritableThreadLocal (used by
  // managedJobTags) only copies values at thread creation time, so reused threads see an empty
  // tag map. This causes withSessionTagsApplied to not attach the user tag to the job, making
  // cancelJobsWithTagWithFuture return empty. We fix this by creating a fresh thread pool AFTER
  // addTag so that new threads inherit the InheritableThreadLocal values.
  testGluten("Tags set from session are prefixed with session UUID") {
    sc = new SparkContext("local[2]", "test")
    val session = classic.SparkSession.builder().sparkContext(sc).getOrCreate()
    import session.implicits._

    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        sem.release()
      }
    })

    session.addTag("one")

    // Use a fresh thread pool created AFTER addTag so that new threads inherit
    // InheritableThreadLocal values (managedJobTags, threadUuid).
    val threadPool = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)
    try {
      Future {
        session.range(1, 10000).map { i => Thread.sleep(100); i }.count()
      }

      assert(sem.tryAcquire(1, 1, TimeUnit.MINUTES))
      val activeJobsFuture =
        session.sparkContext.cancelJobsWithTagWithFuture(
          session.managedJobTags.get()("one"),
          "reason")
      val activeJob = ThreadUtils.awaitResult(activeJobsFuture, 60.seconds).head
      val actualTags = activeJob.properties
        .getProperty(SparkContext.SPARK_JOB_TAGS)
        .split(SparkContext.SPARK_JOB_TAGS_SEP)
      assert(
        actualTags.toSet == Set(
          session.sessionJobTag,
          s"${session.sessionJobTag}-thread-${session.threadUuid.get()}-one",
          SQLExecution.executionIdJobTag(
            session,
            activeJob.properties
              .get(SQLExecution.EXECUTION_ROOT_ID_KEY)
              .asInstanceOf[String]
              .toLong)
        ))
    } finally {
      threadPool.shutdownNow()
    }
  }
}
