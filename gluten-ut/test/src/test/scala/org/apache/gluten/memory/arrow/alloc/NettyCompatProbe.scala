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

import org.apache.arrow.memory.{DefaultAllocationManagerOption, RootAllocator}

/**
 * Entry point of the child JVM started by [[ArrowNettyCompatSuite]]. Allocates through a plain
 * `new RootAllocator()`, the way Spark's `ArrowUtils.rootAllocator` does for PySpark's
 * createDataFrame / toPandas.
 *
 * Prints a single result line and exits 0 on success, 1 on failure.
 */
// scalastyle:off println
object NettyCompatProbe {
  val ResultPrefix = "NETTY_COMPAT_RESULT "

  def main(args: Array[String]): Unit = {
    try {
      val root = new RootAllocator(Long.MaxValue)
      try {
        for (size <- Seq(1024L, 1L << 20, 64L << 20)) {
          val buf = root.buffer(size)
          try {
            buf.setByte(size - 1, 7)
            if (buf.getByte(size - 1) != 7) {
              throw new IllegalStateException(s"Read back a wrong value at offset ${size - 1}")
            }
          } finally {
            buf.close()
          }
        }
      } finally {
        root.close()
      }
      println(s"${ResultPrefix}OK factory=$defaultFactoryClass")
      System.exit(0)
    } catch {
      case t: Throwable =>
        var cause = t
        while (cause.getCause != null) {
          cause = cause.getCause
        }
        println(s"${ResultPrefix}FAIL $cause")
        t.printStackTrace(System.out)
        System.exit(1)
    }
  }

  private def defaultFactoryClass: String = {
    val method =
      classOf[DefaultAllocationManagerOption].getDeclaredMethod(
        "getDefaultAllocationManagerFactory")
    method.setAccessible(true)
    method.invoke(null).getClass.getName
  }
}
// scalastyle:on println
