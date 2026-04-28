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
package org.apache.gluten.integration.stat

import java.io.InputStreamReader
import java.lang.management.ManagementFactory
import java.util.{Scanner, StringTokenizer}

object RamStat {

  private def getPid(): String = {
    val beanName = ManagementFactory.getRuntimeMXBean.getName
    beanName.substring(0, beanName.indexOf('@'))
  }

  def getJvmHeapUsed(): Long = {
    val heapTotalBytes = Runtime.getRuntime.totalMemory()
    val heapUsed = (heapTotalBytes - Runtime.getRuntime.freeMemory()) / 1024L
    heapUsed
  }

  def getJvmHeapTotal(): Long = {
    val heapTotalBytes = Runtime.getRuntime.totalMemory()
    val heapTotal = heapTotalBytes / 1024L
    heapTotal
  }

  def getProcessRamUsed(): Long = {
    val proc = Runtime.getRuntime.exec("ps -p " + getPid() + " -o rss")
    val in = new InputStreamReader(proc.getInputStream)
    val buff = new StringBuilder

    def scan: Unit = {
      while (true) {
        val ch = in.read()
        if (ch == -1) {
          return
        }
        buff.append(ch.asInstanceOf[Char])
      }
    }
    scan
    in.close()
    val output = buff.toString()
    val scanner = new Scanner(output)
    scanner.nextLine()
    scanner.nextLine().toLong
  }

  def getSystemRamUsed(): Long = {
    val proc = Runtime.getRuntime.exec("free")
    val in = new InputStreamReader(proc.getInputStream)
    val buff = new StringBuilder

    def scan: Unit = {
      while (true) {
        val ch = in.read()
        if (ch == -1) {
          return
        }
        buff.append(ch.asInstanceOf[Char])
      }
    }
    scan
    in.close()
    val output = buff.toString()
    val scanner = new Scanner(output)
    scanner.nextLine()
    val memLine = scanner.nextLine()
    val tok = new StringTokenizer(memLine)
    tok.nextToken()
    tok.nextToken()
    tok.nextToken().toLong
  }
}
