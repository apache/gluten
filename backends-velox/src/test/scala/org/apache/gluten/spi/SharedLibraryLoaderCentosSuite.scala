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
package org.apache.gluten.spi

import org.scalatest.funsuite.AnyFunSuite

class SharedLibraryLoaderCentosSuite extends AnyFunSuite {
  private val centos8 = new SharedLibraryLoaderCentos8

  // NAME and VERSION as read from /etc/os-release on TencentOS Server 3.2.
  test("TencentOS Server 3.2 is accepted by the centos8 loader") {
    assert(centos8.accepts("TencentOS Server", "3.2 (Final)"))
  }

  test("other TencentOS versions are not accepted") {
    assert(!centos8.accepts("TencentOS Server", "2.4"))
    assert(!centos8.accepts("TencentOS Server", "3.1 (Final)"))
    assert(!centos8.accepts("TencentOS Server", "4.2"))
  }
}
