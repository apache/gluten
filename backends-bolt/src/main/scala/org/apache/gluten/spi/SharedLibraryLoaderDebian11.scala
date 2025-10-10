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

import org.apache.gluten.jni.BoltJniLibLoader

class SharedLibraryLoaderDebian11 extends SharedLibraryLoader {

  override def accepts(osName: String, osVersion: String): Boolean = {
    osName.contains("Debian") && osVersion.startsWith("11")
  }

  override def loadLib(loader: BoltJniLibLoader): Unit = {
    loader.loadAndCreateLink("libicudata.so.67", "libicudata.so")
    loader.loadAndCreateLink("libre2.so.9", "libre2.so")
    loader.loadAndCreateLink("libicuuc.so.67", "libicuuc.so")
    loader.loadAndCreateLink("liblber-2.4.so.2", "liblber-2.4.so")
    loader.loadAndCreateLink("libsasl2.so.2", "libsasl2.so")
    loader.loadAndCreateLink("libbrotlicommon.so.1", "libbrotlicommon.so")
    loader.loadAndCreateLink("libicui18n.so.67", "libicui18n.so")
    loader.loadAndCreateLink("libunwind.so.8", "libunwind.so")
    loader.loadAndCreateLink("libgflags.so.2.2", "libgflags.so")
    loader.loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so")
    loader.loadAndCreateLink("librtmp.so.1", "librtmp.so")
    loader.loadAndCreateLink("libssh2.so.1", "libssh2.so")
    loader.loadAndCreateLink("libpsl.so.5", "libpsl.so")
    loader.loadAndCreateLink("libldap_r-2.4.so.2", "libldap_r-2.4.so")
    loader.loadAndCreateLink("libbrotlidec.so.1", "libbrotlidec.so")
    loader.loadAndCreateLink("libthrift-0.13.0.so", "libthrift.so")
    loader.loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so")
    loader.loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so")
    loader.loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so")
    loader.loadAndCreateLink("libglog.so.0", "libglog.so")
    loader.loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so")
    loader.loadAndCreateLink("libsnappy.so.1", "libsnappy.so")
    loader.loadAndCreateLink("libcurl.so.4", "libcurl.so")
    loader.loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so")
  }

}
