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

class SharedLibraryLoaderUbuntu2204 extends SharedLibraryLoader {

  override def accepts(osName: String, osVersion: String): Boolean = {
    osName.contains("Ubuntu") && osVersion.startsWith("22.04")
  }
  override def loadLib(loader: BoltJniLibLoader): Unit = {
    loader.loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so")
    loader.loadAndCreateLink("libicudata.so.70", "libicudata.so")
    loader.loadAndCreateLink("libicuuc.so.70", "libicuuc.so")
    loader.loadAndCreateLink("libicui18n.so.70", "libicui18n.so")
    loader.loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so")
    loader.loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so")
    loader.loadAndCreateLink("librtmp.so.1", "librtmp.so")
    loader.loadAndCreateLink("libssh.so.4", "libssh.so")
    loader.loadAndCreateLink("libsasl2.so.2", "libsasl2.so")
    loader.loadAndCreateLink("liblber-2.5.so.0", "liblber-2.5.so")
    loader.loadAndCreateLink("libldap-2.5.so.0", "libldap-2.5.so")
    loader.loadAndCreateLink("libcurl.so.4", "libcurl.so")
    loader.loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so")
    loader.loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so")
    loader.loadAndCreateLink("libgflags.so.2.2", "libgflags.so")
    loader.loadAndCreateLink("libunwind.so.8", "libunwind.so")
    loader.loadAndCreateLink("libglog.so.0", "libglog.so")
    loader.loadAndCreateLink("libidn.so.12", "libidn.so")
    loader.loadAndCreateLink("libntlm.so.0", "libntlm.so")
    loader.loadAndCreateLink("libgsasl.so.7", "libgsasl.so")
    loader.loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so")
    loader.loadAndCreateLink("libxml2.so.2", "libxml2.so")
    loader.loadAndCreateLink("libre2.so.9", "libre2.so")
    loader.loadAndCreateLink("libsnappy.so.1", "libsnappy.so")
    loader.loadAndCreateLink("libthrift-0.16.0.so", "libthrift.so")
  }

}
