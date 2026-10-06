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

class SharedLibraryLoaderUbuntu2004 extends SharedLibraryLoader {

  override def accepts(osName: String, osVersion: String): Boolean = {
    osName.contains("Ubuntu") && osVersion.startsWith("20.04")
  }

  override def loadLib(loader: BoltJniLibLoader): Unit = {
    loader.loadAndCreateLink("libroken.so.18", "libroken.so")
    loader.loadAndCreateLink("libasn1.so.8", "libasn1.so")
    loader.loadAndCreateLink("libboost_context.so.1.84.0", "libboost_context.so")
    loader.loadAndCreateLink("libboost_regex.so.1.84.0", "libboost_regex.so")
    loader.loadAndCreateLink("libbrotlicommon.so.1", "libbrotlicommon.so")
    loader.loadAndCreateLink("libbrotlidec.so.1", "libbrotlidec.so")
    loader.loadAndCreateLink("libpsl.so.5", "libpsl.so")
    loader.loadAndCreateLink("libcrypto.so.1.1", "libcrypto.so")
    loader.loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so")
    loader.loadAndCreateLink("libnettle.so.7", "libnettle.so")
    loader.loadAndCreateLink("libhogweed.so.5", "libhogweed.so")
    loader.loadAndCreateLink("librtmp.so.1", "librtmp.so")
    loader.loadAndCreateLink("libssh.so.4", "libssh.so")
    loader.loadAndCreateLink("libssl.so.1.1", "libssl.so")
    loader.loadAndCreateLink("liblber-2.4.so.2", "liblber-2.4.so")
    loader.loadAndCreateLink("libsasl2.so.2", "libsasl2.so")
    loader.loadAndCreateLink("libwind.so.0", "libwind.so")
    loader.loadAndCreateLink("libheimbase.so.1", "libheimbase.so")
    loader.loadAndCreateLink("libhcrypto.so.4", "libhcrypto.so")
    loader.loadAndCreateLink("libhx509.so.5", "libhx509.so")
    loader.loadAndCreateLink("libkrb5.so.26", "libkrb5.so")
    loader.loadAndCreateLink("libheimntlm.so.0", "libheimntlm.so")
    loader.loadAndCreateLink("libgssapi.so.3", "libgssapi.so")
    loader.loadAndCreateLink("libldap_r-2.4.so.2", "libldap_r-2.4.so")
    loader.loadAndCreateLink("libcurl.so.4", "libcurl.so")
    loader.loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so")
    loader.loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so")
    loader.loadAndCreateLink("libgflags.so.2.2", "libgflags.so")
    loader.loadAndCreateLink("libunwind.so.8", "libunwind.so")
    loader.loadAndCreateLink("libglog.so.0", "libglog.so")
    loader.loadAndCreateLink("libidn.so.11", "libidn.so")
    loader.loadAndCreateLink("libntlm.so.0", "libntlm.so")
    loader.loadAndCreateLink("libgsasl.so.7", "libgsasl.so")
    loader.loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so")
    loader.loadAndCreateLink("libicudata.so.66", "libicudata.so")
    loader.loadAndCreateLink("libicuuc.so.66", "libicuuc.so")
    loader.loadAndCreateLink("libxml2.so.2", "libxml2.so")
    loader.loadAndCreateLink("libre2.so.5", "libre2.so")
    loader.loadAndCreateLink("libsnappy.so.1", "libsnappy.so")
    loader.loadAndCreateLink("libthrift-0.13.0.so", "libthrift.so")
  }
}
