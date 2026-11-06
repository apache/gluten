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

#pragma once

#include <optional>
#include <string>
#include <vector>

namespace gluten {

/// The native HTTP clients (libcurl + OpenSSL, statically linked) no longer compile in a CA bundle path and look for
/// trusted certificates at runtime, in this order: the file named by SSL_CERT_FILE (or /etc/ssl/cert.pem when it is not
/// set), plus the hashed certificate directory named by SSL_CERT_DIR (or /etc/ssl/certs). Some distributions provide
/// neither (e.g. RHEL 8 family, which keeps its bundle under /etc/pki), which makes every TLS handshake fail.

/// Locations used to decide whether OpenSSL finds trusted certificates, and where to look for a replacement.
struct TlsSearchPaths {
  /// OpenSSL's default CA file, used when SSL_CERT_FILE is not set.
  std::string defaultCaFile;
  /// OpenSSL's default hashed certificate directory, used when SSL_CERT_DIR is not set.
  std::string defaultCaDir;
  /// Well-known system CA bundles, in the order they are tried.
  std::vector<std::string> bundles;
};

/// The locations of the running system.
const TlsSearchPaths& systemTlsSearchPaths();

/// Returns the path of a CA bundle that should be used through SSL_CERT_FILE, or std::nullopt when nothing needs to be
/// done: either OpenSSL already finds trusted certificates, or none of the bundles in 'paths' exists. 'sslCertFile' and
/// 'sslCertDir' are the values of the environment variables of the same name, if set.
std::optional<std::string> findFallbackCaBundle(
    const std::optional<std::string>& sslCertFile,
    const std::optional<std::string>& sslCertDir,
    const TlsSearchPaths& paths);

/// Reads SSL_CERT_FILE and SSL_CERT_DIR from the environment, calls findFallbackCaBundle() and, if it returns a path,
/// exports it as SSL_CERT_FILE. A SSL_CERT_FILE that points to an existing file is never changed.
void ensureTlsTrustStore(const TlsSearchPaths& paths = systemTlsSearchPaths());

} // namespace gluten
