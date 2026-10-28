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

#include "utils/TlsTrustStore.h"

#include <glog/logging.h>
#include <openssl/x509.h>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>

namespace gluten {

namespace {

namespace fs = std::filesystem;

bool isRegularFile(const std::string& path) {
  std::error_code ec;
  return fs::is_regular_file(path, ec) && !ec;
}

// OpenSSL finds certificates in a directory only through files named <8 hex digits>.<digit>, created by
// `openssl rehash`.
bool isHashedCertificateName(const std::string& name) {
  if (name.size() < 10 || name[8] != '.') {
    return false;
  }
  for (size_t i = 0; i < 8; ++i) {
    if (!std::isxdigit(static_cast<unsigned char>(name[i]))) {
      return false;
    }
  }
  for (size_t i = 9; i < name.size(); ++i) {
    if (!std::isdigit(static_cast<unsigned char>(name[i]))) {
      return false;
    }
  }
  return true;
}

bool hasHashedCertificate(const std::string& dir) {
  std::error_code ec;
  fs::directory_iterator it(dir, fs::directory_options::skip_permission_denied, ec);
  if (ec) {
    return false;
  }
  for (const auto& entry : it) {
    if (isHashedCertificateName(entry.path().filename().string())) {
      return true;
    }
  }
  return false;
}

std::optional<std::string> getEnv(const char* name) {
  const char* value = std::getenv(name);
  return value != nullptr ? std::optional<std::string>(value) : std::nullopt;
}

} // namespace

const TlsSearchPaths& systemTlsSearchPaths() {
  // OpenSSL's default locations are fixed when OpenSSL is built (--openssldir). They are /etc/ssl for the statically
  // linked vcpkg OpenSSL but can differ for the system OpenSSL of a VCPKG_DYNAMIC_OPENSSL=ON build, so ask the library
  // that is actually linked instead of assuming them.
  static const TlsSearchPaths paths{
      X509_get_default_cert_file(),
      X509_get_default_cert_dir(),
      {
          "/etc/ssl/certs/ca-certificates.crt", // Debian, Ubuntu, Alpine
          "/etc/pki/tls/certs/ca-bundle.crt", // RHEL, CentOS, Rocky, Alma, Fedora
          "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // RHEL family, what the link above points to
          "/etc/ssl/ca-bundle.pem", // openSUSE
      }};
  return paths;
}

std::optional<std::string> findFallbackCaBundle(
    const std::optional<std::string>& sslCertFile,
    const std::optional<std::string>& sslCertDir,
    const TlsSearchPaths& paths) {
  // OpenSSL does not fall back to its default file when SSL_CERT_FILE is set but unusable.
  const bool fileUsable = isRegularFile(sslCertFile.value_or(paths.defaultCaFile));
  if (fileUsable) {
    return std::nullopt;
  }
  if (hasHashedCertificate(sslCertDir.value_or(paths.defaultCaDir))) {
    return std::nullopt;
  }

  for (const auto& candidate : paths.bundles) {
    if (isRegularFile(candidate)) {
      return candidate;
    }
  }
  return std::nullopt;
}

void ensureTlsTrustStore(const TlsSearchPaths& paths) {
  const auto previous = getEnv("SSL_CERT_FILE");
  const auto certDir = getEnv("SSL_CERT_DIR");
  // Shown with --v=1 or higher; lets one see which locations the decision below was based on.
  VLOG(1) << "OpenSSL default certificate file '" << paths.defaultCaFile << "' and directory '" << paths.defaultCaDir
          << "'; SSL_CERT_FILE=" << previous.value_or("<unset>") << ", SSL_CERT_DIR=" << certDir.value_or("<unset>");
  const auto bundle = findFallbackCaBundle(previous, certDir, paths);
  if (!bundle.has_value()) {
    return;
  }
  // Only report the new value once it is in place.
  if (::setenv("SSL_CERT_FILE", bundle->c_str(), 1) != 0) {
    LOG(ERROR) << "Failed to set SSL_CERT_FILE to '" << bundle.value() << "': " << std::strerror(errno)
               << ". TLS connections made by the native S3, GCS and ABFS clients may fail.";
    return;
  }
  if (previous.has_value()) {
    LOG(WARNING) << "SSL_CERT_FILE='" << previous.value() << "' is not an existing file; using '" << bundle.value()
                 << "' instead.";
  } else {
    LOG(INFO) << "No trusted certificates found at the OpenSSL default locations; using '" << bundle.value()
              << "' as SSL_CERT_FILE.";
  }
}

} // namespace gluten
