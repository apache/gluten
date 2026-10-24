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

#include <gtest/gtest.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>

namespace gluten {

namespace {

namespace fs = std::filesystem;

// All locations are inside a temporary directory, so the result never depends on the CA files of the host.
class TlsTrustStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saveEnv("SSL_CERT_FILE", savedFile_);
    saveEnv("SSL_CERT_DIR", savedDir_);
    ::unsetenv("SSL_CERT_FILE");
    ::unsetenv("SSL_CERT_DIR");

    root_ = fs::temp_directory_path() / ("gluten_tls_trust_store_test_" + std::to_string(::getpid()));
    fs::remove_all(root_);
    fs::create_directories(root_ / "certs");

    paths_.defaultCaFile = (root_ / "cert.pem").string();
    paths_.defaultCaDir = (root_ / "certs").string();
    paths_.bundles = {(root_ / "debian.crt").string(), (root_ / "rhel.crt").string()};
  }

  void TearDown() override {
    restoreEnv("SSL_CERT_FILE", savedFile_);
    restoreEnv("SSL_CERT_DIR", savedDir_);
    fs::remove_all(root_);
  }

  std::string touch(const fs::path& path) {
    std::ofstream(path) << "x";
    return path.string();
  }

  std::optional<std::string> find(
      const std::optional<std::string>& sslCertFile = std::nullopt,
      const std::optional<std::string>& sslCertDir = std::nullopt) {
    return findFallbackCaBundle(sslCertFile, sslCertDir, paths_);
  }

  fs::path root_;
  TlsSearchPaths paths_;

 private:
  static void saveEnv(const char* name, std::optional<std::string>& out) {
    const char* value = std::getenv(name);
    out = value != nullptr ? std::optional<std::string>(value) : std::nullopt;
  }

  static void restoreEnv(const char* name, const std::optional<std::string>& value) {
    if (value.has_value()) {
      ::setenv(name, value->c_str(), 1);
    } else {
      ::unsetenv(name);
    }
  }

  std::optional<std::string> savedFile_;
  std::optional<std::string> savedDir_;
};

TEST_F(TlsTrustStoreTest, noBundleAnywhereMeansNothingToDo) {
  EXPECT_EQ(find(), std::nullopt);
}

TEST_F(TlsTrustStoreTest, defaultCaFileIsEnough) {
  touch(root_ / "cert.pem");
  touch(root_ / "debian.crt");
  EXPECT_EQ(find(), std::nullopt);
}

TEST_F(TlsTrustStoreTest, existingSslCertFileIsEnough) {
  touch(root_ / "debian.crt");
  EXPECT_EQ(find(touch(root_ / "mine.pem")), std::nullopt);
}

TEST_F(TlsTrustStoreTest, hashedCertificateInDefaultDirectoryIsEnough) {
  touch(root_ / "debian.crt");
  touch(root_ / "certs" / "0a1b2c3d.0");
  EXPECT_EQ(find(), std::nullopt);
}

TEST_F(TlsTrustStoreTest, hashedCertificateIsEnoughEvenWhenSslCertFileIsMissing) {
  // OpenSSL still loads the hashed directory, so there is nothing to replace.
  touch(root_ / "debian.crt");
  touch(root_ / "certs" / "0a1b2c3d.0");
  EXPECT_EQ(find((root_ / "missing.pem").string()), std::nullopt);
}

TEST_F(TlsTrustStoreTest, sslCertDirOverridesDefaultDirectory) {
  touch(root_ / "debian.crt");
  touch(root_ / "certs" / "0a1b2c3d.0");
  fs::create_directories(root_ / "empty");
  // The default directory has a hashed certificate, but SSL_CERT_DIR points to an empty one.
  EXPECT_EQ(find(std::nullopt, (root_ / "empty").string()), (root_ / "debian.crt").string());
  // And the other way round.
  fs::create_directories(root_ / "mine");
  touch(root_ / "mine" / "ffffffff.12");
  EXPECT_EQ(find(std::nullopt, (root_ / "mine").string()), std::nullopt);
}

TEST_F(TlsTrustStoreTest, filesThatOpenSslDoesNotPickUpDoNotCount) {
  touch(root_ / "rhel.crt");
  touch(root_ / "certs" / "ca-certificates.crt");
  touch(root_ / "certs" / "0a1b2c3z.0"); // not hex
  touch(root_ / "certs" / "0a1b2c3d.pem"); // not a digit suffix
  touch(root_ / "certs" / "0a1b2c3.0"); // too short
  EXPECT_EQ(find(), (root_ / "rhel.crt").string());
}

TEST_F(TlsTrustStoreTest, bundlesAreTriedInOrder) {
  touch(root_ / "debian.crt");
  touch(root_ / "rhel.crt");
  EXPECT_EQ(find(), (root_ / "debian.crt").string());

  fs::remove(root_ / "debian.crt");
  EXPECT_EQ(find(), (root_ / "rhel.crt").string());
}

TEST_F(TlsTrustStoreTest, missingSslCertFileIsReplaced) {
  // OpenSSL does not fall back to its default file when SSL_CERT_FILE is set, so cert.pem does not help here.
  touch(root_ / "cert.pem");
  touch(root_ / "rhel.crt");
  EXPECT_EQ(find((root_ / "missing.pem").string()), (root_ / "rhel.crt").string());
}

TEST_F(TlsTrustStoreTest, directoryIsNotAFile) {
  fs::create_directories(root_ / "debian.crt");
  touch(root_ / "rhel.crt");
  EXPECT_EQ(find(), (root_ / "rhel.crt").string());
}

TEST_F(TlsTrustStoreTest, ensureExportsTheBundle) {
  touch(root_ / "rhel.crt");
  ensureTlsTrustStore(paths_);
  ASSERT_NE(std::getenv("SSL_CERT_FILE"), nullptr);
  EXPECT_EQ(std::string(std::getenv("SSL_CERT_FILE")), (root_ / "rhel.crt").string());
}

TEST_F(TlsTrustStoreTest, ensureReplacesAMissingSslCertFile) {
  touch(root_ / "debian.crt");
  ::setenv("SSL_CERT_FILE", (root_ / "missing.pem").c_str(), 1);
  ensureTlsTrustStore(paths_);
  EXPECT_EQ(std::string(std::getenv("SSL_CERT_FILE")), (root_ / "debian.crt").string());
}

TEST_F(TlsTrustStoreTest, ensureKeepsAnExistingSslCertFile) {
  touch(root_ / "debian.crt");
  const auto mine = touch(root_ / "mine.pem");
  ::setenv("SSL_CERT_FILE", mine.c_str(), 1);
  ensureTlsTrustStore(paths_);
  EXPECT_EQ(std::string(std::getenv("SSL_CERT_FILE")), mine);
}

TEST_F(TlsTrustStoreTest, ensureLeavesTheEnvironmentAloneWhenNothingIsFound) {
  ensureTlsTrustStore(paths_);
  EXPECT_EQ(std::getenv("SSL_CERT_FILE"), nullptr);
}

TEST_F(TlsTrustStoreTest, ensureHonorsSslCertDirFromTheEnvironment) {
  touch(root_ / "debian.crt");
  fs::create_directories(root_ / "mine");
  touch(root_ / "mine" / "0a1b2c3d.0");
  ::setenv("SSL_CERT_DIR", (root_ / "mine").c_str(), 1);
  ensureTlsTrustStore(paths_);
  EXPECT_EQ(std::getenv("SSL_CERT_FILE"), nullptr);
}

} // namespace

} // namespace gluten
