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

#include "utils/GlutenS3TokenProvider.h"

#include <gtest/gtest.h>

#include "velox/common/file/PlainUserNameTokenProvider.h"

namespace gluten {
namespace {

using namespace facebook::velox::filesystems;

std::unordered_map<std::string, std::string> readProperties(
    const std::string& location,
    const std::string& accessKeyId,
    const std::string& secretAccessKey,
    const std::string& sessionToken = "") {
  std::unordered_map<std::string, std::string> properties{
      {kReadPropertiesLocation, location},
      {kReadPropertiesAccessKeyId, accessKeyId},
      {kReadPropertiesSecretAccessKey, secretAccessKey}};
  if (!sessionToken.empty()) {
    properties[kReadPropertiesSessionToken] = sessionToken;
  }
  return properties;
}

std::shared_ptr<S3AccessToken> tokenFor(const TokenProvider& provider, const std::string& path) {
  return std::dynamic_pointer_cast<S3AccessToken>(provider.getToken(S3AccessTokenKey{path}));
}

// A key belonging to some other file system.
class OtherAccessTokenKey : public AccessTokenKey {};

} // namespace

TEST(GlutenS3TokenProviderTest, noProviderWithoutCredentials) {
  ASSERT_EQ(GlutenS3TokenProvider::create({}), nullptr);
  // A scan of a table the process credentials can read carries nothing.
  ASSERT_EQ(GlutenS3TokenProvider::create({{}}), nullptr);
  // An incomplete credential set is not usable and must not be installed.
  ASSERT_EQ(
      GlutenS3TokenProvider::create(
          {{{kReadPropertiesLocation, "s3://bucket/db/t"}, {kReadPropertiesAccessKeyId, "ASIA"}}}),
      nullptr);
}

TEST(GlutenS3TokenProviderTest, resolvesCredentialsOfTheTableOwningThePath) {
  const auto provider = GlutenS3TokenProvider::create(
      {readProperties("s3://bucket/db/first", "ASIAFIRST", "first-secret", "first-token"),
       readProperties("s3a://bucket/db/second", "ASIASECOND", "second-secret")});
  ASSERT_NE(provider, nullptr);

  const auto first = tokenFor(*provider, "bucket/db/first/data/00000-0-a.parquet");
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->accessKeyId(), "ASIAFIRST");
  EXPECT_EQ(first->secretAccessKey(), "first-secret");
  EXPECT_EQ(first->sessionToken(), "first-token");

  // Same bucket, different table: the other credential set, and no session
  // token because the catalog vended none.
  const auto second = tokenFor(*provider, "bucket/db/second/data/00000-0-b.parquet");
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(second->accessKeyId(), "ASIASECOND");
  EXPECT_EQ(second->secretAccessKey(), "second-secret");
  EXPECT_EQ(second->sessionToken(), "");

  // A path no scan covers gets no token, which leaves the file system on its
  // configured credentials.
  EXPECT_EQ(tokenFor(*provider, "bucket/db/third/data/00000-0-c.parquet"), nullptr);
  // A table name the prefix is a string prefix of is a different table.
  EXPECT_EQ(tokenFor(*provider, "bucket/db/firstborn/data/00000-0-d.parquet"), nullptr);
}

TEST(GlutenS3TokenProviderTest, theLongestMatchingPrefixWins) {
  // A table whose location is nested inside another table's location must get
  // its own credentials, not the enclosing one's.
  const auto provider = GlutenS3TokenProvider::create(
      {readProperties("s3://bucket/db", "ASIAOUTER", "outer-secret"),
       readProperties("s3://bucket/db/nested", "ASIAINNER", "inner-secret")});
  ASSERT_NE(provider, nullptr);

  EXPECT_EQ(tokenFor(*provider, "bucket/db/nested/data/f.parquet")->accessKeyId(), "ASIAINNER");
  EXPECT_EQ(tokenFor(*provider, "bucket/db/other/data/f.parquet")->accessKeyId(), "ASIAOUTER");
}

TEST(GlutenS3TokenProviderTest, identityCoversAllCredentials) {
  const auto provider = GlutenS3TokenProvider::create({readProperties("s3://bucket/db/t", "ASIA", "secret", "token")});
  const auto same = GlutenS3TokenProvider::create({readProperties("s3://bucket/db/t", "ASIA", "secret", "token")});
  // A re-vended credential set for the same table is a different identity, so
  // velox's file handle cache cannot serve handles opened with the old one.
  const auto rotated =
      GlutenS3TokenProvider::create({readProperties("s3://bucket/db/t", "ASIA2", "secret2", "token2")});

  EXPECT_TRUE(provider->equals(*same));
  EXPECT_EQ(provider->hash(), same->hash());
  EXPECT_FALSE(provider->equals(*rotated));
  EXPECT_NE(provider->hash(), rotated->hash());

  // Providers of another kind are never equal.
  PlainUserNameTokenProvider other{"user"};
  EXPECT_FALSE(provider->equals(other));
  // A key belonging to another file system resolves nothing.
  EXPECT_EQ(provider->getToken(OtherAccessTokenKey{}), nullptr);
}

TEST(GlutenS3TokenProviderTest, normalizesS3Schemes) {
  EXPECT_EQ(GlutenS3TokenProvider::normalizeS3Path("s3://bucket/db/t"), "bucket/db/t");
  EXPECT_EQ(GlutenS3TokenProvider::normalizeS3Path("s3a://bucket/db/t"), "bucket/db/t");
  EXPECT_EQ(GlutenS3TokenProvider::normalizeS3Path("s3n://bucket/db/t"), "bucket/db/t");
  EXPECT_EQ(GlutenS3TokenProvider::normalizeS3Path("bucket/db/t"), "bucket/db/t");
}

} // namespace gluten
