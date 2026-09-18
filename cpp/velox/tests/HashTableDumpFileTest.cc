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

#include "utils/HashTableDumpFile.h"

#include <gtest/gtest.h>

#include "utils/Exception.h"

namespace gluten {

// The dumping side and the replaying side are different binaries, so this format is a contract
// between them. See https://github.com/apache/gluten/issues/12504.
class HashTableDumpFileTest : public ::testing::Test {
 protected:
  static HashTableDump makeDump() {
    HashTableDump dump;
    dump.cacheKey = "421";
    dump.ignoreNullKeys = true;
    dump.joinHasNullKeys = false;
    dump.payload = {0x00, 0x01, 0x7f, 0x80, 0xff};
    return dump;
  }
};

TEST_F(HashTableDumpFileTest, roundTrip) {
  const auto dump = makeDump();
  const auto decoded = decodeHashTableDump(encodeHashTableDump(dump));

  EXPECT_EQ(decoded.cacheKey, dump.cacheKey);
  EXPECT_EQ(decoded.ignoreNullKeys, dump.ignoreNullKeys);
  EXPECT_EQ(decoded.joinHasNullKeys, dump.joinHasNullKeys);
  EXPECT_EQ(decoded.payload, dump.payload);
}

TEST_F(HashTableDumpFileTest, roundTripPreservesFlagsIndependently) {
  // The two flags are easy to transpose, and transposing them would deserialize into the wrong
  // HashTable specialization rather than fail outright.
  for (const bool ignoreNullKeys : {false, true}) {
    for (const bool joinHasNullKeys : {false, true}) {
      auto dump = makeDump();
      dump.ignoreNullKeys = ignoreNullKeys;
      dump.joinHasNullKeys = joinHasNullKeys;

      const auto decoded = decodeHashTableDump(encodeHashTableDump(dump));
      EXPECT_EQ(decoded.ignoreNullKeys, ignoreNullKeys);
      EXPECT_EQ(decoded.joinHasNullKeys, joinHasNullKeys);
    }
  }
}

TEST_F(HashTableDumpFileTest, roundTripEmptyPayload) {
  auto dump = makeDump();
  dump.payload.clear();

  const auto decoded = decodeHashTableDump(encodeHashTableDump(dump));
  EXPECT_EQ(decoded.cacheKey, dump.cacheKey);
  EXPECT_TRUE(decoded.payload.empty());
}

TEST_F(HashTableDumpFileTest, rejectsForeignContent) {
  EXPECT_THROW(decodeHashTableDump(""), GlutenException);
  EXPECT_THROW(decodeHashTableDump("not a hash table dump at all"), GlutenException);
}

TEST_F(HashTableDumpFileTest, rejectsTruncatedContent) {
  const auto encoded = encodeHashTableDump(makeDump());
  // Truncating anywhere past the magic must be reported, not read past.
  for (size_t size = 8; size < encoded.size(); ++size) {
    EXPECT_THROW(decodeHashTableDump(encoded.substr(0, size)), GlutenException) << "size " << size;
  }
}

TEST_F(HashTableDumpFileTest, rejectsEmptyCacheKey) {
  HashTableDump dump;
  dump.payload = {0x01};
  EXPECT_THROW(encodeHashTableDump(dump), GlutenException);
}

TEST_F(HashTableDumpFileTest, fileNameKeepsCacheKeyWithinTheDumpDirectory) {
  EXPECT_EQ(hashTableDumpFileName(1, 2, 3, "421"), "hashtable_1_2_3_421.bin");
  // A cache key is a Spark plan id, but never let one reach the filesystem verbatim.
  EXPECT_EQ(hashTableDumpFileName(1, 2, 3, "../../etc/passwd"), "hashtable_1_2_3_______etc_passwd.bin");
}

} // namespace gluten
