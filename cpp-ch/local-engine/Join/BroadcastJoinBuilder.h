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
#include <cstdint>
#include <memory>
#include <jni.h>
#include <substrait/algebra.pb.h>

namespace DB
{
class ReadBuffer;
}

namespace local_engine
{
class StorageJoinFromReadBuffer;
namespace BroadcastJoinBuilder
{
std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
    const std::string & key,
    DB::ReadBuffer & input,
    jlong row_count,
    const std::string & join_keys,
    jint join_type,
    bool is_bhj,
    bool has_mixed_join_condition,
    bool is_existence_join,
    const std::string & named_struct,
    bool is_null_aware_anti_join,
    bool has_null_key_values);
void cleanBuildHashTable(const std::string & hash_table_id, jlong instance);

std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & hash_table_id);

void init(JNIEnv *);

void destroy(JNIEnv *);
}
}
