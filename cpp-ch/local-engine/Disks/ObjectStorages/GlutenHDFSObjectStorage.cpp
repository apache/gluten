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

#include "GlutenHDFSObjectStorage.h"
#if USE_HDFS
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
using namespace DB;
namespace local_engine
{
std::unique_ptr<ReadBufferFromFileBase> GlutenHDFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>) const
{
    size_t begin_of_path = object.remote_path.find('/', object.remote_path.find("//") + 2);
    auto hdfs_path = object.remote_path.substr(begin_of_path);
    auto hdfs_uri = object.remote_path.substr(0, begin_of_path);
    return std::make_unique<ReadBufferFromHDFS>(
        hdfs_uri,
        hdfs_path,
        config,
        HDFSObjectStorage::patchSettings(read_settings),
        0,
        read_settings.remote_read_buffer_use_external_buffer);
}

}
#endif
