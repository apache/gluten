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
#include "config.h"

#if USE_HDFS
#include <Disks/DiskObjectStorage/ObjectStorages/HDFS/HDFSObjectStorage.h>
#endif

namespace local_engine
{

#if USE_HDFS
class GlutenHDFSObjectStorage final : public DB::HDFSObjectStorage
{
public:
    GlutenHDFSObjectStorage(
            const String & hdfs_root_path_,
            SettingsPtr settings_,
            const Poco::Util::AbstractConfiguration & config_)
        : HDFSObjectStorage(hdfs_root_path_, std::move(settings_), config_, /* lazy_initialize */false)
    {
    }
    std::unique_ptr<DB::ReadBufferFromFileBase> readObject( /// NOLINT
      const DB::StoredObject & object,
      const DB::ReadSettings & read_settings = DB::ReadSettings{},
      std::optional<size_t> read_hint = {}) const override;
    hdfsFS getHDFSFS() const { return hdfs_fs.get(); }
};
#endif

}


