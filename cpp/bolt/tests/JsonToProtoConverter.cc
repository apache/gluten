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

#include "JsonToProtoConverter.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>

#include "utils/Exception.h"

void JsonToProtoConverter::readFromFile(const std::string& msgPath, google::protobuf::Message& msg) {
  std::ifstream msgJson(msgPath);
  GLUTEN_CHECK(!msgJson.fail(), std::string("Failed to open file: ") + msgPath + ". " + std::strerror(errno));
  std::stringstream buffer;
  buffer << msgJson.rdbuf();
  std::string msgData = buffer.str();
  auto status = google::protobuf::util::JsonStringToMessage(msgData, &msg);
  GLUTEN_CHECK(
      status.ok(),
      std::string("Failed to parse Substrait JSON: ") + std::to_string(static_cast<int8_t>(status.code())) +
          " " + status.message().ToString());
}
