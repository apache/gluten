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

#include <gtest/gtest.h>
#include <vector>
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/SimpleFunctionRegistry.h"
#include "udf/UdfLoader.h"

using namespace bytedance::bolt;

class MyUdfTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    auto udfLoader = gluten::UdfLoader::getInstance();
    udfLoader->loadUdfLibraries("../udf/examples/libmyudf.so,../udf/examples/libmyudaf.so");
    udfLoader->registerUdf();
  }
};

TEST_F(MyUdfTest, hivestringstring) {
  const std::string name = "org.apache.spark.sql.hive.execution.UDFStringString";
  EXPECT_EQ(TypeKind::VARCHAR, exec::simpleFunctions().resolveFunction(name, {VARCHAR(), VARCHAR()})->type()->kind());
}

TEST_F(MyUdfTest, aggregateSharesBackendRegistry) {
  const auto signatures = exec::getAggregateFunctionSignatures("test.org.apache.spark.sql.MyDoubleAvg");
  ASSERT_TRUE(signatures.has_value());
  EXPECT_FALSE(signatures->empty());
}
