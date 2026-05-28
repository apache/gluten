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

#include "FilePathGenerator.h"
#include "JsonToProtoConverter.h"

#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/SubstraitToVeloxPlanValidator.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/process/StackTrace.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace gluten {

class Substrait2VeloxPlanValidatorTest : public exec::test::HiveConnectorTestBase {
 protected:
  bool validatePlan(std::string file) {
    std::string subPlanPath = FilePathGenerator::getDataFilePath(file);

    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
    return validatePlan(substraitPlan);
  }

  bool validatePlan(::substrait::Plan& plan) {
    auto planValidator = std::make_shared<SubstraitToVeloxPlanValidator>(pool_.get());
    return planValidator->validate(plan);
  }
};

TEST_F(Substrait2VeloxPlanValidatorTest, group) {
  std::string subPlanPath = FilePathGenerator::getDataFilePath("group.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);

  ASSERT_FALSE(validatePlan(substraitPlan));
}

// Pins down the VeloxException accessor contract that the JNI diagnostic path
// in VeloxJniWrapper.cc depends on, so any future Velox bump that breaks it
// surfaces here instead of at runtime.
TEST_F(Substrait2VeloxPlanValidatorTest, veloxExceptionDiagnosticAccessors) {
  try {
    VELOX_USER_FAIL("duplicated rule");
  } catch (const VeloxException& e) {
    ASSERT_NE(e.message().find("duplicated rule"), std::string::npos)
        << "message() should contain the original VELOX_USER_FAIL text but was: '" << e.message() << "'";
    EXPECT_FALSE(e.errorSource().empty()) << "errorSource() must not be empty";
    EXPECT_FALSE(e.errorCode().empty()) << "errorCode() must not be empty";
    // failingExpression() and context() may be empty outside of expression
    // evaluation; we only require the accessors to be callable.
    (void)e.failingExpression();
    (void)e.context();
    // stackTrace() is optional (controlled by FLAGS_velox_exception_*_stacktrace_enabled).
    if (const auto* trace = e.stackTrace()) {
      EXPECT_FALSE(trace->toString().empty());
    }
    return;
  }
  FAIL() << "Expected VELOX_USER_FAIL to throw a VeloxException";
}

// Verifies that what() returns the bare message, matching what the JVM sees
// via JNI_METHOD_END and motivating the extra logging in the JNI catch handler.
TEST_F(Substrait2VeloxPlanValidatorTest, veloxExceptionWhatIsBareMessage) {
  try {
    VELOX_USER_FAIL("duplicated rule");
  } catch (const VeloxException& e) {
    const std::string what = e.what();
    EXPECT_NE(what.find("duplicated rule"), std::string::npos)
        << "what() should contain the original error text but was: '" << what << "'";
    return;
  }
  FAIL() << "Expected VELOX_USER_FAIL to throw a VeloxException";
}

} // namespace gluten
