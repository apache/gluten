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

#include "JniCastException.h"

#include <string_view>
#include "velox/common/base/VeloxException.h"
#include "velox/expression/ExpressionExceptionProperties.h"

namespace gluten {
namespace {

std::string castDiagnostic(const facebook::velox::VeloxException& error) {
  const auto hasNul = [](const std::string& text) { return text.find('\0') != std::string::npos; };
  if (!hasNul(error.message()) && !hasNul(error.failingExpression()) && !hasNul(error.context()) &&
      !hasNul(error.additionalContext()) && !hasNul(error.exceptionName())) {
    return error.what();
  }

  // what() exposes a C string, not its length. Render the original fields and captured stack
  // instead of truncating at the NUL or reading past the end of that C string.
  std::string diagnostic;
  const auto field = [&](std::string_view name, std::string_view value) {
    if (!value.empty()) {
      diagnostic.append(name).append(": ").append(value).append("\n");
    }
  };
  field("Exception", error.exceptionName());
  field("Error Source", error.errorSource());
  field("Error Code", error.errorCode());
  field("Reason", error.message());
  field("Retriable", error.isRetriable() ? "True" : "False");
  field("Expression", error.failingExpression());
  field("Context", error.context());
  field("Additional Context", error.additionalContext());
  if (error.function()) {
    field("Function", error.function());
  }
  if (error.file()) {
    field("File", error.file());
  }
  if (error.line()) {
    field("Line", std::to_string(error.line()));
  }
  diagnostic.append("Stack trace:\n");
  if (const auto* stack = error.stackTrace()) {
    diagnostic.append(stack->toString());
  } else {
    diagnostic.append("No native stack trace was captured.");
  }
  return diagnostic;
}

} // namespace

std::optional<JniExceptionMetadata> describeNativeCastException(const std::exception& error) {
  using namespace facebook::velox;
  const auto* native = dynamic_cast<const VeloxException*>(&error);
  if (!native || native->errorSource() != error_source::kErrorSourceUser ||
      native->errorCode() != error_code::kInvalidArgument) {
    return std::nullopt;
  }
  const auto* properties = dynamic_cast<const ExpressionExceptionProperties*>(native->properties().get());
  if (!properties || !properties->owner.empty() || properties->functionName != "cast") {
    return std::nullopt;
  }
  // Velox's cast failures start with this prefix. Other errors raised while the cast runs, such as
  // lazily loading its input, inherit the cast's properties but keep the default handling.
  if (!std::string_view(native->message()).starts_with("Cannot cast ")) {
    return std::nullopt;
  }
  return JniExceptionMetadata{
      "org/apache/gluten/exception/NativeCastException", native->message(), castDiagnostic(*native)};
}

} // namespace gluten
