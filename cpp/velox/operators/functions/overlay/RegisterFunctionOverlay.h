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

namespace gluten {

/// Registers all functions implemented in Gluten's function overlay
/// (operators/functions/overlay). The overlay hosts function implementations
/// managed on the Gluten side, either functions not yet available in Velox or
/// Gluten-specific overrides of Velox functions. It is registered after all
/// Velox functions, so a function registered here with the same name and
/// signature takes precedence over the Velox implementation.
void registerFunctionOverlay();

} // namespace gluten
