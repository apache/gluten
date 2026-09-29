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
package org.apache.gluten.config

/**
 * The ReservedKeys retains the configuration constants used internally by Gluten, which are not
 * exposed to users.
 *
 * TODO: Other internal constant key should be moved here.
 */
object ReservedKeys {

  // Tokens of current user, split by `\0`
  val GLUTEN_UGI_TOKENS = "spark.gluten.ugi.tokens"

  // Principal of current user
  val GLUTEN_UGI_USERNAME = "spark.gluten.ugi.username"

  // Shuffle writer type.
  val GLUTEN_HASH_SHUFFLE_WRITER = "hash"
  val GLUTEN_SORT_SHUFFLE_WRITER = "sort"
  val GLUTEN_RSS_SORT_SHUFFLE_WRITER = "rss_sort"
}
