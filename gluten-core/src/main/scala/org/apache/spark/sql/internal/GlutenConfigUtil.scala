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
package org.apache.spark.sql.internal

import org.apache.gluten.config._

object GlutenConfigUtil {
  private def getConfString(
      configProvider: GlutenConfigProvider,
      key: String,
      value: String): String = {
    ConfigRegistry
      .findEntry(key)
      .map {
        _.readFrom(configProvider) match {
          case o: Option[_] => o.map(_.toString).getOrElse(value)
          case null => value
          case v => v.toString
        }
      }
      .getOrElse(value)
  }

  def parseConfig(conf: Map[String, String]): Map[String, String] = {
    val (glutenConf, otherConf) = conf.partition(_._1.startsWith("spark.gluten."))
    val provider = new MapProvider(glutenConf)
    val parsedConf = glutenConf.map { case (k, v) => (k, getConfString(provider, k, v)) }
    parsedConf ++ otherConf
  }
}
