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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.hadoop.ParquetOutputFormat

import java.nio.charset.StandardCharsets
import java.util.{HashMap => JHashMap, Map => JMap}

object ParquetEncryption extends Logging {
  def getFileEncryptionProperties(conf: Configuration, path: Path): FileEncryptionProperties = {
    ParquetOutputFormat.createEncryptionProperties(conf, path, null)
  }
  def generateEncryptionOptionsFromProperties(
      encryptionProperties: FileEncryptionProperties): (String, JMap[String, Array[Byte]]) = {
    val map = new JHashMap[String, String]()
    val mapBytes = new JHashMap[String, Array[Byte]]()
    var algorithm = ""
    if (encryptionProperties == null) {
      return (algorithm, mapBytes)
    }
    if (encryptionProperties.getAlgorithm.isSetAES_GCM_V1) {
      algorithm = "AES_GCM_V1"
    } else if (encryptionProperties.getAlgorithm.isSetAES_GCM_CTR_V1) {
      algorithm = "AES_GCM_CTR_V1"
    } else {
      throw new IllegalArgumentException(encryptionProperties.getAlgorithm.toString)
    }
    mapBytes.put("footer_key_metadata", encryptionProperties.getFooterKeyMetadata)
    mapBytes.put("footer_key", encryptionProperties.getFooterKey)
    if (
      encryptionProperties.getEncryptedColumns != null &&
      !encryptionProperties.getEncryptedColumns.isEmpty
    ) {
      encryptionProperties.getEncryptedColumns.forEach(
        (key, value) => {
          // [key] => key
          val path = key.toString.substring(1, key.toString.length - 1)
          logInfo(
            s"path=>$path, encrypt=>${value.isEncrypted}, " +
              s"footerEncrypt=>${value.isEncryptedWithFooterKey}," +
              s"key=>${new String(value.getKeyBytes)}, meta=>${new String(value.getKeyMetaData)}")
          mapBytes.put("column_encrypted" + path, bool2bytes(value.isEncrypted))
          mapBytes.put("column_footer" + path, bool2bytes(value.isEncryptedWithFooterKey))
          mapBytes.put("column_key" + path, value.getKeyBytes)
          mapBytes.put("column_meta" + path, value.getKeyMetaData)
        })
    }
    (algorithm, mapBytes)
  }
  private def bytesToString(bytes: Array[Byte]): String = {
    new String(bytes, StandardCharsets.UTF_8)
  }
  private def bool2bytes(flag: Boolean) = {
    if (flag) {
      Array[Byte](1)
    } else {
      Array[Byte](0)
    }
  }
}
