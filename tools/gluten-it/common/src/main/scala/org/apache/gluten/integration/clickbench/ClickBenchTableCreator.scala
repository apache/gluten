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
package org.apache.gluten.integration.clickbench

import org.apache.gluten.integration.table.TableCreator

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.types.StructType

import java.io.File

object ClickBenchTableCreator extends TableCreator {
  private val TABLE_NAME = "hits"
  private val SCHEMA: StructType = StructType.fromDDL("""
                                                        |watchid bigint,
                                                        |javaenable smallint,
                                                        |title varchar(65535),
                                                        |goodevent smallint,
                                                        |eventtime timestamp,
                                                        |eventdate date,
                                                        |counterid int,
                                                        |clientip int,
                                                        |regionid int,
                                                        |userid bigint,
                                                        |counterclass smallint,
                                                        |os smallint,
                                                        |useragent smallint,
                                                        |url varchar(65535),
                                                        |referer varchar(65535),
                                                        |isrefresh smallint,
                                                        |referercategoryid smallint,
                                                        |refererregionid int,
                                                        |urlcategoryid smallint,
                                                        |urlregionid int,
                                                        |resolutionwidth smallint,
                                                        |resolutionheight smallint,
                                                        |resolutiondepth smallint,
                                                        |flashmajor smallint,
                                                        |flashminor smallint,
                                                        |flashminor2 varchar(65535),
                                                        |netmajor smallint,
                                                        |netminor smallint,
                                                        |useragentmajor smallint,
                                                        |useragentminor varchar(65535),
                                                        |cookieenable smallint,
                                                        |javascriptenable smallint,
                                                        |ismobile smallint,
                                                        |mobilephone smallint,
                                                        |mobilephonemodel varchar(65535),
                                                        |params varchar(65535),
                                                        |ipnetworkid int,
                                                        |traficsourceid smallint,
                                                        |searchengineid smallint,
                                                        |searchphrase varchar(65535),
                                                        |advengineid smallint,
                                                        |isartifical smallint,
                                                        |windowclientwidth smallint,
                                                        |windowclientheight smallint,
                                                        |clienttimezone smallint,
                                                        |clienteventtime timestamp,
                                                        |silverlightversion1 smallint,
                                                        |silverlightversion2 smallint,
                                                        |silverlightversion3 int,
                                                        |silverlightversion4 smallint,
                                                        |pagecharset varchar(65535),
                                                        |codeversion int,
                                                        |islink smallint,
                                                        |isdownload smallint,
                                                        |isnotbounce smallint,
                                                        |funiqid bigint,
                                                        |originalurl varchar(65535),
                                                        |hid int,
                                                        |isoldcounter smallint,
                                                        |isevent smallint,
                                                        |isparameter smallint,
                                                        |dontcounthits smallint,
                                                        |withhash smallint,
                                                        |hitcolor varchar(65535),
                                                        |localeventtime timestamp,
                                                        |age smallint,
                                                        |sex smallint,
                                                        |income smallint,
                                                        |interests smallint,
                                                        |robotness smallint,
                                                        |remoteip int,
                                                        |windowname int,
                                                        |openername int,
                                                        |historylength smallint,
                                                        |browserlanguage varchar(65535),
                                                        |browsercountry varchar(65535),
                                                        |socialnetwork varchar(65535),
                                                        |socialaction varchar(65535),
                                                        |httperror smallint,
                                                        |sendtiming int,
                                                        |dnstiming int,
                                                        |connecttiming int,
                                                        |responsestarttiming int,
                                                        |responseendtiming int,
                                                        |fetchtiming int,
                                                        |socialsourcenetworkid smallint,
                                                        |socialsourcepage varchar(65535),
                                                        |paramprice bigint,
                                                        |paramorderid varchar(65535),
                                                        |paramcurrency varchar(65535),
                                                        |paramcurrencyid smallint,
                                                        |openstatservicename varchar(65535),
                                                        |openstatcampaignid varchar(65535),
                                                        |openstatadid varchar(65535),
                                                        |openstatsourceid varchar(65535),
                                                        |utmsource varchar(65535),
                                                        |utmmedium varchar(65535),
                                                        |utmcampaign varchar(65535),
                                                        |utmcontent varchar(65535),
                                                        |utmterm varchar(65535),
                                                        |fromtag varchar(65535),
                                                        |hasgclid smallint,
                                                        |refererhash bigint,
                                                        |urlhash bigint,
                                                        |clid int
                                                        |""".stripMargin)

  override def create(spark: SparkSession, source: String, dataPath: String): Unit = {
    if (spark.catalog.tableExists(TABLE_NAME)) {
      println("Table exists: " + TABLE_NAME)
      return
    }
    println("Creating catalog table: " + TABLE_NAME)
    val file = new File(dataPath + File.separator + ClickBenchDataGen.FILE_NAME)
    spark.catalog.createTable(TABLE_NAME, source, SCHEMA, Map("path" -> file.getAbsolutePath))
    try {
      spark.catalog.recoverPartitions(TABLE_NAME)
    } catch {
      case _: AnalysisException =>
    }
  }
}
