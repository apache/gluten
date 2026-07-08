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
package org.apache.gluten.substrait

import java.lang.{Long => JLong}
import java.security.InvalidParameterException
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

case class JoinParams() {
  // Whether preProjection is needed in streamed side.
  var streamPreProjectionNeeded = false

  // Whether preProjection is needed in build side.
  var buildPreProjectionNeeded = false

  // Whether postProjection is needed after Join.
  var postProjectionNeeded = true

  // Whether is BHJ
  var isBHJ = false

  // Whether the join is with condition
  var isWithCondition = false

  // Whether this join actually uses a pre-built serialized hash table from driver-side build.
  var usesDriverSideSerializedHashTable = false
}

case class AggregationParams() {
  // Whether rowConstruction is needed.
  var rowConstructionNeeded = false

  // Whether extraction from intermediate struct is needed.
  var extractionNeeded = false
}

class SubstraitContext extends Serializable {
  // A map stores the relationship between function name and function id.
  private val functionMap = new JHashMap[String, JLong]()

  // A map stores the relationship between Spark operator id and its respective Substrait Rel ids.
  private val operatorToRelsMap: JMap[JLong, JList[JLong]] = new JHashMap[JLong, JList[JLong]]()

  // Only for debug conveniently
  private val operatorToPlanNameMap = new JHashMap[JLong, String]()

  // A map stores the relationship between join operator id and its param.
  private val joinParamsMap = new JHashMap[JLong, JoinParams]()

  // A map stores the relationship between aggregation operator id and its param.
  private val aggregationParamsMap = new JHashMap[JLong, AggregationParams]()

  private var iteratorIndex: JLong = 0L
  private var operatorId: JLong = 0L
  private var relId: JLong = 0L

  def registerFunction(funcName: String): JLong = {
    if (!functionMap.containsKey(funcName)) {
      val newFunctionId: JLong = functionMap.size.toLong
      functionMap.put(funcName, newFunctionId)
      newFunctionId
    } else {
      functionMap.get(funcName)
    }
  }

  def registeredFunction: JHashMap[String, JLong] = functionMap

  def nextIteratorIndex: JLong = {
    val id = this.iteratorIndex
    this.iteratorIndex += 1
    id
  }

  def currentIteratorIndex: JLong = {
    assert(iteratorIndex > 0)
    this.iteratorIndex - 1
  }

  /**
   * Register a rel to certain operator id.
   * @param operatorId
   *   operator id
   */
  def registerRelToOperator(operatorId: JLong): Unit = {
    if (operatorToRelsMap.containsKey(operatorId)) {
      val rels = operatorToRelsMap.get(operatorId)
      rels.add(relId)
    } else {
      val rels = new JArrayList[JLong]()
      rels.add(relId)
      operatorToRelsMap.put(operatorId, rels)
    }
    relId += 1
  }

  /** Add the relId and register to operator later */
  def nextRelId(): JLong = {
    val id = this.relId
    this.relId += 1
    id
  }

  /**
   * Return the registered map.
   * @return
   */
  def registeredRelMap: JMap[JLong, JList[JLong]] = operatorToRelsMap

  /**
   * Register the join params to certain operator id.
   * @param operatorId
   *   operator id
   * @param param
   *   join params
   */
  def registerJoinParam(operatorId: JLong, param: JoinParams): Unit = {
    if (joinParamsMap.containsKey(operatorId)) {
      throw new InvalidParameterException("Join param has already been registered.")
    } else {
      joinParamsMap.put(operatorId, param)
    }
  }

  /**
   * return the registered map
   * @return
   */
  def registeredJoinParams: JHashMap[JLong, JoinParams] = joinParamsMap

  /**
   * Register the aggregation params to certain operator id.
   * @param operatorId
   *   operator id
   * @param param
   *   aggregation params
   */
  def registerAggregationParam(operatorId: JLong, param: AggregationParams): Unit = {
    if (aggregationParamsMap.containsKey(operatorId)) {
      throw new InvalidParameterException("Aggregation param has already been registered.")
    } else {
      aggregationParamsMap.put(operatorId, param)
    }
  }

  /**
   * return the registered map
   * @return
   */
  def registeredAggregationParams: JHashMap[JLong, AggregationParams] = aggregationParamsMap

  def nextOperatorId(planName: String): JLong = {
    val id = this.operatorId
    operatorToPlanNameMap.put(id, planName)
    this.operatorId += 1
    id
  }

  /** Only for debug the plan id and plan name in `operatorToRelsMap` */
  def getOperatorToPlanNameMap: JHashMap[JLong, String] = operatorToPlanNameMap
}
