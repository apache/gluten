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
package org.apache.gluten.execution

trait StageExecutionMode {
  def name: String = this.getClass.getSimpleName.replaceAll("\\$", "")
  def id: Int
}

case object CPUStageMode extends StageExecutionMode {
  override def id: Int = 0
}
case object GPUStageMode extends StageExecutionMode {
  override def id: Int = 1
}
case object MockGPUStageMode extends StageExecutionMode {
  override def id: Int = 0
}

object StageExecutionMode {
  def fromId(id: Int): StageExecutionMode = id match {
    case 0 => CPUStageMode
    case 1 => GPUStageMode
    case _ => throw new IllegalArgumentException(s"Unknown StageExecutionMode id: $id")
  }
}
