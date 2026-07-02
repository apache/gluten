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
package org.apache.gluten.streaming.api.operators;

import org.apache.gluten.table.runtime.operators.GlutenMailboxOperatorHelper;

import io.github.zhztheplayer.velox4j.serde.Serde;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SetupableStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Two input operator factory for gluten. */
public class GlutenTwoInputOperatorFactory<IN1, IN2, OUT> extends AbstractStreamOperatorFactory<OUT>
    implements TwoInputStreamOperatorFactory<IN1, IN2, OUT> {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenTwoInputOperatorFactory.class);

  private final StreamOperator<OUT> operator;

  public GlutenTwoInputOperatorFactory(StreamOperator<OUT> operator) {
    this.operator = operator;
    if (!(operator instanceof GlutenOperator)) {
      throw new RuntimeException("Operator is not gluten operator");
    }
  }

  public GlutenOperator getOperator() {
    return (GlutenOperator) operator;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<OUT>> T createStreamOperator(
      StreamOperatorParameters<OUT> parameters) {
    LOG.debug("Build gluten operator {}", Serde.toJson(getOperator().getPlanNode()));
    if (operator instanceof GlutenAbstractStreamOperator) {
      ((GlutenAbstractStreamOperator<OUT>) operator)
          .setProcessingTimeService(parameters.getProcessingTimeService());
    }
    if (operator instanceof AbstractStreamOperator) {
      ((AbstractStreamOperator) operator).setProcessingTimeService(processingTimeService);
    }
    if (operator instanceof SetupableStreamOperator) {
      ((SetupableStreamOperator<OUT>) operator)
          .setup(
              parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }
    GlutenMailboxOperatorHelper.bindAtTaskStartup(getOperator().mailboxHolder(), parameters);
    return (T) operator;
  }

  @Override
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
    return operator.getClass();
  }
}
