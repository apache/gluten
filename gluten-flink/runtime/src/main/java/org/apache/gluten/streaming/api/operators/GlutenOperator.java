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

import org.apache.gluten.table.runtime.operators.GlutenMailboxHolder;

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Map;

/** Interface for all gluten operators. */
public interface GlutenOperator {
  public StatefulPlanNode getPlanNode();

  public RowType getInputType();

  public Map<String, RowType> getOutputTypes();

  public String getId();

  public default String getDescription() {
    return "";
  }

  /** Mailbox drain helper holder; must be a non-transient field on the concrete operator. */
  default GlutenMailboxHolder mailboxHolder() {
    return new GlutenMailboxHolder();
  }

  default void processElementInternal() {}

  default void bindMailboxExecutor(MailboxExecutor mailboxExecutor) {
    mailboxHolder().get().bindMailboxExecutor(mailboxExecutor);
  }

  default void ensureMailboxInitialized(StreamTask<?, ?> containingTask) {
    mailboxHolder().get().ensureMailboxInitialized(containingTask);
  }

  default void drainOutput(Runnable drainAction) {
    mailboxHolder().get().runDrain(drainAction);
  }

  default void scheduleDrainOnMailbox(Runnable drainAction) {
    mailboxHolder().get().scheduleDrain(drainAction);
  }

  /** Schedules native output drain on the mailbox thread. Implemented by concrete operators. */
  default void scheduleProcessElementOnMailbox() {}
}
