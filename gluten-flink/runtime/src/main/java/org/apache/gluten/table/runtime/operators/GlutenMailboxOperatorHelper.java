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
package org.apache.gluten.table.runtime.operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * Schedules Velox output drain onto the Flink task mailbox thread.
 *
 * <p>JNI callbacks from Velox must not call {@code SerialTask.advance()} or emit records directly.
 * When a callback arrives while a drain is already in progress on the mailbox thread, a follow-up
 * drain is requested via {@code pendingMailboxDrain} instead of nesting another drain.
 */
public final class GlutenMailboxOperatorHelper {

  private transient MailboxExecutor mailboxExecutor;
  private transient boolean draining;
  private transient boolean pendingMailboxDrain;
  private transient boolean mailboxDrainScheduled;

  public void bindMailboxExecutor(MailboxExecutor mailboxExecutor) {
    this.mailboxExecutor = mailboxExecutor;
  }

  public void ensureMailboxInitialized(StreamTask<?, ?> containingTask) {
    if (mailboxExecutor == null) {
      mailboxExecutor = containingTask.getMailboxExecutorFactory().createExecutor(0);
    }
  }

  public boolean isMailboxBound() {
    return mailboxExecutor != null;
  }

  public void runDrain(Runnable drainAction) {
    draining = true;
    try {
      boolean repeat;
      do {
        pendingMailboxDrain = false;
        drainAction.run();
        repeat = pendingMailboxDrain;
      } while (repeat);
    } finally {
      draining = false;
    }
  }

  public void scheduleDrain(Runnable drainAction) {
    if (mailboxExecutor == null) {
      runDrain(drainAction);
      return;
    }
    if (draining) {
      pendingMailboxDrain = true;
      return;
    }
    synchronized (this) {
      if (mailboxDrainScheduled) {
        return;
      }
      mailboxDrainScheduled = true;
    }
    try {
      mailboxExecutor.submit(
          () -> {
            try {
              runDrain(drainAction);
            } finally {
              synchronized (GlutenMailboxOperatorHelper.this) {
                mailboxDrainScheduled = false;
              }
            }
          },
          "gluten-drain-output");
    } catch (RuntimeException e) {
      synchronized (this) {
        mailboxDrainScheduled = false;
      }
      throw e;
    }
  }

  /** Binds mailbox from {@link StreamOperatorParameters} during operator factory startup on TM. */
  public static void bindAtTaskStartup(
      GlutenMailboxHolder mailboxHolder, StreamOperatorParameters<?> parameters) {
    mailboxHolder
        .get()
        .bindMailboxExecutor(
            parameters.getContainingTask().getMailboxExecutorFactory().createExecutor(0));
  }
}
