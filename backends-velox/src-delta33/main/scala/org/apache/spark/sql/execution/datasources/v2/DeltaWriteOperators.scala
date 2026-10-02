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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{GlutenOptimisticTransaction, OptimisticTransaction, TransactionExecutionObserver}
import org.apache.spark.sql.execution.command.{LeafRunnableCommand, RunnableCommand}
import org.apache.spark.sql.execution.metric.SQLMetric

case class GlutenDeltaLeafV2CommandExec(delegate: LeafV2CommandExec) extends LeafV2CommandExec {

  override def metrics: Map[String, SQLMetric] = delegate.metrics

  override protected def run(): Seq[InternalRow] = {
    DeltaV2WriteOperators.withColumnarTransaction {
      delegate.executeCollect()
    }
  }

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

case class GlutenDeltaLeafRunnableCommand(delegate: LeafRunnableCommand)
  extends LeafRunnableCommand {
  override lazy val metrics: Map[String, SQLMetric] = delegate.metrics

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    DeltaV2WriteOperators.withColumnarTransaction {
      delegate.run(sparkSession)
    }
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

case class GlutenDeltaRunnableCommand(delegate: RunnableCommand) extends LeafRunnableCommand {
  override lazy val metrics: Map[String, SQLMetric] = delegate.metrics

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    DeltaV2WriteOperators.withColumnarTransaction {
      delegate.run(sparkSession)
    }
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

object DeltaV2WriteOperators {
  private[sql] def withColumnarTransaction[T](f: => T): T = {
    TransactionExecutionObserver.getObserver match {
      case _: UseColumnarDeltaTransactionLog => f
      case observer =>
        TransactionExecutionObserver.setObserver(wrap(observer))
        try {
          f
        } finally {
          // A completed transaction may have advanced to the next observer.
          TransactionExecutionObserver.setObserver(unwrap(TransactionExecutionObserver.getObserver))
        }
    }
  }

  private def wrap(observer: TransactionExecutionObserver): TransactionExecutionObserver =
    observer match {
      case _: UseColumnarDeltaTransactionLog => observer
      case _ => new UseColumnarDeltaTransactionLog(observer)
    }

  private def unwrap(observer: TransactionExecutionObserver): TransactionExecutionObserver =
    observer match {
      case columnar: UseColumnarDeltaTransactionLog => columnar.underlying
      case _ => observer
    }

  private class UseColumnarDeltaTransactionLog(val underlying: TransactionExecutionObserver)
    extends TransactionExecutionObserver {
    override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
      underlying.startingTransaction {
        new GlutenOptimisticTransaction(f)
      }
    }

    override def preparingCommit[T](f: => T): T = underlying.preparingCommit(f)

    override def beginDoCommit(): Unit = underlying.beginDoCommit()

    override def beginBackfill(): Unit = underlying.beginBackfill()

    override def beginPostCommit(): Unit = underlying.beginPostCommit()

    override def transactionCommitted(): Unit = withObserverAdvance {
      underlying.transactionCommitted()
    }

    override def transactionAborted(): Unit = withObserverAdvance {
      underlying.transactionAborted()
    }

    override def createChild(): TransactionExecutionObserver = {
      wrap(underlying.createChild())
    }

    override def setNextObserver(nextTxnObserver: TransactionExecutionObserver): Unit = {
      underlying.setNextObserver(unwrap(nextTxnObserver))
    }

    override def advanceToNextThreadObserver(): Unit = withObserverAdvance {
      underlying.advanceToNextThreadObserver()
    }

    private def withObserverAdvance(f: => Unit): Unit = {
      val inColumnarScope =
        TransactionExecutionObserver.getObserver.isInstanceOf[UseColumnarDeltaTransactionLog]
      try {
        f
      } finally {
        if (inColumnarScope) {
          // Delta's observer can replace itself from inside its commit/abort callbacks.
          TransactionExecutionObserver.setObserver(wrap(TransactionExecutionObserver.getObserver))
        }
      }
    }
  }
}
