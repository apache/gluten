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
package org.apache.gluten.utils

import org.apache.gluten.execution.{BroadcastHashJoinExecTransformerBase, ShuffledHashJoinExecTransformerBase}

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec

import scala.collection.mutable

/**
 * Computes the order in which Velox will first pull data from each shuffle reader of a whole stage.
 */
object ShuffleReaderOrderUtil {

  /**
   * Assigns each [[ColumnarAQEShuffleReadExec]] in the stage rooted at `stageRoot` the order in
   * which Velox will first pull data from it at runtime (0-based, via `setReaderOrder`).
   *
   * Why the order is not simply the plan-tree order:
   *
   * When Gluten hands a whole stage to Velox, Velox's LocalPlanner chops the operator tree into
   * "pipelines" (linear chains of operators driven by one source). The cut points are hash-join
   * build sides: the probe (streamed) side of a join stays in the current pipeline, while the build
   * side becomes a NEW pipeline, appended to the pipeline list at the moment the planner's
   * depth-first, probe-side-first walk reaches it. Every pipeline therefore has one source (a
   * shuffle read, a scan, or a broadcast input) and may depend on the build pipelines feeding the
   * hash joins it contains.
   *
   * Gluten then executes the Velox task single-threaded via Task::next(). The single thread
   * repeatedly walks the pipeline list from index 0 upward — each full top-to-bottom walk is called
   * a "sweep" below. On each sweep, a pipeline whose hash-join build inputs are not all finished is
   * blocked (HashProbe reports kWaitForJoinBuild before consuming any input, so even the pipeline's
   * source is not touched) and gets skipped; an unblocked pipeline runs, pulling its source — that
   * is the moment its shuffle reader is first invoked. A pipeline finishing mid-sweep is visible to
   * later-indexed pipelines within the same sweep, but earlier-indexed pipelines only notice on the
   * next sweep.
   *
   * Net effect: all independent build-side shuffle readers fire first (in pipeline-list order),
   * then intermediate probe readers as their builds complete, and the top-level probe reader fires
   * last. This method reproduces the pipeline list and replays the sweeps to compute each reader's
   * first-read order without running anything.
   */
  def assign(stageRoot: SparkPlan): Unit = {
    // One entry per Velox pipeline:
    // - source: the shuffle reader at the bottom of the pipeline's operator chain, if any
    //   (pipelines fed by scans or broadcast inputs have none and get no order number);
    // - builds: indices of the build pipelines that must finish before this pipeline may run
    //   (one per hash join contained in this pipeline);
    // - done: completion flag used while replaying the sweeps.
    class PipelineSim {
      var source: Option[ColumnarAQEShuffleReadExec] = None
      val builds = mutable.ArrayBuffer.empty[Int]
      var done = false
    }

    // Ordered as Velox's LocalPlanner orders its driver factories; the index in this buffer is
    // the pipeline index the executor sweeps over.
    val pipelines = mutable.ArrayBuffer.empty[PipelineSim]

    // Walk the plan tree and rebuild the pipeline list, mirroring LocalPlanner: `pipelineIdx`
    // is the pipeline the current node belongs to; non-join nodes just stay in it, joins split.
    def planPipelines(plan: SparkPlan, pipelineIdx: Int): Unit = {
      // The probe side is walked first and stays in the current pipeline (so any joins nested
      // inside it append THEIR build pipelines before this one); then the build side is placed
      // in a fresh pipeline and recorded as a prerequisite of the current pipeline.
      def splitBuildPipeline(probe: SparkPlan, build: SparkPlan): Unit = {
        planPipelines(probe, pipelineIdx)
        val buildIdx = pipelines.size
        pipelines += new PipelineSim
        planPipelines(build, buildIdx)
        pipelines(pipelineIdx).builds += buildIdx
      }
      plan match {
        // Broadcast builds contain no shuffle reader, but still get a pipeline: a probe blocked
        // only on a broadcast build must still wait one sweep, which can delay its shuffle read
        // past readers that fire on the first sweep.
        case bhj: BroadcastHashJoinExecTransformerBase =>
          bhj.joinBuildSide match {
            case BuildLeft => splitBuildPipeline(bhj.right, bhj.left)
            case BuildRight => splitBuildPipeline(bhj.left, bhj.right)
          }

        // For BuildLeft, Gluten swaps the children when lowering to Velox's HashJoinNode (whose
        // build side is always the right source), so the left child is the build pipeline here.
        case shj: ShuffledHashJoinExecTransformerBase =>
          shj.joinBuildSide match {
            case BuildLeft => splitBuildPipeline(shj.right, shj.left)
            case BuildRight => splitBuildPipeline(shj.left, shj.right)
          }

        case c: ColumnarAQEShuffleReadExec =>
          pipelines(pipelineIdx).source = Some(c)

        // Any other operator (project, filter, aggregate, input iterator, ...) is a single-input
        // link in the current pipeline's chain. NOTE: operators that Velox would also split into
        // extra pipelines (union/local exchange, nested-loop join, ...) are not modeled; if one
        // appears in a stage, the computed order may not match the runtime order.
        case other =>
          other.children.foreach(planPipelines(_, pipelineIdx))
      }
    }

    // Pipeline 0 is the output pipeline: the chain from the stage root down its probe sides.
    pipelines += new PipelineSim
    planPipelines(stageRoot, 0)

    // Replay the serial executor: each `while` iteration is one sweep over the pipeline list.
    // A pipeline runs once all its build prerequisites are done; running it assigns the next
    // order number to its shuffle reader (its source is pulled to exhaustion at that point).
    // Marking `done` mid-sweep lets later-indexed pipelines run in the same sweep, matching the
    // executor's forward-only scan. `progressed` guards against a malformed dependency graph.
    var readerOrder = 0
    var progressed = true
    while (progressed && pipelines.exists(!_.done)) {
      progressed = false
      pipelines.foreach {
        p =>
          if (!p.done && p.builds.forall(pipelines(_).done)) {
            p.source.foreach {
              reader =>
                reader.setReaderOrder(readerOrder)
                readerOrder += 1
            }
            p.done = true
            progressed = true
          }
      }
    }
  }
}
