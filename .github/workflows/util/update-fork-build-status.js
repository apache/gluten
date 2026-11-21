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

// Run by update_fork_build_status.yml. For each open pull request from a fork, reports
// the fork's push run of each workflow below as the commit status named next to it.
// A check run would join the check suite of some other workflow run on the commit, and
// the pull request page stops listing it once that workflow runs again.

const WORKFLOWS = [
  { file: 'velox_backend_x86.yml', context: 'Velox Backend (x86) in fork' },
  { file: 'velox_backend_arm.yml', context: 'Velox Backend (ARM) in fork' },
  { file: 'velox_kafka.yml', context: 'Velox Kafka in fork' },
];

// Job conclusions that make the run fail. No job in these workflows sets
// continue-on-error, so a status can turn red before the run ends.
const FAILED = ['failure', 'timed_out'];
const PASSED = ['success', 'skipped', 'neutral'];

const MAX_DESCRIPTION = 140;
const SETUP_URL = 'https://github.com/apache/gluten/blob/main/CONTRIBUTING.md#velox-backend-ci';
const NO_RUN = 'No run in your fork for this commit. Enable Actions, allow all actions, rebase on main and push.';
const UNLISTED = 'The run failed. Its failed jobs could not be read yet.';
// Marks a failure list that can still grow.
const SO_FAR = ' so far';

// Rate limiting, which GitHub reports as 429 or as 403 with rate-limit headers.
function isRateLimited(e) {
  if (e.status == 429) return true;
  if (e.status != 403) return false;
  const h = (e.response && e.response.headers) || {};
  return !!h['retry-after'] || h['x-ratelimit-remaining'] === '0' || /rate limit/i.test(e.message || '');
}

// 5xx and network errors are retried on a later pass.
function isTransient(e) {
  return !e.status || e.status >= 500;
}

const runPath = (w) => '.github/workflows/' + w.file;

// The fork run attempt a status links to, or null.
function attemptOf(s) {
  const m = /\/actions\/runs\/(\d+)\/attempts\/(\d+)$/.exec((s && s.target_url) || '');
  return m ? { id: Number(m[1]), attempt: Number(m[2]) } : null;
}

// A status that later passes leave alone: its run has ended and all its failed jobs are listed.
const isFinal = (s) => s.state != 'pending' && !s.description.includes(SO_FAR) && s.description != UNLISTED;

const truncate = (s) => (s.length <= MAX_DESCRIPTION ? s : s.slice(0, MAX_DESCRIPTION - 1) + '…');

// The status for a run and the jobs of its latest attempt, or null jobs when they
// couldn't be read. GitHub reports a run as queued while any job waits for a runner,
// so a run with a job that has started counts as running.
function statusOf(run, jobs) {
  const target_url = `${run.html_url}/attempts/${run.run_attempt}`;
  const done = run.status == 'completed';
  const failed = (jobs || []).filter((j) => FAILED.includes(j.conclusion)).map((j) => j.name);
  if (failed.length) {
    const count = failed.length == 1 ? '1 job failed' : `${failed.length} jobs failed`;
    const description = truncate(`${count}${done ? '' : SO_FAR}: ${failed.join(', ')}`);
    return { state: 'failure', target_url, description };
  }
  if (!done) {
    const started =
      run.status == 'in_progress' || (jobs || []).some((j) => j.status == 'in_progress' || j.status == 'completed');
    return { state: 'pending', target_url, description: started ? 'Running in the fork' : 'Queued in the fork' };
  }
  if (PASSED.includes(run.conclusion)) return { state: 'success', target_url, description: 'Passed in the fork' };
  if (run.conclusion == 'cancelled') return { state: 'error', target_url, description: 'Cancelled in the fork' };
  const description = jobs ? `The run ended with conclusion ${run.conclusion}` : UNLISTED;
  return { state: 'failure', target_url, description };
}

module.exports = async ({ github, context, core }) => {
  const repo = { owner: context.repo.owner, repo: context.repo.repo };
  const contexts = WORKFLOWS.map((w) => w.context);

  // The latest status of each context on the commit.
  const listStatuses = async (sha) =>
    (await github.request('GET /repos/{owner}/{repo}/commits/{ref}/status', { ...repo, ref: sha, per_page: 100 })).data
      .statuses;

  // Check runs that earlier versions of this script wrote. Remove once no open pull
  // request has them.
  const listLegacyChecks = async (sha) =>
    (
      await github.paginate('GET /repos/{owner}/{repo}/commits/{ref}/check-runs', {
        ...repo,
        ref: sha,
        filter: 'all',
        per_page: 100,
      })
    ).filter((c) => contexts.includes(c.name));

  // The fork's push runs for the pull request's head commit. Re-polled until every
  // workflow in `wanted` has one, because a run can take a few seconds to register.
  const listForkRuns = async (pr, wanted, attempts) => {
    let runs = [];
    for (let i = 0; i < attempts; i++) {
      try {
        const { data } = await github.request('GET /repos/{owner}/{repo}/actions/runs', {
          owner: pr.head.repo.owner.login,
          repo: pr.head.repo.name,
          event: 'push',
          branch: pr.head.ref,
          head_sha: pr.head.sha,
          per_page: 100,
        });
        runs = data.workflow_runs.filter((r) => r.head_sha == pr.head.sha);
      } catch (e) {
        if (e.status == 404) return [];
        throw e;
      }
      if (wanted.every((w) => runs.some((r) => r.path == runPath(w)))) break;
      if (i < attempts - 1) await new Promise((r) => setTimeout(r, 3000));
    }
    return runs;
  };

  // Jobs of the run's latest attempt, or null if they can't be read. Not read for a run
  // that passed. Small pages: a page of 100 jobs can take longer than GitHub's 10-second
  // gateway timeout.
  const jobsOf = async (pr, run) => {
    if (run.status == 'completed' && PASSED.includes(run.conclusion)) return [];
    try {
      return await github.paginate('GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs', {
        owner: pr.head.repo.owner.login,
        repo: pr.head.repo.name,
        run_id: run.id,
        filter: 'latest',
        per_page: 30,
      });
    } catch (e) {
      if (isRateLimited(e)) throw e;
      console.log(`#${pr.number}: could not read the jobs of ${run.html_url}: ${e.status || e.message}`);
      return null;
    }
  };

  const setStatus = (pr, w, s) => {
    console.log(`#${pr.number} ${w.file}: ${s.state} ${s.description}`);
    return github.request('POST /repos/{owner}/{repo}/statuses/{sha}', {
      ...repo,
      sha: pr.head.sha,
      context: w.context,
      ...s,
    });
  };

  const syncPull = async (pr) => {
    const statuses = await listStatuses(pr.head.sha);
    const legacy = await listLegacyChecks(pr.head.sha);
    const statusOfContext = (w) => statuses.find((s) => s.context == w.context);
    const legacyOf = (w) => legacy.filter((c) => c.name == w.context);
    // Only a workflow with nothing reported yet can have a run that hasn't registered.
    const fresh = WORKFLOWS.filter((w) => !statusOfContext(w) && legacyOf(w).length == 0);
    const runs = await listForkRuns(pr, fresh, fresh.length ? 3 : 1);
    for (const w of WORKFLOWS) {
      const current = statusOfContext(w);
      const run = runs.filter((r) => r.path == runPath(w)).sort((a, b) => b.id - a.id)[0];
      if (run) {
        const at = attemptOf(current);
        // Two branches can share a head commit; the newer run keeps the status.
        if (at && at.id > run.id) continue;
        if (at && at.id == run.id && at.attempt == run.run_attempt && isFinal(current)) continue;
        const next = statusOf(run, await jobsOf(pr, run));
        const same = current && ['state', 'description', 'target_url'].every((k) => current[k] == next[k]);
        if (!same) await setStatus(pr, w, next);
      } else if (!current) {
        await setStatus(pr, w, { state: 'error', target_url: SETUP_URL, description: NO_RUN });
      }
    }
    // A legacy check would show next to the status with an older result. The status
    // above is written first, so a pass stopped here leaves the commit with a result.
    for (const w of WORKFLOWS) {
      for (const c of legacyOf(w)) {
        if (c.conclusion == 'neutral') continue;
        console.log(`#${pr.number}: retiring check ${c.id} (${c.name})`);
        await github.request('PATCH /repos/{owner}/{repo}/check-runs/{check_run_id}', {
          ...repo,
          check_run_id: c.id,
          status: 'completed',
          conclusion: 'neutral',
          output: { title: 'Replaced', summary: `The result is now the commit status "${c.name}".` },
        });
      }
    }
  };

  const failed = [];
  let rateLimited = false;
  for await (const { data: prs } of github.paginate.iterator(github.rest.pulls.list, {
    ...repo,
    state: 'open',
    base: 'main',
    per_page: 100,
  })) {
    for (const pr of prs) {
      // Branches of this repository (Dependabot) run upstream; skip deleted forks.
      if (!pr.head.repo || pr.head.repo.full_name == `${repo.owner}/${repo.repo}`) continue;
      try {
        await syncPull(pr);
      } catch (e) {
        // Stop the pass instead of sending more requests; the next pass picks up here.
        if (isRateLimited(e)) {
          console.log(`#${pr.number}: rate limited, stopping this pass`);
          rateLimited = true;
          break;
        }
        console.log(`#${pr.number}: ${e.status || ''} ${e.message}`);
        if (isTransient(e)) continue;
        failed.push(pr.number);
      }
    }
    if (rateLimited) break;
  }
  // Keep a recurring error visible instead of hiding it in a green run.
  if (failed.length) core.setFailed('Failed to sync #' + failed.join(', #'));
};
