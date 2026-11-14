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
// the fork's push run of each workflow below as the check named next to it.

const WORKFLOWS = [
  { file: 'velox_backend_x86.yml', check: 'Velox Backend (x86) in fork' },
  { file: 'velox_backend_arm.yml', check: 'Velox Backend (ARM) in fork' },
  { file: 'velox_kafka.yml', check: 'Velox Kafka in fork' },
];

// Check runs accept fewer conclusions than a workflow run reports; anything else
// means the run produced no result.
const CONCLUSIONS = ['success', 'failure', 'neutral', 'cancelled', 'timed_out', 'action_required', 'skipped'];

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

// Job conclusions that make the run fail. No job in these workflows sets
// continue-on-error, so a check can turn red before the run ends.
const FAILED = ['failure', 'timed_out'];

// What a check records in its output text about its fork run, or null.
function recordOf(check) {
  if (check.conclusion == 'action_required') return null;
  try {
    const p = JSON.parse(check.output.text);
    return p && p.run_id ? p : null;
  } catch (e) {
    return null;
  }
}

// The fork run attempt a check points at, as "run_id/attempt", or null. Checks
// written before attempts were recorded count as attempt 1.
function attemptOf(check) {
  const p = recordOf(check);
  return p ? `${p.run_id}/${p.run_attempt || 1}` : null;
}

// The check status (and conclusion) for a run's current state and its jobs. GitHub
// reports a run as queued while any job waits for a runner, so a run with a job that
// has started counts as in progress.
function stateOf(run, jobs) {
  if (jobs.some((j) => FAILED.includes(j.conclusion))) return { status: 'completed', conclusion: 'failure' };
  if (run.status == 'completed') {
    return { status: 'completed', conclusion: CONCLUSIONS.includes(run.conclusion) ? run.conclusion : 'failure' };
  }
  const started = run.status == 'in_progress' || jobs.some((j) => j.status == 'in_progress' || j.status == 'completed');
  return { status: started ? 'in_progress' : 'queued' };
}

function noRunSummary(w, sha) {
  return [
    `${w.file} runs in your fork on each push to a branch other than main. No run was found for ${sha}.`,
    '',
    '1. Enable GitHub Actions in the Actions tab of your fork and allow all actions in Settings > Actions > General, then push again. See [Managing GitHub Actions settings for a repository](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository).',
    '2. If your branch predates fork runs, rebase it on the latest apache/gluten main and push again.',
    "3. If this pull request comes from your fork's main branch, push it to another branch and open the pull request from there.",
  ].join('\n');
}

module.exports = async ({ github, context, core }) => {
  const repo = { owner: context.repo.owner, repo: context.repo.repo };

  const listChecks = (sha) =>
    github.paginate('GET /repos/{owner}/{repo}/commits/{ref}/check-runs', { ...repo, ref: sha, filter: 'all', per_page: 100 });

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

  // Jobs of the run's latest attempt. Read only while the run is going or when it
  // didn't pass.
  const jobsOf = async (pr, run) => {
    if (run.status == 'completed' && ['success', 'skipped', 'neutral'].includes(run.conclusion)) return [];
    return github.paginate('GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs', {
      owner: pr.head.repo.owner.login,
      repo: pr.head.repo.name,
      run_id: run.id,
      filter: 'latest',
      per_page: 100,
    });
  };

  const checkFor = async (pr, run) => {
    const jobs = await jobsOf(pr, run);
    const failedJobs = jobs.filter((j) => FAILED.includes(j.conclusion));
    const done = run.status == 'completed';
    const count = failedJobs.length == 1 ? '1 job failed' : `${failedJobs.length} jobs failed`;
    const lines = [
      `[See the run in ${pr.head.repo.full_name}](${run.html_url}). ` +
        'Rerun failed jobs there if the failure is unrelated to this pull request.',
    ];
    if (failedJobs.length) {
      lines.push('', done ? 'Failed jobs:' : 'Failed jobs so far (the run is still going):');
      for (const j of failedJobs) lines.push(`- [${j.name}](${j.html_url})`);
    }
    return {
      ...stateOf(run, jobs),
      output: {
        title: failedJobs.length ? count + (done ? '' : ', run still going') : 'Fork run',
        summary: lines.join('\n'),
        // `done` stops further reads of a check whose run has ended.
        text: JSON.stringify({
          owner: pr.head.repo.owner.login,
          repo: pr.head.repo.name,
          run_id: run.id,
          run_attempt: run.run_attempt,
          done,
        }),
      },
    };
  };

  const syncCheck = async (pr, w, check, run) => {
    if (recordOf(check).done) return;
    const next = await checkFor(pr, run);
    const same = (k) => (check.output[k] || '') == next.output[k];
    if (
      check.status == next.status &&
      check.conclusion == (next.conclusion || null) &&
      same('title') &&
      same('summary') &&
      same('text')
    ) {
      return;
    }
    console.log(`#${pr.number} ${w.file}: ${next.status} ${next.conclusion || ''} ${next.output.title}`);
    // An update without `output` clears its text, which is how the next pass finds this check.
    return github.request('PATCH /repos/{owner}/{repo}/check-runs/{check_run_id}', {
      ...repo,
      check_run_id: check.id,
      details_url: run.html_url,
      ...next,
    });
  };

  const createRunCheck = async (pr, w, run) => {
    console.log(`#${pr.number} ${w.file}: tracking ${run.html_url}`);
    return github.rest.checks.create({
      ...repo,
      name: w.check,
      head_sha: pr.head.sha,
      details_url: run.html_url,
      ...(await checkFor(pr, run)),
    });
  };

  const createNoRunCheck = (pr, w) => {
    console.log(`#${pr.number} ${w.file}: no fork run`);
    return github.rest.checks.create({
      ...repo,
      name: w.check,
      head_sha: pr.head.sha,
      status: 'completed',
      conclusion: 'action_required',
      output: { title: 'No run found in your fork', summary: noRunSummary(w, pr.head.sha) },
    });
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
        const checks = await listChecks(pr.head.sha);
        const checksOf = (w) => checks.filter((c) => c.name == w.check);
        // Only a workflow without any check yet can have a run that hasn't registered.
        const fresh = WORKFLOWS.filter((w) => checksOf(w).length == 0);
        const runs = await listForkRuns(pr, fresh, fresh.length ? 3 : 1);
        for (const w of WORKFLOWS) {
          const run = runs.filter((r) => r.path == runPath(w)).sort((a, b) => b.id - a.id)[0];
          if (run) {
            // A newer run or attempt than any check points at (a rerun, or a push back to
            // an earlier commit) gets a check of its own, so a rerun shows as in progress
            // instead of keeping the previous attempt's result.
            const check = checksOf(w).find((c) => attemptOf(c) == `${run.id}/${run.run_attempt}`);
            await (check ? syncCheck(pr, w, check, run) : createRunCheck(pr, w, run));
          } else if (!checksOf(w).some((c) => c.conclusion == 'action_required')) {
            await createNoRunCheck(pr, w);
          }
        }
      } catch (e) {
        // Stop the pass instead of sending more requests; the next pass picks up here.
        if (isRateLimited(e)) {
          console.log(`#${pr.number}: rate limited, stopping this pass`);
          rateLimited = true;
          break;
        }
        if (isTransient(e)) continue;
        console.error(`#${pr.number}: ${e.message}`);
        failed.push(pr.number);
      }
    }
    if (rateLimited) break;
  }
  // Keep a recurring error visible instead of hiding it in a green run.
  if (failed.length) core.setFailed('Failed to sync #' + failed.join(', #'));
};
