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
//
// When the fork has no run of a workflow, the pull request gets an error only if it
// changes a file the workflow covers, by the `paths` and `paths-ignore` inputs its
// merge-upstream job passes to squash-merge-upstream. Other pull requests, such as a
// Flink-only change, get a passing status saying the workflow is not needed. A run in
// the fork is always reported by its result.

const fs = require('fs');
const path = require('path');

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
const NO_RUN =
  'No run in your fork for this commit, which changes files this workflow tests. Enable Actions, allow all actions, rebase on main and push.';
// What earlier versions wrote without checking the changed files. Rechecked once. Still
// written when a workflow's paths can't be read, so that a later pass that can read
// them rechecks it.
const LEGACY_NO_RUN = 'No run in your fork for this commit. Enable Actions, allow all actions, rebase on main and push.';
const NOT_NEEDED = 'Not needed: this pull request changes no file this workflow tests.';
// The pull request files API lists at most this many files.
const MAX_PR_FILES = 3000;
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

// The `paths` and `paths-ignore` regexes the workflow passes to squash-merge-upstream,
// read from its file so that the two cannot drift apart. Only the `with:` block of that
// step is searched, and a value may be single- or double-quoted and followed by a
// comment. Returns null, so that the workflow counts as relevant for every pull
// request, when the inputs can't be read; a warning is logged instead of failing the
// pass, which would stop the statuses of every pull request.
function pathFilterOf(w, core) {
  try {
    const text = fs.readFileSync(path.join(process.env.GITHUB_WORKSPACE || '.', runPath(w)), 'utf8');
    const lines = text.split('\n');
    const start = lines.findIndex((l) => /^\s*(-\s+)?uses:\s*\.\/\.github\/actions\/squash-merge-upstream\s*$/.test(l));
    if (start < 0) throw new Error('no squash-merge-upstream step');
    // The step ends at the next step or at a line indented less than its `uses:`.
    const indent = lines[start].search(/\S/);
    let end = start + 1;
    while (end < lines.length && !/^\s*-\s/.test(lines[end]) &&
           (lines[end].trim() == '' || lines[end].search(/\S/) >= indent)) end++;
    const step = lines.slice(start + 1, end);
    // The value of input `name`, or null when the step doesn't set it. Throws when the
    // step sets it in a form this function can't read, or to an expression, which is
    // only evaluated when the workflow runs.
    const input = (name) => {
      const key = new RegExp(`^\\s*${name}:`);
      const value = new RegExp(`^\\s*${name}:\\s*(?:'((?:[^']|'')*)'|"((?:[^"\\\\]|\\\\.)*)")\\s*(#.*)?$`);
      const l = step.find((l) => key.test(l));
      if (l === undefined) return null;
      const m = value.exec(l);
      if (!m) throw new Error(`\`${name}\` is not a quoted one-line value in the squash-merge-upstream step`);
      const v = m[1] !== undefined ? m[1].replace(/''/g, "'") : JSON.parse(`"${m[2]}"`);
      if (v.includes('${{')) throw new Error(`\`${name}\` is an expression`);
      return v;
    };
    const paths = input('paths');
    if (!paths) throw new Error('no `paths` input in the squash-merge-upstream step');
    // The action's own default when the step sets no `paths-ignore`.
    return { paths: new RegExp(paths), ignore: new RegExp(input('paths-ignore') ?? '^$') };
  } catch (e) {
    core.warning(`${runPath(w)}: ${e.message}. Treating every pull request as relevant to it.`);
    return null;
  }
}

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
  const filters = new Map(WORKFLOWS.map((w) => [w.file, pathFilterOf(w, core)]));

  // Files the pull request changes, including the old name of a renamed file, or null
  // when the list may be cut off, in which case every workflow counts as relevant.
  const listPullFiles = async (pr) => {
    const files = await github.paginate('GET /repos/{owner}/{repo}/pulls/{pull_number}/files', {
      ...repo,
      pull_number: pr.number,
      per_page: 100,
    });
    if (files.length >= MAX_PR_FILES) return null;
    return files.flatMap((f) => (f.previous_filename ? [f.filename, f.previous_filename] : [f.filename]));
  };

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
    // Read only when a status is about to be written, and at most once per pull request.
    let files;
    const isRelevant = async (w) => {
      if (files === undefined) files = await listPullFiles(pr);
      const filter = filters.get(w.file);
      if (files === null || filter === null) return true;
      const { paths, ignore } = filter;
      return files.some((f) => !ignore.test(f) && paths.test(f));
    };
    const update = (w, current, next) => {
      const same = current && ['state', 'description', 'target_url'].every((k) => current[k] == next[k]);
      return same ? null : setStatus(pr, w, next);
    };
    for (const w of WORKFLOWS) {
      const current = statusOfContext(w);
      const run = runs.filter((r) => r.path == runPath(w)).sort((a, b) => b.id - a.id)[0];
      if (run) {
        const at = attemptOf(current);
        // Two branches can share a head commit; the newer run keeps the status.
        if (at && at.id > run.id) continue;
        if (at && at.id == run.id && at.attempt == run.run_attempt && isFinal(current)) continue;
        // The run decides for itself, after merging onto main, whether to skip its jobs.
        await update(w, current, statusOf(run, await jobsOf(pr, run)));
      } else if (!current || current.description == LEGACY_NO_RUN) {
        // When the workflow's paths can't be read, nothing says whether the pull request
        // needs a run. Write the old error, or leave it, for a pass that can read them.
        if (filters.get(w.file) === null) {
          if (!current) await setStatus(pr, w, { state: 'error', target_url: SETUP_URL, description: LEGACY_NO_RUN });
          continue;
        }
        const next = (await isRelevant(w))
          ? { state: 'error', target_url: SETUP_URL, description: NO_RUN }
          : { state: 'success', target_url: SETUP_URL, description: NOT_NEEDED };
        await update(w, current, next);
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
