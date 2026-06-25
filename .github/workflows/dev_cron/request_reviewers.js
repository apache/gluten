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

const fs = require('fs');
const path = require('path');

function normalizeLogin(login) {
  if (!login) {
    return '';
  }
  let normalized = String(login).trim();
  if (normalized.startsWith('@')) {
    normalized = normalized.substring(1);
  }
  return normalized;
}

function uniqueLogins(logins) {
  const seen = new Set();
  const result = [];
  for (const login of logins || []) {
    const normalized = normalizeLogin(login);
    if (!normalized) {
      continue;
    }
    const key = normalized.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      result.push(normalized);
    }
  }
  return result;
}

function lowerSet(logins) {
  return new Set(uniqueLogins(logins).map((login) => login.toLowerCase()));
}

function isBotLogin(login) {
  return normalizeLogin(login).toLowerCase().endsWith('[bot]');
}

/**
 * Minimal glob matcher — supports * (within a segment) and ** (any path depth).
 * No external dependencies required.
 *
 * @param {string} pattern  e.g. "backends-clickhouse/**" or ".github/**"
 * @param {string} filePath e.g. "backends-clickhouse/src/Foo.java"
 * @returns {boolean}
 */
function matchGlob(pattern, filePath) {
  // Escape regex special chars except * which we handle specially
  const regexStr = pattern
    .split('**')
    .map((part) =>
      part
        .split('*')
        .map((seg) => seg.replace(/[.+^${}()|[\]\\]/g, '\\$&'))
        .join('[^/]*')
    )
    .join('.*');
  return new RegExp(`^${regexStr}$`).test(filePath);
}

/**
 * Returns true if any of the given glob patterns matches the file path.
 */
function matchesAnyPattern(patterns, filePath) {
  return (patterns || []).some((pattern) => matchGlob(pattern, filePath));
}

function loadConfig(core) {
  const workspace = process.env.GITHUB_WORKSPACE || process.cwd();
  const configPath = path.resolve(workspace, process.env.CONFIG_PATH || '.github/reviewer_rotation.json');

  if (!fs.existsSync(configPath)) {
    throw new Error(`Reviewer rotation config not found: ${configPath}`);
  }

  const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  config.global_reviewers = uniqueLogins(config.global_reviewers || []);
  config.excluded_reviewers = uniqueLogins(config.excluded_reviewers || []);
  // Normalise each area's reviewer list
  for (const area of config.areas || []) {
    area.reviewers = uniqueLogins(area.reviewers || []);
    area.paths = area.paths || [];
  }

  if (config.global_reviewers.length === 0 && (config.areas || []).length === 0) {
    core.notice('No reviewers are configured in .github/reviewer_rotation.json. Nothing to request.');
  }

  return config;
}

function getPullRequestNumber(context) {
  if (context.payload.pull_request && context.payload.pull_request.number) {
    return context.payload.pull_request.number;
  }

  const rawPrNumber = String(process.env.PR_NUMBER || '').trim();
  if (rawPrNumber) {
    const parsed = Number.parseInt(rawPrNumber, 10);
    if (Number.isInteger(parsed) && parsed > 0 && String(parsed) === rawPrNumber) {
      return parsed;
    }
    throw new Error(`Invalid PR_NUMBER value: ${rawPrNumber}`);
  }

  return null;
}

async function getPullRequest(github, context, pullRequestNumber) {
  const { data: pullRequest } = await github.rest.pulls.get({
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: pullRequestNumber
  });
  return pullRequest;
}

async function getChangedFiles(github, context, pullRequestNumber) {
  const files = await github.paginate(github.rest.pulls.listFiles, {
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: pullRequestNumber,
    per_page: 100
  });
  return files.map((f) => f.filename);
}

async function getReviewAuthors(github, context, pullRequestNumber) {
  const reviews = await github.paginate(github.rest.pulls.listReviews, {
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: pullRequestNumber,
    per_page: 100
  });

  return lowerSet(reviews
    .filter((review) => review.user && review.user.login)
    .map((review) => review.user.login));
}

function buildExclusionSet(config, pullRequest) {
  const excluded = lowerSet(config.excluded_reviewers);

  if (config.skip_author !== false && pullRequest.user && pullRequest.user.login) {
    excluded.add(pullRequest.user.login.toLowerCase());
  }

  return excluded;
}

async function requestReviewers(github, context, core, pullRequestNumber, candidates, requestedReviewerCount) {
  const requested = [];
  const rejected = [];

  for (const reviewer of candidates) {
    if (requestedReviewerCount !== null && requested.length >= requestedReviewerCount) {
      break;
    }

    try {
      await github.rest.pulls.requestReviewers({
        owner: context.repo.owner,
        repo: context.repo.repo,
        pull_number: pullRequestNumber,
        reviewers: [reviewer]
      });
      requested.push(reviewer);
    }
    catch (error) {
      if (error.status === 422) {
        rejected.push(`${reviewer}: ${error.message}`);
        core.warning(
          `GitHub rejected reviewer request for ${reviewer} on PR #${pullRequestNumber}: ${error.message}`
        );
        continue;
      }
      throw error;
    }
  }

  return { requested, rejected };
}

module.exports = async ({ github, context, core }) => {
  const config = loadConfig(core);
  const pullRequestNumber = getPullRequestNumber(context);

  if (!pullRequestNumber) {
    if (context.eventName === 'workflow_dispatch') {
      core.setFailed('pr_number is required for manual reviewer rotation runs.');
      return;
    }
    core.notice('No pull request number found. For manual runs, pass the pr_number input.');
    return;
  }

  const pullRequest = await getPullRequest(github, context, pullRequestNumber);

  if (config.skip_draft !== false && pullRequest.draft) {
    core.notice(`PR #${pullRequestNumber} is a draft. No reviewers requested.`);
    return;
  }

  const reviewAuthors = config.request_even_if_already_reviewed === true
    ? new Set()
    : await getReviewAuthors(github, context, pullRequestNumber);

  // Build the merged pool: global_reviewers + reviewers from every area that
  // touches this PR's changed files. Deduplicate while preserving order.
  const changedFiles = await getChangedFiles(github, context, pullRequestNumber);

  const poolLogins = uniqueLogins([
    ...config.global_reviewers,
    ...(config.areas || [])
      .filter((area) => changedFiles.some((file) => matchesAnyPattern(area.paths, file)))
      .flatMap((area) => area.reviewers)
  ]);

  // Apply exclusions (author, already-reviewed, bots, already-requested).
  const excluded = buildExclusionSet(config, pullRequest);
  if (config.request_even_if_already_reviewed !== true) {
    for (const a of reviewAuthors) {
      excluded.add(a);
    }
  }
  const alreadyRequestedKeys = lowerSet(
    (pullRequest.requested_reviewers || []).map((r) => r.login)
  );

  const pool = poolLogins.filter((r) => {
    const key = r.toLowerCase();
    return !excluded.has(key) && !alreadyRequestedKeys.has(key) && !(config.skip_bots !== false && isBotLogin(r));
  });

  if (pool.length === 0) {
    core.notice(`No eligible reviewers found for PR #${pullRequestNumber}.`);
    return;
  }

  // Pick 1 via round-robin across the merged pool.
  const pick = pool[(pullRequest.number - 1) % pool.length];

  const { requested, rejected } = await requestReviewers(
    github,
    context,
    core,
    pullRequestNumber,
    [pick],
    1
  );

  if (requested.length === 0) {
    core.setFailed(
      `No reviewers could be requested for PR #${pullRequestNumber}. ` +
      'Check that configured reviewers are valid GitHub usernames with access to review this repository.' +
      (rejected.length ? ` Rejected candidates: ${rejected.join('; ')}` : '')
    );
    return;
  }

  core.notice(`Requested reviewer for PR #${pullRequestNumber}: ${requested[0]}`);
};

module.exports._private = {
  normalizeLogin,
  uniqueLogins,
  lowerSet,
  isBotLogin,
  matchGlob,
  matchesAnyPattern,
  requestReviewers
};
