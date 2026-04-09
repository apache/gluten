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

function detectIssueID(title) {
  if (!title) {
    return null;
  }
  const matched = /^\[GLUTEN-\d+\]/.exec(title);
  if (!matched) {
    return null;
  }
  // Currently only consider one GitHub issue is referenced.
  const issueID = matched[0].replace(/[^0-9]/ig, "");
  return issueID;
}

async function appendToPRDescription(github, context, pullRequestNumber, issuesID) {
  const issueURL = `https://github.com/apache/gluten/issues/${issuesID}`;
  const issueReference = `#${issuesID}`

  // Fetch the current PR description.
  const { data: pullRequest } = await github.rest.pulls.get({
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: pullRequestNumber
  });

  const currentBody = pullRequest.body || "";

  // Check if the issues URL or reference is already in the PR description.
  if (currentBody.includes(issueURL) || currentBody.includes(issueReference)) {
    return;
  }

  // Append the issues URL to the PR description.
  const updatedBody = `${currentBody}\n\nRelated issue: ${issueReference}`;

  // Update the PR description.
  await github.rest.pulls.update({
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: pullRequestNumber,
    body: updatedBody
  });
}

module.exports = async ({ github, context }) => {
  const pullRequestNumber = context.payload.number;
  const title = context.payload.pull_request.title;
  const issuesID = detectIssueID(title);
  if (issuesID) {
    await appendToPRDescription(github, context, pullRequestNumber, issuesID);
  }
};
