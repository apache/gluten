<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Bolt PR CI

The listener dispatches the reviewed PR commit to the existing Bolt build
workflow. GitHub-hosted jobs maintain `Bolt CI / gate` and a single PR comment.
The worker runs native tests, fourteen packaging combinations and twenty-seven
Spark test groups on `bolt-medium` runners.

## Request a build

A same-repository PR starts automatically when its changes affect Bolt.
A repository maintainer can request a fresh run by commenting `/bolt ci`.
An external fork requires a maintainer with write permission to review and
approve each new commit with `/bolt ci <full 40-character SHA>`.
The same authorized SHA can subsequently be rerun with `/bolt ci`.

The listener can also be dispatched manually with a PR number and an optional
reviewed SHA. A standalone manual dispatch of the build workflow builds its
selected branch. PR checks are associated with listener requests.

Use `/bolt ci` for PR retries. A retry registered through the listener resets
the check before dispatching a fresh run. An Actions UI rerun is blocked by the
worker's request validation; its lifecycle event resets the recorded gate and
reports that a fresh listener request is required.

## Gate policy

Success requires all forty-three expected jobs, including request validation,
to complete successfully. Each native, packaging and Spark job must also have
successful required execution steps. Failed tests, missing jobs or steps,
manual cancellation and execution timeouts prevent success. A verified set of
changes outside the Bolt paths receives a successful check without a build.

The GitHub-hosted watcher samples native job eligibility every minute. Approval
and workflow-concurrency waits do not count. An API error or sampling gap over
ninety seconds restarts the interval. Runner assignment permanently ends queue
waiver monitoring for the request.

After at least thirty continuous minutes and thirty-one samples, the monitor
rechecks the job under the same per-PR lock as the listener and commenter,
requests cancellation and verifies the terminal state. A neutral result requires
no runner assignment or started steps before or after cancellation. A terminal
state that cannot be verified keeps the check blocking.

Verified queue timeouts receive a neutral result. This also applies when a human
cancels at the same time as the monitor: eligibility, thirty-minute observation
and proof that execution never started determine the exemption. A human cancel
before the monitor establishes those conditions does not qualify.

The controller stores the dispatched run ID in the check's `external_id`.
GitHub rewrites `details_url` for checks created with `GITHUB_TOKEN`, so that URL
is not used to bind a request to its result.

## Deploy in a fork

Install the three control workflows, the build workflow and this directory on
the fork's default branch. These trusted workflows must be available there for
PR target, comment and workflow-run events. The controller loads code from the
workflow's own commit, then dispatches the default-branch build definition with
the reviewed PR source SHA.

Dispatch the listener for a test PR. After verifying the check and comment,
configure branch protection to require the `Bolt CI / gate` check from the
GitHub Actions app. Each repository supplies its own `GITHUB_TOKEN`; the job
permissions in the workflows declare the required API access.

## Verify locally

```bash
python3 -m unittest discover -s .github/workflows/util/bolt-ci -p 'test_*.py'
```

The tests cover current-SHA authorization, duplicate triggers, stale results,
missing execution, UI reruns and queue cancellation races. GitHub API integration
and the thirty-minute queue observation need an actual fork execution.
