---
layout: page
title: Release Cadence Improvement Proposal
nav_order: 18
parent: Developer Overview
---

# Release Cadence Improvement Proposal

## Problem

Gluten ships formal releases quarterly (~3 months apart). For a fast-moving project this means users wait
a long time to try new features without building from source.

## What Already Exists

Gluten publishes pre-built nightly bundle JARs at **<https://nightlies.apache.org/gluten/>**:

| Directory | JDK | Spark versions | Architectures |
|---|---|---|---|
| `nightly-release-jdk8/` | 8 | 3.4, 3.5, 4.0, 4.1 | x86_64, aarch64 |
| `nightly-release-jdk17/` | 17 | 3.4, 3.5, 4.0, 4.1 | x86_64, aarch64 |
| `nightly-release-jdk21/` | 21 | 3.4, 3.5, 4.0, 4.1 | x86_64, aarch64 |
| `nightly-release-jdk17-enhanced/` | 17 | 3.4, 3.5, 4.0, 4.1 | x86_64, aarch64 |

> **These are not official ASF releases.** They carry Apache's standard nightly disclaimer and are intended
> only for development and testing by the developer community.

Per [ASF release policy](https://www.apache.org/legal/release-policy.html#publication), nightly builds must
not be promoted to the general public. Public users should be directed to official releases. The real fix for
"users wait too long" is therefore to ship official releases more often, not to advertise nightlies.

## Proposed Strategy for Gluten

### Tier 1 — Keep nightlies developer-facing

- Do **not** add nightly download links to the website or `README.md`.
- Mention the nightly server only in developer-community channels (`dev@` list, contributor docs, GitHub
  discussions) for contributors who want to test unreleased changes.
- Remove stale SNAPSHOT JARs from previous release cycles (1.5.0, 1.6.0, 1.7.0 currently accumulate).

### Tier 2 — Minor releases every 6–8 weeks with a release-branch model

Move from quarterly to a **six-to-eight-week target cadence**, run as an experiment. The cadence is a
*target*, not a commitment: a release slips when the readiness gates below are not met, rather than shipping
on a fixed date.

Release-branch workflow (indicative timeline):

1. **Week 0 — Branch cut.** Cut `branch-1.8` from `main`. Pin the exact Velox commit used for the release
   and record it in the branch. `main` moves on to 1.9.0-SNAPSHOT.
2. **Week 1 — Qualification.** Audit the branch for regressions (benchmarks, CI health, operator coverage).
3. **Weeks 2–3 — Stabilization.** Fix regressions on `main` and backport them to `branch-1.8`.
4. **Week 4 — First RC.** Remove `-SNAPSHOT`, generate changelog, tag `v1.8.0-rc0`, open a 72-hour vote.
5. **Weeks 5–7 — Additional RCs (contingency).** Past releases have often needed two or three RCs, each
   requiring a fresh round of verification and voting. The schedule reserves time for this.
6. **On vote pass** — Promote artifacts, publish the Maven release, announce.

#### Release-readiness gates

An RC is only cut, and a release only proceeds, when:

- The exact packaged RC artifacts have been qualified. The current `dev/release/build-release.sh` builds with
  `-DskipTests`, so a separate test/qualification run against the packaged bundles is required.
- Branch CI is green for all supported Spark / JDK / architecture combinations.
- Artifact provenance (Gluten commit, Velox commit, build environment) is recorded, and `LICENSE` / `NOTICE`
  files are validated against the bundled dependencies — ideally automated.
- No open blocker issues are tagged for the release.

#### Supported Spark versions

Only released Spark versions with a stable profile and passing CI are supported targets. Profiles that track
unreleased Spark (e.g. Spark `master`) are treated as **CI compatibility canaries** pinned to a specific Spark
commit — they are not public release targets. A Spark profile being buildable does not by itself establish
support.

### Tier 3 — Patch releases (as needed)

Backport critical fixes to a maintained release branch and release as `1.8.1` without waiting for the next
cycle. Same 72-hour RC vote; no new branch required.

### Branch maintenance policy

A faster cadence increases maintenance load, so the policy must be explicit:

- **Maintained branches** — the latest minor release branch is maintained. The previous one is maintained only
  for security and data-correctness fixes until the next minor release ships (i.e. at most two maintained
  branches at any time).
- **EOL** — a branch reaches end-of-life when it falls out of the maintained set. EOL branches receive no
  further patches; EOL dates are announced on `dev@` and recorded in the release docs.
- **Backport eligibility** — security fixes, data-correctness bugs, crashes, and regressions introduced on
  the branch. New features and refactors are not backported. Backports are tracked with a
  `backport-X.Y` label and must land on `main` first.
- **Security handling** — follows the [ASF security process](https://www.apache.org/security/committers.html):
  reported privately to `private@` / `security@`, fixed on `main` and all maintained branches, and released
  as coordinated patch releases.
- **Branch qualification** — every backport to a release branch must pass the full branch CI before merge.

## Experiment Review

After two or three releases on the new cadence, review the experiment on `dev@` using:

- Number of RCs per release and the time from branch cut to release.
- Verification effort and Release Manager / reviewer workload.
- Maintenance and user-support cost for maintained branches.
- CI stability on release branches.
- Community feedback.

Based on the results, the community may keep, lengthen, or shorten the cadence to a pace it can sustainably
support.

## Adoption Roadmap

| Phase | What | When |
|---|---|---|
| **1** | Keep nightlies developer-facing; clean up stale JARs | Before 1.8.0 |
| **2** | Rehearse the release-branch workflow and maintenance policy for 1.8.0; keep quarterly cadence | 1.8.0 cycle |
| **3** | Pilot the 6–8 week cadence; introduce Release Manager rotation | 1.9.0 or later |
| **4** | Review the experiment and adjust the cadence | After 2–3 pilot releases |

## Work Required

| Area | Change | Effort |
|---|---|---|
| Nightly server | Clean up old SNAPSHOT JARs | Low |
| Release scripts | Make `build-release.sh` runnable on a branch (not just a tag) | Medium |
| Release scripts | Record Gluten/Velox commits and build info as artifact provenance | Medium |
| Release scripts | Automate `LICENSE` / `NOTICE` validation for bundled dependencies | Medium |
| CI | Add a qualification job that tests the exact packaged RC bundles | Medium |
| CI | Pin unreleased-Spark profiles to a specific Spark commit as canaries | Low |
| `.asf.yaml` | Add `branch-X.Y` to `protected_branches` | Low |
| `HowToRelease.md` | Document release-branch model, readiness gates, maintenance/EOL policy and backport labels | Medium |
| Process | Introduce rotating Release Manager role | Process |
