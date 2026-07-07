#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Gate / seed / aggregate the Delta-on-Gluten unit test results.

Running delta-io/delta's ScalaTest suite against the Gluten Velox bundle
produces many *expected* failures (Gluten does not yet support every Delta
code path). To keep the red/green signal meaningful while we fix those
failures incrementally, we maintain a committed baseline of known failing
tests (``known-failures.txt``) and compare each CI run against it.

This script has three modes:

``enforce`` (default, per shard)
    Parse the JUnit XML produced by ``sbt spark/test`` (ScalaTest ``-u``
    reporter) and compare against the baseline:

      * regression -- a test that FAILED but is NOT in the baseline. These
        fail the build: a previously-passing test just started failing.
      * expected   -- a test that failed and IS in the baseline. Ignored.
      * fixed      -- a baseline test that now PASSES. By default these also
        fail the build (``--fail-on-fixed true``) so the baseline stays honest
        and contributors remove entries as they fix them.

    If the baseline file exists but is empty (not yet bootstrapped) the mode
    automatically degrades to ``seed`` so the first run is never spuriously red.
    A *missing* ``--known-failures`` file is treated as a configuration error
    (the gate fails) so a mis-referenced path can't silently pass.

``seed`` (bootstrap / ``update_baseline``)
    Never fails. Just writes the current shard's failing tests so the baseline
    can be (re)generated from a real run.

``aggregate`` (final job)
    Merge every shard's ``--failures-out`` / ``--ran-out`` file into a single,
    sorted, ready-to-commit ``known-failures.txt`` and report stale baseline
    entries (tests no longer present in any shard). Pass ``--expected-shards N``
    to fail when fewer than ``N`` shards contributed gate lists (a shard that
    died before writing them), so an incomplete baseline is never produced.

Flaky quarantine (``--flaky-tests``)
    Some Delta-on-Gluten failures are non-deterministic (e.g. a native bug that
    only triggers on certain runtime plans), so they are neither a stable pass
    nor a stable failure and cannot live in the baseline: baselining them turns
    the gate red on every run where they pass, and leaving them out turns it red
    on every run where they fail. ``flaky-tests.txt`` quarantines them -- a
    quarantined test never counts as a regression (when it fails) nor as
    now-passing (when it passes), and is excluded from the regenerated baseline.
    Its SUITE is an fnmatch glob (so one line covers a root-cause family across
    generated suite variants); its TEST name is matched exactly.

Baseline file format (``known-failures.txt``)::

    # comment lines start with '#'
    <fully.qualified.SuiteName>#<test display name>

The suite is always a JVM class name (dot-separated, never starts with '#'),
so a line whose first non-space character is '#' is unambiguously a comment,
and the FIRST '#' after the suite separates suite from the (possibly
'#'-containing) test name.

Only the Python standard library is used so the script runs in the bare
centos image used by the Delta UT pipeline with no ``pip install``.
"""

import argparse
import fnmatch
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

# Synthetic "test name" recorded when a whole suite aborts (e.g. beforeAll
# throws) so that the JUnit XML reports a suite-level error with no per-test
# <testcase>. Without this, a suite that used to pass but now aborts entirely
# would record zero failing testcases and the regression would be missed.
SUITE_ABORTED = "<suite aborted>"


class NoReportsError(RuntimeError):
    """Raised when no JUnit <testsuite> elements are found under reports_dir."""


class CorruptReportError(NoReportsError):
    """Raised when an expected JUnit report file (TEST-*.xml) fails to parse.

    Subclasses NoReportsError so the enforce/seed handler treats a truncated
    report as a hard data error (exit 2) instead of silently dropping the
    suite's results and letting the gate pass on partial data.
    """


SEP = "#"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# --------------------------------------------------------------------------- #
# Baseline (known-failures.txt) parsing / formatting
# --------------------------------------------------------------------------- #
def format_entry(suite, test):
    return "{}{}{}".format(suite, SEP, test)


def parse_entry(line):
    """Parse a 'suite#test' line into (suite, test) or return None for blanks/comments."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    idx = stripped.find(SEP)
    if idx < 0:
        # No separator: treat the whole line as a suite-level entry.
        return (stripped, SUITE_ABORTED)
    return (stripped[:idx], stripped[idx + len(SEP) :])


def load_entries(path):
    """Load a set of (suite, test) tuples from a baseline/shard-list file."""
    entries = set()
    if not path or not os.path.exists(path):
        return entries
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            parsed = parse_entry(line)
            if parsed is not None:
                entries.add(parsed)
    return entries


def make_is_flaky(flaky_entries):
    """Build a predicate that matches a (suite, test) tuple against flaky entries.

    A flaky entry quarantines a test whose failure is known to be non-deterministic
    (see flaky-tests.txt). The entry's SUITE is treated as an fnmatch glob so a
    single line can cover a root-cause family across generated suite variants
    (e.g. ``*DVs*Suite`` matches every deletion-vector merge suite, ``*`` matches
    any suite); the TEST name is matched exactly (test names are freeform and may
    contain glob metacharacters, so they are never globbed).
    """
    exact = set()
    globbed = []
    for suite, test in flaky_entries:
        if any(ch in suite for ch in "*?["):
            globbed.append((suite, test))
        else:
            exact.add((suite, test))

    def is_flaky(entry):
        if entry in exact:
            return True
        suite, test = entry
        for glob_suite, glob_test in globbed:
            if test == glob_test and fnmatch.fnmatchcase(suite, glob_suite):
                return True
        return False

    return is_flaky


def write_entries(path, entries, header=None):
    """Write a sorted set of (suite, test) tuples to a file."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        if header:
            for hl in header.splitlines():
                fh.write(hl.rstrip() + "\n")
        for suite, test in sorted(entries):
            # Defensive: collapse any stray newlines so each entry stays on one line.
            safe_test = test.replace("\r", " ").replace("\n", " ")
            fh.write(format_entry(suite, safe_test) + "\n")


# --------------------------------------------------------------------------- #
# JUnit XML parsing
# --------------------------------------------------------------------------- #
def _iter_testsuites(root):
    """Yield every <testsuite> element regardless of whether the file root is
    <testsuites> (wrapper) or a single <testsuite>."""
    tag = root.tag.split("}")[-1]  # strip any namespace
    if tag == "testsuites":
        for child in root:
            if child.tag.split("}")[-1] == "testsuite":
                yield child
    elif tag == "testsuite":
        yield root


def _child_local_tags(elem):
    return {c.tag.split("}")[-1] for c in elem}


def parse_reports(reports_dir):
    """Walk reports_dir for JUnit XML and classify every test.

    Returns (passed, failed, skipped) sets of (suite, test) tuples. A test is
    'failed' if its <testcase> has a <failure> or <error> child, 'skipped' if
    it has a <skipped> child, otherwise 'passed'. Suite-level aborts (a
    <testsuite> reporting errors/failures with no failing <testcase>) are
    recorded as a synthetic (suite, SUITE_ABORTED) failure.
    """
    passed, failed, skipped = set(), set(), set()

    xml_files = []
    # ScalaTest's -u reporter and Maven surefire both write `TEST-<suite>.xml`
    # under a `target/.../*-reports/` dir. Restrict the secondary glob to
    # `target/` so we never parse Delta's own XML *test resources* (which live
    # under src/test/resources and are not reports). The <testsuite>-root guard
    # below is a final safety net.
    for pattern in ("**/TEST-*.xml", "**/target/**/*.xml"):
        xml_files.extend(glob.glob(os.path.join(reports_dir, pattern), recursive=True))
    xml_files = sorted(set(xml_files))

    parsed_any = False
    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
        except ET.ParseError as exc:
            # A TEST-*.xml that fails to parse is almost always a report truncated
            # when a forked test JVM was killed mid-write (e.g. OOM). Silently
            # skipping it drops that suite's results and could let the gate go
            # green on partial data, so fail hard for report files. Other XML that
            # merely matched the broad `target/**` glob is still skipped.
            if os.path.basename(xml_file).startswith("TEST-"):
                raise CorruptReportError(
                    "corrupt or truncated JUnit report {}: {}. Refusing to "
                    "evaluate the gate on partial data.".format(xml_file, exc)
                )
            eprint("WARNING: could not parse {}: {}".format(xml_file, exc))
            continue
        root = tree.getroot()
        root_tag = root.tag.split("}")[-1]
        if root_tag not in ("testsuites", "testsuite"):
            continue  # not a JUnit report

        for ts in _iter_testsuites(root):
            parsed_any = True
            suite_name = ts.get("name") or ""
            suite_has_failing_tc = False
            for tc in ts:
                if tc.tag.split("}")[-1] != "testcase":
                    continue
                suite = tc.get("classname") or suite_name
                name = tc.get("name") or ""
                key = (suite, name)
                tags = _child_local_tags(tc)
                if "failure" in tags or "error" in tags:
                    failed.add(key)
                    suite_has_failing_tc = True
                elif "skipped" in tags:
                    skipped.add(key)
                else:
                    passed.add(key)

            # Suite-level abort: counters say something failed but no testcase
            # carried the failure (the suite blew up in beforeAll/constructor).
            # Record a
            # synthetic entry so the regression is visible.
            try:
                errors = int(ts.get("errors", "0") or "0")
                failures = int(ts.get("failures", "0") or "0")
            except ValueError:
                errors = failures = 0
            if (errors + failures) > 0 and not suite_has_failing_tc:
                failed.add((suite_name, SUITE_ABORTED))

    if not parsed_any:
        raise NoReportsError(
            "No JUnit <testsuite> elements found under {}. The test reports are "
            "missing or in an unexpected format -- refusing to evaluate the gate "
            "on an empty result set (this would otherwise pass silently).".format(
                reports_dir
            )
        )

    # A test can't be both passed and failed; failure wins. Skipped only counts
    # if the test was not otherwise seen (e.g. retried).
    passed -= failed
    skipped -= failed
    skipped -= passed
    return passed, failed, skipped


# --------------------------------------------------------------------------- #
# Reporting helpers
# --------------------------------------------------------------------------- #
def _summary_sink():
    """Return a writer that mirrors to GITHUB_STEP_SUMMARY when available."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    handle = open(path, "a", encoding="utf-8") if path else None

    def write(line=""):
        print(line)
        if handle:
            handle.write(line + "\n")

    return write, handle


def _print_block(write, title, entries, limit=50):
    write("")
    write("### {} ({})".format(title, len(entries)))
    if not entries:
        return
    write("")
    write("```")
    for i, (suite, test) in enumerate(sorted(entries)):
        if i >= limit:
            write("... and {} more".format(len(entries) - limit))
            break
        write(format_entry(suite, test))
    write("```")


# --------------------------------------------------------------------------- #
# Modes
# --------------------------------------------------------------------------- #
def run_enforce(args):
    # In enforce mode a missing baseline file would make load_entries() return an
    # empty set, silently degrading to seed mode and passing the gate without
    # enforcing anything. Treat a missing path as a configuration error; an
    # existing-but-empty file is still allowed (it legitimately seeds).
    if args.mode == "enforce" and (
        not args.known_failures or not os.path.exists(args.known_failures)
    ):
        eprint(
            "ERROR: --known-failures '{}' does not exist. In enforce mode the "
            "baseline file must exist (an existing-but-empty file is allowed and "
            "triggers seed mode). Refusing to silently pass.".format(
                args.known_failures
            )
        )
        return 2
    baseline = load_entries(args.known_failures)
    flaky_is = make_is_flaky(load_entries(args.flaky_tests))
    try:
        passed, failed, skipped = parse_reports(args.reports_dir)
    except NoReportsError as exc:
        eprint("ERROR: {}".format(exc))
        return 2

    # Always emit this shard's artifacts for the aggregation job.
    if args.failures_out:
        write_entries(args.failures_out, failed)
    if args.ran_out:
        write_entries(args.ran_out, passed | failed)

    write, handle = _summary_sink()
    try:
        seeding = args.mode == "seed" or not baseline
        if seeding and args.mode != "seed":
            write(
                "> NOTE: baseline `{}` is empty -- running in SEED mode "
                "(no failures will be enforced). Bootstrap the baseline from "
                "the aggregated artifact, commit it, then enforcement begins.".format(
                    args.known_failures
                )
            )

        write(
            "## Delta-on-Gluten test gate -- shard {}".format(
                os.environ.get("SHARD_ID", "?")
            )
        )
        write("")
        write("| Category | Count |")
        write("|---|---:|")
        write("| Ran (pass+fail) | {} |".format(len(passed) + len(failed)))
        write("| Passed | {} |".format(len(passed)))
        write("| Failed | {} |".format(len(failed)))
        write("| Skipped | {} |".format(len(skipped)))
        write("| Baseline (known failures) | {} |".format(len(baseline)))

        if seeding:
            write("")
            write(
                "Seed mode: recorded {} failing test(s) for this shard. "
                "Nothing enforced.".format(len(failed))
            )
            return 0

        regressions = {e for e in (failed - baseline) if not flaky_is(e)}
        quarantined = {e for e in (failed - baseline) if flaky_is(e)}
        # `- flaky` on fixed is defensive: flaky tests are excluded from the
        # regenerated baseline (aggregate mode), so a flaky test should never be in
        # `baseline` in the first place -- but if one slips in, don't let its
        # non-deterministic pass trip the now-passing gate.
        fixed = {e for e in (baseline & passed) if not flaky_is(e)}
        expected = failed & baseline

        write("")
        write("| Gate result | Count |")
        write("|---|---:|")
        write("| Expected failures (in baseline) | {} |".format(len(expected)))
        write("| **Regressions (new failures)** | {} |".format(len(regressions)))
        write("| Now-passing (remove from baseline) | {} |".format(len(fixed)))
        write("| Quarantined flaky failures (ignored) | {} |".format(len(quarantined)))

        _print_block(
            write, "Regressions -- new failures NOT in the baseline", regressions
        )
        if regressions:
            write("")
            write(
                "These tests were not previously known to fail. Either fix "
                "the regression, or (if it is a genuinely new expected "
                "failure) add the lines above to `known-failures.txt`."
            )

        _print_block(
            write,
            "Quarantined flaky failures -- ignored (see flaky-tests.txt)",
            quarantined,
        )

        if args.fail_on_fixed:
            _print_block(
                write, "Now-passing -- delete these lines from the baseline", fixed
            )

        exit_code = 0
        if regressions:
            for suite, test in sorted(regressions):
                eprint("::error::REGRESSION {}".format(format_entry(suite, test)))
            exit_code = 1
        if args.fail_on_fixed and fixed:
            for suite, test in sorted(fixed):
                eprint(
                    "::error::NOW-PASSING (remove from baseline) {}".format(
                        format_entry(suite, test)
                    )
                )
            exit_code = 1

        if exit_code == 0:
            write("")
            write("All failures are expected (in the baseline). Gate passed.")
        return exit_code
    finally:
        if handle:
            handle.close()


def _shard_ids(files, prefix):
    """Return the set of shard ids from gate-list filenames.

    Gate lists are named ``<prefix><shard-id>.txt`` (e.g. ``failures-shard-0.txt``,
    ``ran-shard-0.txt``); this extracts the ``<shard-id>`` token so aggregate mode
    can count how many shards contributed. Matching is on the basename, so nested
    download dirs are fine.
    """
    ids = set()
    pat = re.compile(r"^" + re.escape(prefix) + r"(.+)\.txt$")
    for f in files:
        m = pat.match(os.path.basename(f))
        if m:
            ids.add(m.group(1))
    return ids


def run_aggregate(args):
    failure_files = sorted(
        glob.glob(os.path.join(args.inputs_dir, "**", "failures-*.txt"), recursive=True)
    )
    ran_files = sorted(
        glob.glob(os.path.join(args.inputs_dir, "**", "ran-*.txt"), recursive=True)
    )

    # No per-shard gate lists means the artifacts were never produced or the
    # download failed (the workflow's download step is continue-on-error). Bail
    # out before writing an empty baseline-out, which could otherwise be committed
    # and wipe the entire known-failures.txt.
    if not failure_files and not ran_files:
        eprint(
            "ERROR: no per-shard failures-*.txt / ran-*.txt files found under "
            "{}. Refusing to aggregate an empty baseline (gate-list artifacts are "
            "missing or were not downloaded).".format(args.inputs_dir)
        )
        return 2

    # Completeness guard: each shard writes its failures-<id>.txt and ran-<id>.txt
    # together (see run_enforce), so a shard that died before the gate step -- or
    # whose artifact failed to download -- contributes neither. The empty-inputs
    # check above only catches losing *all* shards; without this a partial set
    # would silently regenerate a baseline missing that shard's failures, wrongly
    # shrinking known-failures.txt and reddening the next run. A shard counts as
    # complete only when BOTH its files are present (robust to partial downloads).
    if args.expected_shards:
        complete = _shard_ids(failure_files, "failures-") & _shard_ids(
            ran_files, "ran-"
        )
        if len(complete) != args.expected_shards:
            eprint(
                "ERROR: expected {} shard gate-list set(s) but found {} complete "
                "(shards: {}). A shard's failures-*/ran-* artifact is missing -- it "
                "likely died before writing its gate lists or its artifact failed "
                "to download -- so the regenerated baseline would be incomplete and "
                "could wrongly shrink known-failures.txt. Refusing to aggregate.".format(
                    args.expected_shards,
                    len(complete),
                    ", ".join(sorted(complete)) or "none",
                )
            )
            return 2

    union_failed = set()
    for f in failure_files:
        union_failed |= load_entries(f)
    union_ran = set()
    for f in ran_files:
        union_ran |= load_entries(f)

    flaky_is = make_is_flaky(load_entries(args.flaky_tests))
    # Exclude quarantined flaky tests from the regenerated baseline: a flaky test
    # that happened to fail this run must never be baked into known-failures.txt
    # (otherwise it would trip the now-passing gate on the next run where it
    # passes). Flaky failures are tracked in flaky-tests.txt, not the baseline.
    baseline_body = {e for e in union_failed if not flaky_is(e)}

    header = (
        "# Known Delta-on-Gluten unit test failures.\n"
        "#\n"
        "# Auto-generated by compare-test-results.py --mode aggregate.\n"
        "# Format: <fully.qualified.SuiteName>#<test display name>\n"
        "# Lines starting with '#' are comments.\n"
        "#\n"
        "# Regenerate by running the 'Delta Spark UT (Gluten)' workflow with\n"
        "# update_baseline=true and committing the produced artifact.\n"
    )
    if args.baseline_out:
        write_entries(args.baseline_out, baseline_body, header=header)

    write, handle = _summary_sink()
    try:
        write("## Delta-on-Gluten aggregated results")
        write("")
        write("| Metric | Count |")
        write("|---|---:|")
        write("| Shards with failure lists | {} |".format(len(failure_files)))
        write("| Distinct failing tests | {} |".format(len(union_failed)))
        write("| Distinct tests run | {} |".format(len(union_ran)))

        exit_code = 0
        if args.known_failures and os.path.exists(args.known_failures):
            baseline = load_entries(args.known_failures)
            if baseline:
                regressions = {e for e in (union_failed - baseline) if not flaky_is(e)}
                quarantined = {e for e in (union_failed - baseline) if flaky_is(e)}
                fixed = {
                    e
                    for e in (baseline & (union_ran - union_failed))
                    if not flaky_is(e)
                }
                stale = baseline - union_ran
                write("| Baseline entries | {} |".format(len(baseline)))
                write("| Regressions (global) | {} |".format(len(regressions)))
                write("| Now-passing (global) | {} |".format(len(fixed)))
                write(
                    "| Quarantined flaky failures (ignored) | {} |".format(
                        len(quarantined)
                    )
                )
                write("| Stale (not seen this run) | {} |".format(len(stale)))
                _print_block(write, "Regressions (global)", regressions)
                _print_block(write, "Now-passing (global)", fixed)
                _print_block(
                    write,
                    "Quarantined flaky failures -- ignored (see flaky-tests.txt)",
                    quarantined,
                )
                _print_block(write, "Stale baseline entries (suite/test gone)", stale)
                if args.fail_on_regression and regressions:
                    exit_code = 1
        return exit_code
    finally:
        if handle:
            handle.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def str2bool(value):
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--mode", choices=("enforce", "seed", "aggregate"), default="enforce"
    )
    parser.add_argument(
        "--known-failures", help="Path to the committed known-failures.txt baseline."
    )
    parser.add_argument(
        "--flaky-tests",
        help="Path to flaky-tests.txt: tests quarantined as non-deterministic. A "
        "flaky test is neither counted as a regression when it fails nor as "
        "now-passing when it passes, and is excluded from the regenerated baseline "
        "(aggregate mode). Optional; omitting it disables quarantining.",
    )
    parser.add_argument(
        "--reports-dir", help="Root dir to search for JUnit XML (enforce/seed)."
    )
    parser.add_argument(
        "--failures-out", help="Write this shard's failing tests here (enforce/seed)."
    )
    parser.add_argument(
        "--ran-out", help="Write this shard's run tests (pass+fail) here."
    )
    parser.add_argument(
        "--fail-on-fixed",
        type=str2bool,
        default=True,
        help="Fail when a baseline test now passes (default true).",
    )
    parser.add_argument(
        "--inputs-dir", help="Dir with per-shard failures-*/ran-* files (aggregate)."
    )
    parser.add_argument(
        "--expected-shards",
        type=int,
        default=0,
        help="In aggregate mode, the number of shards expected to contribute gate "
        "lists. When >0, fail if fewer complete shard gate-list pairs "
        "(failures-*/ran-*) are found -- e.g. a shard died before writing them -- so "
        "an incomplete baseline is never produced. 0 (default) disables the check.",
    )
    parser.add_argument(
        "--baseline-out", help="Write the merged baseline here (aggregate)."
    )
    parser.add_argument(
        "--fail-on-regression",
        type=str2bool,
        default=False,
        help="In aggregate mode, fail if global regressions exist.",
    )
    args = parser.parse_args(argv)

    if args.mode in ("enforce", "seed"):
        if not args.reports_dir:
            parser.error("--reports-dir is required for --mode {}".format(args.mode))
        return run_enforce(args)
    return run_aggregate(args)


if __name__ == "__main__":
    sys.exit(main())
