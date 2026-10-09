#!/usr/bin/env python3
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

"""Bound concurrent runners across matrices and local reusable workflow calls.

Conditions are deliberately ignored: independent jobs may overlap even when they
usually finish at different times. Dependencies, not observed scheduling, provide
the guarantee. Requires PyYAML, also used by the repository's CI tooling.
"""

import argparse
from collections import deque
import itertools
import json
import math
from pathlib import Path
import re
import subprocess

import yaml


def matrix_width(strategy):
    matrix = strategy.get("matrix", {})
    cap = strategy.get("max-parallel")
    if cap is not None and (type(cap) is not int or cap < 1):
        raise ValueError("max-parallel must be a positive, literal integer")
    if not isinstance(matrix, dict):
        if cap is None:
            raise ValueError("Dynamic matrices require an explicit max-parallel")
        return cap
    axes = {k: v for k, v in matrix.items() if k not in ("include", "exclude")}
    if any(not isinstance(value, list) for value in axes.values()):
        if cap is None:
            raise ValueError("Dynamic matrices require an explicit max-parallel")
        return cap
    rows = [dict(zip(axes, row)) for row in itertools.product(*axes.values())]
    rows = [
        row
        for row in rows
        if not any(
            all(row.get(key) == value for key, value in exclude.items())
            for exclude in matrix.get("exclude", [])
        )
    ]
    # Counting every include as an additional job is conservative: GitHub may
    # instead merge it into an existing combination.
    count = len(rows) + len(matrix.get("include", []))
    return min(count, cap) if cap else count


def resolve(value, inputs):
    """Resolve literal workflow-call inputs used for bounded matrices."""
    if isinstance(value, dict):
        return {key: resolve(item, inputs) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item, inputs) for item in value]
    if isinstance(value, str):
        match = re.fullmatch(r"\$\{\{\s*(fromJSON\()?inputs\.([\w-]+)\)?\s*\}\}", value)
        if match and match[2] in inputs:
            result = inputs[match[2]]
            return json.loads(result) if match[1] else result
    return value


def expand(path, read, prefix="", stack=(), inputs=None, job_weight=None):
    if path in stack:
        raise ValueError("Recursive workflow call: " + path)
    workflow = yaml.safe_load(read(path))
    triggers = workflow.get("on", workflow.get(True, {}))
    definitions = (
        triggers.get("workflow_call", {}) if isinstance(triggers, dict) else {}
    )
    definitions = definitions or {}
    defaults = {
        key: value["default"]
        for key, value in definitions.get("inputs", {}).items()
        if "default" in value
    }
    defaults.update(inputs or {})
    jobs = resolve(workflow["jobs"], defaults)
    weights, edges, groups = {}, set(), {}
    for name, job in jobs.items():
        identifier = prefix + name
        if "uses" in job:
            target = job["uses"]
            if not target.startswith("./.github/workflows/"):
                raise ValueError("Cannot bound external workflow: " + target)
            if "strategy" in job:
                raise ValueError(
                    "Matrix workflow calls need an explicit graph expansion"
                )
            child_weights, child_edges = expand(
                target[2:],
                read,
                identifier + "/",
                stack + (path,),
                job.get("with", {}),
                job_weight,
            )
            weights.update(child_weights)
            edges.update(child_edges)
            groups[name] = set(child_weights)
        else:
            weights[identifier] = (
                job_weight(job) if job_weight else matrix_width(job.get("strategy", {}))
            )
            groups[name] = {identifier}
    for name, job in jobs.items():
        needs = job.get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        for dependency in needs:
            for before in groups[dependency]:
                for after in groups[name]:
                    edges.add((before, after))
    return weights, edges


def pr_timeout(job):
    """Budget all PR matrix waves; unresolved expressions fail rather than undercount."""
    timeout = job.get("timeout-minutes")
    if type(timeout) is not int or timeout < 1:
        raise ValueError("PR jobs require a positive literal timeout-minutes")
    strategy = dict(job.get("strategy", {}))
    matrix = dict(strategy.get("matrix", {}))
    for key, value in matrix.items():
        if isinstance(value, str):
            # The integration matrices explicitly choose full or PR lists. Keep
            # this deliberately narrow so a different expression requires review.
            match = re.fullmatch(
                r"\$\{\{\s*fromJSON\(fromJSON\(inputs\.changes\)\.full_run == 'true'"
                r"\s*&& '[^']*'\s*\|\| '([^']*)'\)\s*\}\}",
                value,
            )
            if not match:
                raise ValueError("Unresolved PR matrix: " + value)
            matrix[key] = json.loads(match[1])
    strategy["matrix"] = matrix
    cap = strategy.pop("max-parallel", None)
    count = matrix_width(strategy)
    return timeout * math.ceil(count / (cap or count))


def critical_path(weights, edges):
    """Active execution budget; runner queueing is outside job timeouts."""
    remaining, finished = dict(weights), {}
    while remaining:
        ready = [
            node
            for node in remaining
            if all(before in finished for before, after in edges if after == node)
        ]
        if not ready:
            raise ValueError("Cyclic job dependencies")
        for node in ready:
            start = max(
                (finished[before] for before, after in edges if after == node),
                default=0,
            )
            finished[node] = start + remaining.pop(node)
    return max(finished.values(), default=0)


def peak(weights, edges):
    """Maximum weighted antichain of the dependency DAG (weighted Dilworth)."""
    ancestors = {node: set() for node in weights}
    for before, after in edges:
        ancestors[after].add(before)
    changed = True
    while changed:
        changed = False
        for node, previous in ancestors.items():
            expanded = previous | set().union(*(ancestors[p] for p in previous))
            if node in expanded:
                raise ValueError("Cyclic job dependencies")
            if expanded != previous:
                ancestors[node] = expanded
                changed = True
    source, sink = ("source",), ("sink",)
    graph = {source: {}, sink: {}}

    def connect(a, b, capacity):
        graph.setdefault(a, {})[b] = capacity
        graph.setdefault(b, {})[a] = 0

    total = sum(weights.values())
    for node, weight in weights.items():
        connect(source, (node, "left"), weight)
        connect((node, "right"), sink, weight)
        for ancestor in ancestors[node]:
            connect((ancestor, "left"), (node, "right"), total)
    flow = 0
    while True:
        parents, queue = {source: None}, deque([source])
        while queue and sink not in parents:
            a = queue.popleft()
            for b, capacity in graph[a].items():
                if capacity and b not in parents:
                    parents[b] = a
                    queue.append(b)
        if sink not in parents:
            return total - flow
        amount, node = total, sink
        while parents[node] is not None:
            amount = min(amount, graph[parents[node]][node])
            node = parents[node]
        node = sink
        while parents[node] is not None:
            parent = parents[node]
            graph[parent][node] -= amount
            graph[node][parent] += amount
            node = parent
        flow += amount


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", help="Inspect a Git revision instead of the worktree")
    args = parser.parse_args()
    if args.ref:
        paths = subprocess.check_output(
            ["git", "ls-tree", "--name-only", args.ref, ".github/workflows/"],
            text=True,
        ).splitlines()

        def read(path):
            return subprocess.check_output(
                ["git", "show", args.ref + ":" + path], text=True
            )

    else:
        paths = [
            str(path) for path in Path(".github/workflows").iterdir() if path.is_file()
        ]

        def read(path):
            return Path(path).read_text()

    failed = False
    for path in sorted(p for p in paths if p.endswith((".yml", ".yaml"))):
        try:
            width = peak(*expand(path, read))
            print(f"{path}: at most {width} concurrent jobs")
            failed |= width > 20
        except ValueError as error:
            print(f"{path}: {error}")
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
