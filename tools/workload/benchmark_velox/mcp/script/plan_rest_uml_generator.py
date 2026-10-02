#!/usr/bin/env python3
"""
plan_rest_uml_generator.py
Parse a Spark query plan from the Spark History Server REST API response
(nodes + edges JSON) and draw a PlantUML diagram.

The REST API endpoint that produces this format is:
    GET /api/v1/applications/{appId}/sql/{queryId}
    (also returned per-query inside the /sql list response)

The JSON structure used here:
    {
      "id": <queryId>,
      "description": "...",
      "nodes": [
        { "nodeId": <int>, "nodeName": "...", "metrics": [{"name": "...", "value": "..."}, ...] },
        ...
      ],
      "edges": [
        { "fromId": <child>, "toId": <parent> },
        ...
      ]
    }

Coloring / skip rules follow PlantUMLGenerator in sparklog_extend.py.

Usage (standalone):
    python plan_rest_uml_generator.py output/app-1787794607022-0000/1.plan
    python plan_rest_uml_generator.py output/app-1787794607022-0000/1.plan --simple

    # Fetch directly from Spark History Server:
    python plan_rest_uml_generator.py \
        --url http://spark-history:18080 \
        --appid app-1787794607022-0000 \
        --queryid 1

Programmatic usage:
    from plan_rest_uml_generator import RestPlanUMLGenerator
    gen = RestPlanUMLGenerator()
    puml = gen.generate_from_file("output/app-1787794607022-0000/1.plan")
    print(puml)
"""

from __future__ import annotations

import argparse
import json
import os as _os
import re
import sys
import urllib.request
from collections import defaultdict as _defaultdict
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Skip rules (matching PlantUMLGenerator.should_skip_node in sparklog_extend)
# ---------------------------------------------------------------------------
_SKIP_PATTERNS: List[str] = [
    r'^InputAdapter$',
    r'^InputIteratorTransformer$',
    r'^BroadcastQueryStage$',
    r'^AdaptiveSparkPlan$',
    r'^WholeStageCodegenTransformer\s*\(\d+\)',
    r'^AQEShuffleRead$',
    r'^VeloxResizeBatches$',
    r'^ReusedExchange$',
    r'^ShuffleQueryStage$',
]

# Extra nodes suppressed in simple_chart mode
_SIMPLE_SKIP_PATTERNS: List[str] = [
    r'Project',
    r'Filter',
    r'Exchange',
]


def _should_skip(name: str) -> bool:
    for pat in _SKIP_PATTERNS:
        if re.search(pat, name, re.IGNORECASE):
            return True
    return False


def _should_skip_simple(name: str) -> bool:
    for pat in _SIMPLE_SKIP_PATTERNS:
        if re.search(pat, name, re.IGNORECASE):
            return True
    return False


# ---------------------------------------------------------------------------
# Node colour (matching PlantUMLGenerator.get_node_color in sparklog_extend)
# ---------------------------------------------------------------------------
def _get_color(name: str) -> str:
    n = name.lower()
    if 'scan' in n:
        return '#lightblue'
    if 'join' in n:
        return '#lightgreen'
    if 'aggregate' in n or 'agg' in n:
        return '#lightyellow'
    if 'exchange' in n or 'shuffle' in n:
        return '#lightcoral'
    if 'filter' in n:
        return '#lightgray'
    if 'project' in n:
        return '#wheat'
    if 'sort' in n or 'takeordered' in n:
        return '#plum'
    if 'columnartorrow' in n or 'rowtocolumnar' in n:
        return '#lavender'
    if 'resize' in n:
        return '#lightcyan'
    return '#white'


# Scan colour palette for simple_chart mode (one colour per table)
_SCAN_COLORS: List[str] = [
    '#AED6F1', '#A9DFBF', '#F9E79F', '#F5CBA7', '#D2B4DE',
    '#FADADD', '#A3E4D7', '#FAD7A0', '#D5DBDB', '#F1948A',
    '#85C1E9', '#82E0AA', '#F8C471', '#C39BD3', '#76D7C4',
]


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------
def _clean_scan_name(name: str) -> str:
    """Shorten fully-qualified table names in scan operators."""
    m = re.match(
        r'(\w*[Ss]can\w*)\s+(?:[\w_]+\.)*?([\w_]+)\s*$',
        name,
        re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return name


def _extract_output_rows(metrics: List[Dict]) -> Optional[int]:
    """Return 'number of output rows' metric value as int, or None."""
    for m in metrics:
        if m.get('name') in ('number of output rows', 'number of final output rows'):
            raw = m.get('value', '')
            # Value may be "1,440,033,112" or contain extra formatting
            digits = re.sub(r'[,\s]', '', raw.split('\n')[0].split('(')[0])
            if digits.isdigit():
                return int(digits)
    return None


def _build_label(node_name: str, metrics: List[Dict], max_len: int = 80) -> str:
    """Build a PlantUML rectangle label."""
    name = _clean_scan_name(node_name)
    name = name.replace('"', "'").replace('\\', '/')
    if len(name) > max_len:
        name = name[: max_len - 3] + '...'

    output_rows = _extract_output_rows(metrics)
    if output_rows is not None:
        return f"{name}\\noutput_rows: {output_rows:,}"
    return name


def _extract_table_name(node_name: str) -> Optional[str]:
    """Return the table name from a scan node for colour assignment."""
    cleaned = _clean_scan_name(node_name)
    m = re.match(r'\w*[Ss]can\w*\s+([\w_]+)\s*$', cleaned)
    if m:
        return m.group(1).lower()
    return None


# ---------------------------------------------------------------------------
# Stage resolution
# ---------------------------------------------------------------------------
# Matches "(stage 9.0: task 118)" anywhere in a metric value string
_STAGE_RE = re.compile(r'\(stage\s+(\d+)\.\d+:')


def _extract_stages_from_metrics(metrics: List[Dict]) -> Set[int]:
    """Return the set of unique stage numbers referenced in any metric value."""
    stages: Set[int] = set()
    for m in metrics:
        for s in _STAGE_RE.findall(m.get('value', '')):
            stages.add(int(s))
    return stages


def resolve_node_stages(
    nodes_raw: List[Dict],
    edges_raw: List[Dict],
) -> Dict[int, Optional[int]]:
    """
    Assign a stage number to every node according to these rules:

    1. Extract all stage numbers that appear as ``(stage N.0: ...)`` in the
       node's own metric values.
    2. Exactly one unique stage found  → that is the node's stage.
    3. Two or more unique stages found → the node straddles a stage boundary
       (e.g. ColumnarExchange writes one stage and is read by another).
       Stage is set to the **minimum** value (the write / producer stage).
    4. No stage numbers found → propagate from neighbours:
       a. Collect the resolved stages of all *children* of this node
          (nodes that appear as ``fromId`` pointing into this node's ``toId``,
          i.e. the nodes that produce data consumed by this node).
          If all children that have a resolved stage agree on one value, use it.
       b. Otherwise, fall back to the *parent's* resolved stage
          (the node pointed to by this node's ``toId`` edge).
       c. If neither resolves, stage stays ``None``.

    Args:
        nodes_raw:  The ``nodes`` list from the REST API JSON.
        edges_raw:  The ``edges`` list from the REST API JSON.
                    Each edge is ``{"fromId": child, "toId": parent}``.

    Returns:
        Dict mapping nodeId → stage number (int) or None.
    """
    # Step 1 – extract stages from own metrics
    raw_stages: Dict[int, Set[int]] = {}
    for n in nodes_raw:
        nid = n['nodeId']
        raw_stages[nid] = _extract_stages_from_metrics(n.get('metrics', []) or [])

    # Build adjacency for propagation.
    # child_to_parent : nodeId -> list of toIds  (the node's parents)
    # parent_to_children : nodeId -> list of fromIds  (the node's children)
    child_to_parent: Dict[int, List[int]] = {}
    parent_to_children: Dict[int, List[int]] = {}
    for e in edges_raw:
        child_to_parent.setdefault(e['fromId'], []).append(e['toId'])
        parent_to_children.setdefault(e['toId'], []).append(e['fromId'])

    # Steps 2 & 3 – assign from own metrics
    # For boundary nodes (2+ stages, e.g. ColumnarExchange) use the *minimum*
    # stage — that is the stage in which the exchange writes its data, which is
    # the natural "home" stage for the operator.
    resolved: Dict[int, Optional[int]] = {}
    needs_propagation: List[int] = []
    for n in nodes_raw:
        nid = n['nodeId']
        stages = raw_stages[nid]
        if len(stages) == 1:
            resolved[nid] = next(iter(stages))
        elif len(stages) >= 2:
            resolved[nid] = min(stages)   # write stage (lower number)
        else:
            resolved[nid] = None          # placeholder; will propagate
            needs_propagation.append(nid)

    # Step 4 – propagate iteratively until stable
    changed = True
    while changed:
        changed = False
        for nid in needs_propagation:
            if resolved[nid] is not None:
                continue  # already resolved

            # 4a: children (fromId nodes) – nodes that feed data into this node
            children = parent_to_children.get(nid, [])
            child_stages = {resolved[c] for c in children if resolved.get(c) is not None}
            if len(child_stages) == 1:
                resolved[nid] = next(iter(child_stages))
                changed = True
                continue

            # 4b: parent (toId node) – the consumer of this node's output
            parents = child_to_parent.get(nid, [])
            parent_stages = {resolved[p] for p in parents if resolved.get(p) is not None}
            if len(parent_stages) == 1:
                resolved[nid] = next(iter(parent_stages))
                changed = True

    return resolved


# ---------------------------------------------------------------------------
# Core generator
# ---------------------------------------------------------------------------
class RestPlanUMLGenerator:
    """
    Generate a PlantUML diagram from a Spark REST API query plan JSON.

    The JSON must be the per-query object returned by:
        GET /api/v1/applications/{appId}/sql/{queryId}
    which contains ``nodes`` and ``edges`` arrays.
    """

    def generate_plantuml(
        self,
        plan_data: Dict,
        title: Optional[str] = None,
        simple_chart: bool = False,
    ) -> str:
        """
        Build a PlantUML diagram string from a parsed plan dict.

        Args:
            plan_data:    Parsed JSON dict with ``nodes`` and ``edges``.
            title:        Diagram title (auto-derived from plan_data if None).
            simple_chart: Suppress Project / Filter / Exchange nodes.

        Returns:
            PlantUML diagram as a string.
        """
        nodes_raw: List[Dict] = plan_data.get('nodes', [])
        edges_raw: List[Dict] = plan_data.get('edges', [])

        if not nodes_raw:
            raise ValueError("No nodes found in plan data.")

        query_id = plan_data.get('id', '?')
        description = plan_data.get('description', '')
        if title is None:
            short_desc = description[:60] + '...' if len(description) > 60 else description
            title = f"Query {query_id}: {short_desc}" if short_desc else f"Query {query_id}"

        # Build node lookup: nodeId -> raw node dict
        node_map: Dict[int, Dict] = {n['nodeId']: n for n in nodes_raw}

        # Build parent map from edges: fromId is child, toId is parent
        # (edges go child → parent, i.e. data flows from child upward to parent)
        child_to_parent: Dict[int, int] = {}
        for e in edges_raw:
            child_to_parent[e['fromId']] = e['toId']

        # Determine which nodes to keep
        kept_ids: Set[int] = set()
        for nid, n in node_map.items():
            name = n.get('nodeName', '')
            if _should_skip(name):
                continue
            if simple_chart and _should_skip_simple(name):
                continue
            kept_ids.add(nid)

        def _nearest_kept_ancestor(nid: int) -> Optional[int]:
            cur = child_to_parent.get(nid)
            while cur is not None:
                if cur in kept_ids:
                    return cur
                cur = child_to_parent.get(cur)
            return None

        # Build edges between kept nodes
        edges: List[Tuple[int, int]] = []
        for nid in kept_ids:
            ancestor = _nearest_kept_ancestor(nid)
            if ancestor is not None:
                edges.append((nid, ancestor))

        # Assign scan colours in simple_chart mode
        scan_color_map: Dict[str, str] = {}

        def _node_color(name: str) -> str:
            if simple_chart:
                table = _extract_table_name(name)
                if table is not None:
                    if table not in scan_color_map:
                        idx = len(scan_color_map) % len(_SCAN_COLORS)
                        scan_color_map[table] = _SCAN_COLORS[idx]
                    return scan_color_map[table]
            return _get_color(name)

        # ------------------------------------------------------------------
        # Resolve a stage number for every node
        # ------------------------------------------------------------------
        node_stage: Dict[int, Optional[int]] = resolve_node_stages(nodes_raw, edges_raw)

        # ------------------------------------------------------------------
        # Group kept nodes by resolved stage
        # ------------------------------------------------------------------
        node_map_raw: Dict[int, Dict] = {n['nodeId']: n for n in nodes_raw}

        stage_to_nodes: Dict[Optional[int], List[int]] = _defaultdict(list)
        for n in nodes_raw:
            nid = n['nodeId']
            if nid in kept_ids:
                stage_to_nodes[node_stage[nid]].append(nid)

        # Sanitize title for PlantUML
        safe_title = title.replace('"', "'")

        lines = [
            "@startuml",
            f'title {safe_title}',
            "skinparam defaultTextAlignment center",
            "skinparam defaultFontName Courier New",
            "skinparam rectangleFontSize 10",
            "skinparam rectangleBorderColor black",
            "skinparam packageBorderColor black",
            "skinparam packageFontSize 12",
            "top to bottom direction",
            "",
        ]

        # ---- Emit stage packages -----------------------------------------
        sorted_stages = sorted(
            (s for s in stage_to_nodes if s is not None),
            key=lambda x: x,
        )
        if None in stage_to_nodes:
            sorted_stages.append(None)  # type: ignore[arg-type]

        for stage in sorted_stages:
            stage_nids = stage_to_nodes[stage]
            pkg_label = f"Stage {stage}" if stage is not None else "Unassigned"
            lines.append(f'package "{pkg_label}" {{')
            for nid in stage_nids:
                n = node_map_raw[nid]
                name = n.get('nodeName', '')
                metrics = n.get('metrics', []) or []
                label = _build_label(name, metrics)
                color = _node_color(name)
                lines.append(f'  rectangle "{label}" as n{nid} {color}')
            lines.append("}")
            lines.append("")

        # ---- Emit data-flow edges (child --> parent) ----------------------
        for child_id, parent_id in edges:
            lines.append(f"n{child_id} --> n{parent_id}")

        lines.append("")
        lines.append("@enduml")

        return '\n'.join(lines)

    # ------------------------------------------------------------------
    # Convenience entry points
    # ------------------------------------------------------------------

    def generate_from_dict(
        self,
        plan_data: Dict,
        title: Optional[str] = None,
        simple_chart: bool = False,
    ) -> str:
        """Generate PlantUML from an already-parsed plan dict."""
        return self.generate_plantuml(plan_data, title=title, simple_chart=simple_chart)

    # ------------------------------------------------------------------
    # PlanTextUMLGenerator delegate
    # ------------------------------------------------------------------

    @staticmethod
    def _puml_from_plan_description(
        plan_description: str,
        title: Optional[str],
        simple_chart: bool,
    ) -> str:
        """Delegate to PlanTextUMLGenerator using the planDescription text."""
        import importlib
        _dir = _os.path.dirname(_os.path.abspath(__file__))
        if _dir not in sys.path:
            sys.path.insert(0, _dir)
        ptg = importlib.import_module('plan_text_uml_generator').PlanTextUMLGenerator()
        if title is None:
            title = "Spark Query Execution Plan"
        return ptg.generate_plantuml(plan_description, title=title, simple_chart=simple_chart)

    def generate_from_file(
        self,
        path: str,
        title: Optional[str] = None,
        simple_chart: bool = False,
        from_explain_text: bool = False,
    ) -> str:
        """
        Read a .plan JSON file, generate PlantUML, write a .puml beside the
        source file, and render it to PNG via PlantUML jar.

        The .puml file is written to the same directory as *path* with the
        same stem and a ``.puml`` extension (e.g. ``1.plan`` → ``1.puml``).

        Args:
            path:             Path to a ``.plan`` JSON file.
            title:            Diagram title (auto-derived if None).
            simple_chart:     Suppress Project / Filter / Exchange nodes.
            from_explain_text: When True, parse ``planDescription`` with
                              PlanTextUMLGenerator instead of using the
                              nodes/edges REST graph.

        Returns:
            Path of the written ``.puml`` file.
        """
        with open(path, 'r', encoding='utf-8') as f:
            plan_data = json.load(f)

        if from_explain_text:
            plan_description = plan_data.get('planDescription', '')
            if not plan_description:
                raise ValueError(f"'planDescription' is empty in {path}")
            if title is None:
                query_id = plan_data.get('id', '?')
                desc = plan_data.get('description', '')
                short = desc[:60] + '...' if len(desc) > 60 else desc
                title = f"Query {query_id}: {short}" if short else f"Query {query_id}"
            puml_content = self._puml_from_plan_description(
                plan_description, title=title, simple_chart=simple_chart
            )
        else:
            puml_content = self.generate_plantuml(plan_data, title=title, simple_chart=simple_chart)

        # Write .puml next to the source file
        base = _os.path.splitext(path)[0]
        puml_path = base + '.puml'
        with open(puml_path, 'w', encoding='utf-8') as f:
            f.write(puml_content)

        # Render to PNG
        _render_png(puml_path)

        return puml_path

    def generate_from_url(
        self,
        base_url: str,
        app_id: str,
        query_id: int,
        output_dir: str = 'output',
        simple_chart: bool = False,
        timeout: int = 30,
        from_explain_text: bool = False,
    ) -> str:
        """
        Fetch query plan from Spark History Server REST API, generate PlantUML,
        write it to ``<output_dir>/<app_id>/<query_id>.puml``, and render PNG.

        Fetches:  GET <base_url>/api/v1/applications/<app_id>/sql/<query_id>

        Args:
            base_url:         e.g. ``http://spark-history:18080``
            app_id:           e.g. ``app-1787794607022-0000``
            query_id:         Integer query / execution ID
            output_dir:       Base directory for written ``.puml`` files
            simple_chart:     Suppress Project / Filter / Exchange nodes
            timeout:          HTTP request timeout in seconds
            from_explain_text: When True, parse ``planDescription`` with
                              PlanTextUMLGenerator instead of using the
                              nodes/edges REST graph.

        Returns:
            Path of the written ``.puml`` file.
        """
        url = f"{base_url.rstrip('/')}/api/v1/applications/{app_id}/sql/{query_id}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            plan_data = json.loads(resp.read().decode('utf-8', errors='replace'))

        if from_explain_text:
            plan_description = plan_data.get('planDescription', '')
            if not plan_description:
                raise ValueError(f"'planDescription' is empty for query {query_id}")
            desc = plan_data.get('description', '')
            short = desc[:60] + '...' if len(desc) > 60 else desc
            title = f"Query {query_id}: {short}" if short else f"Query {query_id}"
            puml_content = self._puml_from_plan_description(
                plan_description, title=title, simple_chart=simple_chart
            )
        else:
            puml_content = self.generate_plantuml(plan_data, simple_chart=simple_chart)

        out_dir = _os.path.join(output_dir, app_id)
        _os.makedirs(out_dir, exist_ok=True)
        puml_path = _os.path.join(out_dir, f"{query_id}.puml")
        with open(puml_path, 'w', encoding='utf-8') as f:
            f.write(puml_content)

        # Render to PNG
        _render_png(puml_path)

        return puml_path


# ---------------------------------------------------------------------------
# PlantUML rendering helper
# ---------------------------------------------------------------------------
PLANTUML_JAR = '/home/ec2-user/mcp/bin/plantuml-1.2026.5.jar'


def _render_png(puml_path: str) -> None:
    """
    Invoke the PlantUML jar to render *puml_path* to a PNG in the same directory.

    The PNG is written beside the .puml file (PlantUML's default behaviour
    when given a single file path with no -o flag).
    """
    puml_pattern = _os.path.abspath(puml_path)
    _os.system(
        f'java -Xmx2048m -DPLANTUML_LIMIT_SIZE=8192 '
        f'-jar {PLANTUML_JAR} "{puml_pattern}"'
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate PlantUML from a Spark REST API query plan JSON (.plan file or live API)."
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument(
        'plan_file',
        nargs='?',
        help="Path to a .plan JSON file (e.g. output/app-xxx/1.plan)",
    )
    src.add_argument(
        '--url',
        help="Spark History Server base URL (e.g. http://spark-history:18080)",
    )

    p.add_argument('--appid', help="Application ID (required with --url)")
    p.add_argument('--queryid', type=int, help="Query / execution ID (required with --url)")
    p.add_argument(
        '--simple', action='store_true',
        help="Simple chart: suppress Project / Filter / Exchange nodes",
    )
    p.add_argument(
        '--from-explain-text', action='store_true',
        help="Parse planDescription with PlanTextUMLGenerator instead of the nodes/edges graph",
    )
    p.add_argument(
        '--output-dir', default='output',
        help="Base output directory when using --url (default: output)",
    )
    return p


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    gen = RestPlanUMLGenerator()

    if args.plan_file:
        puml_path = gen.generate_from_file(
            args.plan_file,
            simple_chart=args.simple,
            from_explain_text=args.from_explain_text,
        )
        print(f"Written to {puml_path}")
        png_path = _os.path.splitext(puml_path)[0] + '.png'
        if _os.path.exists(png_path):
            print(f"PNG rendered: {png_path}")
    else:
        # --url mode
        if not args.appid or args.queryid is None:
            parser.error("--appid and --queryid are required when using --url")
        puml_path = gen.generate_from_url(
            base_url=args.url,
            app_id=args.appid,
            query_id=args.queryid,
            output_dir=args.output_dir,
            simple_chart=args.simple,
            from_explain_text=args.from_explain_text,
        )
        print(f"Written to {puml_path}")
        png_path = _os.path.splitext(puml_path)[0] + '.png'
        if _os.path.exists(png_path):
            print(f"PNG rendered: {png_path}")


if __name__ == '__main__':
    main()
