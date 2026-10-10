"""
PlanTextUMLGenerator: Generate PlantUML diagrams from Spark query plan text (plan.txt format).

Usage:
    from plan_text_uml_generator import PlanTextUMLGenerator
    gen = PlanTextUMLGenerator()
    uml = gen.generate_from_file("plan.txt")
    print(uml)
    # or
    uml = gen.generate_from_text(open("plan.txt").read())
    print(uml)
"""

import os
import re
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple, Any


class PlanNode:
    """Represents a single operator node in the query plan."""

    def __init__(self, node_id: int, name: str, statistics: Optional[str] = None):
        self.node_id = node_id
        self.name = name                    # full name as it appears in the plan
        self.statistics = statistics        # raw Statistics(...) string, or None
        self.parent_id: Optional[int] = None
        self._parsed_stats: Optional[Dict[str, str]] = None

    def get_stats(self) -> Dict[str, str]:
        """Parse Statistics(...) into a key->value dict. Only keys present are returned."""
        if self._parsed_stats is not None:
            return self._parsed_stats
        result: Dict[str, str] = {}
        if self.statistics:
            for m in re.finditer(r'(\w+)=([^,)]+)', self.statistics):
                result[m.group(1)] = m.group(2).strip()
        self._parsed_stats = result
        return result


# Matches the operator name and numeric id on a single line (after stripping
# tree-drawing prefixes and optional ^ markers).
_NODE_RE = re.compile(
    r'\^?\s*(?P<name>[A-Za-z][A-Za-z0-9 _.,()\-]*?)\s+\((?P<id>\d+)\)'
    r'(?:,\s*(?P<stats>Statistics\([^)]*\)))?'
    r'\s*$'
)

# Matches "Subquery:N Hosting operator id = X Hosting Expression = ..."
_SUBQUERY_HEADER_RE = re.compile(
    r'Subquery:(?P<sq_num>\d+)\s+Hosting operator id\s*=\s*(?P<host_id>\d+)'
    r'\s+Hosting Expression\s*=\s*(?P<expression>.+)'
)

# Matches "Subquery subquery#NNNNN" in a hosting expression
_SUBQUERY_ID_RE = re.compile(r'Subquery\s+subquery#(?P<sq_id>\d+)')

# Matches "dynamicpruning#NNNNN" in a hosting expression
_DYNAMICPRUNING_ID_RE = re.compile(r'dynamicpruning#(?P<dp_id>\d+)')

# Matches "(N) ReusedExchange [Reuses operator id: M]"
_REUSED_EXCHANGE_RE = re.compile(
    r'\((?P<node_id>\d+)\)\s+ReusedExchange\s+\[Reuses operator id:\s*(?P<reused_id>\d+)\]'
)


class SubqueryInfo:
    """Holds parsed information about a single subquery block."""

    def __init__(self, sq_num: int, host_op_id: int, expression: str):
        self.sq_num = sq_num
        self.host_op_id = host_op_id
        self.expression = expression
        # If the expression contains "Subquery subquery#NNNNN", this is set
        self.subquery_id: Optional[str] = None
        # If the expression contains "dynamicpruning#NNNNN", this is set
        self.dynamicpruning_id: Optional[str] = None
        # For dynamicpruning / ReusedExchange subqueries:
        # reused_node_id -> reuses_op_id (the real BroadcastQueryStage id)
        self.reused_exchange_map: Dict[int, int] = {}
        # The BroadcastQueryStage node parsed in this subquery's Final Plan
        self.broadcast_stage_id: Optional[int] = None
        # Parsed Final Plan nodes for this subquery (non-empty only when the
        # subquery has its own distinct tree, i.e. NOT a pure ReusedExchange)
        self.nodes: List[PlanNode] = []

        m = _SUBQUERY_ID_RE.search(expression)
        if m:
            self.subquery_id = m.group('sq_id')

        m2 = _DYNAMICPRUNING_ID_RE.search(expression)
        if m2:
            self.dynamicpruning_id = m2.group('dp_id')


class PlanTextUMLGenerator:
    """
    Parse a Spark query plan in textual tree format (as produced by df.explain())
    and generate a PlantUML diagram.

    Supports:
    - Main Final Plan operators
    - Subquery blocks after "===== Subqueries ====="
      * ReusedExchange subqueries: adds a cross-edge from the reused
        BroadcastQueryStage to the hosting operator
      * Subquery#id subqueries: renders the subquery as a separate package
        and adds a dashed dependency edge from each hosting operator

    Only metrics that are explicitly present in the plan text are shown
    (Statistics sizeInBytes / rowCount from ShuffleQueryStage / BroadcastQueryStage).
    Nodes without statistics are shown with just their operator name.
    """

    # ------------------------------------------------------------------ #
    # Node-skip rules (same as PlantUMLGenerator in sparklog_extend.py)   #
    # ------------------------------------------------------------------ #
    SKIP_PATTERNS = [
        r'^InputAdapter$',
        r'^InputIteratorTransformer$',
        r'^AdaptiveSparkPlan$',
        r'^WholeStageCodegenTransformer\s*\(\d+\)',
        r'^AQEShuffleRead$',
        r'^VeloxResizeBatches$',
        r'^ReusedExchange$',
    ]

    # Operators suppressed in simple_chart mode (unless they are a subquery host)
    SIMPLE_SKIP_PATTERNS = [
        r'Project',
        r'Filter',
        r'Exchange',
        r'Exchange',

    ]

    # Palette for per-table BatchScan colouring in simple_chart mode.
    # Colours are assigned in the order tables are first encountered.
    SCAN_COLORS = [
        '#AED6F1',  # soft blue
        '#A9DFBF',  # soft green
        '#F9E79F',  # soft yellow
        '#F5CBA7',  # soft orange
        '#D2B4DE',  # soft purple
        '#FADADD',  # soft pink
        '#A3E4D7',  # soft teal
        '#FAD7A0',  # soft peach
        '#D5DBDB',  # soft grey
        '#F1948A',  # soft red
        '#85C1E9',  # medium blue
        '#82E0AA',  # medium green
        '#F8C471',  # medium amber
        '#C39BD3',  # medium violet
        '#76D7C4',  # medium turquoise
    ]

    # ------------------------------------------------------------------ #
    # Node colour by operator type                                         #
    # ------------------------------------------------------------------ #
    def _get_color(self, name: str) -> str:
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

    def _extract_table_name(self, node_name: str) -> Optional[str]:
        """Return the table name from a Scan operator label, or None."""
        cleaned = self._clean_name(node_name)
        m = re.match(r'\w*[Ss]can\w*\s+([\w_]+)\s*$', cleaned)
        if m:
            return m.group(1).lower()
        return None

    # ------------------------------------------------------------------ #
    # Parsing helpers                                                      #
    # ------------------------------------------------------------------ #
    def _should_skip(self, name: str) -> bool:
        for pat in self.SKIP_PATTERNS:
            if re.search(pat, name, re.IGNORECASE):
                return True
        return False

    def _should_skip_simple(self, name: str) -> bool:
        """Return True for operators that simple_chart mode suppresses."""
        for pat in self.SIMPLE_SKIP_PATTERNS:
            if re.search(pat, name, re.IGNORECASE):
                return True
        return False

    def _clean_name(self, name: str) -> str:
        """
        Shorten fully-qualified table names in Scan operators so labels stay
        readable: keep only the table name (last dot-separated segment).
        """
        m = re.match(
            r'(\w*Scan\w*)\s+(?:[\w_]+\.)*?([\w_]+)\s*$',
            name,
            re.IGNORECASE,
        )
        if m:
            return f"{m.group(1)} {m.group(2)}"
        return name

    # ------------------------------------------------------------------ #
    # Plan-tree parsing (shared between main plan and subquery plans)      #
    # ------------------------------------------------------------------ #
    def _parse_final_plan_section(self, text: str) -> List[PlanNode]:
        """
        Given a block of text that is already the content of a Final Plan
        (i.e. the ``== Final Plan ==`` header has been stripped or we are
        positioned just after it), parse the tree and return PlanNode list.

        Lines with ``== Initial Plan ==`` or any further ``==`` markers stop
        the parse.
        """
        nodes: List[PlanNode] = []
        depth_stack: Dict[int, int] = {}
        in_final = False

        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line:
                continue

            # Detect section markers
            if '==' in line:
                if 'Final Plan' in line:
                    in_final = True
                    depth_stack = {}
                elif 'Initial Plan' in line:
                    # Stop — we only want the Final Plan
                    break
                continue

            if not in_final:
                # Before we hit "== Final Plan ==" treat as already inside
                # (caller may pass pre-sliced text that starts at the tree)
                pass

            # Determine depth from the column of the +- or :- marker
            m_marker = re.search(r'[+:]-', line)
            if m_marker:
                depth = m_marker.start() // 3 + 1
            else:
                depth = 0

            # Extract operator content
            content_m = re.search(r'[+:]-\s*\^?\s*(.*)', line)
            if content_m:
                content = content_m.group(1)
            else:
                content = line.strip().lstrip('^').strip()

            # Strip the WholeStageCodegen marker "* " that Spark prepends to
            # codegen'd operators (e.g. "* Filter (3)").
            content = re.sub(r'^\*\s+', '', content)

            m = _NODE_RE.match(content)
            if not m:
                continue

            name = m.group('name').strip()
            node_id = int(m.group('id'))
            stats_raw = m.group('stats')

            node = PlanNode(node_id=node_id, name=name, statistics=stats_raw)

            parent_depth = depth - 1
            while parent_depth >= 0:
                if parent_depth in depth_stack:
                    node.parent_id = depth_stack[parent_depth]
                    break
                parent_depth -= 1

            depth_stack[depth] = node_id
            for d in list(depth_stack.keys()):
                if d > depth:
                    del depth_stack[d]

            nodes.append(node)

        return nodes

    def parse_plan_text(self, text: str) -> List[PlanNode]:
        """
        Parse the *main* Final Plan section (before ``===== Subqueries =====``)
        and return a list of PlanNode objects with parent_id relationships set.

        Ignores the Initial Plan and everything after the Subqueries marker.
        """
        # Restrict to content before Subqueries section
        sq_split = re.split(r'=====\s*Subqueries\s*=====', text, maxsplit=1)
        main_text = sq_split[0]
        return self._parse_final_plan_section(main_text)

    # ------------------------------------------------------------------ #
    # Subquery parsing                                                     #
    # ------------------------------------------------------------------ #
    def _parse_subqueries(self, text: str) -> List[SubqueryInfo]:
        """
        Extract the ``===== Subqueries =====`` section and parse each
        ``Subquery:N`` block into a SubqueryInfo.
        """
        sq_split = re.split(r'=====\s*Subqueries\s*=====', text, maxsplit=1)
        if len(sq_split) < 2:
            return []

        sq_section = sq_split[1]

        # Split into individual subquery blocks by the "Subquery:N ..." header
        blocks = re.split(r'(?=Subquery:\d+\s+Hosting)', sq_section)

        subqueries: List[SubqueryInfo] = []
        for block in blocks:
            block = block.strip()
            if not block:
                continue

            hdr_m = _SUBQUERY_HEADER_RE.match(block)
            if not hdr_m:
                continue

            sq = SubqueryInfo(
                sq_num=int(hdr_m.group('sq_num')),
                host_op_id=int(hdr_m.group('host_id')),
                expression=hdr_m.group('expression').strip(),
            )

            # Collect ReusedExchange mappings from the operator-detail lines
            # (e.g. "(192) ReusedExchange [Reuses operator id: 7]")
            for rx_m in _REUSED_EXCHANGE_RE.finditer(block):
                sq.reused_exchange_map[int(rx_m.group('node_id'))] = int(rx_m.group('reused_id'))

            # Find BroadcastQueryStage node id in this subquery's Final Plan
            # (used when constructing the ReusedExchange cross-edge)
            bqs_m = re.search(r'BroadcastQueryStage\s+\((\d+)\)', block)
            if bqs_m:
                sq.broadcast_stage_id = int(bqs_m.group(1))

            # Parse Final Plan nodes for Subquery#id subqueries and
            # dynamicpruning subqueries that have their own distinct tree
            if sq.subquery_id is not None or (
                sq.dynamicpruning_id is not None and not sq.reused_exchange_map
            ):
                sq.nodes = self._parse_final_plan_section(block)

            subqueries.append(sq)

        return subqueries

    # ------------------------------------------------------------------ #
    # PlantUML generation                                                  #
    # ------------------------------------------------------------------ #
    def _build_label(self, node: PlanNode) -> str:
        """
        Build the display label.
        Only include metrics that are explicitly present in the plan text
        (i.e. Statistics entries from ShuffleQueryStage / BroadcastQueryStage).
        """
        name = self._clean_name(node.name)
        # Sanitize for PlantUML rectangle labels
        name = name.replace('"', "'").replace('\\', '/')
        if len(name) > 80:
            name = name[:77] + '...'

        stats = node.get_stats()
        parts = [name]
        if 'sizeInBytes' in stats:
            parts.append(f"size: {stats['sizeInBytes']}")
        if 'rowCount' in stats:
            parts.append(f"rows: {stats['rowCount']}")

        return '\\n'.join(parts)

    def _emit_nodes_and_edges(
        self,
        nodes: List[PlanNode],
        lines: List[str],
        id_prefix: str = 'n',
        simple_chart: bool = False,
        subquery_host_ids: Optional[set] = None,
        scan_color_map: Optional[Dict[str, str]] = None,
    ) -> List[Tuple[str, str]]:
        """
        Emit rectangle declarations for *nodes* into *lines*.
        Returns the list of (child_alias, parent_alias) edges.
        Skipped nodes are transparently bridged.

        Args:
            simple_chart:       When True, suppress Project/Filter/Exchange nodes
                                (unless their id is in subquery_host_ids).
            subquery_host_ids:  Set of node ids that must never be suppressed
                                because they are hosting operators for a subquery.
            scan_color_map:     Mutable dict mapping table_name -> hex colour;
                                new tables are assigned the next palette colour.
        """
        if subquery_host_ids is None:
            subquery_host_ids = set()
        if scan_color_map is None:
            scan_color_map = {}

        node_map: Dict[int, PlanNode] = {n.node_id: n for n in nodes}

        def _is_kept(node: PlanNode) -> bool:
            if self._should_skip(node.name):
                return False
            if simple_chart and node.node_id not in subquery_host_ids:
                if self._should_skip_simple(node.name):
                    return False
            return True

        kept_ids: set = {n.node_id for n in nodes if _is_kept(n)}

        def nearest_kept_ancestor(node: PlanNode) -> Optional[int]:
            cur_id = node.parent_id
            while cur_id is not None:
                if cur_id in kept_ids:
                    return cur_id
                parent = node_map.get(cur_id)
                if parent is None:
                    break
                cur_id = parent.parent_id
            return None

        edges: List[Tuple[str, str]] = []
        for node in nodes:
            if node.node_id not in kept_ids:
                continue
            anc = nearest_kept_ancestor(node)
            if anc is not None:
                edges.append((f'{id_prefix}{node.node_id}', f'{id_prefix}{anc}'))

        for node in nodes:
            if node.node_id not in kept_ids:
                continue
            label = self._build_label(node)
            if simple_chart:
                table = self._extract_table_name(node.name)
                if table is not None:
                    if table not in scan_color_map:
                        idx = len(scan_color_map) % len(self.SCAN_COLORS)
                        scan_color_map[table] = self.SCAN_COLORS[idx]
                    color = scan_color_map[table]
                else:
                    color = self._get_color(node.name)
            else:
                color = self._get_color(node.name)
            lines.append(f'rectangle "{label}" as {id_prefix}{node.node_id} {color}')

        return edges

    def generate_plantuml(
        self,
        plan_text: str,
        title: str = "Spark Query Execution Plan",
        simple_chart: bool = False,
    ) -> str:
        """
        Parse *plan_text* and return a PlantUML diagram string.

        Handles:
        - Main Final Plan
        - Subquery:N with ReusedExchange  → cross-edge n<reused_id> --> n<host_op>
        - Subquery:N with subquery#id     → package + dashed edge to host operators

        Args:
            plan_text:    Raw text from plan.txt / df.explain() output.
            title:        Diagram title shown in the @startuml header.
            simple_chart: When True, suppress Project / Filter / BroadcastExchange /
                          Exchange nodes (unless they are subquery host operators),
                          and colour each BatchScan with a unique per-table colour.
        """
        # ---- Parse main plan ---- #
        main_nodes = self.parse_plan_text(plan_text)
        if not main_nodes:
            raise ValueError("No operator nodes found in plan text.")

        # ---- Parse subqueries ---- #
        subqueries = self._parse_subqueries(plan_text)

        # ---- Build subquery host-id set (used by simple_chart) ---- #
        # These node ids must never be suppressed even in simple_chart mode.
        subquery_host_ids: set = {sq.host_op_id for sq in subqueries}

        # ---- Shared per-table colour map (persists across main + subquery nodes) ---- #
        scan_color_map: Dict[str, str] = {}

        # ---- Start PlantUML output ---- #
        lines = [
            "@startuml",
            f'title {title}',
            "skinparam defaultTextAlignment center",
            "skinparam defaultFontName Courier New",
            "skinparam rectangleFontSize 10",
            "skinparam rectangleBorderColor black",
            "top to bottom direction",
            "",
        ]

        # ---- Main plan nodes ---- #
        main_edges = self._emit_nodes_and_edges(
            main_nodes, lines, id_prefix='n',
            simple_chart=simple_chart,
            subquery_host_ids=subquery_host_ids,
            scan_color_map=scan_color_map,
        )
        lines.append("")

        # ---- Main plan edges ---- #
        for child_alias, parent_alias in main_edges:
            lines.append(f"{child_alias} --> {parent_alias}")
        lines.append("")

        # ---- Build a node_id -> alias map for all emitted nodes ---- #
        # Used to correctly resolve host_op_id aliases when a hosting node
        # lives inside a subquery package rather than the main plan.
        node_alias_map: Dict[int, str] = {}
        for node in main_nodes:
            node_alias_map[node.node_id] = f'n{node.node_id}'

        # ---- Subquery packages and cross-edges ---- #
        # Track which subquery#id packages have already been emitted
        emitted_sq_ids: set = set()

        # First pass: collect ReusedExchange cross-edges
        # (these don't need a new package; they reuse a node already in the main plan)
        reused_cross_edges: List[str] = []
        for sq in subqueries:
            if sq.reused_exchange_map:
                # The hosting operator gains a dependency on the real
                # BroadcastQueryStage in the main plan.
                # ReusedExchange (node X) reuses operator id Y →
                # the subquery's BroadcastQueryStage wraps the ReusedExchange,
                # so the effective dependency is: n<Y> (BroadcastQueryStage)
                # feeds into n<host_op_id>
                for _reused_node_id, real_op_id in sq.reused_exchange_map.items():
                    host_alias = node_alias_map.get(sq.host_op_id, f'n{sq.host_op_id}')
                    reused_cross_edges.append(
                        f"n{real_op_id} --> {host_alias} : subquery:{sq.sq_num}"
                    )

        # Second pass: subquery#id and dynamicpruning#id packages
        # Register subquery node aliases before emitting dep edges so that a
        # host_op_id that lives inside one subquery package can be resolved
        # when another subquery references it.
        subquery_node_aliases: Dict[str, Tuple[str, str, str]] = {}  # pkg_key -> (pkg_alias, pkg_label, id_prefix)
        for sq in subqueries:
            if sq.subquery_id is not None:
                pkg_key = sq.subquery_id
                pkg_alias = f"sq{pkg_key}"
                pkg_label = f"subquery#{pkg_key}"
                id_prefix = f's{pkg_key}_n'
            elif sq.dynamicpruning_id is not None and sq.nodes:
                pkg_key = sq.dynamicpruning_id
                pkg_alias = f"dp{pkg_key}"
                pkg_label = f"dynamicpruning#{pkg_key}"
                id_prefix = f'dp{pkg_key}_n'
            else:
                continue

            subquery_node_aliases[pkg_key] = (pkg_alias, pkg_label, id_prefix)

            if pkg_key not in emitted_sq_ids:
                emitted_sq_ids.add(pkg_key)
                # Register this subquery's node aliases
                for node in sq.nodes:
                    node_alias_map[node.node_id] = f'{id_prefix}{node.node_id}'
                # Emit a package for this subquery
                lines.append(f'package "{pkg_label}" as {pkg_alias} #ffe4b5 {{')
                sq_edges = self._emit_nodes_and_edges(
                    sq.nodes, lines, id_prefix=id_prefix,
                    simple_chart=simple_chart,
                    subquery_host_ids=subquery_host_ids,
                    scan_color_map=scan_color_map,
                )
                lines.append("}")
                lines.append("")
                # Intra-subquery edges
                for child_alias, parent_alias in sq_edges:
                    lines.append(f"{child_alias} --> {parent_alias}")
                lines.append("")

        # Emit subquery dependency edges (resolve host alias from node_alias_map)
        subquery_dep_edges: List[str] = []
        for sq in subqueries:
            if sq.subquery_id is not None:
                pkg_key = sq.subquery_id
            elif sq.dynamicpruning_id is not None and sq.nodes:
                pkg_key = sq.dynamicpruning_id
            else:
                continue

            pkg_alias = subquery_node_aliases[pkg_key][0]
            host_alias = node_alias_map.get(sq.host_op_id, f'n{sq.host_op_id}')
            subquery_dep_edges.append(
                f"{pkg_alias} ..> {host_alias} : subquery:{sq.sq_num}"
            )

        # Emit ReusedExchange cross-edges
        if reused_cross_edges:
            lines.append("' ReusedExchange subquery cross-edges")
            lines.extend(reused_cross_edges)
            lines.append("")

        # Emit subquery dependency edges
        if subquery_dep_edges:
            lines.append("' Subquery dependency edges")
            lines.extend(subquery_dep_edges)
            lines.append("")

        lines.append("@enduml")

        return '\n'.join(lines)

    def generate_from_text(
        self,
        plan_text: str,
        title: str = "Spark Query Execution Plan",
        simple_chart: bool = False,
    ) -> str:
        """Convenience wrapper: generate PlantUML from a plan text string."""
        return self.generate_plantuml(plan_text, title=title, simple_chart=simple_chart)

    def generate_from_file(
        self,
        path: str,
        title: Optional[str] = None,
        simple_chart: bool = False,
    ) -> str:
        """Read plan text from *path* and generate PlantUML."""
        with open(path, 'r', encoding='utf-8') as f:
            text = f.read()
        if title is None:
            title = f"Spark Query Execution Plan ({os.path.basename(path)})"
        return self.generate_plantuml(text, title=title, simple_chart=simple_chart)

    # ------------------------------------------------------------------ #
    # URL download + Physical Plan extraction                             #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_app_id_and_query_id(url: str) -> Tuple[str, str]:
        """
        Parse app_id and query_id from a Spark UI SQL execution URL.

        Supported forms:
            https://<host>/proxy/<app_id>/SQL/execution/?id=<query_id>
            https://<host>/<app_id>/SQL/execution/?id=<query_id>

        In both cases the app_id is the path segment immediately before ``SQL``.
        """
        parsed = urllib.parse.urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]

        # Find the segment immediately before 'SQL'
        app_id: Optional[str] = None
        try:
            sql_idx = path_parts.index('SQL')
            if sql_idx > 0:
                app_id = path_parts[sql_idx - 1]
        except ValueError:
            pass

        if not app_id:
            raise ValueError(
                f"Cannot extract app_id from URL path '{parsed.path}'. "
                "Expected pattern: /<app_id>/SQL/execution/?id=<query_id> "
                "(with or without a leading /proxy/ segment)."
            )

        qs = urllib.parse.parse_qs(parsed.query)
        if 'id' not in qs:
            raise ValueError(f"URL has no 'id' query parameter: {url}")
        query_id = qs['id'][0]

        return app_id, query_id

    @staticmethod
    def _extract_physical_plan(html: str) -> str:
        """
        Extract the text that follows the ``== Physical Plan ==`` header
        from raw HTML returned by the Spark UI SQL execution page.

        The plan text is stored inside a ``<pre>`` or ``<textarea>`` tag
        on the page, with the ``== Physical Plan ==`` marker inside it.
        Falls back to a plain-text scan if no such tag is found.
        """
        # 1. Try to find a <pre> or <textarea> block that contains the marker
        block_re = re.compile(
            r'<(?:pre|textarea)[^>]*>(.*?)</(?:pre|textarea)>',
            re.DOTALL | re.IGNORECASE,
        )
        for m in block_re.finditer(html):
            content = m.group(1)
            # Decode basic HTML entities
            content = content.replace('&lt;', '<').replace('&gt;', '>') \
                             .replace('&amp;', '&').replace('&#39;', "'") \
                             .replace('&quot;', '"')
            if '== Physical Plan ==' in content:
                # Slice from the marker onward
                idx = content.index('== Physical Plan ==')
                return content[idx:]

        # 2. Fallback: strip all tags and scan plain text
        plain = re.sub(r'<[^>]+>', '', html)
        plain = plain.replace('&lt;', '<').replace('&gt;', '>') \
                     .replace('&amp;', '&').replace('&#39;', "'") \
                     .replace('&quot;', '"')
        if '== Physical Plan ==' in plain:
            idx = plain.index('== Physical Plan ==')
            return plain[idx:]

        raise ValueError(
            "Could not find '== Physical Plan ==' in the downloaded page."
        )

    def generate_from_url(
        self,
        url: str,
        output_dir: str = 'output',
        simple_chart: bool = False,
        timeout: int = 30,
    ) -> str:
        """
        Download a Spark UI SQL execution page, extract ``== Physical Plan ==``,
        generate a PlantUML diagram, and write it to
        ``<output_dir>/<app_id>/<query_id>.puml``.

        Args:
            url:         Spark UI URL, e.g.
                         ``https://<host>/proxy/<app_id>/SQL/execution/?id=1``
            output_dir:  Base output directory (default: ``output``).
            simple_chart: Pass ``True`` to suppress Project/Filter/Exchange nodes.
            timeout:     HTTP request timeout in seconds (default: 30).

        Returns:
            The path of the written ``.puml`` file.
        """
        app_id, query_id = self._extract_app_id_and_query_id(url)

        # Download the page
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            html = resp.read().decode('utf-8', errors='replace')

        # Extract the Physical Plan section
        plan_text = self._extract_physical_plan(html)

        # Generate PlantUML
        title = f"Physical Plan — {app_id} query {query_id}"
        puml = self.generate_plantuml(plan_text, title=title, simple_chart=simple_chart)

        # Write output
        out_folder = os.path.join(output_dir, app_id)
        os.makedirs(out_folder, exist_ok=True)
        out_path = os.path.join(out_folder, f"{query_id}.puml")
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(puml)

        return out_path
