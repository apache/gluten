#!/usr/bin/env python3
"""
Generate HTML comparison of query plans from two Spark applications.

This script is designed to be run from Jupyter notebooks where a Spark session is already available.

Usage from Jupyter:
    from compare_query_plans_html import compare_query_plans
    compare_query_plans("app-20260223232457-0000", "app-20260304012015-0000", "comparison.html", query_id="q23a", spark=spark)

Usage from command line (requires active Spark environment):
    python3 compare_query_plans_html.py app-20260223232457-0000 app-20260304012015-0000 comparison.html q23a
"""

import re
from html import escape
from sparklog import App_Log_Analysis

def extract_operators_from_data(data):
    """
    Extract operators from query plan data dictionary.
    
    Parameters:
    -----------
    data : dict
        Query plan data from get_query_plan()
        
    Returns:
    --------
    list of dict
        List of operators with 'name', 'display', 'output', and 'level' properties
    """
    operators = []
    
    # Get the first query's nodes
    if not data.get('queries') or len(data['queries']) == 0:
        return operators
    
    query = data['queries'][0]
    nodes = query.get('nodes', [])
    
    for node in nodes:
        # Skip WholeStageCodegenTransformer operators
        nodename = node.get('nodename', '')
        if 'WholeStageCodegenTransformer' in nodename or 'WholeStageCodegen' in nodename:
            continue
        
        # Simplify table scan operators
        display_operator = nodename
        normalized_name = nodename
        if 'IcebergScanTransformer' in nodename or 'FileSourceScanExecTransformer' in nodename:
            # Extract table name from the full path
            table_match = re.search(r'\.([a-zA-Z0-9_]+)$', nodename)
            if table_match:
                table_name = table_match.group(1)
                display_operator = f'table scan {table_name}'
                normalized_name = display_operator
        
        # Get indentation level
        level = node.get('level', 0)
        
        # Get keystage information
        is_keystage = node.get('is_keystage', False)
        keystage_color = node.get('keystage_color', None)
        stageid = node.get('stageid', '')
        
        # Get stage time (without the stdev part in parentheses)
        stagetime = node.get('stagetime', 0)
        stagetime_str = f"{stagetime:.2f}" if stagetime > 0 else ""
        
        # Get stage parts
        stageparts = node.get('stageParts', '')
        stageparts_str = str(stageparts) if stageparts else ""
        
        # Create indented display with light gray | characters
        indent_str = ''.join(['<span class="indent">|</span>' for _ in range(level)])
        if indent_str:
            display_name = f'{indent_str} {display_operator}'
        else:
            display_name = display_operator
        
        # Get output rows (convert to M format)
        output_rows = node.get('output_rows', 0)
        if output_rows > 0:
            output_str = f"{output_rows/1000000:.1f}"
        else:
            output_str = ""
        
        operators.append({
            'name': normalized_name,
            'display': display_name,
            'output': output_str,
            'level': level,
            'is_keystage': is_keystage,
            'keystage_color': keystage_color,
            'stageid': stageid,
            'stagetime': stagetime_str,
            'stageparts': stageparts_str
        })
    
    return operators


def generate_comparison_html(plan1_ops, plan2_ops, query_id="query"):
    """
    Generate HTML comparison table from two lists of operators.
    
    Parameters:
    -----------
    plan1_ops : list of dict
        Operators from first plan
    plan2_ops : list of dict
        Operators from second plan
    query_id : str
        Query identifier for the title
        
    Returns:
    --------
    str
        Complete HTML document
    """
    # Reverse to compare from bottom to top
    plan1_reversed = list(reversed(plan1_ops))
    plan2_reversed = list(reversed(plan2_ops))
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Query Plan Comparison</title>
    <style>
        .header-section {
            margin-bottom: 15px;
        }
        .header-section label {
            margin-left: 20px;
            cursor: pointer;
        }
        .header-section input[type="checkbox"] {
            cursor: pointer;
        }
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 10px;
        }
        table {
            border-collapse: collapse;
            table-layout: fixed;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
            font-size: 11px;
        }
        th {
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tr:nth-child(odd) {
            background-color: #ffffff;
        }
        .match {
            background-color: #d4edda !important;
        }
        .diff {
            background-color: #fff3cd !important;
        }
        .missing {
            background-color: #f8d7da !important;
        }
        .operator {
            font-family: 'Courier New', monospace;
            word-wrap: break-word;
            word-break: break-word;
        }
        .indent {
            color: #cccccc;
        }
        .output {
            text-align: right;
            font-weight: bold;
            width: 1%;
            white-space: nowrap;
        }
        h1 {
            color: #333;
        }
        .collapse-btn {
            cursor: pointer;
            user-select: none;
            font-size: 10px;
            padding: 2px 5px;
            border: 1px solid #ddd;
            background: #f0f0f0;
            border-radius: 3px;
            display: inline-block;
            min-width: 20px;
            text-align: center;
        }
        .collapse-btn:hover {
            background: #e0e0e0;
        }
        .btn-col {
            width: 30px;
            text-align: center;
        }
        .keystage {
            color: #ff0000;
            font-weight: bold;
        }
        tr.collapsed {
            display: none;
        }
    </style>
    <script>
        function toggleChildren(btn) {
            const table = btn.closest('table');
            const rows = Array.from(table.querySelectorAll('tr'));
            const currentRow = btn.closest('tr');
            const currentRowIndex = rows.findIndex(r => r === currentRow);
            const currentLevel = parseInt(currentRow.getAttribute('data-level'));
            const currentStageId = currentRow.getAttribute('data-stageid');
            const isCurrentKeystage = currentRow.getAttribute('data-is-keystage') === 'true';
            
            let isCollapsed = btn.textContent === '+';
            
            // Check if this is an unimportant operator (not a keystage)
            if (!isCurrentKeystage && !isCollapsed) {
                // Check if there are important operators in descendant stages
                let hasImportantInDifferentStage = false;
                let firstDifferentStageIndex = -1;
                
                for (let i = currentRowIndex + 1; i < rows.length; i++) {
                    const row = rows[i];
                    const levelAttr = row.getAttribute('data-level');
                    if (levelAttr === null) continue;
                    
                    const rowLevel = parseInt(levelAttr);
                    if (rowLevel <= currentLevel) break;
                    
                    const rowStageId = row.getAttribute('data-stageid');
                    
                    // Check if we've moved to a different stage
                    if (rowStageId !== currentStageId) {
                        if (firstDifferentStageIndex === -1) {
                            firstDifferentStageIndex = i;
                        }
                        
                        // Check if this row in different stage is a keystage
                        if (row.getAttribute('data-is-keystage') === 'true') {
                            hasImportantInDifferentStage = true;
                            break;
                        }
                    }
                }
                
                // Collapse logic for unimportant operators
                if (hasImportantInDifferentStage && firstDifferentStageIndex !== -1) {
                    // Collapse only operators in the same stage
                    for (let i = currentRowIndex + 1; i < rows.length; i++) {
                        const row = rows[i];
                        const levelAttr = row.getAttribute('data-level');
                        if (levelAttr === null) continue;
                        
                        const rowLevel = parseInt(levelAttr);
                        if (rowLevel <= currentLevel) break;
                        
                        const rowStageId = row.getAttribute('data-stageid');
                        
                        // Only collapse rows in the same stage
                        if (rowStageId === currentStageId) {
                            row.classList.add('collapsed');
                        } else {
                            break; // Stop when we reach a different stage
                        }
                    }
                } else {
                    // Collapse all child rows (including different stages)
                    for (let i = currentRowIndex + 1; i < rows.length; i++) {
                        const row = rows[i];
                        const levelAttr = row.getAttribute('data-level');
                        if (levelAttr === null) continue;
                        
                        const rowLevel = parseInt(levelAttr);
                        if (rowLevel <= currentLevel) break;
                        
                        row.classList.add('collapsed');
                    }
                }
                
                btn.textContent = '+';
            } else {
                // Standard expand/collapse for important operators or expanding
                for (let i = currentRowIndex + 1; i < rows.length; i++) {
                    const row = rows[i];
                    const levelAttr = row.getAttribute('data-level');
                    if (levelAttr === null) continue;
                    
                    const rowLevel = parseInt(levelAttr);
                    if (rowLevel <= currentLevel) break;
                    
                    if (isCollapsed) {
                        row.classList.remove('collapsed');
                        // Also reset any toggle buttons in child rows to expanded state
                        const nestedBtn = row.querySelector('.collapse-btn');
                        if (nestedBtn) nestedBtn.textContent = '-';
                    } else {
                        row.classList.add('collapsed');
                    }
                }
                
                btn.textContent = isCollapsed ? '-' : '+';
            }
        }
        
        function filterUnimportantOperators(checkbox) {
            const table = document.querySelector('table');
            if (!table) return;
            
            const rows = Array.from(table.querySelectorAll('tr[data-level]'));
            
            if (checkbox.checked) {
                // Hide unimportant operators using smart collapse logic
                // Process all rows and collapse unimportant operators with children
                rows.forEach((row, index) => {
                    const toggleBtn = row.querySelector('.collapse-btn');
                    const isKeystage = row.getAttribute('data-is-keystage') === 'true';
                    
                    // Only process unimportant operators (non-keystage) that have toggle buttons
                    if (!isKeystage && toggleBtn && toggleBtn.textContent === '-') {
                        const level = parseInt(row.getAttribute('data-level'));
                        const currentStageId = row.getAttribute('data-stageid');
                        
                        // Check if there are keystages in different stages among descendants
                        let hasKeystageInDifferentStage = false;
                        
                        for (let i = index + 1; i < rows.length; i++) {
                            const descendantRow = rows[i];
                            const descendantLevel = parseInt(descendantRow.getAttribute('data-level'));
                            
                            if (descendantLevel <= level) break;
                            
                            const descendantStageId = descendantRow.getAttribute('data-stageid');
                            
                            // Check if we're in a different stage and it's a keystage
                            if (descendantStageId !== currentStageId &&
                                descendantRow.getAttribute('data-is-keystage') === 'true') {
                                hasKeystageInDifferentStage = true;
                                break;
                            }
                        }
                        
                        // Always trigger collapse for unimportant operators
                        // The toggleChildren function will handle the smart logic
                        toggleBtn.click();
                    }
                });
            } else {
                // Show all operators - expand all collapsed rows
                rows.forEach(row => {
                    const toggleBtn = row.querySelector('.collapse-btn');
                    if (toggleBtn && toggleBtn.textContent === '+') {
                        toggleBtn.click();
                    }
                });
            }
        }
    </script>
</head>
<body>
    <div class="header-section">
        <h1 style="display: inline-block; margin-right: 20px;">Query Plan Comparison (""" + query_id + """)</h1>
        <label>
            <input type="checkbox" id="filter-checkbox" onchange="filterUnimportantOperators(this)">
            Hide unimportant operators
        </label>
    </div>
    <p>Comparing from last row (bottom) to first row (top)</p>
    <table>
        <tr>
            <th class="btn-col"></th>
            <th>Stage</th>
            <th>Time</th>
            <th>Parts</th>
            <th>Plan1 Operator</th>
            <th>Rows</th>
            <th>Stage</th>
            <th>Time</th>
            <th>Parts</th>
            <th>Plan2 Operator</th>
            <th>Rows</th>
        </tr>
"""
    
    i = 0
    j = 0
    rows = []
    row_index = 0
    
    while i < len(plan1_reversed) or j < len(plan2_reversed):
        if i >= len(plan1_reversed):
            # Only plan2 has remaining operators
            op2 = plan2_reversed[j]
            level = op2.get('level', 0)
            is_keystage = op2.get('is_keystage', False)
            stageid = op2.get('stageid', '')
            stagetime = op2.get('stagetime', '')
            stageparts = op2.get('stageparts', '')
            op_class = 'keystage' if is_keystage else ''
            rows.append(f"""        <tr class="missing" data-row="{row_index}" data-level="{level}" data-is-keystage="{str(is_keystage).lower()}" data-stageid="{stageid}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="operator {op_class}">{stageid}</td>
            <td class="output {op_class}">{stagetime}</td>
            <td class="output {op_class}">{stageparts}</td>
            <td class="operator {op_class}">{op2['display']}</td>
            <td class="output {op_class}">{escape(op2['output'])}</td>
        </tr>
""")
            j += 1
            row_index += 1
        elif j >= len(plan2_reversed):
            # Only plan1 has remaining operators
            op1 = plan1_reversed[i]
            level = op1.get('level', 0)
            is_keystage = op1.get('is_keystage', False)
            stageid = op1.get('stageid', '')
            stagetime = op1.get('stagetime', '')
            stageparts = op1.get('stageparts', '')
            op_class = 'keystage' if is_keystage else ''
            rows.append(f"""        <tr class="missing" data-row="{row_index}" data-level="{level}" data-is-keystage="{str(is_keystage).lower()}" data-stageid="{stageid}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class}">{stageid}</td>
            <td class="output {op_class}">{stagetime}</td>
            <td class="output {op_class}">{stageparts}</td>
            <td class="operator {op_class}">{op1['display']}</td>
            <td class="output {op_class}">{escape(op1['output'])}</td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
        </tr>
""")
            i += 1
            row_index += 1
        else:
            op1 = plan1_reversed[i]
            op2 = plan2_reversed[j]
            
            # Normalize operator names by removing leading digits/spaces for comparison
            # This handles cases where indent numbers differ (e.g., "01234567890123456789 table scan" vs "0123456789012345678901 table scan")
            def normalize_operator_name(name):
                """Remove leading digits and spaces from operator name for comparison."""
                return re.sub(r'^[\d\s]+', '', name).strip()
            
            op1_normalized = normalize_operator_name(op1['name'])
            op2_normalized = normalize_operator_name(op2['name'])
            
            # Look ahead up to 3 positions to find a match
            match_found = False
            skip_plan1 = 0
            skip_plan2 = 0
            
            if op1_normalized == op2_normalized:
                match_found = True
            else:
                # Look ahead in plan2 (up to 3 positions)
                for k in range(1, min(4, len(plan2_reversed) - j)):
                    op2_ahead = plan2_reversed[j + k]
                    op2_ahead_normalized = normalize_operator_name(op2_ahead['name'])
                    if op1_normalized == op2_ahead_normalized:
                        match_found = True
                        skip_plan2 = k
                        break
                
                # If not found in plan2, look ahead in plan1 (up to 3 positions)
                if not match_found:
                    for k in range(1, min(4, len(plan1_reversed) - i)):
                        op1_ahead = plan1_reversed[i + k]
                        op1_ahead_normalized = normalize_operator_name(op1_ahead['name'])
                        if op1_ahead_normalized == op2_normalized:
                            match_found = True
                            skip_plan1 = k
                            break
            
            # Output skipped operators from plan2 before the match
            for k in range(skip_plan2):
                op2_skip = plan2_reversed[j + k]
                level = op2_skip.get('level', 0)
                is_keystage = op2_skip.get('is_keystage', False)
                stageid = op2_skip.get('stageid', '')
                stagetime = op2_skip.get('stagetime', '')
                stageparts = op2_skip.get('stageparts', '')
                op_class = 'keystage' if is_keystage else ''
                rows.append(f"""        <tr class="missing" data-row="{row_index}" data-level="{level}" data-is-keystage="{str(is_keystage).lower()}" data-stageid="{stageid}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="operator {op_class}">{stageid}</td>
            <td class="output {op_class}">{stagetime}</td>
            <td class="output {op_class}">{stageparts}</td>
            <td class="operator {op_class}">{op2_skip['display']}</td>
            <td class="output {op_class}">{escape(op2_skip['output'])}</td>
        </tr>
""")
                row_index += 1
            j += skip_plan2
            
            # Output skipped operators from plan1 before the match
            for k in range(skip_plan1):
                op1_skip = plan1_reversed[i + k]
                level = op1_skip.get('level', 0)
                is_keystage = op1_skip.get('is_keystage', False)
                stageid = op1_skip.get('stageid', '')
                stagetime = op1_skip.get('stagetime', '')
                stageparts = op1_skip.get('stageparts', '')
                op_class = 'keystage' if is_keystage else ''
                rows.append(f"""        <tr class="missing" data-row="{row_index}" data-level="{level}" data-is-keystage="{str(is_keystage).lower()}" data-stageid="{stageid}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class}">{stageid}</td>
            <td class="output {op_class}">{stagetime}</td>
            <td class="output {op_class}">{stageparts}</td>
            <td class="operator {op_class}">{op1_skip['display']}</td>
            <td class="output {op_class}">{escape(op1_skip['output'])}</td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="output {op_class}">-</td>
            <td class="operator {op_class}">-</td>
            <td class="output {op_class}">-</td>
        </tr>
""")
                row_index += 1
            i += skip_plan1
            
            # Refresh op1 and op2 after skipping
            if i < len(plan1_reversed) and j < len(plan2_reversed):
                op1 = plan1_reversed[i]
                op2 = plan2_reversed[j]
                op1_normalized = normalize_operator_name(op1['name'])
                op2_normalized = normalize_operator_name(op2['name'])
            
            # Check if operators match (after normalization and look-ahead)
            if match_found and op1_normalized == op2_normalized:
                # Same operator - use the larger indentation level for display
                level1 = op1.get('level', 0)
                level2 = op2.get('level', 0)
                level = max(level1, level2)
                
                # Get keystage info - use OR logic (if either is keystage, mark as keystage)
                is_keystage1 = op1.get('is_keystage', False)
                is_keystage2 = op2.get('is_keystage', False)
                is_keystage = is_keystage1 or is_keystage2
                stageid1 = op1.get('stageid', '')
                stageid2 = op2.get('stageid', '')
                stageid = stageid1 if stageid1 else stageid2
                stagetime1 = op1.get('stagetime', '')
                stagetime2 = op2.get('stagetime', '')
                stageparts1 = op1.get('stageparts', '')
                stageparts2 = op2.get('stageparts', '')
                op_class = 'keystage' if is_keystage else ''
                
                # Use the display with the larger indentation for both columns
                if level1 >= level2:
                    display1 = op1['display']
                    display2 = op1['display']  # Use op1's indentation for both
                else:
                    display1 = op2['display']  # Use op2's indentation for both
                    display2 = op2['display']
                
                if op1['output'] == op2['output']:
                    row_class = "match"
                else:
                    row_class = "diff"
                
                rows.append(f"""        <tr class="{row_class}" data-row="{row_index}" data-level="{level}" data-is-keystage="{str(is_keystage).lower()}" data-stageid="{stageid}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class}">{stageid1}</td>
            <td class="output {op_class}">{stagetime1}</td>
            <td class="output {op_class}">{stageparts1}</td>
            <td class="operator {op_class}">{display1}</td>
            <td class="output {op_class}">{escape(op1['output'])}</td>
            <td class="operator {op_class}">{stageid2}</td>
            <td class="output {op_class}">{stagetime2}</td>
            <td class="output {op_class}">{stageparts2}</td>
            <td class="operator {op_class}">{display2}</td>
            <td class="output {op_class}">{escape(op2['output'])}</td>
        </tr>
""")
                i += 1
                j += 1
                row_index += 1
            else:
                # Different operators - put them in separate rows
                level1 = op1.get('level', 0)
                is_keystage1 = op1.get('is_keystage', False)
                stageid1 = op1.get('stageid', '')
                stagetime1 = op1.get('stagetime', '')
                stageparts1 = op1.get('stageparts', '')
                op_class1 = 'keystage' if is_keystage1 else ''
                rows.append(f"""        <tr class="missing" data-row="{row_index}" data-level="{level1}" data-is-keystage="{str(is_keystage1).lower()}" data-stageid="{stageid1}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class1}">{stageid1}</td>
            <td class="output {op_class1}">{stagetime1}</td>
            <td class="output {op_class1}">{stageparts1}</td>
            <td class="operator {op_class1}">{op1['display']}</td>
            <td class="output {op_class1}">{escape(op1['output'])}</td>
            <td class="operator {op_class1}">-</td>
            <td class="output {op_class1}">-</td>
            <td class="output {op_class1}">-</td>
            <td class="operator {op_class1}">-</td>
            <td class="output {op_class1}">-</td>
        </tr>
""")
                row_index += 1
                level2 = op2.get('level', 0)
                is_keystage2 = op2.get('is_keystage', False)
                stageid2 = op2.get('stageid', '')
                stagetime2 = op2.get('stagetime', '')
                stageparts2 = op2.get('stageparts', '')
                op_class2 = 'keystage' if is_keystage2 else ''
                rows.append(f"""        <tr class="missing" data-row="{row_index}" data-level="{level2}" data-is-keystage="{str(is_keystage2).lower()}" data-stageid="{stageid2}">
            <td class="btn-col"><span class="collapse-btn" onclick="toggleChildren(this)">-</span></td>
            <td class="operator {op_class2}">-</td>
            <td class="output {op_class2}">-</td>
            <td class="output {op_class2}">-</td>
            <td class="operator {op_class2}">-</td>
            <td class="output {op_class2}">-</td>
            <td class="operator {op_class2}">{stageid2}</td>
            <td class="output {op_class2}">{stagetime2}</td>
            <td class="output {op_class2}">{stageparts2}</td>
            <td class="operator {op_class2}">{op2['display']}</td>
            <td class="output {op_class2}">{escape(op2['output'])}</td>
        </tr>
""")
                i += 1
                j += 1
                row_index += 1
    
    # Reverse rows to maintain original order (last row to first row in input)
    rows.reverse()
    html += ''.join(rows)
    
    html += """    </table>
    <div style="margin-top: 20px;">
        <h3>Legend:</h3>
        <p><span style="background-color: #d4edda; padding: 5px;">Green</span> - Operators match with same output rows</p>
        <p><span style="background-color: #fff3cd; padding: 5px;">Yellow</span> - Operators match but different output rows</p>
        <p><span style="background-color: #f8d7da; padding: 5px;">Red</span> - Operator only in one plan</p>
    </div>
</body>
</html>
"""
    
    return html


def compare_query_plans(appid1, appid2, output_filename, query_id="q25", spark=None, appals1=None, appals2=None):
    """
    Compare query plans from two Spark applications and generate HTML comparison.
    
    Parameters:
    -----------
    appid1 : str
        First application ID (e.g., "app-20260223232457-0000")
    appid2 : str
        Second application ID (e.g., "app-20260304012015-0000")
    output_filename : str
        Output HTML filename
    query_id : str, optional
        Query ID to compare (default: "q25")
    spark : SparkSession, optional
        Spark session (if None, will try to get from environment)
    appals1 : App_Log_Analysis, optional
        Pre-loaded App_Log_Analysis object for first app (avoids reloading data)
    appals2 : App_Log_Analysis, optional
        Pre-loaded App_Log_Analysis object for second app (avoids reloading data)
        
    Returns:
    --------
    tuple
        (plan1_operator_count, plan2_operator_count)
    """
    import os
    
    # Load data from both applications only if not provided
    if appals1 is None:
        print(f"Loading data from {appid1}...")
        appals1 = App_Log_Analysis(None, None, spark=spark)
        appals1.load_data_from_database(appid1)
    else:
        print(f"Reusing loaded data for {appid1}...")
    
    if appals2 is None:
        print(f"Loading data from {appid2}...")
        appals2 = App_Log_Analysis(None, None, spark=spark)
        appals2.load_data_from_database(appid2)
    else:
        print(f"Reusing loaded data for {appid2}...")
    
    # Get query plan data (dict format)
    print(f"Extracting query plan for {query_id}...")
    plan1_data = appals1.get_query_plan(queryid=query_id)
    plan2_data = appals2.get_query_plan(queryid=query_id)
    
    # Generate HTML for individual plans (for saving)
    plan1_html_full = appals1.show_query_plan(data=plan1_data, plot=False, show_plan_only=False)
    plan2_html_full = appals2.show_query_plan(data=plan2_data, plot=False, show_plan_only=False)

    plan1_puml_full = appals1.print_query_plan_puml(query_plan=plan1_data)
    plan2_puml_full = appals2.print_query_plan_puml(query_plan=plan2_data)
    
    # Save individual query plans
    output_dir = os.path.dirname(output_filename)
    if output_dir:
        # Extract short app IDs from full app IDs (e.g., "0946" from "app-20260307210946-0001")
        appid1_parts = appid1.split('-')
        appid2_parts = appid2.split('-')
        appid1_short = appid1_parts[1][-4:] if len(appid1_parts) > 1 else appid1
        appid2_short = appid2_parts[1][-4:] if len(appid2_parts) > 1 else appid2
        
        # Create subdirectories for organized file storage
        full_plan_dir = os.path.join(output_dir, "../full_plan")
        puml_dir = os.path.join(output_dir, "../puml")
        os.makedirs(full_plan_dir, exist_ok=True)
        os.makedirs(puml_dir, exist_ok=True)
        
        plan1_file = os.path.join(full_plan_dir, f"{appid1_short}-{query_id}.html")
        plan2_file = os.path.join(full_plan_dir, f"{appid2_short}-{query_id}.html")

        print(f"Saving individual query plans to full_plan/...")
        with open(plan1_file, 'w') as f:
            f.write(plan1_html_full)
        print(f"  Plan1 saved to {plan1_file}")
        
        with open(plan2_file, 'w') as f:
            f.write(plan2_html_full)
        print(f"  Plan2 saved to {plan2_file}")
    
        plan1_puml = os.path.join(puml_dir, f"{appid1_short}-{query_id}.puml")
        plan2_puml = os.path.join(puml_dir, f"{appid2_short}-{query_id}.puml")

        print(f"Saving PUML files to puml/...")
        with open(plan1_puml, 'w') as f:
            f.write(plan1_puml_full)
        print(f"  Plan1 saved to {plan1_puml}")
        
        with open(plan2_puml, 'w') as f:
            f.write(plan2_puml_full)
        print(f"  Plan2 saved to {plan2_puml}")

    


    # Extract operators from data
    plan1_ops = extract_operators_from_data(plan1_data)
    plan2_ops = extract_operators_from_data(plan2_data)
    
    print(f"Plan1 has {len(plan1_ops)} operators")
    print(f"Plan2 has {len(plan2_ops)} operators")
    
    # Generate HTML comparison
    html = generate_comparison_html(plan1_ops, plan2_ops, query_id)
    
    # Write to file
    with open(output_filename, 'w') as f:
        f.write(html)
    
    print(f"\nComparison saved to {output_filename}")
    
    return len(plan1_ops), len(plan2_ops)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python compare_query_plans_html.py <appid1> <appid2> <output_filename> [query_id]")
        print("Example: python compare_query_plans_html.py app-20260223232457-0000 app-20260304012015-0000 comparison.html q25")
        print("\nNote: This script requires an active Spark environment with Java installed.")
        print("It is recommended to run this from a Jupyter notebook where Spark is already configured.")
        sys.exit(1)
    
    appid1 = sys.argv[1]
    appid2 = sys.argv[2]
    output_filename = sys.argv[3]
    query_id = sys.argv[4] if len(sys.argv) > 4 else "q25"
    
    # Try to initialize Spark session
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://127.0.0.1:15002").getOrCreate()
        
        try:
            compare_query_plans(appid1, appid2, output_filename, query_id, spark=spark)
        finally:
            spark.stop()
    except Exception as e:
        print(f"\nError: Failed to initialize Spark session: {e}")
        print("\nThis script requires:")
        print("1. Java Runtime Environment (JRE) installed")
        print("2. SPARK_HOME environment variable set")
        print("3. Active Spark environment")
        print("\nRecommended: Run this from a Jupyter notebook with:")
        print(f"  from compare_query_plans_html import compare_query_plans")
        print(f"  compare_query_plans('{appid1}', '{appid2}', '{output_filename}', '{query_id}', spark=spark)")
        sys.exit(1)

# Made with Bob
