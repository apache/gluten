#!/usr/bin/env python3
"""
Generate index.html listing all subfolders in the jupyter directory
grouped by app ID, with dashboard links.
"""

import re
import sys
import os
from collections import defaultdict
from pathlib import Path

# Configuration
JUPYTER_DIR = Path("/mnt/data1/mcp/output")
OUTPUT_FILE = JUPYTER_DIR / "index.html"
DASHBOARD_URL = "https://grafana.ibm.prestodb.dev/d/da275934-f05a-4e5e-a732-5dd7cbbad4f9/tpcds-comparison"


def get_subdirectories():
    """Get all subdirectories with app IDs and generated time."""
    subdirs = []
    ignored_dirs = {'__pycache__', '.venv'}

    for item in JUPYTER_DIR.iterdir():
        if item.is_dir() and item.name != '.' and item.name not in ignored_dirs:
            app_id1, app_id2, generated_time, start_time1, start_time2 = extract_app_ids(item)
            if not app_id1:
                continue  # skip folders without comparison_app-* file
            subdirs.append({
                'name': item.name,
                'path': item,
                'generated_time': generated_time or '',
                'app_id1': app_id1,
                'app_id2': app_id2 or '',
                'start_time1': start_time1 or '',
                'start_time2': start_time2 or '',
            })

    return subdirs


def extract_app_ids(folder_path):
    """Extract app IDs, Generated time, and Start Times from comparison_app-*.md files in the folder."""
    comparison_files = list(folder_path.glob("comparison_app-*.md"))

    if not comparison_files:
        return None, None, None, None, None

    # Use the first comparison file found
    filename = comparison_files[0].name

    # Pattern: comparison_app-APPID1_vs_app-APPID2.md
    pattern = r'comparison_app-([^_]+)_vs_app-([^.]+)\.md'
    match = re.match(pattern, filename)

    if not match:
        return None, None, None, None, None

    app_id1 = f"app-{match.group(1)}"
    app_id2 = f"app-{match.group(2)}"

    # Extract Generated timestamp and Start Times from file content
    generated_time = None
    start_time1 = None
    start_time2 = None
    content = comparison_files[0].read_text(encoding='utf-8', errors='ignore')
    gen_match = re.search(r'\*\*Generated\*\*:\s*(.+)', content)
    if gen_match:
        generated_time = gen_match.group(1).strip()
    # Row: | **Start Time** | <time1> | <time2> | ... |
    st_match = re.search(r'\*\*Start Time\*\*\s*\|\s*([^|]+)\|\s*([^|]+)\|', content)
    if st_match:
        start_time1 = st_match.group(1).strip()
        start_time2 = st_match.group(2).strip()

    return app_id1, app_id2, generated_time, start_time1, start_time2


def generate_dashboard_link(app_id1, app_id2):
    """Generate Grafana dashboard comparison link."""
    if not app_id1 or not app_id2:
        return ""

    params = f"orgId=1&var-query3={app_id2}&var-query1=tpcds%20sf2500%20vanilla&var-query2={app_id1}"
    return f"{DASHBOARD_URL}?{params}"


def group_by_appid(subdirs):
    """
    Group folders by app ID. Each folder appears under every app ID it mentions
    (either as app_id1 or app_id2). Groups are sorted by the app ID's Start Time
    descending; within each group folders are sorted by generated_time descending.
    """
    groups = defaultdict(list)
    for subdir in subdirs:
        groups[subdir['app_id1']].append(subdir)
        if subdir['app_id2'] and subdir['app_id2'] != subdir['app_id1']:
            groups[subdir['app_id2']].append(subdir)

    # Sort each group by generated_time descending
    for app_id in groups:
        groups[app_id].sort(key=lambda x: x['generated_time'], reverse=True)

    def group_start_time(item):
        """Return the most recent Start Time for this app_id across all its folders."""
        app_id, group_subdirs = item
        times = []
        for s in group_subdirs:
            if s['app_id1'] == app_id and s['start_time1']:
                times.append(s['start_time1'])
            elif s['app_id2'] == app_id and s['start_time2']:
                times.append(s['start_time2'])
        return max(times) if times else ''

    # Return groups sorted by the app ID's Start Time descending
    return sorted(groups.items(), key=group_start_time, reverse=True)


def generate_html(subdirs):
    """Generate the HTML content grouped by app ID."""
    html_header = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Jupyter Subfolders</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
            text-align: center;
        }
        table {
            width: 100%;
            max-width: 1400px;
            margin: 20px auto;
            border-collapse: collapse;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        th {
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #ddd;
        }
        tr:hover {
            background-color: #f0f0f0;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .group-header td {
            background-color: #BBDEFB;
            color: #0D47A1;
            font-family: 'Courier New', monospace;
            font-size: 1em;
            font-weight: bold;
            padding: 8px 12px;
        }
        .group-header:hover td {
            background-color: #BBDEFB;
        }
        .appid-comment {
            font-weight: normal;
            font-style: italic;
            color: #1565C0;
            margin-left: 6px;
        }
        .folder-name {
            font-weight: 500;
        }
        .folder-name a {
            color: #2196F3;
            text-decoration: none;
        }
        .folder-name a:hover {
            text-decoration: underline;
        }
        .timestamp {
            color: #666;
            font-family: 'Courier New', monospace;
        }
        .index {
            text-align: center;
            color: #999;
        }
        .appid {
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            color: #555;
        }
        .dashboard-link {
            text-align: center;
        }
        .dashboard-link a {
            color: #FF5722;
            text-decoration: none;
            font-weight: 500;
        }
        .dashboard-link a:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <h1>Jupyter Folder - Subfolders List</h1>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Folder Name</th>
                <th>Generated</th>
                <th>App ID 1</th>
                <th>App ID 2</th>
                <th>Dashboard</th>
            </tr>
        </thead>
        <tbody>
"""

    html_footer = """        </tbody>
    </table>
</body>
</html>
"""

    groups = group_by_appid(subdirs)

    # Collect all unique app IDs and fetch their comments in one query
    all_app_ids = [app_id for app_id, _ in groups]
    comments_map = {}

    rows = []
    global_idx = 1

    for app_id, group_subdirs in groups:
        comment = comments_map.get(app_id, '')
        comment_html = f' <span class="appid-comment">— {comment}</span>' if comment else ''
        # Group header row — spans all 6 columns
        rows.append(f"""            <tr class="group-header">
                <td colspan="6">{app_id}{comment_html}</td>
            </tr>
""")

        for subdir in group_subdirs:
            name = subdir['name']
            generated_time = subdir['generated_time']
            app_id1 = subdir['app_id1']
            app_id2 = subdir['app_id2']
            dashboard_link = generate_dashboard_link(app_id1, app_id2)

            row = f"""            <tr>
                <td class="index">{global_idx}</td>
                <td class="folder-name"><a href="{name}/">{name}</a></td>
                <td class="timestamp">{generated_time}</td>
                <td class="appid">{app_id1}</td>
                <td class="appid">{app_id2}</td>
                <td class="dashboard-link">"""

            if dashboard_link:
                row += f'<a href="{dashboard_link}" target="_blank">View</a>'

            row += """</td>
            </tr>
"""
            rows.append(row)
            global_idx += 1

    return html_header + ''.join(rows) + html_footer


def generate_html_main():
    """Main function to generate index.html."""
    print(f"Scanning directory: {JUPYTER_DIR}")

    subdirs = get_subdirectories()
    print(f"Found {len(subdirs)} subdirectories")

    html_content = generate_html(subdirs)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"Generated: {OUTPUT_FILE}")
    print(f"Total folders: {len(subdirs)}")

if __name__ == "__main__":
    generate_html_main()
# Made with Bob
