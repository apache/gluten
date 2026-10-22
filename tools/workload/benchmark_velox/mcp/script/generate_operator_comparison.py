#!/usr/bin/env python3
"""
Generate operator comparison markdown file from two Spark application IDs.

Usage:
    python generate_operator_comparison.py <appid1> <appid2>

Example:
    python generate_operator_comparison.py app-20260223232457-0000 app-20260304012015-0000
    
This will:
1. Create folder "2457-2015" (last 4 digits before "-" from each appid)
2. Generate operator_comparison.md in that folder
3. For queries with different operators, generate q<id>.html comparison files
"""

import os
import re
import pandas as pd

def extract_folder_name(appid1, appid2):
    """
    Extract last 4 digits before the last "-" from both appids and create folder name.
    
    Example:
        app-20260223232457-0000 -> 2457
        app-20260304012015-0000 -> 2015
        Result: "2457-2015"
    """
    # Pattern to extract last 4 digits before the final "-"
    pattern = r'(\d{4})-\d+$'
    
    match1 = re.search(pattern, appid1)
    match2 = re.search(pattern, appid2)
    
    if not match1 or not match2:
        raise ValueError(f"Could not extract 4 digits from appids: {appid1}, {appid2}")
    
    digits1 = match1.group(1)
    digits2 = match2.group(1)
    
    return f"{digits1}-{digits2}"


def normalize_operator_name(operator_name):
    """
    Normalize operator names by combining scan operators that access the same table.
    
    Examples:
    - FileSourceScanExecTransformer parquet spark_catalog.db.table -> Scan: table
    - IcebergScanTransformer spark_catalog.db.table -> Scan: table
    - Other operators remain unchanged
    """
    # Check if it's a scan operator
    if 'Scan' in operator_name and ('FileSourceScanExecTransformer' in operator_name or 'IcebergScanTransformer' in operator_name):
        # Extract table name from the catalog path
        # Supports: spark_catalog.db.table, awss3_data_iceberg.db.table, etc.
        
        # Try pattern: any_catalog.database.table_name
        match = re.search(r'\b[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.([a-zA-Z0-9_]+)', operator_name)
        if match:
            table_name = match.group(1)
            return f'Scan: {table_name}'
    
    return operator_name


def compare_operator_counts(query_id, hive_df, iceberg_df, perf_df=None):
    """
    Compare operator counts between Hive, Iceberg, and optionally Perf for a specific query.
    Normalizes scan operators to combine those accessing the same table.
    
    Parameters:
    -----------
    query_id : str
        The query ID to compare
    hive_df : pd.DataFrame
        DataFrame with operators as index and query IDs as columns (from hivecnt)
    iceberg_df : pd.DataFrame
        DataFrame with operators as index and query IDs as columns (from icebergcnt)
    perf_df : pd.DataFrame, optional
        DataFrame with operators as index and query IDs as columns (from perfcnt)
    
    Returns:
    --------
    pd.DataFrame
        Comparison table with operators as index and 'hive', 'iceberg', and optionally 'perf' columns
    """
    # Get the column for the specified query_id from all dataframes
    hive_col = hive_df[query_id] if query_id in hive_df.columns else pd.Series(dtype=float)
    iceberg_col = iceberg_df[query_id] if query_id in iceberg_df.columns else pd.Series(dtype=float)
    
    # Create comparison dataframe
    comparison_dict = {
        'hive': hive_col,
        'iceberg': iceberg_col
    }
    
    # Add perf column if provided
    if perf_df is not None:
        perf_col = perf_df[query_id] if query_id in perf_df.columns else pd.Series(dtype=float)
        comparison_dict['perf'] = perf_col
    
    comparison = pd.DataFrame(comparison_dict)
    
    # Normalize operator names in the index
    comparison.index = comparison.index.map(normalize_operator_name)
    
    # Group by normalized operator name and sum counts
    comparison = comparison.groupby(comparison.index).sum()
    
    # Fill NaN with empty string for better display
    comparison = comparison.fillna('')
    
    # Convert to int where values exist, keep empty string otherwise
    columns = ['hive', 'iceberg'] + (['perf'] if perf_df is not None else [])
    for col in columns:
        comparison[col] = comparison[col].apply(lambda x: int(x) if x != '' and x != 0 else ('' if x == 0 else int(x)))
    
    # Filter out rows where all columns are empty or zero
    mask = False
    for col in columns:
        mask = mask | (comparison[col] != '')
    comparison = comparison[mask]
    
    # Filter out ignored operators
    ignored_operators = ['DeserializeToObject']
    comparison = comparison[~comparison.index.isin(ignored_operators)]
    
    return comparison


def find_queries_with_differences(hive_df, iceberg_df, perf_df=None):
    """
    Compare all queries and return only those with different operator counts.
    
    Parameters:
    -----------
    hive_df : pd.DataFrame
        DataFrame with operators as index and query IDs as columns
    iceberg_df : pd.DataFrame
        DataFrame with operators as index and query IDs as columns
    perf_df : pd.DataFrame, optional
        DataFrame with operators as index and query IDs as columns
    
    Returns:
    --------
    dict
        Dictionary with query_id as key and comparison DataFrame as value,
        only for queries with differences
    """
    # Get all unique query IDs
    all_query_ids = set(hive_df.columns) | set(iceberg_df.columns)
    if perf_df is not None:
        all_query_ids = all_query_ids | set(perf_df.columns)
    
    queries_with_diff = {}
    
    for query_id in sorted(all_query_ids):
        # Get comparison for this query
        comparison = compare_operator_counts(query_id, hive_df, iceberg_df, perf_df)
        
        # Check if there are differences
        has_diff = False
        
        if perf_df is not None:
            # Three-way comparison: check if any two columns differ
            for idx in comparison.index:
                hive_val = comparison.loc[idx, 'hive']
                iceberg_val = comparison.loc[idx, 'iceberg']
                perf_val = comparison.loc[idx, 'perf']
                
                # Convert empty strings to None for comparison
                hive_val = None if hive_val == '' else hive_val
                iceberg_val = None if iceberg_val == '' else iceberg_val
                perf_val = None if perf_val == '' else perf_val
                
                if not (hive_val == iceberg_val == perf_val):
                    has_diff = True
                    break
        else:
            # Two-way comparison: check if hive and iceberg differ
            for idx in comparison.index:
                hive_val = comparison.loc[idx, 'hive']
                iceberg_val = comparison.loc[idx, 'iceberg']
                
                # Convert empty strings to None for comparison
                hive_val = None if hive_val == '' else hive_val
                iceberg_val = None if iceberg_val == '' else iceberg_val
                
                if hive_val != iceberg_val:
                    has_diff = True
                    break
        
        if has_diff:
            queries_with_diff[query_id] = comparison
    
    return queries_with_diff


def sort_queries_by_runtime_gap(hive_df, iceberg_df, hive_runtime, iceberg_runtime, perf_df=None, perf_runtime=None):
    """
    Sort queries with different operator counts by their runtime gap.
    
    Parameters:
    -----------
    hive_df : pd.DataFrame
        DataFrame with operators as index and query IDs as columns
    iceberg_df : pd.DataFrame
        DataFrame with operators as index and query IDs as columns
    hive_runtime : pd.DataFrame or dict
        Runtime data for hive queries (query_id -> runtime)
    iceberg_runtime : pd.DataFrame or dict
        Runtime data for iceberg queries (query_id -> runtime)
    perf_df : pd.DataFrame, optional
        DataFrame with operators as index and query IDs as columns
    perf_runtime : pd.DataFrame or dict, optional
        Runtime data for perf queries (query_id -> runtime)
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: query_id, hive_runtime, iceberg_runtime, runtime_gap, runtime_gap_pct
        Sorted by absolute runtime gap (descending)
    """
    # Get queries with differences
    diff_queries = find_queries_with_differences(hive_df, iceberg_df, perf_df)
    
    if not diff_queries:
        print("No queries with operator differences found.")
        return pd.DataFrame()
    
    # Convert runtime data to dict if it's a DataFrame
    if isinstance(hive_runtime, pd.DataFrame):
        hive_runtime_dict = hive_runtime.to_dict()
        # If it's a multi-column DataFrame, try to get the runtime column
        if 'runtime' in hive_runtime_dict:
            hive_runtime_dict = hive_runtime_dict['runtime']
        elif len(hive_runtime_dict) == 1:
            hive_runtime_dict = list(hive_runtime_dict.values())[0]
    else:
        hive_runtime_dict = hive_runtime
    
    if isinstance(iceberg_runtime, pd.DataFrame):
        iceberg_runtime_dict = iceberg_runtime.to_dict()
        if 'runtime' in iceberg_runtime_dict:
            iceberg_runtime_dict = iceberg_runtime_dict['runtime']
        elif len(iceberg_runtime_dict) == 1:
            iceberg_runtime_dict = list(iceberg_runtime_dict.values())[0]
    else:
        iceberg_runtime_dict = iceberg_runtime
    
    # Build result list
    results = []
    for query_id in diff_queries.keys():
        hive_time = hive_runtime_dict.get(query_id, None)
        iceberg_time = iceberg_runtime_dict.get(query_id, None)
        
        if hive_time is not None and iceberg_time is not None:
            runtime_gap = iceberg_time - hive_time
            runtime_gap_pct = (runtime_gap / hive_time * 100) if hive_time != 0 else 0
            
            result_row = {
                'query_id': query_id,
                'hive_runtime': hive_time,
                'iceberg_runtime': iceberg_time,
                'runtime_gap': runtime_gap,
                'runtime_gap_pct': runtime_gap_pct
            }
            
            # Add perf runtime if available
            if perf_runtime is not None:
                if isinstance(perf_runtime, pd.DataFrame):
                    perf_runtime_dict = perf_runtime.to_dict()
                    if 'runtime' in perf_runtime_dict:
                        perf_runtime_dict = perf_runtime_dict['runtime']
                    elif len(perf_runtime_dict) == 1:
                        perf_runtime_dict = list(perf_runtime_dict.values())[0]
                else:
                    perf_runtime_dict = perf_runtime
                
                perf_time = perf_runtime_dict.get(query_id, None)
                if perf_time is not None:
                    result_row['perf_runtime'] = perf_time
            
            results.append(result_row)
    
    # Create DataFrame and sort by absolute runtime gap
    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values('runtime_gap', ascending=False)
    
    return result_df


def export_sorted_comparison_to_markdown(hive_df, iceberg_df, hive_runtime, iceberg_runtime,
                                         perf_df=None, perf_runtime=None, filename='sorted_comparison.md',
                                         folder_name=None, appid1=None, appid2=None, appname1=None, appname2=None,
                                         appals1=None, appals2=None, slow_threshold=50):
    """
    Export queries with operator differences sorted by runtime gap to markdown.
    
    Parameters:
    -----------
    folder_name : str, optional
        Folder name for generating HTML links (e.g., "2457-2015")
    appid1 : str, optional
        First application ID (e.g., "app-20260223232457-0000")
    appid2 : str, optional
        Second application ID (e.g., "app-20260304012015-0000")
    appname1 : str, optional
        First application name (e.g., "iceberg")
    appname2 : str, optional
        Second application name (e.g., "hive")
    appals1 : App_Log_Analysis_Enhanced, optional
        First application analysis object
    appals2 : App_Log_Analysis_Enhanced, optional
        Second application analysis object
    slow_threshold : float, optional
        Include queries where either app's elapsed time exceeds this value (seconds). Default 50.
    """
    # Extract short app IDs if provided
    appid1_short = None
    appid2_short = None
    if appid1:
        appid1_parts = appid1.split('-')
        appid1_short = appid1_parts[1][-4:] if len(appid1_parts) > 1 else appid1
    if appid2:
        appid2_parts = appid2.split('-')
        appid2_short = appid2_parts[1][-4:] if len(appid2_parts) > 1 else appid2
    
    # Use appnames if provided, otherwise use generic names
    app1_label = appname1 if appname1 else "App1"
    app2_label = appname2 if appname2 else "App2"
    # Get sorted queries with operator differences
    sorted_queries = sort_queries_by_runtime_gap(hive_df, iceberg_df, hive_runtime, iceberg_runtime,
                                                  perf_df, perf_runtime)
    
    if sorted_queries.empty:
        print("No queries with differences found.")
        return []
    
    # Get operator comparisons
    diff_queries = find_queries_with_differences(hive_df, iceberg_df, perf_df)
    
    # Convert runtime data to dict for all queries (not just those with operator differences)
    if isinstance(hive_runtime, pd.DataFrame):
        hive_runtime_dict = hive_runtime.to_dict()
        if 'runtime' in hive_runtime_dict:
            hive_runtime_dict = hive_runtime_dict['runtime']
        elif len(hive_runtime_dict) == 1:
            hive_runtime_dict = list(hive_runtime_dict.values())[0]
    else:
        hive_runtime_dict = hive_runtime
    
    if isinstance(iceberg_runtime, pd.DataFrame):
        iceberg_runtime_dict = iceberg_runtime.to_dict()
        if 'runtime' in iceberg_runtime_dict:
            iceberg_runtime_dict = iceberg_runtime_dict['runtime']
        elif len(iceberg_runtime_dict) == 1:
            iceberg_runtime_dict = list(iceberg_runtime_dict.values())[0]
    else:
        iceberg_runtime_dict = iceberg_runtime
    
    # Build list of ALL queries for runtime summary
    all_query_ids = set(hive_runtime_dict.keys()) | set(iceberg_runtime_dict.keys())
    runtime_summary_rows = []
    queries_for_plan_generation = []  # Track queries that need plan comparison/full plans
    
    for query_id in all_query_ids:
        hive_time = hive_runtime_dict.get(query_id, None)
        iceberg_time = iceberg_runtime_dict.get(query_id, None)
        
        if hive_time is not None and iceberg_time is not None:
            runtime_gap = iceberg_time - hive_time
            runtime_gap_pct = (runtime_gap / hive_time * 100) if hive_time != 0 else 0
            
            # Check if operator count changed for this query
            operator_changed = False
            if query_id in diff_queries:
                comparison = diff_queries[query_id]
                app1_total = sum([v for v in comparison['hive'] if v != ''])
                app2_total = sum([v for v in comparison['iceberg'] if v != ''])
                operator_changed = (app1_total != app2_total)
            
            # Include if runtime gap is significant, operator count changed, OR either app is slow
            is_slow = (hive_time >= slow_threshold or iceberg_time >= slow_threshold)
            if abs(runtime_gap) > 2 or operator_changed or is_slow:
                runtime_summary_rows.append({
                    'query_id': query_id,
                    'hive_runtime': hive_time,
                    'iceberg_runtime': iceberg_time,
                    'runtime_gap': runtime_gap,
                    'runtime_gap_pct': runtime_gap_pct,
                    'operator_changed': operator_changed
                })
                # Add to plan generation list
                queries_for_plan_generation.append(query_id)
    
    # Sort by runtime gap (descending)
    runtime_summary_df = pd.DataFrame(runtime_summary_rows)
    if not runtime_summary_df.empty:
        runtime_summary_df = runtime_summary_df.sort_values('runtime_gap', ascending=True)
    
    with open(filename, 'w') as f:
        # Write header
        f.write("# Grafana Comparisons\n\n")
        f.write(f"**[Link](https://grafana.ibm.prestodb.dev/d/da275934-f05a-4e5e-a732-5dd7cbbad4f9/tpcds-comparison?orgId=1&var-query3={appid2}&var-query1=tpcds%20sf2500%20vanilla&var-query2={appid1})**\n\n")
        
        # Add hottest stages comparison if appals objects are provided
        if appals1 is not None and appals2 is not None:
            f.write("# Hottest Stages Comparison\n\n")
            f.write("Stages with total_time > 10s:\n\n")
            
            try:
                # Get hottest stages from both apps
                hottest1 = appals1.get_hottest_stages(plot=False)
                hottest2 = appals2.get_hottest_stages(plot=False)
                
                # Filter stages with total_time > 10
                hottest1_filtered = hottest1[hottest1['total_time'] > 10].copy()
                hottest2_filtered = hottest2[hottest2['total_time'] > 10].copy()
                
                # Select required columns
                columns_to_show = ['Stage ID', 'real_queryid', 'total_time', 'partition#', 'acc_total', 'total']
                
                if not hottest1_filtered.empty or not hottest2_filtered.empty:
                    # Create side-by-side table
                    f.write(f"| {app1_label} | | | | | | | {app2_label} | | | | | | |\n")
                    f.write(f"| Stage ID | Query ID | Total Time | Partition# | Acc Total% | Total% | | Stage ID | Query ID | Total Time | Partition# | Acc Total% | Total% |\n")
                    f.write("|--:|--:|--:|--:|--:|--:|---|--:|--:|--:|--:|--:|--:|\n")
                    
                    # Get max rows between both dataframes
                    max_rows = max(len(hottest1_filtered), len(hottest2_filtered))
                    
                    for i in range(max_rows):
                        # Get row from app1
                        if i < len(hottest1_filtered):
                            row1 = hottest1_filtered.iloc[i]
                            stage_id1 = row1['Stage ID']
                            query_id1 = row1['real_queryid']
                            total_time1 = f"{row1['total_time']:.2f}s"
                            partition1 = row1['partition#']
                            acc_total1 = f"{row1['acc_total']*100:.1f}%"
                            total1 = f"{row1['total']*100:.1f}%"
                        else:
                            stage_id1 = query_id1 = total_time1 = partition1 = acc_total1 = total1 = "-"
                        
                        # Get row from app2
                        if i < len(hottest2_filtered):
                            row2 = hottest2_filtered.iloc[i]
                            stage_id2 = row2['Stage ID']
                            query_id2 = row2['real_queryid']
                            total_time2 = f"{row2['total_time']:.2f}s"
                            partition2 = row2['partition#']
                            acc_total2 = f"{row2['acc_total']*100:.1f}%"
                            total2 = f"{row2['total']*100:.1f}%"
                        else:
                            stage_id2 = query_id2 = total_time2 = partition2 = acc_total2 = total2 = "-"
                        
                        f.write(f"| {stage_id1} | {query_id1} | {total_time1} | {partition1} | {acc_total1} | {total1} | | {stage_id2} | {query_id2} | {total_time2} | {partition2} | {acc_total2} | {total2} |\n")
                    
                    f.write("\n")
                else:
                    f.write("No stages with total_time > 10s found.\n\n")
            except Exception as e:
                f.write(f"Error generating hottest stages comparison: {e}\n\n")
        
        f.write("# Operator Count Comparison - Sorted by Runtime Gap\n\n")
        f.write(f"Found **{len(sorted_queries)}** queries with operator differences.\n\n")
        
        # Write runtime summary table
        f.write("## Runtime Summary\n\n")
        f.write(f"Queries with runtime gap > 2s or < -2s, OR operator count changed, OR either app elapsed time > {slow_threshold}s:\n\n")
        
        if not runtime_summary_df.empty:
            f.write(f"| Query ID | {app1_label} Runtime | {app2_label} Runtime | Runtime Gap | Gap % | Operator # Changed | plan_comparison | {app1_label} plan | {app2_label} plan | plan_chart |\n")
            f.write("|---|---|---|---|---|---|---|---|---|---|\n")
            
            for _, row in runtime_summary_df.iterrows():
                query_id = row['query_id']
                gap_highlight = "**" if row['runtime_gap'] > 2 or row['runtime_gap'] < -2 else ''
                gap_sign = '+' if row['runtime_gap'] >= 0 else ''
                operator_changed_str = f"[🐙](#query-{query_id})" if row['operator_changed'] else "🐠"
                
                # Generate links for the new columns with subdirectories
                plan_comparison_link = f"[link](plan_comp/{query_id}.html)"
                plan1_link = f"[{app1_label}](full_plan/{appid1_short}-{query_id}.html)" if appid1_short else ""
                plan2_link = f"[{app2_label}](full_plan/{appid2_short}-{query_id}.html)" if appid2_short else ""
                plan_chart_link = f"[chart](plan_comp/{query_id}_chart.html)"
                
                f.write(f"| {query_id} | {row['hive_runtime']:.2f}s | {row['iceberg_runtime']:.2f}s | "
                       f"{gap_highlight}{gap_sign}{row['runtime_gap']:.2f}s{gap_highlight} | {gap_highlight}{gap_sign}{row['runtime_gap_pct']:.1f}%{gap_highlight} | {operator_changed_str} | "
                       f"{plan_comparison_link} | {plan1_link} | {plan2_link} | {plan_chart_link} |\n")
        else:
            f.write("No queries found matching the criteria.\n\n")
        
        f.write("\n---\n\n")
        
        # Write detailed operator comparisons for each query
        f.write("## Detailed Operator Comparisons\n\n")
        
        for _, row in sorted_queries.iterrows():
            query_id = row['query_id']
            comparison = diff_queries[query_id]
            
            f.write(f"### Query: {query_id}\n\n")
            
            # Add links to HTML files if folder_name is provided
            if folder_name:
                # Link to comparison HTML in plan_comp subdirectory
                comparison_link = f"plan_comp/{query_id}.html"
                f.write(f"[View detailed plan comparison]({comparison_link})")
                
                # Add links to individual plan HTML files in full_plan subdirectory
                if appid1_short:
                    plan1_link = f"full_plan/{appid1_short}-{query_id}.html"
                    f.write(f" | [{app1_label} Full Plan]({plan1_link})")
                if appid2_short:
                    plan2_link = f"full_plan/{appid2_short}-{query_id}.html"
                    f.write(f" | [{app2_label} Full Plan]({plan2_link})")
                
                f.write("\n\n")
            
            f.write(f"**Runtime:** {app1_label}={row['hive_runtime']:.2f}s, {app2_label}={row['iceberg_runtime']:.2f}s, "
                   f"Gap={row['runtime_gap']:+.2f}s ({row['runtime_gap_pct']:+.1f}%)\n\n")
            
            # Write operator comparison table with Diff column
            f.write(f"| Operator | {app1_label} | {app2_label} | Diff |\n")
            f.write("|---|---|---|---|\n")
            
            for operator in comparison.index:
                app1_val = comparison.loc[operator, 'hive']
                app2_val = comparison.loc[operator, 'iceberg']
                
                # Calculate difference
                app1_num = app1_val if app1_val != '' else 0
                app2_num = app2_val if app2_val != '' else 0
                diff = app2_num - app1_num
                
                # Format diff with highlighting
                if diff > 0:
                    diff_str = f"**+{diff}** ⚠️"
                elif diff < 0:
                    diff_str = f"**{diff}** ✓"
                else:
                    diff_str = "—"
                
                f.write(f"| {operator} | {app1_val} | {app2_val} | {diff_str} |\n")
            
            f.write("\n---\n\n")
    
    print(f"Sorted comparison exported to {filename}")
    print(f"Total queries with operator differences: {len(sorted_queries)}")
    print(f"Total queries for plan generation: {len(queries_for_plan_generation)}")
    
    # Return list of query IDs that need plan comparison/full plans generated
    return queries_for_plan_generation


def generate_chart_html(query_id, appid1_short, appid2_short, output_file, appname1="App1", appname2="App2"):
    """
    Generate a chart HTML file that displays two PNG diagrams side by side.
    
    Parameters:
    -----------
    query_id : str
        Query ID
    appid1_short : str
        Short app ID for first application
    appid2_short : str
        Short app ID for second application
    output_file : str
        Output HTML file path
    appname1 : str
        Name for first application
    appname2 : str
        Name for second application
    """
    # Get the relative path to the PNG files (they're in puml/)
    png1_path = f"../puml/{appid1_short}-{query_id}.png"
    png2_path = f"../puml/{appid2_short}-{query_id}.png"
    
    # Link to the plan comparison HTML
    plan_comparison_link = f"{query_id}.html"
    
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Query Plan Chart - {query_id}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .header h1 {{
            color: #333;
            margin-bottom: 10px;
        }}
        .header a {{
            color: #4CAF50;
            text-decoration: none;
            font-size: 16px;
            font-weight: bold;
        }}
        .header a:hover {{
            text-decoration: underline;
        }}
        .container {{
            display: flex;
            justify-content: space-around;
            align-items: flex-start;
            gap: 20px;
            max-width: 100%;
        }}
        .chart-section {{
            flex: 1;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .chart-section h2 {{
            color: #333;
            margin-top: 0;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        .chart-section img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Query Plan Chart: {query_id}</h1>
        <a href="{plan_comparison_link}">← View Detailed Plan Comparison</a>
    </div>
    <div class="container">
        <div class="chart-section">
            <h2>{appname1}</h2>
            <img src="{png1_path}" alt="{appname1} Plan Diagram">
        </div>
        <div class="chart-section">
            <h2>{appname2}</h2>
            <img src="{png2_path}" alt="{appname2} Plan Diagram">
        </div>
    </div>
</body>
</html>
"""
    
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"  Chart HTML saved to {output_file}")


def compare_store_sales_scan_metrics(appals1, appals2, appname1, appname2, output_md):
    """
    Compare store_sales table scan metrics (tbsvout) between two app runs.
    Outputs a markdown file with metrics as rows and runs as columns,
    similar to compare_table_scan_metrics_consolidated.py.

    Parameters:
    -----------
    appals1 : App_Log_Analysis_Enhanced
        First application analysis object
    appals2 : App_Log_Analysis_Enhanced
        Second application analysis object
    appname1 : str
        Label for first application (e.g. "iceberg")
    appname2 : str
        Label for second application (e.g. "hive")
    output_md : str
        Output markdown file path
    """
    print(f"\nExtracting table scan metrics for {appname1}...")
    dfout1, _ = appals1.get_table_scan_metrics(plot=False)

    print(f"Extracting table scan metrics for {appname2}...")
    dfout2, _ = appals2.get_table_scan_metrics(plot=False)

    # Filter to store_sales nodes only (nodename contains "store_sales")
    store_sales1 = dfout1[dfout1['nodename'].str.contains('store_sales', case=False, na=False)]
    store_sales2 = dfout2[dfout2['nodename'].str.contains('store_sales', case=False, na=False)]

    # Aggregate across all matching nodes (sum the sums), metric name is in column "Name"
    agg1 = store_sales1.groupby('Name')['sum'].sum()
    agg2 = store_sales2.groupby('Name')['sum'].sum()

    # Time metrics first, then count/size metrics, then everything else
    time_metrics = [
        'time of scan',
        'time of scan and filter',
        'data source read time',
        'io wait time',
        'page load time',
        'remaining filter time',
        'data source add split time',
    ]
    other_key_metrics = [
        'number of raw input rows',
        'number of output rows',
        'number of raw input bytes',
        'number of output bytes',
        'number of input rows',
        'number of input bytes',
        'number of processed splits',
        'number of processed row groups',
        'number of skipped splits',
        'number of skipped row groups',
        'peak memory bytes',
    ]
    all_metrics = sorted(set(agg1.index.tolist()) | set(agg2.index.tolist()))
    priority = [m for m in time_metrics if m in all_metrics] + \
               [m for m in other_key_metrics if m in all_metrics]
    others = [m for m in all_metrics if m not in time_metrics and m not in other_key_metrics]
    ordered_metrics = priority + others

    lines = []
    lines.append("# Store Sales Table Scan Metrics Comparison\n")
    lines.append(f"Comparing `*store_sales` nodes from `dfout` (aggregated sum across all matching nodes).\n")
    lines.append(f"| Metric | {appname1} | {appname2} | Diff | Diff % |")
    lines.append("|--------|------------|------------|------|--------|")

    for metric in ordered_metrics:
        v1 = agg1.get(metric, None)
        v2 = agg2.get(metric, None)

        # Skip metrics where both values are 0 or missing
        v1_num = v1 if v1 is not None else 0
        v2_num = v2 if v2 is not None else 0
        if v1_num == 0 and v2_num == 0:
            continue

        s1 = f"{v1:,.0f}" if v1 is not None else "N/A"
        s2 = f"{v2:,.0f}" if v2 is not None else "N/A"

        if v1 is not None and v2 is not None:
            diff = v2 - v1
            diff_str = f"{diff:+,.0f}"
            if v1 != 0:
                diff_pct = diff / v1 * 100
                diff_pct_str = f"{diff_pct:+.1f}%"
                diff_pct_str = f"**{diff_pct_str}**" if abs(diff_pct) > 5 else diff_pct_str
            else:
                diff_pct_str = "N/A"
        else:
            diff_str = "N/A"
            diff_pct_str = "N/A"

        lines.append(f"| **{metric}** | {s1} | {s2} | {diff_str} | {diff_pct_str} |")

    report = "\n".join(lines)
    with open(output_md, 'w') as f:
        f.write(report)

    print(f"Store sales scan metrics comparison saved to {output_md}")


DEFAULT_OUTPUT_ROOT = "/mnt/data1/mcp/output"
