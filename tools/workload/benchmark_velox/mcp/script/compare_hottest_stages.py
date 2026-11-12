import pandas as pd
import os
import matplotlib
import matplotlib.colors

def compare_hottest_stages(appid1, appid2, appals1, appals2, folder_name, appname1=None, appname2=None):
    """
    Compare hottest stages between two Spark applications and generate HTML report.
    
    Parameters:
    -----------
    appid1 : str
        First application ID
    appid2 : str
        Second application ID
    appals1 : App_Log_Analysis
        First application analysis object (already loaded)
    appals2 : App_Log_Analysis
        Second application analysis object (already loaded)
    folder_name : str
        Folder name to save the output HTML file
    appname1 : str, optional
        Display name for first application (defaults to appid1)
    appname2 : str, optional
        Display name for second application (defaults to appid2)
    
    Returns:
    --------
    str
        Path to the generated HTML file
    """
    # Use appid as fallback if appname not provided
    app1_display = appname1 if appname1 else appid1
    app2_display = appname2 if appname2 else appid2
    
    # Get hottest stages for both applications
    print(f"Extracting hottest stages for {appid1}...")
    stages1 = appals1.get_hottest_stages(plot=False)
    
    print(f"Extracting hottest stages for {appid2}...")
    stages2 = appals2.get_hottest_stages(plot=False)
    
    # Function to generate background color based on real_queryid (similar to sparklog.py)
    def get_background_color(real_queryid, queryid_to_color):
        """Generate background color based on real_queryid, so same queries have same colors"""
        if real_queryid in queryid_to_color:
            return queryid_to_color[real_queryid]
        return 'rgba(255,255,255,0.7)'  # Default white
    
    # Create HTML comparison with single combined table
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hottest Stages Comparison</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            text-align: center;
        }}
        .container {{
            max-width: 98%;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-box {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid #4CAF50;
        }}
        .summary-box h3 {{
            margin-top: 0;
            color: #4CAF50;
        }}
        .metric {{
            font-weight: bold;
            color: #333;
        }}
        .value {{
            color: #666;
        }}
        .table-wrapper {{
            overflow-x: auto;
            margin-top: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 8px;
            text-align: left;
            position: sticky;
            top: 0;
            font-size: 11px;
        }}
        th.separator {{
            background-color: #000000;
            width: 20px;
            padding: 0;
        }}
        td {{
            padding: 6px 8px;
            border-bottom: 1px solid #ddd;
        }}
        td.separator {{
            background-color: #000000;
            width: 20px;
            padding: 0;
        }}
        tr:hover {{
            opacity: 0.8;
        }}
        .fire {{
            color: #ff5722;
        }}
        .check {{
            color: #4CAF50;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔥 Hottest Stages Comparison</h1>
        <p style="text-align: center; color: #666;">
            Comparing: <strong>{app1_name}</strong> vs <strong>{app2_name}</strong>
        </p>
        
        <div class="summary">
            <div class="summary-box">
                <h3>{app1_name}</h3>
                <p><span class="metric">Total Stages:</span> <span class="value">{total_stages1}</span></p>
                <p><span class="metric">Total Time:</span> <span class="value">{total_time1:.2f}s</span></p>
                <p><span class="metric">Top Stage Time:</span> <span class="value">{top_stage_time1:.2f}s ({top_stage_pct1:.1f}%)</span></p>
            </div>
            <div class="summary-box">
                <h3>{app2_name}</h3>
                <p><span class="metric">Total Stages:</span> <span class="value">{total_stages2}</span></p>
                <p><span class="metric">Total Time:</span> <span class="value">{total_time2:.2f}s</span></p>
                <p><span class="metric">Top Stage Time:</span> <span class="value">{top_stage_time2:.2f}s ({top_stage_pct2:.1f}%)</span></p>
            </div>
        </div>

        <div class="table-wrapper">
            {combined_table}
        </div>
    </div>
</body>
</html>
"""
    
    # Calculate summary statistics
    total_stages1 = len(stages1)
    total_time1 = stages1['total_time'].sum()
    top_stage_time1 = stages1['total_time'].iloc[0] if len(stages1) > 0 else 0
    top_stage_pct1 = (top_stage_time1 / total_time1 * 100) if total_time1 > 0 else 0
    
    total_stages2 = len(stages2)
    total_time2 = stages2['total_time'].sum()
    top_stage_time2 = stages2['total_time'].iloc[0] if len(stages2) > 0 else 0
    top_stage_pct2 = (top_stage_time2 / total_time2 * 100) if total_time2 > 0 else 0
    
    # Drop columns: "Job ID", "queryid", "acc_total"
    display_cols = ['Stage ID', 'real_queryid', 'total_time', 'stdev_time', 'partition#', 'total']
    
    df1_display = stages1[display_cols].copy()
    df2_display = stages2[display_cols].copy()
    
    # Create color mapping based on real_queryid (so same queries have same colors)
    all_queryids = set(stages1['real_queryid'].unique()) | set(stages2['real_queryid'].unique())
    all_queryids = sorted(list(all_queryids))
    
    # Generate colors for all unique real_queryids
    norm = matplotlib.colors.Normalize(vmin=0, vmax=len(all_queryids)-1)
    cmap = matplotlib.cm.get_cmap('Set2')
    queryid_to_color = {}
    for idx, qid in enumerate(all_queryids):
        rgba = cmap(norm(idx))
        queryid_to_color[qid] = 'rgba({:d},{:d},{:d},0.7)'.format(int(rgba[0]*255), int(rgba[1]*255), int(rgba[2]*255))
    
    # Format numeric columns
    df1_display['total_time'] = df1_display['total_time'].apply(lambda x: f'{x:,.2f}')
    df1_display['stdev_time'] = df1_display['stdev_time'].apply(lambda x: f'{x:,.2f}' if pd.notna(x) else 'N/A')
    df1_display['total'] = df1_display['total'].apply(lambda x: f'{x:.2%}')
    
    df2_display['total_time'] = df2_display['total_time'].apply(lambda x: f'{x:,.2f}')
    df2_display['stdev_time'] = df2_display['stdev_time'].apply(lambda x: f'{x:,.2f}' if pd.notna(x) else 'N/A')
    df2_display['total'] = df2_display['total'].apply(lambda x: f'{x:.2%}')
    
    # Create combined table with both apps side by side
    def create_combined_table(df1, df2, stages1_df, stages2_df, queryid_to_color):
        """Create a single table with both apps side by side, separated by a black column"""
        html = '<table>\n<thead>\n<tr>\n'
    
        # Add headers for App 1
        html += f'<th colspan="6" style="text-align: center; background-color: #2196F3;">{app1_display}</th>\n'
        html += '<th class="separator"></th>\n'
        # Add headers for App 2
        html += f'<th colspan="6" style="text-align: center; background-color: #FF9800;">{app2_display}</th>\n'
        html += '</tr>\n<tr>\n'
        
        # Column headers for App 1
        for col in df1.columns:
            html += f'<th>{col}</th>\n'
        html += '<th class="separator"></th>\n'
        # Column headers for App 2
        for col in df2.columns:
            html += f'<th>{col}</th>\n'
        html += '</tr>\n</thead>\n<tbody>\n'
        
        # Add rows - iterate through the longer dataframe
        max_rows = max(len(df1), len(df2))
        for idx in range(max_rows):
            html += '<tr>\n'
            
            # App 1 columns
            if idx < len(df1):
                row1 = df1.iloc[idx]
                real_queryid1 = stages1_df.iloc[idx]['real_queryid']
                bg_color = get_background_color(real_queryid1, queryid_to_color)
                
                for col in df1.columns:
                    val = row1[col]
                    if col == 'partition#':
                        if '✅' in str(val):
                            val = f'<span class="check">{val}</span>'
                        elif '🔥' in str(val):
                            val = f'<span class="fire">{val}</span>'
                    html += f'<td style="background-color: {bg_color};">{val}</td>\n'
            else:
                # Empty cells if App 1 has fewer rows
                for col in df1.columns:
                    html += '<td></td>\n'
            
            # Black separator column
            html += '<td class="separator"></td>\n'
            
            # App 2 columns
            if idx < len(df2):
                row2 = df2.iloc[idx]
                real_queryid2 = stages2_df.iloc[idx]['real_queryid']
                bg_color = get_background_color(real_queryid2, queryid_to_color)
                
                for col in df2.columns:
                    val = row2[col]
                    if col == 'partition#':
                        if '✅' in str(val):
                            val = f'<span class="check">{val}</span>'
                        elif '🔥' in str(val):
                            val = f'<span class="fire">{val}</span>'
                    html += f'<td style="background-color: {bg_color};">{val}</td>\n'
            else:
                # Empty cells if App 2 has fewer rows
                for col in df2.columns:
                    html += '<td></td>\n'
            
            html += '</tr>\n'
        
        html += '</tbody>\n</table>'
        return html

    combined_table_html = create_combined_table(df1_display, df2_display, stages1, stages2, queryid_to_color)
    
    # Fill in the template
    html_output = html_content.format(
        app1_name=app1_display,
        app2_name=app2_display,
        total_stages1=total_stages1,
        total_time1=total_time1,
        top_stage_time1=top_stage_time1,
        top_stage_pct1=top_stage_pct1,
        total_stages2=total_stages2,
        total_time2=total_time2,
        top_stage_time2=top_stage_time2,
        top_stage_pct2=top_stage_pct2,
        combined_table=combined_table_html
    )
    
    # Write to file
    output_file = os.path.join(folder_name, 'hottest_stages_comparison.html')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_output)
    
    print(f"\n✅ HTML comparison file created: {output_file}")
    print(f"\nSummary:")
    print(f"  {app1_display}: {total_stages1} stages, {total_time1:.2f}s total")
    print(f"  {app2_display}: {total_stages2} stages, {total_time2:.2f}s total")
    
    return output_file


# Main execution when run as standalone script
if __name__ == "__main__":
    import sparklog
    from pyspark.sql import SparkSession
    
    # Initialize Spark session
    spark = SparkSession.builder.remote("sc://127.0.0.1:15002").getOrCreate()
    
    # Get hottest stages for both applications
    print("Loading app-20260308091712-0000...")
    app1 = sparklog.App_Log_Analysis(None, spark=spark)
    app1.load_data_from_database("app-20260308091712-0000")
    
    print("Loading app-20260310105854-0005...")
    app2 = sparklog.App_Log_Analysis(None, spark=spark)
    app2.load_data_from_database("app-20260310105854-0005")
    
    # Compare and generate HTML
    compare_hottest_stages(
        "app-20260308091712-0000",
        "app-20260310105854-0005",
        app1,
        app2,
        "."  # Current directory
    )

# Made with Bob
