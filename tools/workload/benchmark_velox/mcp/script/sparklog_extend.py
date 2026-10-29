#!/usr/bin/env python
# coding: utf-8
from __future__ import nested_scopes
from IPython.display import display, HTML
display(HTML('<style>.container { width:100% !important; }</style>'))
display(HTML('<style>.CodeMirror{font-family: "Courier New";font-size: 12pt;}</style>'))

try:
    from itables import show as itables_show
    ITABLES_AVAILABLE = True
except ImportError:
    ITABLES_AVAILABLE = False

import logging
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

import warnings
warnings.filterwarnings('ignore')

server="127.0.0.1"

import os
import datetime
from datetime import date
import time
import threading
import gzip
import json
import math
import re
import html
import builtins

import collections
import numpy
import pandas
pandas.options.display.max_rows=500
pandas.options.display.max_columns=200
pandas.options.display.float_format = '{:,}'.format

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.lines as mlines
from matplotlib import colors
from matplotlib import rcParams
rcParams['font.sans-serif'] =  'Courier New'
rcParams['font.family'] = 'Courier New'
rcParams['font.size'] = '12'

from ipywidgets import IntProgress,Layout

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, floor, lit, rank, col, lag, when, pandas_udf, PandasUDFType, avg, sum as _sum
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import udf

import seaborn as sns
from functools import reduce
from itertools import chain
import duckdb

import boto3
import numbers
from collections import OrderedDict

import json
import sys
from typing import Dict, List, Any, Set, Optional

"""
Extended Spark Log Analysis Module
This module extends the base App_Log_Analysis class from sparklog.py with additional
analysis capabilities including enhanced metrics, query plan visualization, and 
comparative analysis features.
"""

# Import the base classes and utilities from sparklog.py
from sparklog import (
    SparkLog_Analysis,
    Analysis,
    Telegraf_analysis,
    Telegraf_cpu_analysis,
    Telegraf_mem_analysis,
    Telegraf_PageCache_analysis,
    Telegraf_disk_analysis,
    Telegraf_nic_analysis,
    App_Log_Analysis,
    Run,
    Application_Run,
    background_gradient,
    notlist,
    comp_spark_conf
)
class PlantUMLGenerator:
    """Generate PlantUML diagrams from Spark query plans."""
    
    def __init__(self):
        self.edges: List[tuple] = []
        self.nodes: Dict[int, Dict[str, Any]] = {}
        
    def sanitize_label(self, text: str, max_length: int = 80) -> str:
        """Sanitize text for PlantUML labels."""
        text = text.replace('"', "'")
        text = text.replace('\n', '\\n')
        text = text.replace('\\', '/')
        
        if len(text) > max_length:
            text = text[:max_length-3] + "..."
        
        return text
    
    def should_skip_node(self, node_name: str) -> bool:
        """Check if node should be skipped based on name patterns."""
        skip_patterns = [
            'InputAdapter',
            'InputIteratorTransformer',
            'BroadcastQueryStage',
            'AdaptiveSparkPlan',
            r'WholeStageCodegenTransformer \(\d+\)'
        ]
        
        import re
        for pattern in skip_patterns:
            if re.search(pattern, node_name, re.IGNORECASE):
                return True
        return False
    
    def clean_scan_name(self, node_name: str) -> str:
        """Remove database prefix from IcebergScan node names."""
        if 'IcebergScanTransformer' in node_name:
            # Remove spark_catalog.tpcds_sf10000_parquet_zstd_part_512mb. prefix
            import re
            # Match pattern: IcebergScanTransformer spark_catalog.database.table
            match = re.search(r'IcebergScanTransformer\s+(?:spark_catalog\.[\w_]+\.)?(\w+)', node_name)
            if match:
                table_name = match.group(1)
                return f"IcebergScanTransformer {table_name}"
        return node_name
    
    def generate_node_label(self, node: Dict[str, Any]) -> str:
        """Generate a readable label for the node."""
        node_name = node.get('nodename', 'Unknown')
        output_rows = node.get('output_rows', 0)
        
        # Clean up scan names
        node_name = self.clean_scan_name(node_name)
        
        # Create a simple label with node name and output rows
        if output_rows > 0:
            return f"{node_name}\\noutput_rows: {output_rows:,.0f}"
        else:
            return node_name
    
    def get_node_color(self, node: Dict[str, Any]) -> str:
        """Get color for node based on type. Always use type-based colors for consistency."""
        node_name = node.get('nodename', '')
        
        # Color based on node type - always prioritize type over keystage
        if 'IcebergScanTransformer' in node_name or 'Scan' in node_name:
            return '#lightblue'
        elif 'Join' in node_name:
            return '#lightgreen'
        elif 'Aggregate' in node_name or 'Agg' in node_name:
            return '#lightyellow'
        elif 'Exchange' in node_name or 'Shuffle' in node_name:
            return '#lightcoral'
        elif 'Filter' in node_name:
            return '#lightgray'
        elif 'Project' in node_name:
            return '#wheat'
        elif 'Sort' in node_name or 'TakeOrdered' in node_name:
            return '#plum'
        elif 'ColumnarToRow' in node_name or 'RowToColumnar' in node_name:
            return '#lavender'
        elif 'Resize' in node_name:
            return '#lightcyan'
        else:
            return '#white'
    
    def get_stage_color(self, stagetime: float) -> str:
        """Get color for stage package based on execution time.
        Light green (fast) -> white (medium) -> light red (slow)
        """
        if stagetime <= 0:
            return '#white'
        elif stagetime < 1.0:
            # Fast: light green
            return '#90EE90'
        elif stagetime < 5.0:
            # Medium-fast: pale green
            return '#D0F0C0'
        elif stagetime < 10.0:
            # Medium: white
            return '#FFFFFF'
        elif stagetime < 20.0:
            # Medium-slow: light pink
            return '#FFE4E1'
        else:
            # Slow: light red
            return '#FFB6C1'
    
    def build_graph_from_nodes(self, nodes: List[Dict[str, Any]]) -> None:
        """Build graph structure from flat list of nodes with parent_nodeid."""
        # Create a lookup map for all nodes
        node_map = {n.get('nodeid'): n for n in nodes if n.get('nodeid') is not None}
        
        # Track reused exchanges: map (plan_id, nodename) -> first nodeid
        reused_exchanges: Dict[tuple, int] = {}
        # Track which nodes to skip (reused nodes and their descendants)
        skip_nodes: Set[int] = set()
        # Map reused nodeid -> original nodeid
        reuse_mapping: Dict[int, int] = {}
        # Map ReusedExchange nodeid -> original exchange nodeid
        reused_exchange_mapping: Dict[int, int] = {}
        
        # First pass: identify reused exchanges (first occurrence wins)
        for node in nodes:
            nodeid = node.get('nodeid')
            node_name = node.get('nodename', '')
            plan_id = node.get('plan_id', -1)
            
            if nodeid is None:
                continue
            
            # Special handling for ReusedExchange nodes
            if node_name == 'ReusedExchange' and plan_id != -1:
                # Find the first exchange with this plan_id (should already be in reused_exchanges)
                for other_node in nodes:
                    other_id = other_node.get('nodeid')
                    other_plan_id = other_node.get('plan_id', -1)
                    other_name = other_node.get('nodename', '')
                    if (other_id != nodeid and
                        other_plan_id == plan_id and
                        'Exchange' in other_name and
                        other_name != 'ReusedExchange'):
                        # Check if this is the first occurrence
                        key = (plan_id, other_name)
                        if key in reused_exchanges and reused_exchanges[key] == other_id:
                            reused_exchange_mapping[nodeid] = other_id
                            break
                continue
            
            # Check for node reuse (plan_id != -1, but not ReusedExchange)
            # This applies to all node types: Exchange, Subquery, etc.
            if plan_id != -1 and node_name != 'ReusedExchange':
                key = (plan_id, node_name)
                if key in reused_exchanges:
                    # This is a reused node, skip it and all descendants
                    original_nodeid = reused_exchanges[key]
                    reuse_mapping[nodeid] = original_nodeid
                    skip_nodes.add(nodeid)
                    # Mark all descendants to skip
                    self._mark_descendants_to_skip(nodeid, node_map, skip_nodes)
                else:
                    # First occurrence, remember it
                    reused_exchanges[key] = nodeid
        
        # Second pass: store all nodes except skipped ones and filtered ones
        for node in nodes:
            nodeid = node.get('nodeid')
            node_name = node.get('nodename', '')
            
            if nodeid is None or nodeid in skip_nodes:
                continue
            
            # Skip nodes that match filter patterns
            if not self.should_skip_node(node_name):
                label = self.generate_node_label(node)
                color = self.get_node_color(node)
                
                self.nodes[nodeid] = {
                    'label': label,
                    'color': color,
                    'node': node
                }
        
        # Third pass: build edges, handling reuse mapping and ReusedExchange
        for nodeid in self.nodes.keys():
            node = node_map.get(nodeid)
            if not node:
                continue
            
            # Special case: if this is a ReusedExchange, connect original exchange to it
            # (reversed edge direction so ReusedExchange appears below the original)
            if nodeid in reused_exchange_mapping:
                original_exchange_id = reused_exchange_mapping[nodeid]
                if original_exchange_id in self.nodes:
                    self.edges.append((original_exchange_id, nodeid))
                    # Don't continue - still need to connect ReusedExchange to its children
            
            parent_nodeid = node.get('parent_nodeid')
            
            # Walk up the tree to find the first non-filtered/non-skipped ancestor
            current_parent = parent_nodeid
            while current_parent is not None:
                # Check if parent was reused, use original instead
                if current_parent in reuse_mapping:
                    current_parent = reuse_mapping[current_parent]
                
                if current_parent in self.nodes:
                    # Found a kept parent, create edge
                    self.edges.append((nodeid, current_parent))
                    break
                # Parent was filtered or skipped, continue to its parent
                parent_node = node_map.get(current_parent)
                if parent_node:
                    current_parent = parent_node.get('parent_nodeid')
                else:
                    # No more parents
                    break
        
        # Fourth pass: for nodes that are originals of skipped duplicates,
        # also add edges from the duplicate's parent chains if they provide
        # a different immediate parent connection
        for skipped_id, original_id in reuse_mapping.items():
            # Skip if the original was also skipped (shouldn't happen but be safe)
            if original_id not in self.nodes:
                continue
            
            # Skip if this node was skipped as a descendant of another skipped node
            # (not as a direct duplicate)
            if skipped_id in skip_nodes:
                # Check if it's in reuse_mapping - if yes, it's a direct duplicate
                # If it's ONLY in skip_nodes but not as a key in reuse_mapping,
                # it was skipped as a descendant
                # Actually, if it's in reuse_mapping, it IS a direct duplicate
                # So we should process it. But we need to check if its parent chain
                # goes through other skipped nodes
                pass
            
            skipped_node = node_map.get(skipped_id)
            original_node = node_map.get(original_id)
            if not skipped_node or not original_node:
                continue
            
            # Get the original's immediate parent (after filtering/reuse mapping)
            original_immediate_parent = original_node.get('parent_nodeid')
            if original_immediate_parent and original_immediate_parent in reuse_mapping:
                original_immediate_parent = reuse_mapping[original_immediate_parent]
            
            # Walk up from original's parent through filtered nodes to find first kept parent
            current = original_immediate_parent
            while current is not None and current not in self.nodes:
                parent_node = node_map.get(current)
                if parent_node:
                    current = parent_node.get('parent_nodeid')
                    if current in reuse_mapping:
                        current = reuse_mapping[current]
                else:
                    current = None
            original_first_kept_parent = current
            
            # Walk up from the skipped node's parent to find non-skipped ancestors
            parent_nodeid = skipped_node.get('parent_nodeid')
            current_parent = parent_nodeid
            
            # Check if the immediate parent is in skip_nodes (meaning this whole branch was skipped)
            if current_parent in skip_nodes:
                # This skipped node's parent was also skipped, so don't add its connections
                continue
            
            while current_parent is not None:
                # Check if this parent was also reused
                if current_parent in reuse_mapping:
                    current_parent = reuse_mapping[current_parent]
                
                # Skip if we encounter a skipped node in the chain
                if current_parent in skip_nodes:
                    break
                
                if current_parent in self.nodes:
                    # Found a kept parent, add if it's different from original's first kept parent
                    if current_parent != original_first_kept_parent:
                        # Check if this edge doesn't already exist
                        if (original_id, current_parent) not in self.edges:
                            self.edges.append((original_id, current_parent))
                    break
                
                # Parent was filtered or skipped, continue to its parent
                parent_node = node_map.get(current_parent)
                if parent_node:
                    current_parent = parent_node.get('parent_nodeid')
                else:
                    break
    
    def _mark_descendants_to_skip(self, nodeid: int, node_map: Dict[int, Dict[str, Any]],
                                   skip_nodes: Set[int]) -> None:
        """Recursively mark all descendants of a node to be skipped."""
        for node in node_map.values():
            if node.get('parent_nodeid') == nodeid:
                child_id = node.get('nodeid')
                if child_id is not None:
                    skip_nodes.add(child_id)
                    self._mark_descendants_to_skip(child_id, node_map, skip_nodes)
    
    def generate_plantuml(self, plan_data: Dict[str, Any]) -> str:
        """Generate complete PlantUML diagram from query plan."""
        # Extract queryid from plan_data
        queryid = plan_data.get('queryid', 'Unknown')
        
        # Extract nodes from the plan data
        nodes = []
        
        # Try different possible structures
        if 'plan_nodes' in plan_data:
            nodes = plan_data['plan_nodes']
        elif 'queries' in plan_data and len(plan_data['queries']) > 0:
            nodes = plan_data['queries'][0].get('nodes', [])
        elif isinstance(plan_data, list):
            nodes = plan_data
        
        if not nodes:
            raise ValueError("No nodes found in plan data")
        
        # Build the graph
        self.build_graph_from_nodes(nodes)
        
        # Group nodes by stage ID and collect stage metadata
        stages: Dict[int, Dict[str, Any]] = {}
        for nodeid, node_info in self.nodes.items():
            node = node_info['node']
            stageid = node.get('stageid', 0)
            
            if stageid not in stages:
                stages[stageid] = {
                    'nodes': [],
                    'stagetime': node.get('stagetime', 0.0),
                    'stageparts': node.get('stageParts', 0)
                }
            stages[stageid]['nodes'].append(nodeid)
            
            # Update stage time to max (in case nodes have different times)
            stages[stageid]['stagetime'] = max(
                stages[stageid]['stagetime'],
                node.get('stagetime', 0.0)
            )
        
        # Generate PlantUML
        output = [
            "@startuml",
            f"title Spark Query Execution Plan ({queryid})",
            "skinparam defaultTextAlignment center",
            "skinparam rectangleFontSize 10",
            "skinparam rectangleBorderColor black",
            "skinparam packageBorderColor black",
            "skinparam packageFontSize 12",
            "top to bottom direction",
            ""
        ]
        
        # Define nodes grouped by stage
        for stageid in sorted(stages.keys()):
            stage_info = stages[stageid]
            node_ids = stage_info['nodes']
            stagetime = stage_info['stagetime']
            stageparts = stage_info['stageparts']
            
            # Calculate stage box color based on time (light green -> white -> light red)
            stage_color = self.get_stage_color(stagetime)
            
            # Create package title with time and partitions
            title = f"Stage {stageid}"
            if stagetime > 0:
                title += f" | {stagetime:.2f}s"
            if stageparts > 0:
                title += f" | {stageparts} partitions"
            
            output.append(f'package "{title}" {stage_color} {{')
            
            for nodeid in sorted(node_ids):
                node_info = self.nodes[nodeid]
                label = node_info['label']
                color = node_info['color']
                output.append(f'  rectangle "{label}" as n{nodeid} {color}')
            
            output.append("}")
            output.append("")
        
        # Define all edges (child --> parent for top-to-bottom layout)
        for child_id, parent_id in self.edges:
            output.append(f"n{child_id} --> n{parent_id}")
        
        output.append("")
        output.append("@enduml")
        
        return '\n'.join(output)

# Import required libraries for extended functionality
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import numpy
import builtins
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.clustering import KMeans
import json
import logging

logger = logging.getLogger()


class App_Log_Analysis_Enhanced(App_Log_Analysis):
    """
    Enhanced Spark application log analysis with additional features:
    - Database loading capabilities
    - Advanced visualization methods
    - Stage histogram analysis
    - Metrics correlation analysis
    - Query plan comparison
    - Velox statistics extraction
    
    This class extends App_Log_Analysis from sparklog.py with additional
    methods for deeper performance analysis and visualization.
    """
    
    def __init__(self, file, jobids=None, qlist=None, appid=None, spark=None):
        """
        Initialize the App_Log_Analysis_Enhanced instance.
        
        Args:
            file (str): Path to the Spark event log file
            jobids (str|list): Job IDs to filter analysis (optional)
            qlist (list): Query list for custom query mapping (optional)
            appid (str): Application ID (optional)
            spark: Spark session instance (optional)
        """
        super().__init__(file, jobids, qlist, appid, spark)

    def load_data_from_database(self, appid):
        """
        Load pre-processed Spark metrics from database tables.
        
        This method loads data from Iceberg tables instead of parsing event logs,
        which is faster for repeated analysis of the same application.
        
        Args:
            appid (str): Application ID to load from database
        """
        self.appid = appid
        self.dfacc = self.spark.table("spark_profile.dfacc").where(f"appid='{appid}'")
        self.queryplans = self.spark.table("spark_profile.query_plan_info").where(f"appid='{appid}'")
        self.df = self.spark.table("spark_profile.spark_metrics_iceberg").where(f"appid='{appid}'")
        self.metric_df = self.spark.table("spark_profile.task_metrics").where(f"appid='{appid}'")

        self.queryplans.cache()
        self.df.cache()
        self.metric_df.cache()
        self.dfacc.cache()

        # Initialize attributes
        self.query_num = self.df.select("real_queryid").distinct().count()

        # update self.allmetrics, self.metricscollect
        self._extract_metrics()

        # update criticaltasks
        self._calculate_critical_path()

        cfgdict = self.spark.table("spark_profile.spark_config").where(f"appid='{appid}'").collect()
        self.config = {r['key']: r['value'] for r in cfgdict}

        self.parallelism = int(self.config.get('spark.sql.shuffle.partitions', 1))
        self.executor_cores = int(self.config.get('spark.executor.cores', 1))
        self.executor_instances = int(self.config.get('spark.executor.instances', 1))
        self.taskcpus = int(self.config.get('spark.task.cpus', 1))
        self.batchsize = int(self.config.get('spark.gluten.sql.columnar.maxBatchSize', 4096))
        self.realexecutors = int(self.config.get('realexecutors', 1))
        
        return
    
    def show_Stage_histogram(self, stageid, bincount=15):
        """
        Display histogram analysis for a specific stage.
        
        Shows distribution of elapsed time and input data size across tasks,
        along with scatter plots and violin plots by host.
        
        Args:
            stageid (int): Stage ID to analyze
            bincount (int): Number of bins for histogram (default: 15)
        """
        if self.df is None:
            self.load_data()
        
        tasks=self.df.where("`Stage ID`={:d}".format(stageid)).select("Task ID").distinct()
        inputsize = self.metric_df.join(tasks, "Task ID") \
                      .where("Name='input size in bytes' or Name='size of files read'") \
                      .groupBy("Task ID") \
                      .agg((F.sum("Update")).alias("input read"))


        stage37=self.df.where("`Stage ID`={:d} and event='SparkListenerTaskEnd'".format(stageid) )\
                        .join(inputsize,on=["Task ID"],how="left")\
                        .fillna(0) \
                        .select(F.col('Host'), 
                                F.round((F.col('Finish Time')/1000-F.col('Launch Time')/1000),2).alias('elapsedtime'),
                                F.round((F.col('`input read`')+F.col('`Bytes Read`')+F.col('`Local Bytes Read`')+F.col('`Remote Bytes Read`'))/1024/1024,2).alias('input'))
        stage37=stage37.cache()
        hist_elapsedtime=stage37.select('elapsedtime').rdd.flatMap(lambda x: x).histogram(15)
        hist_input=stage37.select('input').rdd.flatMap(lambda x: x).histogram(15)
        fig, axs = plt.subplots(figsize=(30, 5),nrows=1, ncols=2)
        ax=axs[0]
        binSides, binCounts = hist_elapsedtime
        binSides=[builtins.round(l,2) for l in binSides]

        N = len(binCounts)
        ind = numpy.arange(N)
        width = 0.5

        rects1 = ax.bar(ind+0.5, binCounts, width, color='b')

        ax.set_ylabel('Frequencies')
        ax.set_title('stage{:d} elapsed time breakdown'.format(stageid))
        ax.set_xticks(numpy.arange(N+1))
        ax.set_xticklabels(binSides)

        ax=axs[1]
        binSides, binCounts = hist_input
        binSides=[builtins.round(l,2) for l in binSides]

        N = len(binCounts)
        ind = numpy.arange(N)
        width = 0.5
        rects1 = ax.bar(ind+0.5, binCounts, width, color='b')

        ax.set_ylabel('Frequencies')
        ax.set_title('stage{:d} input data breakdown'.format(stageid))
        ax.set_xticks(numpy.arange(N+1))
        ax.set_xticklabels(binSides)

        out=stage37
        outpds=out.toPandas()

        fig, axs = plt.subplots(nrows=1, ncols=3, sharey=False,figsize=(30,8),gridspec_kw = {'width_ratios':[1, 1, 1]})
        plt.subplots_adjust(wspace=0.01)

        groups= outpds.groupby('Host')
        for name, group in groups:
            axs[0].plot(group.input, group.elapsedtime, marker='o', linestyle='', ms=5, label=name)
        axs[0].set_xlabel('input size (MB)')
        axs[0].set_ylabel('elapsed time (s)')

        axs[0].legend()

        axs[0].get_shared_y_axes().join(axs[0], axs[1])

        sns.violinplot(y='elapsedtime', x='Host', data=outpds,palette=['g'],ax=axs[1])

        sns.violinplot(y='input', x='Host', data=outpds,palette=['g'],ax=axs[2])

        #ax.xaxis.set_major_formatter(mtick.FormatStrFormatter(''))
        #ax.yaxis.set_major_formatter(mtick.FormatStrFormatter(''))

        if False:
            out=stage37
            vecAssembler = VectorAssembler(inputCols=["input",'elapsedtime'], outputCol="features").setHandleInvalid("skip")
            new_df = vecAssembler.transform(out)
            kmeans = KMeans(k=2, seed=1)  # 2 clusters here
            model = kmeans.fit(new_df.select('features'))
            transformed = model.transform(new_df)


            outpds=transformed.select('Host','elapsedtime','input','prediction').toPandas()

            fig, axs = plt.subplots(nrows=1, ncols=2, sharey=False,figsize=(30,8),gridspec_kw = {'width_ratios':[1, 1]})
            plt.subplots_adjust(wspace=0.01)

            groups= outpds.groupby('prediction')
            for name, group in groups:
                axs[0].plot(group.input, group.elapsedtime, marker='o', linestyle='', ms=5, label=name)
            axs[0].legend()

            bars=transformed.where('prediction=1').groupBy("Host").count().toPandas()

            axs[1].bar(bars['Host'], bars['count'], 0.4, color='coral')
            axs[1].set_title('cluster=1')

        plt.show()

    def draw_metrics_xy(self, stageid, metric_namex, metric_namey):
        """
        Draw scatter plot comparing two metrics for a stage.
        
        Args:
            stageid (int): Stage ID to analyze
            metric_namex (str): Name of metric for X axis
            metric_namey (str): Name of metric for Y axis
        """
        if self.df is None:
            self.load_data()
        tasks=self.df.where("`Stage ID`={:d}".format(stageid)).select("Task ID").distinct()
        x = self.metric_df.join(tasks, on="Task ID") \
                      .where(f"Name='{metric_namex}'") \
                      .groupBy("Task ID") \
                      .agg((F.sum("Update")).alias("x"))
        
        y = self.metric_df.join(tasks, on="Task ID") \
                      .where(f"Name='{metric_namey}'") \
                      .groupBy("Task ID") \
                      .agg((F.sum("Update")).alias("y"))
        
        stage37=self.df.where("`Stage ID`={:d} and event='SparkListenerTaskEnd'".format(stageid) )\
                        .select(F.col('Host'),F.col("Task ID"))
        
        stage37=stage37.join(x,on=["Task ID"],how="left").join(y,on=["Task ID"],how="left")
        
        
        out=stage37
        outpds=out.toPandas()

        plt.figure(figsize=(30, 8))
        groups= outpds.groupby('Host')
        for name, group in groups:
            plt.plot(group.x, group.y, marker='o', linestyle='', ms=5, label=name)
        plt.xlabel(metric_namex)
        plt.ylabel(metric_namey)

        plt.legend()
        plt.grid()
        plt.show()

    def draw_metrics_elapsetime(self, stageid, metric_name):
        """
        Draw scatter plot of metric vs elapsed time for a stage.
        
        Args:
            stageid (int): Stage ID to analyze
            metric_name (str): Name of metric to plot against elapsed time
        """
        if self.df is None:
            self.load_data()
        tasks=self.df.where("`Stage ID`={:d}".format(stageid)).select("Task ID").distinct()
        inputsize = self.metric_df.join(tasks, on="Task ID") \
                      .where(f"Name='{metric_name}'") \
                      .groupBy("Task ID") \
                      .agg((F.sum("Update")).alias("input read"))


        stage37=self.df.where("`Stage ID`={:d} and event='SparkListenerTaskEnd'".format(stageid) )\
                        .join(inputsize,on=["Task ID"],how="left")\
                        .fillna(0) \
                        .select(F.col('Host'), 
                                F.round((F.col('Finish Time')/1000-F.col('Launch Time')/1000),2).alias('elapsedtime'),
                                F.round((F.col('`input read`')),2).alias('input'))
        stage37=stage37.cache()
        hist_elapsedtime=stage37.select('elapsedtime').rdd.flatMap(lambda x: x).histogram(15)
        hist_input=stage37.select('input').rdd.flatMap(lambda x: x).histogram(15)
        
        out=stage37
        outpds=out.toPandas()

        plt.figure(figsize=(30, 8))
        groups= outpds.groupby('Host')
        for name, group in groups:
            plt.plot(group.input, group.elapsedtime, marker='o', linestyle='', ms=5, label=name)
        plt.xlabel(metric_name)
        plt.ylabel('elapsed time (s)')

        plt.legend()
        plt.grid()
        plt.show()

    def show_Stages_hist(self, **kwargs):
        """
        Show histogram of stage durations.
        
        Args:
            **kwargs: Additional filtering parameters
        """
        if self.df is None:
            self.load_data()
        
        bincount=kwargs.get("bincount",15)
        threshold=kwargs.get("threshold",0.9)
        
        query=kwargs.get("queryid",None)
        if query and type(query)==int:
            query = [query,]
        df=self.df.where(F.col("real_queryid").isin(query)) if query else self.df
        
        totaltime=df.where("event='SparkListenerTaskEnd'" ).agg(F.sum(F.col('Finish Time')-F.col('Launch Time')).alias('total_time')).collect()[0]['total_time']
        stage_time=df.where("event='SparkListenerTaskEnd'" ).groupBy('`Stage ID`').agg(F.sum(F.col('Finish Time')-F.col('Launch Time')).alias('total_time')).orderBy('total_time', ascending=False).toPandas()
        stage_time['acc_total'] = stage_time['total_time'].cumsum()/totaltime
        stage_time=stage_time.reset_index()
        fig, ax = plt.subplots(figsize=(30, 5))

        rects1 = ax.plot(stage_time['index'],stage_time['acc_total'],'b.-')
        ax.set_xticks(stage_time['index'])
        ax.set_xticklabels(stage_time['Stage ID'])
        ax.set_xlabel('stage')
        ax.grid(which='major', axis='x')
        plt.show()
        shownstage=[]
        for x in stage_time.index:
            if stage_time['acc_total'][x]<=threshold:
                shownstage.append(stage_time['Stage ID'][x])
            else:
                shownstage.append(stage_time['Stage ID'][x])
                break
        for row in shownstage:
            self.show_Stage_histogram(row,bincount)

    def get_hottest_stages(self, **kwargs):
        """
        Get the stages with highest elapsed time.
        
        Returns top stages sorted by total elapsed time, useful for identifying
        performance bottlenecks.
        
        Args:
            **kwargs: Additional filtering parameters (e.g., queryid, top_n)
        
        Returns:
            pandas.DataFrame: DataFrame with stage statistics
        """
        if self.df is None:
            self.load_data()
        
        bincount=kwargs.get("bincount",15)
        threshold=kwargs.get("threshold",0.9)
        min_total_time=kwargs.get("min_total_time",10)
        plot=kwargs.get("plot",True)
        
        query=kwargs.get("queryid",None)
        if query and type(query)==int:
            query = [query,]
        df=self.df.where(F.col("real_queryid").isin(query)) if query else self.df.where("queryid is not NULL")

        stage_time=df.where("event='SparkListenerTaskEnd'" ).groupBy('`Stage ID`','Job ID','real_queryid').agg(
            F.sum(F.col('Finish Time')-F.col('Launch Time')).alias('total_time'),
            F.stddev(F.col('Finish Time')/1000-F.col('Launch Time')/1000).alias('stdev_time'),
            F.count("*").alias("cnt"),
            F.first('queryid').astype(IntegerType()).alias('queryid')
            )\
            .select('`Stage ID`','Job ID','real_queryid','queryid',
                    (F.col("total_time")/1000/(F.when(F.col("cnt")>F.lit(self.executor_instances*self.executor_cores/self.taskcpus),F.lit(self.executor_instances*self.executor_cores/self.taskcpus)).otherwise(F.col("cnt")))).alias("total_time"),
                    F.col("stdev_time"),
                    (F.when(F.col("cnt") % (self.executor_instances*self.executor_cores) == 0, F.concat(F.col("cnt"),F.lit(" ✅"))).otherwise(F.concat(F.col("cnt"),F.lit(" 🔥")))).alias("partition#")
                   ).orderBy('total_time', ascending=False).toPandas()

        totaltime=stage_time['total_time'].sum()
        stage_time['acc_total'] = stage_time['total_time'].cumsum()/totaltime
        stage_time['total'] = stage_time['total_time']/totaltime
        stage_time=stage_time.reset_index()

        shownstage=stage_time.loc[(stage_time['acc_total'] <=threshold) & (stage_time['total_time'] > min_total_time)]
        shownstage['stg']=shownstage['real_queryid'].astype(str)+'_'+shownstage['Job ID'].astype(str)+'_'+shownstage['Stage ID'].astype(str)
        if plot:
            shownstage.plot.bar(x="stg",y="total",figsize=(30,8))



        norm = matplotlib.colors.Normalize(vmin=0, vmax=max(stage_time.queryid))
        cmap = matplotlib.cm.get_cmap('Set2')
        def setbkcolor(x):
            rgba=cmap(norm(x['queryid']))
            return ['background-color:rgba({:d},{:d},{:d},1); color:white'.format(int(rgba[0]*255),int(rgba[1]*255),int(rgba[2]*255))]*10

        if plot:
            styled_df = stage_time.loc[stage_time['total_time'] > min_total_time].style.apply(setbkcolor,axis=1).format({"total_time":lambda x: '{:.2f}'.format(x),"stdev_time":lambda x: '{:.2f}'.format(x),"acc_total":lambda x: '{:.2%}'.format(x),"total":lambda x: '{:.2%}'.format(x)})
            if ITABLES_AVAILABLE:
                itables_show(styled_df, allow_html=True)
            else:
                display(styled_df)
        
        return stage_time

    def get_velox_stats(self, taskid, **kwargs):
        """
        Extract Velox execution statistics for a specific task.
        
        Args:
            taskid (int): Task ID to analyze
            **kwargs: Additional parameters
        
        Returns:
            dict: Velox statistics for the task
        """
        
        if appals.df is None:
            appals.load_data()
        
        plot = kwargs.get("plot",True)

        stats=appals.metric_df.where(f"`Task ID`={taskid}").where("Name='velox task stats'").select("Value").collect()
        if len(stats)==0:
            print("There is no velox stat in metrics")
            return
        
        s=json.loads(stats[0]['Value'])
        if plot:
            for l in s:
                html="<table>"
                html+=f'<tr><td colspan="2" style="text-align: center;"><b>{l["operatorType"]}</b></td></tr>'
                for k in l.keys():
                    html+="<tr>"
                    if k.endswith("Timing"):
                        timing="<table><tr>"
                        for fld in l[k].split(","):
                            timing+=f"<td>{fld}</td>"
                        timing+="</tr></table>"
                        html+=f"<td>{k}</td><td>{timing}</td>"
                    elif k.endswith("customStats"):
                        customStats="<table>"
                        for fldx in l[k].keys():
                            customStats+="<tr>"
                            fldxv="<table><tr>"
                            for p in l[k][fldx].split(","):
                                fldxv+=f"<td>{p}</td>"
                            fldxv+="</tr></table>"
                            customStats+=f"<td>{fldx}</td><td>{fldxv}</td>"
                            customStats+="</tr>"
                        customStats+="</table>"
                        html+=f"<td>{k}</td><td>{customStats}</td>"
                    else:
                        if isinstance(l[k], numbers.Number):
                            vl=l[k]
                            html+=f'<td>{k}</td><td  style="text-align: left;">{vl:,}</td>'
                        else:
                            html+=f'<td>{k}</td><td  style="text-align: left;">{l[k]}</td>'
                    html+="</tr>"
                html+="</table>"
                display(HTML(html))
        return s    

    def scatter_elapsetime_input(self, stageid):
        """
        Create scatter plot of elapsed time vs input size for a stage.
        
        Args:
            stageid (int): Stage ID to analyze
        """
        if self.df is None:
            self.load_data()
        stage37=self.df.where("`Stage ID`={:d} and event='SparkListenerTaskEnd'".format(stageid) ).select(F.round((F.col('Finish Time')/1000-F.col('Launch Time')/1000),2).alias('elapsedtime'),F.round((F.col('`Bytes Read`')+F.col('`Local Bytes Read`')+F.col('`Remote Bytes Read`'))/1024/1024,2).alias('input')).toPandas()
        stage37.plot.scatter('input','elapsedtime',figsize=(30, 5))

    def get_critical_path_stages(self):
        """
        Get stages that are on the critical path.
        
        Returns:
            list: List of stage IDs on the critical path
        """
        df=self.df.where("Event='SparkListenerTaskEnd'")
        criticaltasks=self.criticaltasks
        cripds=pandas.DataFrame(criticaltasks)
        cripds.columns=['task_id',"launch","finish"]
        cridf=self.spark.createDataFrame(cripds)
        df_ctsk=df.join(cridf,on=[F.col("task_id")==F.col("Task ID")],how="inner")
        df_ctsk=df_ctsk.withColumn("elapsed",(F.col("Finish Time")-F.col("Launch Time"))/1000)
        return df_ctsk.where("elapsed>10").orderBy(F.desc("elapsed")).select("real_queryid",F.round("elapsed",2).alias("elapsed"),"Host","executor ID","Stage ID","Task ID",F.round(F.col("Bytes Read")/1000000,0).alias("file read"),F.round((F.col("Local Bytes Read")+F.col("Remote Bytes Read"))/1000000,0).alias("shuffle read")).toPandas()


    def show_query_time_metric(self):
        """
        Display query time metrics in a formatted table.
        """
        if self.df is None:
            self.load_data()
        querids=self.df.select("queryid").distinct().collect()
        for idx,q in enumerate([l["queryid"] for l in querids]):
            self.show_time_metric(query=[q,],showexecutor=False)

    def get_query_plan(self, **kwargs):
        """
        Get query execution plan for a specific query or stage.
        
        Args:
            **kwargs: Parameters including:
                - queryid (int): Query ID to get plan for
                - stageid (int): Stage ID to get plan for
                - show_simple_string (bool): Show simplified plan string
        
        Returns:
            DataFrame or dict: Query plan information
        """
        """
        Returns a pure dict representation of the query plan data.
        No HTML generation - just raw data.
        
        Parameters:
            queryid: Query ID(s) to filter
            stageid: Stage ID(s) to filter
            outputstage: Optional list to collect output stage info
        
        Returns:
            dict: Dictionary containing query plan data structure
        """
        if self.df is None:
            self.load_data()

        queryid=kwargs.get("queryid",None)
        stageid=kwargs.get("stageid",None)
        
        outputstage=kwargs.get("outputstage",None)
        
        if queryid is not None:
            if type(queryid)==int or type(queryid)==str:
                queryid = [queryid,]
            shown_stageid = [l["Stage ID"] for l in self.df.where(F.col("real_queryid").isin(queryid)).select("Stage ID").distinct().collect()]
        if stageid is not None:
            if type(stageid)==int:
                shown_stageid = [stageid,]
            elif type(stageid)==list:
                shown_stageid = stageid
            queryid = [l["real_queryid"] for l in self.df.where(F.col("`Stage ID`").isin(shown_stageid)).select("real_queryid").limit(1).collect()]


        queryplans=[]
        queryplans = self.queryplans.where(F.col("real_queryid").isin(queryid)).orderBy("real_queryid").collect() if queryid else self.queryplans.orderBy("real_queryid").collect()
        rawdf = self.df.where(F.col("real_queryid").isin(queryid)) if queryid else self.df
        dfmetric=rawdf.where("Event='SparkListenerTaskEnd'").select("queryid","real_queryid","Stage ID","Job ID","Task ID").join(self.metric_df,on="Task ID").select("Stage ID","ID","Update").groupBy("ID","Stage ID").agg(F.round(F.sum("Update"),1).alias("value"),F.round(F.stddev("Update"),1).alias("stdev")).collect()
        accid2stageid={l.ID:(l["Stage ID"],l["value"],l["stdev"]) for l in dfmetric}

        stagetime=rawdf.where(F.col("Event")=='SparkListenerTaskEnd').groupBy("Stage ID").agg(
            F.round(F.sum(F.col("Finish Time")-F.col("Launch Time"))/1000/self.executor_instances/self.executor_cores*self.taskcpus,1).alias("elapsed time"),
            F.round(F.stddev(F.col("Finish Time")-F.col("Launch Time"))/1000,1).alias("time stdev"),
            F.count(F.col("Task ID")).alias("partitions")
            ).orderBy(F.desc("elapsed time")).collect()

        apptotaltime=reduce(lambda x,y: x+y['elapsed time'], stagetime,0)
        if apptotaltime==0:
            if plot:
                display(HTML("<font size=4 color=red>Error, totaltime is 0 </font>"))
            apptotaltime=1
            return ""

        stagemap={l["Stage ID"]:l["elapsed time"] for l in stagetime}
        stage_time_stdev_map={l["Stage ID"]:l["time stdev"] for l in stagetime}
        stagepartmap={l["Stage ID"]:l["partitions"] for l in stagetime}

        keystage=[]
        keystagetime=[]
        subtotal=0
        for s in stagetime:
            subtotal=subtotal+s['elapsed time']
            keystage.append(s['Stage ID'])
            keystagetime.append(s['elapsed time'])
            if subtotal/apptotaltime>0.9:
                break
        keystagetime=["{:02x}{:02x}".format(int(255*l/keystagetime[0]),255-int(255*l/keystagetime[0])) for l in keystagetime if keystagetime[0]>0]
        keystagemap=dict(zip(keystage,keystagetime))
        
        # Collect data instead of generating HTML
        plan_data = []
        nodeid_counter = [0]  # Use list to maintain mutable counter across recursive calls
        
        def collect_plan_data(real_queryid,level,node,parent_stageid,parent_nodeid=None):
            current_nodeid = nodeid_counter[0]
            nodeid_counter[0] += 1
            stageid = accid2stageid[int(node["metrics"][0]["accumulatorId"])][0]  if node["metrics"] is not None and len(node["metrics"])>0 and node["metrics"][0]["accumulatorId"] in accid2stageid else parent_stageid

            if stageid in shown_stageid:
                stagetime=0 if stageid not in stagemap else stagemap[stageid]
                stageParts=0 if stageid not in stagepartmap else stagepartmap[stageid]

                input_rows=0
                output_rows=0
                timemetrics={}
                input_batches=0
                output_batches=0
                other_metrics={}

                outputrows=0
                outputbatches=0
                if node["metrics"] is not None:
                    for m in node["metrics"]:

                        if m["accumulatorId"] not in accid2stageid:
                            continue
                        
                        if m["name"].endswith("block wall nanos") or m['name'].endswith("cpu nanos"):
                            continue
                            
                        
                        value=accid2stageid[m["accumulatorId"]][1]
                        stdev_value=accid2stageid[m["accumulatorId"]][2]
                        stdev_value=0 if stdev_value is None else stdev_value
                        if m["metricType"] in ['nsTiming','timing']:
                            totaltime=value/1000 if  m["metricType"] == 'timing' else value/1000000000
                            stdev_value=stdev_value/1000 if  m["metricType"] == 'timing' else stdev_value/1000000000
                            
                            timeratio= 0  if stagetime==0 else totaltime/self.executor_instances/self.executor_cores*self.taskcpus/stagetime*100
                            timeratio_query = totaltime/self.executor_instances/self.executor_cores*self.taskcpus/apptotaltime*100
                            
                            timemetrics[m["name"]] = {
                                "totaltime": totaltime,
                                "timeratio": timeratio,
                                "timeratio_query": timeratio_query,
                                "stdev": stdev_value,
                                "highlight": timeratio > 10 or timeratio_query > 10
                            }
                        elif m["name"] in ["number of output rows","number of final output rows"]:
                            output_rows = value
                            outputrows=value
                        elif m["name"] in ["number of output columnar batches","number of output batches","output_batches", "number of output vectors","number of final output vectors", "records read"]:
                            output_batches = int(value)
                            outputbatches=value
                        elif m["name"]=="number of input rows":
                            input_rows = value
                        elif m["name"] in ["number of input batches","input_batches","number of input vectors"]:
                            input_batches = int(value)
                        else:
                            other_metrics[m["name"]] = {
                                "value": value,
                                "stdev": stdev_value
                            }

                rows_per_batch = int(outputrows/outputbatches) if outputrows>0 and outputbatches>0 else 0
                stage_time_stdev=0 if stageid not in stage_time_stdev_map else stage_time_stdev_map[stageid]
                
                nodenamestr=node["nodeName"]
                if nodenamestr is None:
                    nodenamestr=""
                
                simplestring = node['simpleString']
                
                # Extract plan_id from simpleString if it matches pattern [plan_id=\d+]
                plan_id = -1
                if simplestring:
                    match = re.search(r'\[plan_id=(\d+)\]', simplestring)
                    if match:
                        plan_id = int(match.group(1))
                if nodenamestr == "Subquery" and simplestring:
                    match = re.search(r'\[id=#(\d+)\]', simplestring)
                    if match:
                        plan_id = int(match.group(1))
                
                is_conversion_node = nodenamestr in ['ColumnarToRow','RowToArrowColumnar','ArrowColumnarToRow','ArrowRowToColumnarExec','GlutenColumnarToRowExec','GlutenRowToArrowColumnar']
                
                if outputstage is not None:
                    outputstage.append({"queryid":real_queryid,"stageid":stageid,"stagetime":stagetime,"stageParts":stageParts,"nodename":nodenamestr,"output_rowcnt":outputrows,"nodename_level":" ".join(["|_" for l in range(0,level)]) + " " + nodenamestr})
                
                # Collect data structure
                node_data = {
                    "real_queryid": real_queryid,
                    "level": level,
                    "nodeid": current_nodeid,
                    "parent_nodeid": parent_nodeid,
                    "plan_id": plan_id,
                    "stageid": stageid,
                    "stagetime": stagetime,
                    "stage_time_stdev": stage_time_stdev,
                    "stageParts": stageParts,
                    "nodename": nodenamestr,
                    "is_conversion_node": is_conversion_node,
                    "is_keystage": stageid in keystagemap,
                    "keystage_color": keystagemap.get(stageid, None),
                    "color_index": stageid % 20,
                    "input_rows": input_rows,
                    "input_batches": input_batches,
                    "output_rows": output_rows,
                    "output_batches": output_batches,
                    "rows_per_batch": rows_per_batch,
                    "timemetrics": timemetrics,
                    "other_metrics": other_metrics
                }
                plan_data.append(node_data)
                    
            if node["children"] is not None:
                for c in node["children"]:
                    collect_plan_data(real_queryid, level+1,c,stageid,current_nodeid)

        # Collect all query plans
        queries_data = []
        for c in queryplans:
            collect_plan_data(c['real_queryid'],0,json.loads(c['query_plan'])[0],0)
            queries_data.append({
                "real_queryid": c['real_queryid'],
                "nodes": [n for n in plan_data if n['real_queryid'] == c['real_queryid']]
            })
        
        # Return pure data structure
        return {
            "queries": queries_data,
            "plan_nodes": plan_data,
            "queryid": queryid,
            "shown_stageid": shown_stageid if 'shown_stageid' in locals() else None,
            "apptotaltime": apptotaltime,
            "executor_instances": self.executor_instances,
            "executor_cores": self.executor_cores,
            "taskcpus": self.taskcpus
        }

    def show_query_plan(self, data=None, plot=True, show_plan_only=False, show_simple_string=False, **kwargs):
        """
        Visualize query execution plan.
        
        Args:
            data: Pre-loaded plan data (optional)
            plot (bool): Whether to create visualization
            show_plan_only (bool): Show only the plan structure
            show_simple_string (bool): Show simplified plan string
            **kwargs: Additional parameters (queryid, stageid, etc.)
        """
        if data is None:
            data = self.get_query_plan(**kwargs)
        
        if data["apptotaltime"] == 0:
            if plot:
                display(HTML("<font size=4 color=red>Error, totaltime is 0 </font>"))
            return ""
        
        # Generate colors for visualization
        colors = ["#{:02x}{:02x}{:02x}".format(int(l[0]*255),int(l[1]*255),int(l[2]*255)) for l in matplotlib.cm.get_cmap('tab20').colors]
        plan_nodes = data["plan_nodes"]
        queries = data["queries"]

        outstr = []

        # Generate HTML for each query
        for query_index, query in enumerate(queries):
            outstr.append(f"<font color=red size=4>{query['real_queryid']}</font>")
            outstr.append(f'''<label style="margin-left: 20px;">
                                <input type="checkbox" id="filter-checkbox-{query_index}" onchange="filterUnimportantOperators(this, {query_index})">
                                Hide unimportant operators
                            </label>''')
            outstr.append('''<style>
                                .queryplan-table {
                                    border-collapse: collapse;
                                    width: 100%;
                                    margin: 10px 0;
                                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                                }
                                .queryplan-table td, .queryplan-table th {
                                    font-family: "Courier New", monospace;
                                    font-size: 12px;
                                    padding: 8px 12px;
                                    border: 1px solid #ddd;
                                    text-align: left;
                                }
                                .queryplan-table th {
                                    background-color: #f5f5f5;
                                    font-weight: bold;
                                    position: sticky;
                                    top: 0;
                                    z-index: 10;
                                }
                                .queryplan-table tr:hover {
                                    background-color: #f9f9f9;
                                    transition: background-color 0.2s ease;
                                }
                                .queryplan-table tr:nth-child(even) {
                                    background-color: #fafafa;
                                }
                                .queryplan-table .nested-table {
                                    border: none;
                                    margin: 0;
                                    padding: 0;
                                    width: 100%;
                                }
                                .queryplan-table .nested-table td {
                                    border: none;
                                    border-bottom: 1px solid #ddd;
                                    padding: 2px 4px;
                                }
                                .toggle-btn {
                                    cursor: pointer;
                                    user-select: none;
                                    font-weight: bold;
                                    font-size: 14px;
                                    color: #0066cc;
                                    text-align: center;
                                    width: 20px;
                                    display: inline-block;
                                }
                                .toggle-btn:hover {
                                    color: #004499;
                                }
                                tr.collapsed {
                                    display: none;
                                }
                            </style>
                            <script>
                                function toggleRows(btn, rowIndex, currentLevel) {
                                    const table = btn.closest('table');
                                    const rows = Array.from(table.querySelectorAll('tr'));
                                    const currentRowIndex = rows.findIndex(r => r.contains(btn));
                                    const currentRow = rows[currentRowIndex];
                                    const currentStageId = currentRow.querySelector('td:nth-child(2)').textContent.trim();
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
                                            
                                            const rowStageId = row.querySelector('td:nth-child(2)').textContent.trim();
                                            
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
                                                
                                                const rowStageId = row.querySelector('td:nth-child(2)').textContent.trim();
                                                
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
                                            
                                            // Stop when we reach a row at same or lower level
                                            if (rowLevel <= currentLevel) break;
                                            
                                            // Toggle visibility of child rows
                                            if (isCollapsed) {
                                                row.classList.remove('collapsed');
                                                // Also reset any toggle buttons in child rows to expanded state
                                                const childBtn = row.querySelector('.toggle-btn');
                                                if (childBtn) childBtn.textContent = '−';
                                            } else {
                                                row.classList.add('collapsed');
                                            }
                                        }
                                        
                                        btn.textContent = isCollapsed ? '−' : '+';
                                    }
                                }
                                
                                function hasKeystageDescendant(table, startIndex, currentLevel) {
                                    const rows = Array.from(table.querySelectorAll('tr[data-level]'));
                                    for (let i = startIndex + 1; i < rows.length; i++) {
                                        const row = rows[i];
                                        const levelAttr = row.getAttribute('data-level');
                                        if (levelAttr === null) continue;
                                        
                                        const rowLevel = parseInt(levelAttr);
                                        if (rowLevel <= currentLevel) break;
                                        
                                        if (row.getAttribute('data-is-keystage') === 'true') {
                                            return true;
                                        }
                                    }
                                    return false;
                                }
                                
                                function filterUnimportantOperators(checkbox, queryIndex) {
                                    const tables = document.querySelectorAll('.queryplan-table');
                                    const table = tables[queryIndex];
                                    if (!table) return;
                                    
                                    const rows = Array.from(table.querySelectorAll('tr[data-level]'));
                                    
                                    if (checkbox.checked) {
                                        // Hide unimportant operators using smart collapse logic
                                        rows.forEach((row, index) => {
                                            const toggleBtn = row.querySelector('.toggle-btn');
                                            if (toggleBtn && toggleBtn.textContent === '−') {
                                                const level = parseInt(row.getAttribute('data-level'));
                                                const isKeystage = row.getAttribute('data-is-keystage') === 'true';
                                                
                                                // Only collapse unimportant operators (non-keystage)
                                                if (!isKeystage) {
                                                    const currentStageId = row.querySelector('td:nth-child(2)').textContent.trim();
                                                    
                                                    // Check if there are keystages in different stages
                                                    let hasKeystageInDifferentStage = false;
                                                    
                                                    for (let i = index + 1; i < rows.length; i++) {
                                                        const descendantRow = rows[i];
                                                        const descendantLevel = parseInt(descendantRow.getAttribute('data-level'));
                                                        
                                                        if (descendantLevel <= level) break;
                                                        
                                                        const descendantStageId = descendantRow.querySelector('td:nth-child(2)').textContent.trim();
                                                        
                                                        // Check if we're in a different stage and it's a keystage
                                                        if (descendantStageId !== currentStageId &&
                                                            descendantRow.getAttribute('data-is-keystage') === 'true') {
                                                            hasKeystageInDifferentStage = true;
                                                            break;
                                                        }
                                                    }
                                                    
                                                    // Trigger the collapse - the toggleRows function will handle the smart logic
                                                    toggleBtn.click();
                                                }
                                            }
                                        });
                                    } else {
                                        // Show all operators - expand all collapsed rows
                                        rows.forEach(row => {
                                            const toggleBtn = row.querySelector('.toggle-btn');
                                            if (toggleBtn && toggleBtn.textContent === '+') {
                                                toggleBtn.click();
                                            }
                                        });
                                    }
                                }
                            </script>
                            <table class="queryplan-table">''')
            
            # Add table headers
            if not show_plan_only:
                outstr.append('''<tr>
                                    <th></th>
                                    <th>Stage ID</th>
                                    <th>Stage Time</th>
                                    <th>Parts</th>
                                    <th>Operator</th>
                                    <th>Input Rows</th>
                                    <th>Input Batches</th>
                                    <th>Output Rows</th>
                                    <th>Output Batches</th>
                                    <th>Rows/Batch</th>
                                    <th>Time Metric</th>
                                    <th>Time (%stage, %total, stdev)</th>
                                    <th>Other Metric</th>
                                    <th>Value (stdev)</th>
                                </tr>''')
            else:
                outstr.append('''<tr>
                                    <th></th>
                                    <th>Stage ID</th>
                                    <th>Stage Time</th>
                                    <th>Partitions</th>
                                    <th>Operator</th>
                                    <th>Output Rows</th>
                                </tr>''')
            
            # Add rows for each node in this query
            row_id = 0
            for node in query['nodes']:
                row_id += 1
                fontcolor = f"color:#{node['keystage_color']}00;font-weight:bold" if node['is_keystage'] else "color:#000000"
                
                # Format node name
                nodename = node['nodename']
                if node['is_conversion_node']:
                    nodename = f'<span style="color: green; background-color: #ffff42">{nodename}</span>'
                
                nodestr = "".join([f"<span style='color:#cccccc'>|</span>" for _ in range(0, node['level'])]) + " " + nodename
                if show_simple_string:
                    nodestr = nodestr + "<br>\n" + node['simple_string']
                
                # Format metrics
                input_rowcntstr = f"{node['input_rows']/1000/1000:,.1f} M" if node['input_rows'] > 0 else ""
                output_rowcntstr = f"{node['output_rows']/1000/1000:,.1f} M" if node['output_rows'] > 0 else ""
                input_columnarbatch = f"{node['input_batches']:,d}" if node['input_batches'] > 0 else ""
                output_columnarbatch = f"{node['output_batches']:,d}" if node['output_batches'] > 0 else ""
                output_row_batch = f"{node['rows_per_batch']:,d}" if node['rows_per_batch'] > 0 else ""
                
                if not show_plan_only:
                    # Build time metrics table
                    timenametable = '<table class="nested-table">\n'
                    for name in sorted(node['timemetrics'].keys()):
                        tm = node['timemetrics'][name]
                        if tm['highlight']:
                            time_str = f"<span style='background-color:#ffff42; padding:2px 4px; border-radius:3px;'>{tm['totaltime']:.2f}s ({tm['timeratio']:.1f}%, {tm['timeratio_query']:.1f}%, {tm['stdev']:.2f})</span>"
                        else:
                            time_str = f"{tm['totaltime']:.2f}s ({tm['timeratio']:.1f}%, {tm['timeratio_query']:.1f}%, {tm['stdev']:.2f})"
                        timenametable += f"<tr><td>{name}</td><td>{time_str}</td></tr>"
                    timenametable += "</table>\n"
                    
                    # Build other metrics table
                    othertable = '<table class="nested-table">\n'
                    for name in sorted(node['other_metrics'].keys()):
                        om = node['other_metrics'][name]
                        value = om['value']
                        stdev = om['stdev']
                        if value > 1000000000:
                            metric_str = f"{value/1000000000:,.1f} G ({stdev/1000000000:,.1f})"
                        elif value > 1000000:
                            metric_str = f"{value/1000000:,.1f} M ({stdev/1000000:,.1f})"
                        elif value > 1000:
                            metric_str = f"{value/1000:,.1f} K ({stdev/1000:,.1f})"
                        else:
                            metric_str = f"{int(value):,d} ({stdev:,.1f})"
                        othertable += f"<tr><td>{name}</td><td>{metric_str}</td></tr>"
                    othertable += "</table>\n"
                    
                    # Determine if this node has children (check if next node has higher level)
                    node_index = query['nodes'].index(node)
                    has_children = (node_index + 1 < len(query['nodes']) and
                                query['nodes'][node_index + 1]['level'] > node['level'])
                    toggle_btn = f"<span class='toggle-btn' onclick='toggleRows(this, {row_id}, {node['level']})'>−</span>" if has_children else ""
                    
                    outstr.append(f"<tr data-level='{node['level']}' data-is-keystage='{str(node['is_keystage']).lower()}'><td style='{fontcolor}'>{toggle_btn}</td>" +
                                    f"<td style='{fontcolor}'>{node['stageid']}</td>" +
                                    f"<td style='{fontcolor}'> {node['stagetime']}({node['stage_time_stdev']}) </td>" +
                                    f"<td style='{fontcolor}'> {node['stageParts']} </td>" +
                                    f"<td style='text-align:left; background-color:{colors[node['color_index']]}'>" + nodestr + f"</td>" +
                                    f"<td style='{fontcolor}'> {input_rowcntstr} </td>" +
                                    f"<td style='{fontcolor}'> {input_columnarbatch} </td>" +
                                    f"<td style='{fontcolor}'> {output_rowcntstr} </td>" +
                                    f"<td style='{fontcolor}'> {output_columnarbatch} </td>" +
                                    f"<td style='{fontcolor}'> {output_row_batch} </td>" +
                                    f"<td style='{fontcolor}' colspan=2> {timenametable} </td>" +
                                    f"<td style='{fontcolor}' colspan=2> {othertable} </td>" +
                                    "</tr>")
                else:
                    # Determine if this node has children (check if next node has higher level)
                    node_index = query['nodes'].index(node)
                    has_children = (node_index + 1 < len(query['nodes']) and
                                query['nodes'][node_index + 1]['level'] > node['level'])
                    toggle_btn = f"<span class='toggle-btn' onclick='toggleRows(this, {row_id}, {node['level']})'>−</span>" if has_children else ""
                    
                    outstr.append(f"<tr data-level='{node['level']}' data-is-keystage='{str(node['is_keystage']).lower()}'><td style='{fontcolor}'>{toggle_btn}</td>" +
                                    f"<td style='{fontcolor}'>{node['stageid']}</td>" +
                                    f"<td style='{fontcolor}'> {node['stagetime']} </td>" +
                                    f"<td style='{fontcolor}'> {node['stageParts']} </td>" +
                                    f"<td style='text-align:left; background-color:{colors[node['color_index']]}'>" + nodestr + f"</td>" +
                                    f"<td style='{fontcolor}'> {output_rowcntstr} </td></tr>")
            
            outstr.append("</table>")
        html_result = " ".join(outstr)
        if plot:
            display(HTML(html_result))
        return html_result

    def print_query_plan_puml(self, query_plan=None, **kwargs):
        if query_plan is None:
            query_plan = self.get_query_plan(**kwargs)
        generator = PlantUMLGenerator()
        plantuml_content = generator.generate_plantuml(query_plan)
        return plantuml_content

    def show_query_info(self, queryid):
        """
        Display comprehensive information about a specific query.
        
        Args:
            queryid (int): Query ID to analyze
        """
        display(HTML("<font color=red size=7 face='Courier New'><b> time stat info </b></font>",))
        tmp=self.get_query_time(queryid=queryid)
        display(HTML("<font color=red size=7 face='Courier New'><b> stage stat info </b></font>",))
        display(self.get_stage_stat(queryid=queryid))
        display(HTML("<font color=red size=7 face='Courier New'><b> query plan </b></font>",))
        self.get_query_plan(queryid=queryid)
        display(HTML("<font color=red size=7 face='Courier New'><b> stage hist info </b></font>",))
        self.show_Stages_hist(queryid=queryid)
        display(HTML("<font color=red size=7 face='Courier New'><b> time info </b></font>",))
        display(self.show_time_metric(queryid=queryid))
        display(HTML("<font color=red size=7 face='Courier New'><b> operator and rowcount </b></font>",))
        display(self.get_metric_input_rowcnt(queryid=queryid))
        display(self.get_metric_output_rowcnt(queryid=queryid))

    def get_stage_stat(self, **kwargs):
        """
        Get detailed statistics for stages.
        
        Args:
            **kwargs: Filtering parameters (queryid, stageid, etc.)
        
        Returns:
            pandas.DataFrame: Stage statistics
        """
        if self.df is None:
            self.load_data()

        queryid=kwargs.get("queryid",None)

        if queryid and type(queryid)==int:
            queryid = [queryid,]
            
        df=self.df.where(F.col("real_queryid").isin(queryid)).where(F.col("Event")=='SparkListenerTaskEnd')
        
        inputsize = df.select("real_queryid","Stage ID","Executor ID", "Task ID") \
                      .join(self.metric_df,on="Task ID") \
                      .where("Name='input size in bytes' or Name='size of files read'") \
                      .groupBy("Stage ID") \
                      .agg(F.round(F.sum("Update")/1024/1024/1024,2).alias("input read"))
        
        return df.groupBy("Job ID","Stage ID").agg(
            F.round(F.sum(F.col("Finish Time")-F.col("Launch Time"))/1000/self.executor_instances/self.executor_cores*self.taskcpus,1).alias("elapsed time"),
            F.round(F.sum(F.col("Disk Bytes Spilled"))/1024/1024/1024,1).alias("disk spilled"),
            F.round(F.sum(F.col("Memory Bytes Spilled"))/1024/1024/1024,1).alias("mem spilled"),
            F.round(F.sum(F.col("Local Bytes Read"))/1024/1024/1024,1).alias("local read"),
            F.round(F.sum(F.col("Remote Bytes Read"))/1024/1024/1024,1).alias("remote read"),
            F.round(F.sum(F.col("Shuffle Bytes Written"))/1024/1024/1024,1).alias("shuffle write"),
            F.round(F.sum(F.col("Executor Deserialize Time"))/1000,1).alias("deseri time"),
            F.round(F.sum(F.col("Fetch Wait Time"))/1000,1).alias("fetch wait time"),
            F.round(F.sum(F.col("Shuffle Write Time"))/1000000000,1).alias("shuffle write time"),
            F.round(F.sum(F.col("Result Serialization Time"))/1000,1).alias("seri time"),
            F.round(F.sum(F.col("Getting Result Time"))/1000,1).alias("get result time"),
            F.round(F.sum(F.col("JVM GC Time"))/1000,1).alias("gc time"),
            F.round(F.sum(F.col("Executor CPU Time"))/1000000000,1).alias("exe cpu time")    
            ).join(inputsize,on=["Stage ID"],how="left").orderBy("Stage ID").toPandas()
    

    def get_metrics_by_node(self, node_name):
        """
        Get metrics for a specific query plan node.
        
        Args:
            node_name (str): Name of the query plan node
        
        Returns:
            pandas.DataFrame: Metrics for the specified node
        """
        if self.df is None:
            self.load_data()
        
        if type(node_name)==str:
            node_name=[node_name]
        metrics=self.queryplans

        def get_all_node(s,node_name):
            import json
            import uuid
            coalesce=[]
            metricsid=[0]
            def get_node(root,node_name):
                node_name_dict=json.loads(node_name)
                if root['nodeName'] in node_name_dict:
                    metricsid[0]+=1
                    for l in root["metrics"]:
                        coalesce.append({"ID":l['accumulatorId'],
                                         "Unit":l["metricType"],
                                         "metricName":l['name'],
                                         "nodeName":root["nodeName"],
                                         "nodeID":metricsid[0]})
                if root["children"] is not None:
                    for c in root["children"]:
                        get_node(c,node_name)

            children=json.loads(s)
            for row in children:
                get_node(row,node_name)
            
            return coalesce
                        
        return_schema = ArrayType(
        StructType([
                StructField("ID", IntegerType(), True),
                StructField("Unit", StringType(), True),
                StructField("metricName", StringType(), True),
                StructField("nodeName", StringType(), True),
                StructField("nodeID", IntegerType(), True)
            ])
        )
        get_all_node_udf = udf(get_all_node, return_schema)
        allnodes=metrics.select("real_queryid","queryid",get_all_node_udf("query_plan",F.lit(json.dumps(node_name))).alias("node_info"))
        allnodescol=allnodes.select("real_queryid","queryid",F.explode("node_info").alias("col")).select("col.*")

        df=self.df.where("Event='SparkListenerTaskEnd'").select("queryid","real_queryid",'Stage ID','Task ID','Job ID').join(self.metric_df,on="Task ID")
        df=df.join(allnodescol,on=["ID"],how="right")
        coalesce=allnodescol.select("metricName").distinct().collect()
        shufflemetric=[l['metricName'] for l in coalesce]

        for m in shufflemetric:
            df=df.withColumn(m,F.when(F.col("metricName")==m,F.col("Update")).otherwise(None))

        metricdfs = df.groupBy("real_queryid","nodeID","Stage ID").agg(*[F.stddev(l).alias(l+"_stddev") for l in shufflemetric],
                                                                        *[F.mean(l).alias(l+"_mean") for l in shufflemetric],
                                                                        *[F.mean(l).alias(l) if l.startswith("avg") else F.sum(l).alias(l) for l in shufflemetric])
        
        stagetimedf=self.df.where("Event='SparkListenerTaskEnd'").groupBy("Stage ID").agg(F.count("*").alias("partnum"),F.round(F.sum(F.col("Finish Time")-F.col("Launch Time"))/1000,2).alias("ElapsedTime"))
        
        return metricdfs.join(stagetimedf,on="Stage ID")

    def _print_real_queryid(self, ax, dataset):
        """
        Helper method to print query IDs on plot.
        
        Args:
            ax: Matplotlib axis object
            dataset: Data to annotate
        """
        ax.axes.get_xaxis().set_ticks([])

        ymin, ymax = ax.get_ybound()

        real_queryid=list(dataset['real_queryid'])
        s=real_queryid[0]
        lastx=0
        for idx,v in enumerate(real_queryid):
            if v!=s:
                xmin = xmax = idx-1+0.5
                l = mlines.Line2D([xmin,xmax], [ymin,ymax],color="green")
                ax.add_line(l)
                ax.text(lastx+(xmin-lastx)/2-0.25,ymin-(ymax-ymin)/20,f"{s}",size=20)
                s=v
                lastx=xmin

    def get_shuffle_stat(self, **kwargs):
        """
        Get shuffle statistics for the application.
        
        Args:
            **kwargs: Filtering parameters
        
        Returns:
            pandas.DataFrame: Shuffle statistics
        """
        if self.df is None:
            self.load_data()
            
        shufflesize=kwargs.get("shuffle_size",1000000)
        queryid=kwargs.get("queryid",None)
        if queryid is not None:
            if type(queryid) is str or type(queryid) is int:
                queryid=[queryid,]

        exchangedf=self.get_metrics_by_node(["ColumnarExchange","ColumnarExchangeAdaptor"])
        exchangedf.cache()
        if exchangedf.count() == 0:
            return (None, None)

        mapdf=exchangedf.where("`time to split` is not null").select("nodeID",F.col("Stage ID").alias("map_stageid"),"real_queryid",F.floor(F.col("time to split")/F.col("time to split_mean")).alias("map_partnum"),"time to compress","time to split","shuffle write time","time to spill",'shuffle records written','data size','shuffle bytes written','shuffle bytes written_mean','shuffle bytes written_stddev','shuffle bytes spilled','number of input rows','number of input batches')
        reducerdf=exchangedf.where("`time to split` is null").select("nodeID",F.col("Stage ID").alias("reducer_stageid"),"real_queryid",'local blocks read','local bytes read',F.floor(F.col("records read")/F.col("records read_mean")).alias("reducer_partnum"),(F.col('avg read batch num rows')/10).alias("avg read batch num rows"),'remote bytes read','records read','remote blocks read',(F.col("number of output rows")/F.col("records read")).alias("avg rows per split recordbatch"))
        shuffledf=mapdf.join(reducerdf,on=["nodeID","real_queryid"],how="full")
        if queryid is not None:
            shuffledf=shuffledf.where(F.col("real_queryid").isin(queryid))
        shuffle_pdf=shuffledf.where("`shuffle bytes written`>1000000").orderBy("real_queryid","map_stageid","nodeID").toPandas()
        if shuffle_pdf.shape[0] == 0:
            return (shuffledf, None)

        shuffle_pdf["shuffle bytes written"]=shuffle_pdf["shuffle bytes written"]/1000000000
        shuffle_pdf["data size"]=shuffle_pdf["data size"]/1000000000
        shuffle_pdf["shuffle bytes written_mean"]=shuffle_pdf["shuffle bytes written_mean"]/1000000
        shuffle_pdf["shuffle bytes written_stddev"]=shuffle_pdf["shuffle bytes written_stddev"]/1000000
        ax=shuffle_pdf.plot(y=["avg read batch num rows",'avg rows per split recordbatch'],figsize=(30,8),style="-*",title="average batch size after split")
        self._print_real_queryid(ax,shuffle_pdf)
        shuffle_pdf["split_ratio"]=shuffle_pdf["records read"]/shuffle_pdf['number of input batches']
        ax=shuffle_pdf.plot(y=["split_ratio","records read"],secondary_y=["records read"],figsize=(30,8),style="-*",title="Split Ratio")
        self._print_real_queryid(ax,shuffle_pdf)
        shuffle_pdf["compress_ratio"]=shuffle_pdf["data size"]/shuffle_pdf['shuffle bytes written']
        ax=shuffle_pdf.plot(y=["shuffle bytes written","compress_ratio"],secondary_y=["compress_ratio"],figsize=(30,8),style="-*",title="compress ratio")
        self._print_real_queryid(ax,shuffle_pdf)
        shufflewritepdf=shuffle_pdf
        ax=shufflewritepdf.plot.bar(y=["shuffle write time","time to spill","time to compress","time to split"],stacked=True,figsize=(30,8),title="split time + shuffle write time vs. shuffle bytes written")
        ax=shufflewritepdf.plot(ax=ax,y=["shuffle bytes written"],secondary_y=["shuffle bytes written"],style="-*")
        self._print_real_queryid(ax,shufflewritepdf)
        shuffle_pdf['avg input batch size']=shuffle_pdf["number of input rows"]/shuffle_pdf["number of input batches"]
        ax=shuffle_pdf.plot(y=["avg input batch size"],figsize=(30,8),style="b-*",title="average input batch size")
        ax=shuffle_pdf.plot.bar(ax=ax,y=['number of input rows'],secondary_y=True)
        self._print_real_queryid(ax,shuffle_pdf)
        
        metrics=self.queryplans

        def get_all_node(s):
            import json
            import uuid
            coalesce=[]
            metricsid=[0]
            def get_node(root):
                if root['nodeName'] in ["ColumnarExchange","ColumnarExchangeAdaptor"]:
                    metricsid[0]+=1
                    for l in root["metrics"]:
                        coalesce.append({"ID":l['accumulatorId'],
                                         "Unit":l["metricType"],
                                         "metricName":l['name'],
                                         "nodeName":root["nodeName"],
                                         "nodeID":metricsid[0],
                                         "simpleString":root["simpleString"]})
                if root["children"] is not None:
                    for c in root["children"]:
                        get_node(c)

            children=json.loads(s)
            for row in children:
                get_node(row)
            
            return coalesce
                        
        return_schema = ArrayType(
        StructType([
                StructField("ID", IntegerType(), True),
                StructField("Unit", StringType(), True),
                StructField("metricName", StringType(), True),
                StructField("nodeName", StringType(), True),
                StructField("nodeID", IntegerType(), True),
                StructField("simpleString", StringType(), True)
            ])
        )
        get_all_node_udf = udf(get_all_node, return_schema)
        allnodes=metrics.select("real_queryid","queryid",get_all_node_udf("query_plan").alias("node_info"))
        allnodescol=allnodes.select("real_queryid","queryid",F.explode("node_info").alias("col")).select("col.*")


        coalesce= allnodescol.select("ID","Unit","metricName","nodeName","nodeID","simpleString").collect()

        tps={}
        for r in coalesce:
            rx=re.search(r"\[OUTPUT\] List\((.*)\)",r["simpleString"])
            if rx:
                if r['nodeID'] not in tps:
                    tps[r['nodeID']]={}
                    fds=rx.group(1).split(", ")
                    for f in fds:
                        if f.endswith("Type"):
                            tp=re.search(r":(.+Type)",f).group(1)
                            if tp not in tps[r['nodeID']]:
                                tps[r['nodeID']][tp]=1
                            else:
                                tps[r['nodeID']][tp]+=1
        if len(tps)>0:
            typedf=pandas.DataFrame(tps).T.reset_index()
            typedf=typedf.fillna(0)
            shuffle_pdf=pandas.merge(shuffle_pdf,typedf,left_on="nodeID",right_on="index")
            shufflewritepdf=shuffle_pdf
            ax=shufflewritepdf.plot.bar(y=["number of input rows"],stacked=True,figsize=(30,8),title="rows vs. shuffle data type")
            ax=shufflewritepdf.plot(ax=ax,y=list(typedf.columns[1:]),secondary_y=list(typedf.columns[1:]),style="-o")
            self._print_real_queryid(ax,shufflewritepdf)
            ax=shufflewritepdf.plot.bar(y=["time to split"],stacked=True,figsize=(30,8),title="split time vs. shuffle data type")
            ax=shufflewritepdf.plot(ax=ax,y=list(typedf.columns[1:]),secondary_y=list(typedf.columns[1:]),style="-o")
            self._print_real_queryid(ax,shufflewritepdf)

        
        
        shufflewritepdf.plot(x="shuffle bytes written",y=["shuffle write time","time to split"],figsize=(30,8),style="*")
        shufflewritepdf["avg shuffle batch size after split"]=shufflewritepdf["shuffle bytes written"]*1000000/shufflewritepdf['records read']
        shufflewritepdf["avg raw batch size after split"]=shufflewritepdf["data size"]*1000000/shufflewritepdf['records read']
        ax=shufflewritepdf.plot(y=["avg shuffle batch size after split","avg raw batch size after split","shuffle bytes written"],secondary_y=["shuffle bytes written"],figsize=(30,8),style="-*",title="avg batch KB after split")
        self._print_real_queryid(ax,shufflewritepdf)
        shufflewritepdf["avg batch# per splitted partition"]=shufflewritepdf['records read']/(shufflewritepdf['local blocks read']+shufflewritepdf['remote blocks read'])
        ax=shufflewritepdf.plot(y=["avg batch# per splitted partition",'records read'],secondary_y=['records read'],figsize=(30,8),style="-*",title="avg batch# per splitted partition")
        self._print_real_queryid(ax,shufflewritepdf)
        fig, ax = plt.subplots(figsize=(30,8))
        ax.set_title('shuffle wite bytes with stddev')
        ax.errorbar(x=shuffle_pdf.index,y=shuffle_pdf['shuffle bytes written_mean'], yerr=shuffle_pdf['shuffle bytes written_stddev'], linestyle='None', marker='o')
        self._print_real_queryid(ax,shuffle_pdf)
        shuffle_pdf['record batch per mapper per reducer']=shuffle_pdf['records read']/(shuffle_pdf["map_partnum"]*shuffle_pdf['reducer_partnum'])
        ax=shuffle_pdf.plot(y=["record batch per mapper per reducer"],figsize=(30,8),style="b-*",title="record batch per mapper per reducer")
        self._print_real_queryid(ax,shuffle_pdf)
        
        inputsize = self.df.where("Event='SparkListenerTaskEnd'").select("Stage ID","Executor ID", "Task ID").join(self.metric_df,on="Task ID") \
              .where("Name='input size in bytes' or Name='size of files read'") \
              .groupBy("Task ID") \
              .agg((F.sum("Update")).alias("input read"))
        stageinput=self.df.where("event='SparkListenerTaskEnd'" )\
                                .join(inputsize,on=["Task ID"],how="left")\
                                .fillna(0) \
                                .select(F.col('Host'), F.col("real_queryid"),F.col('Stage ID'),F.col('Task ID'),
                                        F.round((F.col('Finish Time')/1000-F.col('Launch Time')/1000),2).alias('elapsedtime'),
                                        F.round((F.col('`input read`')+F.col('`Bytes Read`')+F.col('`Local Bytes Read`')+F.col('`Remote Bytes Read`'))/1024/1024,2).alias('input'))
        baisstage=stageinput.groupBy("real_queryid","Stage ID").agg(F.mean("elapsedtime").alias("elapsed"),F.mean("input").alias("input"),
                                                            (F.stddev("elapsedtime")).alias("elapsedtime_err"),
                                                            (F.stddev("input")).alias("input_err"),
                                                            (F.max("elapsedtime")-F.mean("elapsedtime")).alias("elapsed_max"),
                                                            (F.mean("elapsedtime")-F.min("elapsedtime")).alias("elapsed_min"),
                                                            (F.max("input")-F.mean("input")).alias("input_max"),
                                                            (F.mean("input")-F.min("input")).alias("input_min")).orderBy("real_queryid","Stage ID")
        dfx=baisstage.toPandas()
        fig, ax = plt.subplots(figsize=(30,8))
        ax.set_title('input size')
        ax.errorbar(x=dfx.index,y=dfx['input'], yerr=dfx['input_err'], fmt='ok', ecolor='red', lw=3)
        ax.errorbar(x=dfx.index,y=dfx['input'],yerr=[dfx['input_min'],dfx['input_max']],
                     fmt='.k', ecolor='gray', lw=1)
        self._print_real_queryid(ax,dfx)
        
        fig, ax = plt.subplots(figsize=(30,8))
        ax.set_title('stage time')

        ax.errorbar(x=dfx.index,y=dfx['elapsed'], yerr=dfx['elapsedtime_err'], fmt='ok', ecolor='red', lw=5)
        ax.errorbar(x=dfx.index,y=dfx['elapsed'],yerr=[dfx['elapsed_min'],dfx['elapsed_max']],
                     fmt='.k', ecolor='gray', lw=1)

        self._print_real_queryid(ax,dfx)
        return (shuffle_pdf,dfx)

    def get_stages_w_odd_partitions(self, **kwargs):
        """
        Identify stages with partition counts that don't align with executor configuration.
        
        This helps identify potential inefficiencies where partition count doesn't
        match the parallelism of the cluster.
        
        Args:
            **kwargs: Additional filtering parameters
        
        Returns:
            pandas.DataFrame: Stages with odd partition counts
        """
        if appals.df is None:
            appals.load_data()
        return appals.df.where("Event='SparkListenerTaskEnd'")\
                    .groupBy("Stage ID","real_queryid")\
                    .agg((F.sum(F.col('Finish Time')-F.col('Launch Time'))/1000).alias("elapsed time"),
                         F.count('*').alias('partitions'))\
                    .where(F.col("partitions")%(appals.executor_cores*appals.executor_instances/appals.taskcpus)!=0)\
                    .orderBy(F.desc("elapsed time")).toPandas()
    
    def compare_query(appals,queryid,appbaseals):
        print(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~Query{queryid}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        appals.show_critical_path_time_breakdown(queryid=22)
        s1=appals.get_stage_stat(queryid=queryid)
        s2=appbaseals.get_stage_stat(queryid=queryid)
        ls=s1[['Stage ID','elapsed time']]
        ls.columns=['l sid','l time']
        rs=s2[['Stage ID','elapsed time']]
        rs.columns=['r sid','r time']
        js=ls.join(rs)
        js['gap']=js['r time'] - js['l time']
        js['gap']=js['gap'].round(2)
        display(js)
        display(s1)
        display(s2)
        stagesmap={}
        for x in range(0,min(len(s1),len(s2))):
            stagesmap[s1['Stage ID'][x]]=s2['Stage ID'][x]
        totaltime=sum(s1['elapsed time'])
        acctime=0
        s1time=s1.sort_values("elapsed time",ascending=False,ignore_index=True)
        ldfx=appals.get_metric_output_rowcnt(queryid=queryid)
        rdfx=appbaseals.get_metric_output_rowcnt(queryid=queryid)

        for x in range(0,len(s1time)):
            sid1=int(s1time['Stage ID'][x])
            sid2=int(stagesmap[sid1])
            print(f"============================================================")
            display(ldfx[ldfx['Stage ID']==sid1])
            display(rdfx[ldfx['Stage ID']==sid2])
            print(f" Gazelle  Query {queryid}  Stage {sid1}")
            xf=appals.get_query_plan(stageid=sid1,show_simple_string=True)
            print(f" Photon  Query {queryid}  Stage {sid2}")
            xf=appbaseals.get_query_plan(stageid=sid2,show_simple_string=True)
            acctime+=s1time['elapsed time'][x]
            if acctime/totaltime>=0.9:
                break

# Note: notlist and comp_spark_conf are imported from sparklog.py
    
    def show_peak_memory(appals):
        pdf=appals.df.where("event='SparkListenerTaskEnd'").select("`Finish Time`","`Launch Time`","Stage ID","`Peak Execution Memory`").groupBy("Stage ID").agg(F.min("Launch Time").alias("Launch Time"),(F.max("Peak Execution Memory")/1000000000*appals.executor_cores).alias("Peak Execution Memory")).orderBy("Stage ID").toPandas()
        new_rows = []
        for i in range(len(pdf)):
            # Add original row
            new_rows.append(pdf.iloc[i].copy())
            
            # Add duplicate row with modified launch time
            if i < len(pdf) - 1:  # If not the last row
                duplicate_row = pdf.iloc[i].copy()
                duplicate_row['Launch Time'] = pdf.iloc[i + 1]['Launch Time'] - 1
                new_rows.append(duplicate_row)

        # Create new dataframe from all rows except the last one
        pdf = pandas.DataFrame(new_rows[:-1])
        pdf = pdf.reset_index(drop=True)
        pdf.plot(x='Launch Time',y=['Peak Execution Memory'],figsize=(30,8))
    def compare_app(app2,**kwargs):
        output=[]
        
        lbasedir=kwargs.get("basedir",app2.basedir)
        r_appid=kwargs.get("r_appid",app2.appid)
        
        app=kwargs.get("rapp",Application_Run(r_appid,basedir=lbasedir))

        show_queryplan_diff=kwargs.get("show_queryplan_diff",True)
        
        queryids=kwargs.get("queryids",None)
        
        appals=app.analysis["app"]["als"]
        appals2=app2.analysis["app"]["als"]

        out=appals.get_query_time(plot=False)
        out2=appals2.get_query_time(plot=False)

        lrun=app.appid
        rrun=app2.appid
        cmpcolumns=['runtime','shuffle_write','f_wait_time','input read','acc_task_time','output rows']
        outcut=out[cmpcolumns]
        out2cut=out2[cmpcolumns]
        cmp=outcut.join(out2cut,lsuffix='_'+lrun,rsuffix='_'+rrun)

        pdsout=pandas.DataFrame(outcut.sum(),columns=[lrun])
        pdsout2=pandas.DataFrame(out2cut.sum(),columns=[rrun])
        pdstime=pdsout.join(pdsout2)

        print("sar metric")
        sardf=app.get_sar_stat(**kwargs)
        sardf2=app2.get_sar_stat(**kwargs)
        
        def get_sar_agg(sardf):
            aggs=[]
            for x in sardf.index:
                if "total" in x:
                    aggs.append(sardf.loc[x].sum())
                elif "max" in x:
                    aggs.append(sardf.loc[x].max())
                else:
                    aggs.append(sardf.loc[x].mean())

            sardf['agg']=aggs
            return sardf
        sardf=get_sar_agg(sardf)
        sardf2=get_sar_agg(sardf2)
        #in case we compare two clusters
        sardf2.columns=sardf.columns

        sarcolumns=sardf.columns
        sarcmp=sardf.join(sardf2,lsuffix='_'+lrun,rsuffix='_'+rrun)
        sarsum=sarcmp[["agg_"+lrun,"agg_"+rrun]]

        sarsum.columns=[lrun,rrun]
        
        summary=pandas.concat([pdstime,sarsum])
        if showemon:
            summary=pandas.concat([summary,emonsum])
            
        summary["diff"]=numpy.where(summary[rrun] > 0, summary[lrun]/summary[rrun]-1, 0)
        
        
        def highlight_diff(x):
            styles=[]
            mx=x.max()
            mn=x.min()
            mx=max(mx,-mn,0.2)
            for j in x.index:
                m1=(x[j])/mx*100 if x[j]!=None else 0
                if m1>0:
                    styles.append(f'width: 400px ; background-image: linear-gradient(to right, transparent 50%, #5fba7d 50%, #5fba7d {50+m1/2}%, transparent {50+m1/2}%)')
                else:
                    styles.append(f'width: 400px ;background-image: linear-gradient(to left, transparent 50%, #f1a863 50%, #f1a863 {50-m1/2}%, transparent {50-m1/2}%)')
            return styles

        output.append(summary.style.apply(highlight_diff,subset=['diff']).format({lrun:"{:,.2f}",rrun:"{:,.2f}",'diff':"{:,.2%}"}).render())

        cmp_plot=cmp
        cmp_plot['diff']=cmp_plot['runtime_'+lrun]-cmp_plot['runtime_'+rrun]

        pltx=cmp_plot.sort_values(by='diff',axis=0).plot.bar(y=['runtime_'+lrun,'runtime_'+rrun],figsize=(30,8))
        better_num=duckdb.query('''select count(*) from cmp_plot where diff>0''').df()['count(*)'][0]
        pltx.text(0.1, 0.8,'{:d} queries are better'.format(better_num), ha='center', va='center', transform=pltx.transAxes)

        df1 = pandas.DataFrame('', index=cmp.index, columns=cmpcolumns)
        for l in cmpcolumns:
            for j in cmp.index:
                df1[l][j]=[cmp[l+"_"+lrun][j],cmp[l+"_"+rrun][j],cmp[l+"_"+lrun][j]/cmp[l+"_"+rrun][j]-1]

        def highlight_greater(x,columns):
            df1 = pandas.DataFrame('', index=x.index, columns=x.columns)
            for l in columns:
                m={}
                for j in x.index:
                    m[j] = (x[l][j][1] / x[l][j][0])*100 if x[l][j][0]!=0 else 100
                mx=max(m.values())-100
                mn=100-min(m.values())
                mx=max(mx,mn)
                for j in x.index:
                    m1=-(100-m[j])/mx*100 if x[l][j][0]!=0 else 0
                    if m1>0:
                        df1[l][j] = f'background-image: linear-gradient(to right, transparent 50%, #5fba7d 50%, #5fba7d {50+m1/2}%, transparent {50+m1/2}%)'
                    else:
                        df1[l][j] = f'background-image: linear-gradient(to left, transparent 50%, #f1a863 50%, #f1a863 {50-m1/2}%, transparent {50-m1/2}%)'

            return df1

        def display_compare(df,columns):
            output.append(df.style.set_properties(**{'width': '300px','border-style':'solid','border-width':'1px'}).apply(lambda x: highlight_greater(x,columns), axis=None).format(lambda x: '''
                                                                          <div style='max-width: 30%; min-width:30%;display:inline-block;'>{:,.2f}</div>
                                                                          <div style='max-width: 30%; min-width:30%; display:inline-block;'>{:,.2f}</div>
                                                                          <div style='max-width: 30%; min-width:30%; display:inline-block;color:blue'>{:,.2f}%</div>
                                                                       '''.format(x[0],x[1],x[2]*100)).render())
        display_compare(df1,cmpcolumns)

        df3 = pandas.DataFrame('', index=sarcmp.index, columns=sarcolumns)
        for l in sarcolumns:
            for j in df3.index:
                df3[l][j]=[sarcmp[l+"_"+lrun][j],sarcmp[l+"_"+rrun][j],sarcmp[l+"_"+lrun][j]/sarcmp[l+"_"+rrun][j]-1]
        display_compare(df3,sarcolumns)

        if showemon:
            df2 = pandas.DataFrame('', index=emoncmp.index, columns=emoncolumns)
            for l in emoncolumns:
                for j in df2.index:
                    df2[l][j]=[emoncmp[l+"_"+lrun][j],emoncmp[l+"_"+rrun][j],emoncmp[l+"_"+lrun][j]/emoncmp[l+"_"+rrun][j]-1]
            display_compare(df2,emoncolumns)

        print("time breakdown")
        ################################ time breakdown ##################################################################################################
        timel=appals.show_time_metric(plot=False)
        timer=appals2.show_time_metric(plot=False)
        timer.columns=[l.replace("scan time","time_batchscan") for l in timer.columns]
        timel.columns=[l.replace("scan time","time_batchscan") for l in timel.columns]
        rcols=timer.columns
        lcols=[]
        for c in [l.split("%")[1][1:] for l in rcols]:
            for t in timel.columns:
                if t.endswith(c):
                    lcols.append(t)
        for t in timel.columns:
            if t not in lcols:
                lcols.append(t)
        timel_adj=timel[lcols]

        fig, axs = plt.subplots(nrows=1, ncols=2, sharey=True,figsize=(30,8),gridspec_kw = {'width_ratios':[1, 1]})
        plt.subplots_adjust(wspace=0.01)
        ax=timel_adj.plot.bar(ax=axs[0],stacked=True)
        list_values=timel_adj.loc[0].values
        for rect, value in zip(ax.patches, list_values):
            h = rect.get_height() /2.
            w = rect.get_width() /2.
            x, y = rect.get_xy()
            ax.text(x+w, y+h,"{:,.2f}".format(value),horizontalalignment='center',verticalalignment='center',color="white")
        ax=timer.plot.bar(ax=axs[1],stacked=True)
        list_values=timer.loc[0].values
        for rect, value in zip(ax.patches, list_values):
            h = rect.get_height() /2.
            w = rect.get_width() /2.
            x, y = rect.get_xy()
            ax.text(x+w, y+h,"{:,.2f}".format(value),horizontalalignment='center',verticalalignment='center',color="white")

################################ critical time breakdown ##################################################################################################
        timel=appals.show_time_metric(plot=False,taskids=[l[0].item() for l in appals.criticaltasks])
        timer=appals2.show_time_metric(plot=False,taskids=[l[0].item() for l in appals2.criticaltasks])
        timer.columns=[l.replace("scan time","time_batchscan") for l in timer.columns]
        timel.columns=[l.replace("scan time","time_batchscan") for l in timel.columns]
        rcols=timer.columns
        lcols=[]
        for c in [l.split("%")[1][1:] for l in rcols]:
            for t in timel.columns:
                if t.endswith(c):
                    lcols.append(t)
        for t in timel.columns:
            if t not in lcols:
                lcols.append(t)
        timel_adj=timel[lcols]

        fig, axs = plt.subplots(nrows=1, ncols=2, sharey=True,figsize=(30,8),gridspec_kw = {'width_ratios':[1, 1]})
        plt.subplots_adjust(wspace=0.01)
        ax=timel_adj.plot.bar(ax=axs[0],stacked=True)
        list_values=timel_adj.loc[0].values
        for rect, value in zip(ax.patches, list_values):
            h = rect.get_height() /2.
            w = rect.get_width() /2.
            x, y = rect.get_xy()
            ax.text(x+w, y+h,"{:,.2f}".format(value),horizontalalignment='center',verticalalignment='center',color="white")
        ax=timer.plot.bar(ax=axs[1],stacked=True)
        list_values=timer.loc[0].values
        for rect, value in zip(ax.patches, list_values):
            h = rect.get_height() /2.
            w = rect.get_width() /2.
            x, y = rect.get_xy()
            ax.text(x+w, y+h,"{:,.2f}".format(value),horizontalalignment='center',verticalalignment='center',color="white")


        ################################ hot stage ##########################################################################################################

        hotstagel=appals.get_hottest_stages(plot=False)
        hotstager=appals2.get_hottest_stages(plot=False)
        hotstagel.style.format(lambda x: '''{:,.2f}'''.format(x))

        norm = matplotlib.colors.Normalize(vmin=0, vmax=max(hotstager.queryid))
        cmap = matplotlib.cm.get_cmap('brg')
        def setbkcolor(x):
            rgba=cmap(norm(x['queryid']))
            return ['background-color:rgba({:d},{:d},{:d},1); color:white'.format(int(rgba[0]*255),int(rgba[1]*255),int(rgba[2]*255))]*9

        output.append("<table><tr><td>" + hotstagel.style.apply(setbkcolor,axis=1).format({"total_time":lambda x: '{:,.2f}'.format(x),"stdev_time":lambda x: '{:,.2f}'.format(x),"acc_total":lambda x: '{:,.2%}'.format(x),"total":lambda x: '{:,.2%}'.format(x)}).render()+
             "</td><td>" +  hotstager.style.apply(setbkcolor,axis=1).format({"total_time":lambda x: '{:,.2f}'.format(x),"stdev_time":lambda x: '{:,.2f}'.format(x),"acc_total":lambda x: '{:,.2%}'.format(x),"total":lambda x: '{:,.2%}'.format(x)}).render()+             "</td></tr></table>")

        if not show_queryplan_diff:
            return "\n".join(output)
        
        print("hot stage")

        loperators=appals.getOperatorCount()
        roperators=appals2.getOperatorCount()
        loperators_rowcnt=appals.get_metric_output_rowcnt()
        roperators_rowcnt=appals2.get_metric_output_rowcnt()
        
        def show_query_diff(queryid, always_show=True):
            lops=pandas.DataFrame(loperators[queryid])
            lops.columns=['calls_l']
            lops=lops.loc[lops['calls_l'] >0]

            rops=pandas.DataFrame(roperators[queryid])
            rops.columns=["calls_r"]
            rops=rops.loc[rops['calls_r'] >0]
            lops_row=pandas.DataFrame(loperators_rowcnt[queryid])
            lops_row.columns=["rows_l"]
            lops_row=lops_row.loc[lops_row['rows_l'] >0]

            rops_row=pandas.DataFrame(roperators_rowcnt[queryid])
            rops_row.columns=["rows_r"]
            rops_row=rops_row.loc[rops_row['rows_r'] >0]

            opscmp=pandas.merge(pandas.merge(pandas.merge(lops,rops,how="outer",left_index=True,right_index=True),lops_row,how="outer",left_index=True,right_index=True),rops_row,how="outer",left_index=True,right_index=True)
            opscmp=opscmp.fillna("")
            
            def set_bk_color_opscmp(x):
                calls_l= 0 if x['calls_l']=="" else x['calls_l']
                calls_r= 0 if x['calls_r']=="" else x['calls_r']
                rows_l= 0 if x['rows_l']=="" else x['rows_l']
                rows_r= 0 if x['rows_r']=="" else x['rows_r']

                if calls_l > calls_r or rows_l > rows_r:
                    return ['background-color:#eb6b34']*4
                if calls_l < calls_r or rows_l < rows_r:
                    return ['background-color:#8ad158']*4
                return ['color:#dbd4d0']*4

            if always_show or not (opscmp["rows_l"].equals(opscmp["rows_r"]) and opscmp["calls_l"].equals(opscmp["calls_r"])):
                print(f"query  {queryid}  queryplan diff ")
                if not always_show:
                    output.append(f"<p><font size=4 color=red>query{queryid} is different</font></p>")
                output.append(opscmp.style.apply(set_bk_color_opscmp,axis=1).render())

                planl=appals.get_query_plan(queryid=queryid,show_plan_only=True,plot=False)
                planr=appals2.get_query_plan(queryid=queryid,show_plan_only=True,plot=False)
                output.append("<table><tr><td>"+planl+"</td><td>"+planr+"</td></tr></table>")

        outputx=df1['output rows']
        runtimex = df1['runtime']
        for x in outputx.index:
            if runtimex[x][0]/runtimex[x][1]<0.95 or runtimex[x][0]/runtimex[x][1]>1.05:
                output.append(f"<p><font size=4 color=red>query{x} is different,{lrun} time: {df1['runtime'][x][0]}, {rrun} time: {df1['runtime'][x][1]}</font></p>")
                if queryids is not None and x not in queryids:
                    print("query plan skipped")
                    continue
                try:
                    show_query_diff(x, True)
                except:
                    print(" query diff error")
            else:
                try:
                    show_query_diff(x, False)
                except:
                    print(" query diff error")
                
        return "\n".join(output)
                              

                              
    def show_queryplan_diff(app2, queryid,**kwargs):
        lbasedir=kwargs.get("basedir",app2.basedir)
        r_appid=kwargs.get("r_appid",app2.appid)
        
        app=kwargs.get("rapp",Application_Run(r_appid,basedir=lbasedir))

        appals=app.analysis["app"]["als"]
        appals2=app2.analysis["app"]["als"]

        hotstagel=appals.get_hottest_stages(plot=False)
        hotstager=appals2.get_hottest_stages(plot=False)
        hotstagel.style.format(lambda x: '''{:,.2f}'''.format(x))

        loperators=appals.getOperatorCount()
        roperators=appals2.getOperatorCount()
        loperators_rowcnt=appals.get_metric_output_rowcnt()
        roperators_rowcnt=appals2.get_metric_output_rowcnt()

        lrun=app.appid
        rrun=app2.appid

        output=[]

        def show_query_diff(queryid):
            lops=pandas.DataFrame(loperators[queryid])
            lops.columns=['calls_l']
            lops=lops.loc[lops['calls_l'] >0]

            rops=pandas.DataFrame(roperators[queryid])
            rops.columns=["calls_r"]
            rops=rops.loc[rops['calls_r'] >0]
            lops_row=pandas.DataFrame(loperators_rowcnt[queryid])
            lops_row.columns=["rows_l"]
            lops_row=lops_row.loc[lops_row['rows_l'] >0]

            rops_row=pandas.DataFrame(roperators_rowcnt[queryid])
            rops_row.columns=["rows_r"]
            rops_row=rops_row.loc[rops_row['rows_r'] >0]

            opscmp=pandas.merge(pandas.merge(pandas.merge(lops,rops,how="outer",left_index=True,right_index=True),lops_row,how="outer",left_index=True,right_index=True),rops_row,how="outer",left_index=True,right_index=True)
            opscmp=opscmp.fillna("")

            def set_bk_color_opscmp(x):
                calls_l= 0 if x['calls_l']=="" else x['calls_l']
                calls_r= 0 if x['calls_r']=="" else x['calls_r']
                rows_l= 0 if x['rows_l']=="" else x['rows_l']
                rows_r= 0 if x['rows_r']=="" else x['rows_r']

                if calls_l > calls_r or rows_l > rows_r:
                    return ['background-color:#eb6b34']*4
                if calls_l < calls_r or rows_l < rows_r:
                    return ['background-color:#8ad158']*4
                return ['color:#dbd4d0']*4

            output.append(opscmp.style.apply(set_bk_color_opscmp,axis=1).render())

            planl=appals.get_query_plan(queryid=queryid,show_plan_only=True,plot=False)
            planr=appals2.get_query_plan(queryid=queryid,show_plan_only=True,plot=False)
            output.append("<table><tr><td>"+planl+"</td><td>"+planr+"</td></tr></table>")

        x=queryid
        print("query ",x," queryplan diff ")
        #output.append(f"<p><font size=4 color=red>query{x} is different,{lrun} time: {df1['runtime'][x][0]}, {rrun} time: {df1['runtime'][x][1]}</font></p>")
        show_query_diff(x)
        display(HTML("\n".join(output)))
        return
    
    
def reduce_metric(pdrst,slave_id,metric,core,agg_func):
    pdrst['rst']=pdrst.apply(lambda x:x['app_id'].get_reduce_metric(slave_id,metric,core,agg_func), axis=1)
    for l in agg_func:
        pdrst[get_alias_name(metric,l)]=pdrst.apply(lambda x:x['rst'].iloc[0][get_alias_name(metric,l)],axis=1)
    return pdrst.drop(columns=['rst'])

def cvt_number(n):
    try:
        if str(n).isdigit():
            return f'{n:,}'
        else:
            return f'{round(float(n),2):,}'
    except ValueError:
        return n

def parse_changelog(changelog):
    out=[]
    if fs.exists(changelog):
        with fs.open(changelog) as f:
            for l in f.readlines():
                l = l.decode('utf-8')
                if l.startswith("commit"):
                    out.append(re.sub(r"commit +(.+)",r"<font color=#BDCA57>commit </font><font color=#23C2BF>\1</font>",l))
                elif l.startswith("Author"):
                    out.append(re.sub(r"Author: +([^<]+) <(.+)>",r"<font color=#BDCA57>Author: </font><font color=#C02866>\1</font> <<font color=#BC0DBD>\2</font>> ",l))
                elif l.startswith("Date"):
                    out.append(re.sub(r"Date: +(\d\d\d\d-\d\d-\d\d)",r"<font color=#BDCA57>Author: </font>\1",l))
                else:
                    out.append(l)
    else:
        out.append(f'{os.path.basename(changelog)} not found!')
    return out

def generate_query_diff(name, comp_name, query_time_file, comp_query_time_file):
    result = []
    if fs.exists(query_time_file) and fs.exists(comp_query_time_file):
        result.append(['query', name, comp_name, 'difference', 'percentage'])
        
        qtimes = {}
        comp_qtimes = {}
        with fs.open(query_time_file) as f:
            qtimes = json.loads(f.read().decode('ascii'))
        with fs.open(comp_query_time_file) as f:
            comp_qtimes = json.loads(f.read().decode('ascii'))
        
        query_ids = sorted(qtimes.keys(), key=lambda x: str(len(x))+x if x[-1] != 'a' and x[-1] != 'b' else str(len(x)-1) + x)
        
        if len(comp_qtimes) != len(qtimes):
            raise Exception('Number of queries mismatch!')
        
        query_ids.append('total')
        qtimes['total'] = sum([float(i) for i in qtimes.values()])
        comp_qtimes['total'] = sum([float(i) for i in comp_qtimes.values()])
        
        for q in query_ids:
            t1 = qtimes.get(q)
            t2 = comp_qtimes.get(q)
            delta = str("{:.2f}".format(float(t2) - float(t1)))
            perc = str("{:.2f}".format((float(t2) / float(t1)) * 100)) + '%'
            result.append([q, str(t1), str(t2), delta, perc])
    return result

def append_summary(appid, base_dir, name, comp_appid, comp_base_dir, comp_name, baseline_appid, baseline_base_dir, statsall, output):
    with open(output,"a") as linkfile:

        difftable=''' <table border="1" cellpadding="0" cellspacing="0">
                            <tbody>'''
        for k,v in statsall.items():
            difftable+=f'''
                <tr>
                <td>{k}</td>
                <td>{cvt_number(v)}</td>
                </tr>'''
        difftable+='''
            </tbody>
        </table>\n'''
        linkfile.write(difftable)
        linkfile.write("\n<br><hr/>\n")
        
        linkfile.write("\n<font color=blue> gluten gitlog in last 2 days</font><br>\n")
        out=parse_changelog(os.path.join('/', base_dir, appid, 'changelog_gluten'))
        linkfile.write("<br>".join(out))
        linkfile.write("\n<br><hr/>\n")
        
        linkfile.write("\n<font color=blue> velox gitlog in last 2 days</font><br>\n")
        out=parse_changelog(os.path.join('/', base_dir, appid, 'changelog_velox'))
        linkfile.write("<br>".join(out))
        linkfile.write("\n<br><hr/>\n")
        
        linkfile.write('''<div class="jp-RenderedHTMLCommon jp-RenderedHTML jp-OutputArea-output " data-mime-type="text/html">\n''')
        
        def append_query_diff(their_appid, their_base_dir, their_name):
            query_diff=generate_query_diff(name, their_name, os.path.join('/', base_dir, appid, 'query_time.json'), os.path.join('/', their_base_dir, their_appid, 'query_time.json'))
            if query_diff:
                difftable='''
                <table border="1" cellpadding="0" cellspacing="0">
                    <tbody>'''
                for l in query_diff:
                    difftable+='''
                        <tr>'''
                    base=0
                    pr=0
                    if re.match(r"[0-9.]+",l[1]):
                        base=float(l[1])
                        l[1]="{:.2f}".format(base)
                    if re.match(r"[0-9.]+",l[2]):
                        pr=float(l[2])
                        l[2]="{:.2f}".format(pr)

                    for d in l:
                        color='#000000'
                        if base > pr:
                            color='#6F9915'
                        elif base < pr:
                            color='#F92663'
                        difftable += f'''
                        <td><font color={color}>{d}</font></td>'''

                    difftable+='''
                        </tr>'''

                difftable+='''
                    </tbody>
                </table>'''
                linkfile.write(difftable)
                linkfile.write("\n<br><hr/>\n")
                # return percentage
                return query_diff[-1][-1]
            return ''

        baseline_perc = ''
        if comp_appid:
            append_query_diff(comp_appid, comp_base_dir, comp_name)
        if baseline_appid:
            baseline_perc = append_query_diff(baseline_appid, baseline_base_dir, 'Vanilla Spark')

        linkfile.write("</div>")
        
        return baseline_perc

def generate_email_body_title(appid, base_dir, name, comp_appid, comp_base_dir, comp_name, baseline_appid, baseline_base_dir, notebook, notebook_html, traceview, stats, summary, pr=''):
    statsall=collections.OrderedDict()
    for k,v in stats.items():
        statsall[k]=v
    for k,v in summary.to_dict()[appals.appid].items():
        statsall[k]=v
    
    pr_link=''
    if pr:
        pr_link=f'https://github.com/apache/incubator-gluten/pull/{pr}'
        title=get_ipython().getoutput("wget --quiet -O - $pr_link | sed -n -e 's!.*<title>\\(.*\\)</title>.*!\\1!p'")
        if not title:
            raise Exception(f'Failed to fetch PR link: {pr_link}')
        pr_link=f'pr link: <a href="{pr_link}">{title[0]}</a><br>'
    
    output=f'/tmp/{appid}.html'
    with open(output, 'w+') as f:
        f.writelines(f'''
<font style="font-family: Courier New"">
history event: <a href="http://{local_ip}:18080/tmp/sparkEventLog/{appid}/jobs/">http://{local_ip}:18080/tmp/sparkEventLog/{appid}/jobs/</a><br>
notebook: <a href="http://{local_ip}:8889/notebooks/{base_dir}/{notebook}">http://{local_ip}:8889/notebooks/{base_dir}/{notebook}</a><br>
notebook html: <a href="http://{local_ip}:8889/view/{base_dir}/{notebook_html}">http://{local_ip}:8889/view/{base_dir}/{notebook_html}</a><br>
traceview: <a href="{traceview}">{traceview}</a><br>
{pr_link}
</font><hr/>''')
    baseline_perc = append_summary(appid, base_dir, name, comp_appid, comp_base_dir, comp_name, baseline_appid, baseline_base_dir, statsall, output)
    
    title_prefix = f"[ {datetime.now().strftime('%m_%d_%Y')} ]" if not pr else f"[ PR {pr} ]"
    title = f'{title_prefix} {name} {appid} {baseline_perc}'
    return output,title
    