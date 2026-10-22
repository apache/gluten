#!/usr/bin/env python
# coding: utf-8

from __future__ import nested_scopes
from IPython.display import display, HTML
display(HTML('<style>.container { width:100% !important; }</style>'))
display(HTML('<style>.CodeMirror{font-family: "Courier New";font-size: 12pt;}</style>'))

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

def background_gradient(s, m, M, cmap='PuBu', low=0, high=0):
    from matplotlib import colors
    rng = M - m
    norm = colors.Normalize(m - (rng * low),
                            M + (rng * high))
    normed = norm(s.values)
    c = [colors.rgb2hex(x) for x in plt.cm.get_cmap(cmap)(normed)]
    return ['background-color: {:s}'.format(color) for color in c]

class SparkLog_Analysis:
    def __init__(self, appid,jobids,clients):
        pass

class Analysis:
    def __init__(self,file,spark=None):
        self.file=file
        self.spark=spark
        self.starttime=0
        self.df=None
    
    def load_data(self):
        pass
    
    def generate_trace_view_list(self,id=0, **kwargs):
        if self.df==None:
            self.load_data()
        trace_events=[]
        node=kwargs.get('node',"node")
        trace_events.append(json.dumps({"name": "process_name","ph": "M","pid":id,"tid":0,"args":{"name":" "+node}}))
        return trace_events
   
    def generate_trace_view(self, trace_output, **kwargs):
        traces=[]
        traces.extend(self.generate_trace_view_list(0,**kwargs))
        
        output='''
        {
            "traceEvents": [
        
        ''' + \
        ",\n".join(traces)\
       + '''
            ],
            "displayTimeUnit": "ns"
        }'''

        outputfolder=trace_output
        appidx=trace_output.split("/")[-1]
        
        with open(outputfolder, 'w') as outfile: 
            outfile.write(output)
        
        if appidx.endswith(".json"):
            traceview_link=f'http://{server}:1088/tracing_examples/trace_viewer.html#/tracing/test_data/{appidx}'
        else:
            traceview_link=f'http://{server}:1088/tracing_examples/trace_viewer.html#/tracing/test_data/{appidx}.json'
        display(HTML(f"<a href={traceview_link}>{traceview_link}</a>"))
        return traceview_link

class Telegraf_analysis(Analysis):
    def __init__(self,sar_file, starttime=0,endtime=0,spark=None):
        Analysis.__init__(self,sar_file,spark=spark)
        self.sar_file=sar_file
        self.starttime=starttime
        self.endtime=endtime
    
    def load_data(self):
        schema = StructType(
            [StructField(f"_c{l}", StringType(), True) for l in range(0,13)]
        )
        df=self.spark.read.csv(self.sar_file,schema=schema)
        if self.starttime>0 and self.endtime>0:
            df=df.where(f"_c0>={self.starttime-1} and _c0<={self.endtime+1}")
        self.df=df
        return df

    def col_df(self,cond,colname,args,slaver_id=0, thread_id=0):
        sardf=self.df
        starttime=self.starttime
        cpudf=sardf if len(cond)==0 else sardf.where(cond)
        
        #cpudf.select(F.date_format(F.from_unixtime(F.lit(starttime/1000)), 'yyyy-MM-dd HH:mm:ss').alias('starttime'),'_1').show(1)

        traces=cpudf.orderBy(F.col("time")).select(
                F.lit(thread_id).alias('tid'),
                (F.expr("time")*1000).astype(IntegerType()).alias('ts'),
                F.lit(slaver_id).alias('pid'),
                F.lit('C').alias('ph'),
                F.lit(colname).alias('name'),
                args(cpudf).alias('args')
            ).toJSON().collect()
        return traces

    def generate_trace_view_list(self,id,**kwargs):
        trace_events=Analysis.generate_trace_view_list(self,id, **kwargs)
        return trace_events

    def get_stat(self,**kwargs):
        if self.df is None:
            self.load_data()
    
    def get_plotdf(self,**kwargs):
        if self.df is None:
            self.load_data()
        return self.df.orderBy("time").toPandas()
    
    def plot(self,axis, w):
        pass
    
    def plot_num(self):
        if self.df is None:
            self.load_data()
        return 1
    
class Telegraf_cpu_analysis(Telegraf_analysis):
    def __init__(self,sar_file, starttime=0 ,endtime=0,spark=None):
        Telegraf_analysis.__init__(self,sar_file, starttime ,endtime,spark=spark)
    
    def load_data(self):
        df=Telegraf_analysis.load_data(self)
        
        self.df=df.where("_c1='cpu'").select(((F.col("_c0")-F.lit(self.starttime)).astype(IntegerType())).alias("time"),
             F.col("_c6").astype(FloatType()).alias("usage_iowait"),
             F.col("_c7").astype(FloatType()).alias("usage_system"),
             F.col("_c8").astype(FloatType()).alias("usage_user")).orderBy("time")
        return df
    
    def generate_trace_view_list(self,id,**kwargs):
        trace_events=Telegraf_analysis.generate_trace_view_list(self,id, **kwargs)
        
        self.df=self.df.withColumn("usage_iowait",F.when(F.col("usage_iowait")>100,F.lit(100)).otherwise(F.col("usage_iowait")))
        
        trace_events.extend(self.col_df("",             "cpu%",    lambda l: F.struct(
                                                                                                    F.floor(F.col('usage_user').astype(FloatType())).alias('user'),
                                                                                                    F.floor(F.col('usage_system').astype(FloatType())).alias('system'),
                                                                                                    F.floor(F.col('usage_iowait').astype(FloatType())).alias('iowait')
                                                                                                    ),                            id, 0))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":0,"args":{"sort_index ":0}}))
        return trace_events
    
    def get_stat(self,**kwargs):
        Telegraf_analysis.get_stat(self)
        
        cpuutil=self.df
        cnt=cpuutil.count()
        user_morethan_90=cpuutil.where("`usage_user`>90").count()
        kernel_morethan_10=cpuutil.where("`usage_system`>10").count()
        iowait_morethan_10=cpuutil.where("`usage_iowait`>10").count()
        out=[['%user>90%',user_morethan_90/cnt],['%kernel>10%',kernel_morethan_10/cnt],["%iowait>10%",iowait_morethan_10/cnt]]
        avgutil=cpuutil.agg(*[F.mean(l).alias(l) for l in ["usage_user","usage_system","usage_iowait"]]).collect()
        out.extend([["avg " + l,avgutil[0][l]] for l in ["usage_user","usage_system","usage_iowait"]])
        pdout=pandas.DataFrame(out).set_index(0)
        pdout.columns=[self.file.split("/")[-2]]
        return pdout
    
    def plot(self,axis, w):
        cpudf=self.get_plotdf()
        
        axis.stackplot(cpudf['time'], cpudf['usage_iowait'], cpudf['usage_system'], cpudf['usage_user'], labels=['iowait','system','user'])
        axis.legend(loc='upper left')
        axis.grid(axis = 'y')
        axis.set_title("CPU Utilization on " + w, y=1.1)       
        
class Telegraf_mem_analysis(Telegraf_analysis):
    def __init__(self,sar_file, starttime=0,endtime=0,spark=None):
        Telegraf_analysis.__init__(self,sar_file, starttime ,endtime,spark=spark)
    
    def load_data(self):
        df=Telegraf_analysis.load_data(self)
        self.df=df.where("_c1='mem'").select(((F.col("_c0")-F.lit(self.starttime)).astype(IntegerType())).alias("time"),
                             F.col("_c4").astype(FloatType()).alias("available"),
                             F.col("_c5").astype(FloatType()).alias("available_percent"),
                             F.col("_c6").astype(FloatType()).alias("buffered"),
                             F.col("_c7").astype(FloatType()).alias("cached"),
                             F.col("_c8").astype(FloatType()).alias("dirty"),
                             F.col("_c9").astype(FloatType()).alias("free"),
                             F.col("_c10").astype(FloatType()).alias("used"),
                             F.col("_c11").astype(FloatType()).alias("used_percent")
                            )
        self.df=self.df.withColumn("total",F.col("used")/(F.col("used_percent")/100))
        self.df=self.df.withColumn("realused",F.col("used"))
        return self.df

    
    def generate_trace_view_list(self,id,**kwargs):
        trace_events=Telegraf_analysis.generate_trace_view_list(self,id, **kwargs)
        
        
        trace_events.extend(self.col_df("","mem % ",      lambda l: F.struct(F.floor(l['cached']/l['total']*100).alias('cached'),
                                                                             F.floor(l['buffered']/l['total']*100).alias('buffered'),
                                                                             F.floor(l['realused']/l['total']*100).alias('used')), 
                                          id,1))
        trace_events.extend(self.col_df("","pagecache % ", lambda l: F.struct(F.floor((l['cached']-l['dirty'])/l['total']*100).alias('clean'), 
                                                                              F.floor(l['dirty']/l['total']*100).alias('dirty')),
                                          id,2))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":1,"args":{"sort_index ":1}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":2,"args":{"sort_index ":2}}))
        return trace_events    
    
    def get_stat(sar_mem,**kwargs):
        Telegraf_analysis.get_stat(sar_mem)
        
        memutil=sar_mem.df.select(F.floor(F.col('cached')/F.col('total')*100).alias('cached'),  
                                  F.floor(F.col('buffered')/F.col('total')*100).alias('buffered'),
                                  F.floor(F.col('realused')/F.col('total')*100).alias('used'),
                                  F.floor(F.col('dirty')/F.col('total')*100).alias('dirty'))
        memsum=memutil.summary().toPandas()
        memsum=memsum.set_index("summary")
        out=[
            [[l + ' mean',float(memsum[l]["mean"])],
            [l + ' 75%',float(memsum[l]["75%"])],
            [l + ' max',float(memsum[l]["max"])]] for l in ["cached","used","dirty"]]
        out=[*out[0],*out[1]]
        pdout=pandas.DataFrame(out).set_index(0)
        pdout.columns=[sar_mem.file.split("/")[-2]]
        return pdout
    
    def get_plotdf(self,**kwargs):
        if self.df is None:
            self.load_data()
            
        return self.df.selectExpr("time","used_percent","(cached-dirty)/total*100 as clean_cached", "dirty/total*100 as dirty_cached").orderBy("time").toPandas()
    
    def plot(self,axis, w):
        memdf=self.get_plotdf()
        
        axis.stackplot(memdf['time'], memdf['used_percent'], memdf['clean_cached'], memdf['dirty_cached'], labels=['used','clean_cached','dirty_cached'])
        axis.legend(loc='upper left')
        axis.grid(axis = 'y')
        axis.set_title("MEM Utilization on " + w, y=1.1)
        
class Telegraf_PageCache_analysis(Telegraf_analysis):
    def __init__(self,sar_file, starttime=0 ,endtime=0,spark=None):
        Telegraf_analysis.__init__(self,sar_file, starttime ,endtime,spark=spark)
    
    def load_data(self):
        df=Telegraf_analysis.load_data(self)
        pf_df=df.where("_c1='kernel_vmstat'").select(((F.col("_c0")-F.lit(self.starttime)).astype(IntegerType())).alias("time"),
                             F.col("_c4").astype(FloatType()).alias("pgfault"),
                             F.col("_c5").astype(FloatType()).alias("pgfree"),
                             F.col("_c6").astype(FloatType()).alias("pgmajfault"),
                             F.col("_c7").astype(FloatType()).alias("pgpgin"),
                             F.col("_c8").astype(FloatType()).alias("pgpgout"),
                             F.col("_c9").astype(FloatType()).alias("pgscan_direct"),
                             F.col("_c10").astype(FloatType()).alias("pgscan_kswapd"),
                             F.col("_c11").astype(FloatType()).alias("pgsteal_direct"),
                             F.col("_c12").astype(FloatType()).alias("pgsteal_kswapd")
                                   )
        w=Window.orderBy("time")
        pf_df=pf_df.select("time",
                           (F.col("time")-F.lag(F.col("time"),1).over(w)).alias("time_delta"), 
                           *[((F.col(l)-F.lag(F.col(l),1).over(w))/(F.col("time")-F.lag(F.col("time"),1).over(w))).alias(l) for l in ("pgfault","pgfree","pgmajfault","pgpgin","pgpgout","pgscan_direct","pgscan_kswapd","pgsteal_direct","pgsteal_kswapd")]
                          )
        
        pf_df=pf_df.select(F.col("time"),*[(F.col(l)/1000).alias("k"+l+"/s") for l in ("pgfault","pgfree","pgmajfault","pgpgin","pgpgout","pgscan_direct","pgscan_kswapd","pgsteal_direct","pgsteal_kswapd")]).orderBy("time")
        
        self.df=pf_df
        
    
    def generate_trace_view_list(self,id,**kwargs):
        trace_events=Telegraf_analysis.generate_trace_view_list(self,id, **kwargs)
        
        
        trace_events.extend(self.col_df("","page inout", lambda l: F.struct( F.floor(l['kpgpgin/s']).alias('in'),
                                                                 F.floor(l['kpgpgout/s']).alias('out')),
                                          id,11))
        trace_events.extend(self.col_df("","faults", lambda l: F.struct(F.floor((l['kpgmajfault/s'])).alias('major'), 
                                                            F.floor(l['kpgfault/s']-l['kpgmajfault/s']).alias('minor')),
                                          id,12))
        trace_events.extend(self.col_df("","page free", lambda l: F.struct(F.floor((l['kpgfree/s']*4/1024)).alias('free')),
                                          id,13))
        trace_events.extend(self.col_df("","scan", lambda l: F.struct(F.floor((l['kpgscan_kswapd/s'])*4/1024).alias('kernel'), 
                                                          F.floor(l['kpgscan_direct/s']*4/1024).alias('app')),
                                          id,14))
        trace_events.extend(self.col_df("","vmeff", lambda l: F.struct(F.floor((l['kpgsteal_direct/s']+l['kpgsteal_kswapd/s'])/(l['kpgscan_kswapd/s']+l['kpgscan_kswapd/s'])).alias('vmeff%')),
                                          id,15))
        
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":11,"args":{"sort_index ":11}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":12,"args":{"sort_index ":12}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":13,"args":{"sort_index ":13}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":14,"args":{"sort_index ":14}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":15,"args":{"sort_index ":15}}))
        return trace_events    
    def get_stat(sar_mem,**kwargs):
        Telegraf_analysis.get_stat(sar_mem)
        
        memutil=sar_mem.df.select(F.floor(F.col('kpgpgin/s')/1024).alias('pgin'),  
                                  F.floor(F.col('kpgpgout/s')/1024).alias('pgout'),
                                  F.floor(F.col('kpgfault/s')-F.col('kpgmajfault/s')).alias('fault')
                                  )
        memsum=memutil.summary().toPandas()
        memsum=memsum.set_index("summary")
        memsum=memsum.fillna(0)
        out=[
            [[l + ' mean',float(memsum[l]["mean"])],
            [l + ' 75%',float(memsum[l]["75%"])],
            [l + ' max',float(memsum[l]["max"])]] for l in ["pgin","pgout","fault"]]
        out=[*out[0],*out[1],*out[2]]
        pdout=pandas.DataFrame(out).set_index(0)
        pdout.columns=[sar_mem.file.split("/")[-2]]
        return pdout
    
    def get_plotdf(self,**kwargs):
        if self.df is None:
            self.load_data()
        return self.df.select(F.col("time"),F.col("kpgmajfault/s").alias("major/s"),(F.col("kpgfault/s")-F.col("kpgmajfault/s")).alias("minor/s")).orderBy("time").toPandas()
    
    def plot(self,axs, w):
        pfdf=self.get_plotdf()
        axs.stackplot(pfdf['time'], pfdf['major/s'], pfdf['minor/s'], labels=['major fault k/s','minor fault k/s'])
        axs.legend(loc='upper left')
        axs.grid(axis = 'y')
        axs.set_title("pagefault rate " + w, y=1.1)
        
class Telegraf_disk_analysis(Telegraf_analysis):
    def __init__(self,sar_file, starttime=0 ,endtime=0, disk_list=None,spark=None):
        Telegraf_analysis.__init__(self,sar_file, starttime ,endtime,spark=spark)
        
        if type(disk_list) is str:
            disk_list=[disk_list,]
        self.disk_list=disk_list
        

    def load_data(self):
        Telegraf_analysis.load_data(self)
        disk_df=self.df.where("_c1='diskio'").select(((F.col("_c0")-F.lit(self.starttime)).astype(IntegerType())).alias("time"),
                             F.col("_c4").alias("disk_name"),                                        
                             F.col("_c6").astype(FloatType()).alias("io_time"),
                             F.col("_c7").astype(FloatType()).alias("iops_in_progress"),
                             F.col("_c8").astype(FloatType()).alias("read_bytes"),
                             F.col("_c9").astype(FloatType()).alias("reads"),
                             F.col("_c10").astype(FloatType()).alias("weighted_io_time"),
                             F.col("_c11").astype(FloatType()).alias("write_bytes"),
                             F.col("_c12").astype(FloatType()).alias("writes")          
                                   )
        w=Window.partitionBy("disk_name").orderBy("time")
        disk_df=disk_df.select(F.col('disk_name'),F.col('time'),(F.col("time")-F.lag(F.col("time"),1).over(w)).alias("time_delta"), *[(F.col(l)-F.lag(F.col(l),1).over(w)).alias(l) for l in ("io_time","read_bytes","reads","weighted_io_time","write_bytes","writes")])
        disk_df=disk_df.withColumn("%util",F.col("io_time")/1000/F.col("time_delta")*100)
        disk_df=disk_df.withColumn("io_await",F.col("weighted_io_time")/(F.col("reads")+F.col("writes")))
        disk_df=disk_df.withColumn("io_svctm",F.col("io_time")/(F.col("reads")+F.col("writes")))
        disk_df=disk_df.withColumn("rKB/s",F.col("read_bytes")/1024/F.col("time_delta"))
        disk_df=disk_df.withColumn("wKB/s",F.col("write_bytes")/1024/F.col("time_delta"))
        disk_df=disk_df.withColumn("avgrq-sz",(F.col("read_bytes")+F.col("read_bytes"))/(F.col("reads")+F.col("writes")))
        disk_df=disk_df.withColumn("avgqu-sz",F.col("weighted_io_time")/F.col("io_time"))
        self.df=disk_df
        disksc=disk_df.select("disk_name").distinct().collect()
        
        if self.disk_list is None:
            if self.sar_file.startswith("s3"):
                client = boto3.client('s3')
                bucket=re.split(r"/+",self.sar_file)[1]
                prefix="/".join(re.split(r"/+",self.sar_file)[2:5])+"/"
                response = client.list_objects_v2(Bucket=bucket, Prefix=prefix )
                if f"{prefix}df.txt" in [l['Key'] for l in response['Contents']]:
                    response = client.get_object(Bucket=bucket, Key=f'{prefix}df.txt')
                    content = response['Body'].read().decode('utf-8')
                    shuffle_disks=[l.split(" ")[0].split("/")[-1] for l in content.split("\n") if "mnt_data" in l]
                    self.disk_list=[d['disk_name'] for d in disksc if d['disk_name'] in shuffle_disks]
                else:
                    self.disk_list=[d['disk_name'] for d in disksc]
            elif self.sar_file.startswith("/"):
                if os.path.exists(self.sar_file.replace("telegraf.out","df.txt")):
                    with open(self.sar_file.replace("telegraf.out","df.txt"),"r") as f:
                        shuffle_disks=[l.split(" ")[0].split("/")[-1] for l in f if "mnt_data" in l]
                        self.disk_list=[d['disk_name'] for d in disksc if d['disk_name'] in shuffle_disks]
                    if len(self.disk_list)==0:
                        self.disk_list = [d['disk_name'] for d in disksc]
                else:
                    self.disk_list=[d['disk_name'] for d in disksc]
            else:
                self.disk_list=[d['disk_name'] for d in disksc]
        if len(self.disk_list) >0:
            self.df=self.df.where("disk_name in (" + ",".join([f"'{l}'" for l in self.disk_list])+")")        
        
        
    def generate_trace_view_list(self,id,**kwargs):
        trace_events=Telegraf_analysis.generate_trace_view_list(self,id, **kwargs)

        disk_prefix=kwargs.get('disk_prefix',None)

        devcnt=self.df.select("disk_name").distinct().count()
        
        trace_events.extend(self.col_df("",      "disk b/w",       lambda l: F.struct(
                                                                                                            F.floor(F.col("rKB/s")).alias('read'),
                                                                                                            F.floor(F.col("wKB/s")).alias('write')),id, 3))
        trace_events.extend(self.col_df("",      "disk%",       lambda l: F.struct(
                                                                                                            (F.col("%util")/F.lit(devcnt)).alias('%util')),id, 4))
        trace_events.extend(self.col_df("",      "req size",       lambda l: F.struct(
                                                                                                            (F.col("avgrq-sz")/F.lit(devcnt)).alias('avgrq-sz')),id, 5))
        trace_events.extend(self.col_df("",      "queue size",       lambda l: F.struct(
                                                                                                            (F.col("avgqu-sz")/F.lit(512*devcnt/1024)).alias('avgqu-sz')),id, 6))
        trace_events.extend(self.col_df("",      "await",       lambda l: F.struct(
                                                                                                            (F.col("io_await")/F.lit(devcnt)).alias('await')),id,7))
        
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":3,"args":{"sort_index ":3}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":4,"args":{"sort_index ":4}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":5,"args":{"sort_index ":5}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":6,"args":{"sort_index ":6}}))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":7,"args":{"sort_index ":7}}))
        return trace_events    

    def get_stat(sar_disk,**kwargs):
        Telegraf_analysis.get_stat(sar_disk)

        diskutil=sar_disk.df.groupBy("time").agg(F.mean(F.col("%util").astype(FloatType())).alias("%util")).orderBy("time")
        totalcnt=diskutil.count()
        time_morethan_90=diskutil.where(F.col("%util")>90).count()/totalcnt
        avgutil=diskutil.agg(F.mean("%util")).collect()
        out=[["avg disk util",avgutil[0]["avg(%util)"]],
            ["time more than 90%", time_morethan_90]]
        diskbw=sar_disk.df.groupBy("time").agg(F.sum(F.col("rKB/s")).alias("rd_bw"),F.sum(F.col("wKB/s")).alias("wr_bw"))
        bw=diskbw.agg(F.sum("rd_bw").alias("total read"),F.sum("wr_bw").alias("total write"),F.mean("rd_bw").alias("read bw"),F.mean("wr_bw").alias("write bw"),F.max("rd_bw").alias("max read"),F.max("wr_bw").alias("max write")).collect()
        maxread=bw[0]["max read"]
        maxwrite=bw[0]["max write"]
        rdstat, wrstat = diskbw.stat.approxQuantile(['rd_bw','wr_bw'],[0.75,0.95,0.99],0.0)
        time_rd_morethan_95 = diskbw.where(F.col("rd_bw")>rdstat[1]).count()/totalcnt
        time_wr_morethan_95 = diskbw.where(F.col("wr_bw")>rdstat[1]).count()/totalcnt
        out.append(['total read (G)' , bw[0]["total read"]/1024])
        out.append(['total write (G)', bw[0]["total write"]/1024])
        out.append(['avg read bw (MB/s)', bw[0]["read bw"]/1024])
        out.append(['avg write bw (MB/s)', bw[0]["write bw"]/1024])
        out.append(['read bw %75', rdstat[0]/1024])
        out.append(['read bw %95', rdstat[1]/1024])
        out.append(['read bw max', rdstat[2]/1024])
        out.append(['time_rd_morethan_95', time_rd_morethan_95])
        out.append(['write bw %75', wrstat[0]/1024])
        out.append(['write bw %95', wrstat[1]/1024])
        out.append(['write bw max', wrstat[2]/1024])
        out.append(['time_wr_morethan_95', time_wr_morethan_95])
        pdout=pandas.DataFrame(out).set_index(0)
        pdout.columns=[sar_disk.file.split("/")[-2]]
        return pdout

    def get_plotdf(self,**kwargs):
        if self.df is None:
            self.load_data()
        
        diskdfs=[self.df.where(f"disk_name='{d}'") \
                 .select(F.col("time"),F.col("disk_name"),(F.col("rKB/s")/1024).alias("read_MB/sec"),(F.col("wKB/s")/1024).alias("write_MB/sec")) \
                 .orderBy("time").toPandas() for d in self.disk_list]
        return diskdfs
    
    def plot_num(self):
        Telegraf_analysis.plot_num(self)
        return len(self.disk_list)

    def plot(self,axis, w):
        diskdfs=self.get_plotdf()
        
        axsid=0
        for diskdf in diskdfs:
            axis[axsid].stackplot(diskdf['time'], diskdf['read_MB/sec'], diskdf['write_MB/sec'], labels=['read MB/s','write MB/s'])
            axis[axsid].legend(loc='upper left')
            axis[axsid].grid(axis = 'y')
            axis[axsid].set_title(self.disk_list[axsid] + " utilization on " + w, y=1.1)
            axsid+=1        
        
class Telegraf_nic_analysis(Telegraf_analysis):
    def __init__(self,sar_file, starttime=0 ,endtime=0, nic_list=None,spark=None):
        Telegraf_analysis.__init__(self,sar_file, starttime ,endtime,spark=spark)
        if type(nic_list) is str:
            self.nic_list=[nic_list]
        else:
            self.nic_list=nic_list
    
    def load_data(self):
        Telegraf_analysis.load_data(self)
        net_df=self.df.where("_c1='net'").select(((F.col("_c0")-F.lit(self.starttime)).astype(IntegerType())).alias("time"),
                             F.col("_c4").alias("interface"),                                        
                             F.col("_c5").astype(FloatType()).alias("bytes_recv"),
                             F.col("_c6").astype(FloatType()).alias("bytes_sent"),
                             F.col("_c7").astype(FloatType()).alias("packets_recv"),
                             F.col("_c8").astype(FloatType()).alias("packets_sent")                            
                                   )
        w=Window.partitionBy("interface").orderBy("time")
        net_df=net_df.select(F.col('interface'),F.col('time'),(F.col("time")-F.lag(F.col("time"),1).over(w)).alias("time_delta"), 
                             *[(F.col(l)-F.lag(F.col(l),1).over(w)).alias(l) for l in ("bytes_recv","bytes_sent","packets_recv","packets_sent")])
        net_df=net_df.withColumn("rxkB/s",F.col("bytes_recv")/1024/F.col("time_delta"))
        net_df=net_df.withColumn("txkB/s",F.col("bytes_sent")/1024/F.col("time_delta"))
        
        if self.nic_list is None:
            self.nic_list = [l['interface'] for l in net_df.select("interface").distinct().collect()]
            
        nicfilter= "interface in (" + ",".join([f"'{l}'" for l in self.nic_list]) + ")"

        self.df=net_df.where(nicfilter)
        
        
    def generate_trace_view_list(self,id,**kwargs):
        trace_events=Telegraf_analysis.generate_trace_view_list(self,id, **kwargs)
        
        trace_events.extend(self.col_df("",       "eth ",        lambda l: F.struct(F.floor(F.expr('cast(`rxkB/s` as float)/1024')).alias('rxmb/s'),F.floor(F.expr('cast(`txkB/s` as float)/1024')).alias('txmb/s')),                id, 8))
        trace_events.append(json.dumps({"name": "thread_sort_index","ph": "M","pid":id,"tid":8,"args":{"sort_index ":8}}))
        return trace_events 
    
    def get_stat(sar_nic,**kwargs):
        Telegraf_analysis.get_stat(sar_nic)
                    
        nicbw=sar_nic.df.groupBy("time").agg(F.sum(F.col("rxkB/s").astype(FloatType())/1024).alias("rx MB/s")).orderBy("time")
        if nicbw.count()==0:
            out=[["rx MB/s 75%",0],["rx MB/s 95%",0],["rx MB/s 99%",0]]
        else:
            out=nicbw.stat.approxQuantile(['rx MB/s'],[0.75,0.95,0.99],0.0)[0]
            out=[["rx MB/s 75%",out[0]],["rx MB/s 95%",out[1]],["rx MB/s 99%",out[2]]]
        pdout=pandas.DataFrame(out).set_index(0)
        pdout.columns=[sar_nic.file.split("/")[-2]]
        return pdout
    
    def get_plotdf(self,**kwargs):
        if self.df is None:
            self.load_data()
        
        nicdfs=[self.df.where(f"interface='{d}'") \
                 .select(F.col("time"),F.col("interface"),(F.col("rxkB/s")/1024).alias("recv_MB/sec"),(F.col("txkB/s")/1024).alias("send_MB/sec")) \
                 .orderBy("time").toPandas() for d in self.nic_list]
        return nicdfs
    
    
    def plot_num(self):
        Telegraf_analysis.plot_num(self)
        return len(self.nic_list)

    def plot(self,axs, w):
        nicdfs=self.get_plotdf()
        
        axsid=0
        for netdf in nicdfs:
            axs[axsid].stackplot(netdf['time'], netdf['recv_MB/sec'], netdf['send_MB/sec'], labels=['recv','sent'])
            axs[axsid].legend(loc='upper left')
            axs[axsid].grid(axis = 'y')
            axs[axsid].set_title(self.nic_list[axsid] + " throughput on " + w, y=1.1)
            axsid+=1 
    
class App_Log_Analysis(Analysis):
    """
    Analyzes Spark application event logs to extract performance metrics,
    query execution details, and resource utilization statistics.
    
    Attributes:
        file (str): Path to the Spark event log file
        jobids (str): Job IDs to filter (optional)
        qlist (list): Query list for mapping (optional)
        appid (str): Application ID
        df (DataFrame): Main dataframe with task metrics
        query_num (int): Number of queries analyzed
    """
    
    def __init__(self, file, jobids=None, qlist=None, appid=None, spark=None):
        """
        Initialize the App_Log_Analysis instance.
        
        Args:
            file (str): Path to the Spark event log file
            jobids (str|list): Job IDs to filter analysis (optional)
            qlist (list): Query list for custom query mapping (optional)
            appid (str): Application ID (optional)
            spark: Spark session instance (optional)
        """
        Analysis.__init__(self,file,spark=spark)
        self.file = file
        self.appid = appid
        self.jobids = self._format_jobids(jobids)
        self.qlist = qlist
        
        # Initialize attributes
        self.df = None
        self.dfacc = None
        self.queryplans = None
        self.appid = None
        self.query_num = 0
        self.pids = []
        self.allmetrics = None
        self.metricscollect = None
        self.criticaltasks = []
        
        # Configuration attributes
        self.parallelism = 1
        self.executor_cores = 1
        self.executor_instances = 1
        self.taskcpus = 1
        self.batchsize = 4096
        self.realexecutors = 0
        
        # Failure tracking
        self.failed_stages = []
        self.speculativetask = 0
        self.speculativekilledtask = 0
        self.speculativestage = 0
        
    def _format_jobids(self, jobids):
        """Format job IDs for SQL WHERE clause."""
        if jobids is None:
            return ""
        if isinstance(jobids, str):
            return jobids
        return ' in ({:s})'.format(','.join([str(j) for j in jobids]))

    def append_data(self):
        """
        Load and process Spark event log data.
        
        This method:
        1. Reads the event log JSON file
        2. Extracts application ID and configuration
        3. Processes query execution metrics
        4. Analyzes task-level performance
        5. Identifies critical path tasks
        """
        logger.info(f"Loading data from {self.file}")
        
        try:
            if not self.df:
                self.load_data()
            
            self.dfacc.writeTo("spark_profile.dfacc").overwritePartitions()
            
            self.queryplans.writeTo("spark_profile.query_plan_info").overwritePartitions()
            
            self.df.writeTo("spark_profile.spark_metrics_iceberg").overwritePartitions()

            self.metric_df.writeTo("spark_profile.task_metrics").overwritePartitions()

            configdict = self.config
            dfcfgs = []
            for k,v in configdict.items():
                cfg={}
                cfg['appid']=self.appid
                cfg['key']=k
                cfg['value']=v
                dfcfgs.append(cfg)
            cfgdf = self.spark.createDataFrame(dfcfgs)
            cfgdf.writeTo("spark_profile.spark_config").append()

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise    
    
    def load_data(self):
        """
        Load and process Spark event log data.
        
        This method:
        1. Reads the event log JSON file
        2. Extracts application ID and configuration
        3. Processes query execution metrics
        4. Analyzes task-level performance
        5. Identifies critical path tasks
        """
        logger.info(f"Loading data from {self.file}")
        
        try:
            
            # Read event log
            df = self.spark.read.json(self.file)
            df=df.repartition(6)
            df.cache()
            df.count()
            
            # Extract application ID
            # updated self.appid
            self._extract_app_id(df)
            
            # Process accumulator updates
            # updated self.dfacc
            self._process_accumulator_updates(df)
            
            # Process query plans
            # updated elf.queryplans
            self._process_query_plans(df)
            
            # Extract Spark configuration
            # updated self.config
            self._extract_spark_config(df)
            
            # Process execution times
            exectime = self._process_execution_times(df)
            
            # Process jobs and stages
            # updated self.df_job. -- not used
            df_job = self._process_jobs(df)
            
            # Process tasks
            # updated self.df
            self._process_tasks(df, exectime, df_job)
            
            # Extract query descriptions
            dfdesc = self._extract_query_descriptions(df)

            # Map query IDs
            # update self.df
            self._map_query_ids(dfdesc)
            
            # Calculate critical path
            self._calculate_critical_path()

            self.dfacc=self.dfacc.withColumn("appid",F.lit(self.appid))
            self.queryplans=self.queryplans.select("real_queryid","queryid","physicalPlanDescription","nodeName","simpleString",F.to_json("children").alias("query_plan"),F.lit(self.appid).alias("appid")).where("query_plan is not null")

            df1=self.df.select(F.lit(self.appid).alias("appid"),*[F.col(l) for l in self.df.columns if l!='Accumulables'])
            df1=df1.withColumn("queryid",F.col("queryid").cast(IntegerType()))

            df2=self.df.where("event='SparkListenerTaskEnd'").select(F.col("Task ID"),F.explode("Accumulables")).select(F.lit(self.appid).alias("appid"),"Task ID","col.*")
            
            self.df=df1
            self.df.cache()
            self.df.count()
            self.metric_df=df2
            self.metric_df.cache()
            self.metric_df.count()
                        
            self.queryplans.cache()
            self.queryplans.count()
            self.metricscollect.cache()
            self.metricscollect.count()
            self.dfacc.cache()
            self.dfacc.count()

            df.unpersist()

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def _extract_app_id(self, df):
        """Extract application ID from event log."""
        if 'App ID' in df.columns:
            app_id_row = df.where("`App ID` is not null").first()
            self.appid = app_id_row["App ID"] if app_id_row else "Application-00000000"
        else:
            self.appid = "Application-00000000"
        logger.info(f"Application ID: {self.appid}")
    
    def _extract_query_descriptions(self, df):
        """Extract query descriptions from SQL execution events."""
        retdf = df.where(
            "Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'"
        ).where(
            (F.col('description').rlike(r'/\* 1-(q\d+[ab]?) \*/'))
        ).select(F.regexp_extract(F.col("description"), r"/\* 1-(q\d+[ab]?) \*/", 1).alias("description"), "executionId")

        if retdf.count()>0:
            # it's thrift style queries, use the map instead
            self.qlist=None
            return retdf

        return df.where(
            "Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'"
        ).where(
            (F.length("description") < 30) & (F.col('description').rlike(r'(\d|a|b)$'))
        ).select("description", "executionId")
        
    
    def _process_accumulator_updates(self, df):
        """Process driver accumulator updates."""
        if df.where("Event='org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates'").count() > 0:
            self.dfacc = df.where(
                "Event='org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates'"
            ).select(
                F.col("executionId").alias("queryid"),
                F.explode("accumUpdates")
            )
        else:
            self.dfacc = None
            logger.info("No accumulator updates found")
    
    def _process_query_plans(self, df):
        """Process query execution plans."""
        if "sparkPlanInfo" not in df.columns:
            self.queryplans = None
            logger.info("No query plans found in event log")
            return
        
        # Extract query plans
        self.queryplans = df.where(
            "(Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart' "
            "or Event='org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate') "
            "and (sparkPlanInfo.nodeName!='AdaptiveSparkPlan' "
            "or sparkPlanInfo.simpleString='AdaptiveSparkPlan isFinalPlan=true')"
        ).select(
            F.col("executionId").alias("queryid"),
            'physicalPlanDescription',
            "sparkPlanInfo.*"
        )
        @udf("long")
        def isfinish_udf(s):
            import json
            s=json.loads(s)
            def isfinish(root):
                if "isFinalPlan=false" in root['simpleString'] or root['children'] is None:
                    return 0
                for c in root["children"]:
                    if isfinish(c)==0:
                        return 0
                return 1
            if len(s)>0:
                return isfinish(s[0])
            else:
                return 0
                
        self.queryplans=self.queryplans.where(isfinish_udf(F.to_json("children"))==1)
        
        # Extract metrics
        self._extract_metrics()
    
    def _extract_metrics(self):
        """Extract metrics from query plans."""
        if self.queryplans is None or self.queryplans.count() == 0:
            return
        
        def get_all_metrics(s):
            import json

            seen = set()
            allmetrics = []

            def get_metric(root):
                """Recursively extract metrics from plan nodes."""
                for metric in root.get("metrics", []):
                    acc_id = metric['accumulatorId']
                    if acc_id not in seen:
                        seen.add(acc_id)
                        allmetrics.append({
                            "ID": acc_id,
                            "type":metric["metricType"],
                            "Name": metric['name'],
                            "nodeName":root["nodeName"]
                        })

                if root.get('children'):
                    for child in root["children"]:
                        get_metric(child)
            
            nodes=json.loads(s)
            for row in nodes:
                get_metric(row)
            return allmetrics
            
        
        json_schema = ArrayType(
            StructType([
                StructField("ID", StringType(), True),
                StructField("type", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("nodeName", StringType(), True)
            ])
        )
        get_all_metrics_udf = udf(get_all_metrics, json_schema)
        
        if "children" in self.queryplans.columns:
            mcs=self.queryplans.select(get_all_metrics_udf(F.to_json("children")).alias("mcvector"))
        else:
            mcs=self.queryplans.select(get_all_metrics_udf('query_plan').alias("mcvector"))

        self.allmetrics=mcs.select(F.explode("mcvector").alias("col")).select("col.*")
        
        # Join with accumulator updates
        if self.dfacc is not None and "ID" not in self.dfacc.columns:
            self.dfacc = self.dfacc.select(
                "queryid",
                (F.col("col")[0]).alias("ID"),
                (F.col("col")[1]).alias("Update")
            ).join(self.allmetrics, on=["ID"])
        
        self.metricscollect = self.allmetrics.where('''
            type in ('nsTiming','timing') and (
                startswith(name,'time to') or 
                startswith(name,'time of') or 
                startswith(name,'scan time') or 
                startswith(name,'shuffle write time') or 
                startswith(name,'time to spill') or 
                startswith(name,'task commit time')
            ) and name not in ('time to collect batch', 
                                    'time of scan', 
                                    'time of operator input')''')
    
    def _extract_spark_config(self, df):
        """Extract Spark configuration parameters."""
        config = df.select("Properties.*").where("`spark.app.id` is not null").limit(1).collect()
        
        if not config:
            logger.warning("No Spark configuration found")
            return

        configdic = config[0].asDict()
        
        self.parallelism = int(configdic.get('spark.sql.shuffle.partitions', 1))
        self.executor_cores = int(configdic.get('spark.executor.cores', 1))
        self.executor_instances = int(configdic.get('spark.executor.instances', 1))
        self.taskcpus = int(configdic.get('spark.task.cpus', 1))
        self.batchsize = int(configdic.get('spark.gluten.sql.columnar.maxBatchSize', 4096))
        
        # Count real executors
        self.realexecutors = df.where(~F.isnull(F.col("Executor ID"))) \
                               .select("Executor ID") \
                               .distinct() \
                               .count()
        configdic['realexecutors'] = self.realexecutors

        env=df.where("event='SparkListenerEnvironmentUpdate'").select("System Properties.*").limit(1).collect()
        for e in ["gluten.version","java.version","java.home","java.vendor","os.name","os.version"]:
            if e in env[0]:
                configdic[e] = env[0][e]
        if "info" in df.columns:
            env=df.where("event='org.apache.gluten.events.GlutenBuildInfoEvent'").select("info.*").collect()
            configdic.update(env[0].asDict())
            
        self.config = configdic
    
    def _process_execution_times(self, df):
        """Process query execution start and end times."""
        execstart = df.where(
            "Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'"
        ).select("executionId", "time")
        
        execend = df.where(
            "Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd'"
        ).select("executionId", "time")
        
        execstart = execstart.withColumnRenamed("time", "query_starttime") \
                            .withColumnRenamed("executionId", "queryid")
        execend = execend.withColumnRenamed("time", "query_endtime") \
                        .withColumnRenamed("executionId", "queryid")
        
        return execstart.join(execend, on=["queryid"])
    
    def _process_jobs(self, df):
        """Process job start and end events."""
        # Job start events
        if "spark.sql.execution.id" in df.where("Event='SparkListenerJobStart'") \
                                         .select("Properties.*").columns:
            df_jobstart = df.where("Event='SparkListenerJobStart'").select(
                "Job ID",
                "Submission Time",
                F.col("Properties.`spark.sql.execution.id`").alias("queryid"),
                "Stage IDs"
            )
        else:
            df_jobstart = df.where("Event='SparkListenerJobStart'").select(
                "Job ID",
                "Submission Time",
                F.lit(0).alias("queryid"),
                "Stage IDs"
            )
        
        # Job end events
        df_jobend = df.where("Event='SparkListenerJobEnd'").select(
            "`Job ID`",
            "Completion Time"
        )
        
        # Join and rename
        df_job = df_jobstart.join(df_jobend, "Job ID")
        df_job = df_job.withColumnRenamed("Submission Time", "job_start_time") \
                       .withColumnRenamed("Completion Time", "job_stop_time")
        
        return df_job
    
    def _process_tasks(self, df, exectime, df_job):
        """Process task-level metrics."""
        jobstage = df_job.select("*", F.explode("Stage IDs").alias("Stage ID"))
        
        # Extract task events
        task = df.where(
            "(Event='SparkListenerTaskEnd' or Event='SparkListenerTaskStart')"
        ).select("Event", "Stage ID", "task info.*", "task metrics.*")
        
        # Track failures and speculation
        self.failed_stages = [
            str(row['Stage ID']) 
            for row in task.where("Failed='true'")
                          .select("Stage ID")
                          .distinct()
                          .collect()
        ]
        
        self.speculativetask = task.where("speculative = 'true'").count()
        self.speculativekilledtask = task.where(
            "speculative = true and killed='true'"
        ).count()
        self.speculativestage = task.where(
            "speculative = true and killed='true'"
        ).select("`Stage ID`").distinct().count()
        
        # Filter valid tasks
        validtsk = task.where(
            "Event = 'SparkListenerTaskEnd' and (Failed<>'true' or killed<>'true')"
        ).select("`Task ID`")
        task = task.join(validtsk, on='Task ID', how='inner')
        
        # Join with job information
        taskjob = task.select(
            "Host", "`Event`", "`Launch Time`", "`Executor ID`", "`Task ID`",
            "`Finish Time`", "`Stage ID`",
            "`Input Metrics`.`Bytes Read`",
            "`Disk Bytes Spilled`",
            "`Memory Bytes Spilled`",
            "`Shuffle Read Metrics`.`Local Bytes Read`",
            "`Shuffle Read Metrics`.`Remote Bytes Read`",
            "`Shuffle Write Metrics`.`Shuffle Bytes Written`",
            "`Executor Deserialize Time`",
            "`Shuffle Read Metrics`.`Fetch Wait Time`",
            "`Executor Run Time`",
            "`Shuffle Write Metrics`.`Shuffle Write Time`",
            "`Result Serialization Time`",
            "`Getting Result Time`",
            "`JVM GC Time`",
            "`Executor CPU Time`",
            "Accumulables",
            "Peak Execution Memory",
            F.when(task['Finish Time'] == 0, task['Launch Time'])
             .otherwise(task['Finish Time'])
             .alias('eventtime')
        ).join(jobstage, "Stage ID") \
         .where("`Finish Time` is null or `Finish Time` <= job_stop_time + 5")
        
        # Join with execution times
        taskjob = taskjob.join(exectime, on=['queryid'], how='left')
        
        self.df = taskjob
        
        # Filter by job IDs if specified
        if len(self.jobids) > 0:
            self.df = self.df.where('`Job ID` ' + self.jobids)
    
    def _map_query_ids(self, dfdesc):
        """Map internal query IDs to user-friendly query names."""
        if self.qlist is None:
            self._map_query_ids_auto(dfdesc)
        else:
            self._map_query_ids_custom()
        
        self.df = self.df.fillna(0)
        self.df = self.df.withColumn(
            'Executor ID',
            F.when(F.col("Executor ID") == "driver", 1)
             .otherwise(F.col("Executor ID"))
        )
    
    def _map_query_ids_auto(self, dfdesc):
        """Automatically map query IDs based on stage count."""
                
        # Find queries with multiple stages
        queryids_df = self.df.where(
            "(Event='SparkListenerTaskEnd' or Event='SparkListenerTaskStart')"
        ).select("queryid", "Stage ID") \
         .distinct() \
         .groupBy("queryid") \
         .agg(F.count("*").alias("stage_num")) \
         .where("stage_num > 1") \
         .select(F.col("queryid"))
        
        self.df = self.df.join(queryids_df, on=['queryid'])
        
        # Get ordered query IDs
        queryids = queryids_df.select(F.col("queryid").cast(IntegerType())) \
                             .orderBy("queryid") \
                             .toPandas()
        
        self.query_num = len(queryids)
        
        if self.query_num > 0:
            queryidx = queryids.reset_index()
            queryidx['index'] = queryidx['index'] + 1
            
            # Map TPC-DS queries if applicable
            if self.query_num == 103:
                queryidx['index'] = queryidx['index'].map(tpcds_query_map)
            
            qidx = self.spark.createDataFrame(queryidx)
            qidx = qidx.withColumnRenamed("index", "real_queryid")
            qidx = qidx.join(
                dfdesc,
                qidx.queryid == dfdesc.executionId,
                "left_outer"
            ).select(
                F.coalesce(dfdesc.description, qidx.real_queryid).alias("real_queryid"),
                "queryid"
            )
            
            self.df = self.df.join(qidx, on="queryid", how="right")
            
            # Update related dataframes
            if self.dfacc is not None:
                self.dfacc = self.dfacc.join(qidx, on="queryid", how='left')
            
            if self.queryplans is not None:
                self.queryplans = self.queryplans.join(qidx, "queryid", how="right")
    
    def _map_query_ids_custom(self):
        """Map query IDs using custom query list."""

        qidtime = self.df.groupBy("queryid").agg(
            F.min("job_start_time").alias("start"),
            F.max("job_stop_time").alias("stop")
        ).collect()
        
        qidmap = []
        for row in qidtime:
            midtime = (row['start'] + (row["stop"] - row["start"]) / 2) / 1000
            
            for query in self.qlist:
                if query['query_name'] == "sanity-check":
                    continue
                
                start_time = float(query['start_time'])
                duration = float(query['application_time_taken'])
                
                if start_time < midtime < start_time + duration:
                    query_name = query['query_name'] \
                        .replace(".sql", "") \
                        .replace("tpch-", "") \
                        .replace("tpcds-", "")
                    qidmap.append({
                        "queryid": row['queryid'],
                        "real_queryid": query_name
                    })
                    break
        
        self.query_num = len(qidmap)
        
        if self.query_num > 0:
            qidx = self.spark.createDataFrame(qidmap)
            self.df = self.df.join(qidx, on="queryid", how="right")
            
            if self.dfacc is not None:
                self.dfacc = self.dfacc.join(qidx, on="queryid", how='left')
            
            if self.queryplans is not None:
                self.queryplans = self.queryplans.join(qidx, "queryid", how="right")
    
    def _calculate_critical_path(self):
        """Calculate critical path through task execution."""
        if self.query_num == 0:
            return
        
        # Fill nulls and normalize executor IDs
        self.df.cache()
        
        # Extract task timing information
        dfx = self.df.where("Event='SparkListenerTaskEnd'").select(
            "Stage ID", "Launch Time", "Finish Time", "Task ID"
        )
        
        dfxpds = dfx.toPandas()
        dfxpds.columns = [col.replace(" ", "_") for col in dfxpds.columns]
        
        # Sort by finish time descending
        dfxpds_ods = duckdb.query('SELECT * FROM dfxpds ORDER BY Finish_Time DESC').df()
        
        # Find critical path
        criticaltasks = []
        idx = 0
        total_row = len(dfxpds_ods)
        
        if total_row == 0:
            self.criticaltasks = []
            return
        
        launchtime = dfxpds_ods["Launch_Time"].iloc[0]
        criticaltasks.append([
            dfxpds_ods["Task_ID"].iloc[0],
            launchtime,
            dfxpds_ods["Finish_Time"].iloc[0]
        ])
        
        while True:
            # Find next task in critical path
            while idx < total_row:
                if dfxpds_ods["Finish_Time"].iloc[idx] - 2 < launchtime:
                    break
                idx += 1
            else:
                break
            
            cur_finish = dfxpds_ods["Finish_Time"].iloc[idx]
            cur_finish = launchtime - 1 if cur_finish >= launchtime else cur_finish
            launchtime = dfxpds_ods["Launch_Time"].iloc[idx]
            
            criticaltasks.append([
                dfxpds_ods["Task_ID"].iloc[idx],
                launchtime,
                cur_finish
            ])
        
        self.criticaltasks = criticaltasks   

    def get_basic_state(appals):
        if appals.df is None:
            appals.load_data()
                
        qtime=appals.get_query_time(plot=False)
        sums=qtime.drop(columns=['stages']).sum().round(2)
        
        if len(appals.failed_stages)>0:
            failure="<br>".join(["query: " + str(l["real_queryid"])+"|stage: " + str(l["Stage ID"]) for l in appals.df.where("`Stage ID` in ("+",".join(appals.failed_stages)+")").select("real_queryid","Stage ID").distinct().collect()])
        else:
            failure=""
            
        stats={"appid":appals.appid,
            "executor.instances":appals.executor_instances,
            "executor.cores":appals.executor_cores,
            "shuffle.partitions":appals.parallelism,
            "batch size":appals.batchsize,
            "real executors":appals.realexecutors,
            "Failed Tasks":failure,
            "Speculative Tasks":appals.speculativetask,
            "Speculative Killed Tasks":appals.speculativekilledtask,
            "Speculative Stage":appals.speculativestage,
            "runtime":sums['runtime'],
            "disk spilled":sums['disk spilled'],
            "memspilled":sums['memspilled'],
            "local_read":sums['local_read'],
            "remote_read":sums['remote_read'],
            "shuffle_write":sums['shuffle_write'],
            "task run time":sums['run_time'],
            "ser_time":sums['ser_time'],
            "f_wait_time":sums['f_wait_time'],
            "gc_time":sums['gc_time'],
            "input read":sums['input read'],
            "storage read":sums['storage read'],
            "ram read":sums['ram read'],
            "ssd read":sums['ssd read'],
            "acc_task_time":sums['acc_task_time']
            }
        return stats

    def show_basic_state(appals):
        
        stats = appals.get_basic_state()

        display(HTML(f"<a href=http://{server}:18080/history/{appals.appid}>http://{server}:18080/history/{appals.appid}</a>"))

        errorcolor="#000000" if appals.executor_instances == appals.realexecutors else "#c0392b"

        display(HTML(f'''
        <table border="1" cellpadding="1" cellspacing="1" style="width:500px">
            <tbody>
                <tr>
                    <td style="width:135px">appid</td>
                    <td style="width:351px"><span style="color:#000000"><strong>{appals.appid}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">executor.instances</td>
                    <td style="width:351px"><span style="color:#000000"><strong>{appals.executor_instances}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">executor.cores</td>
                    <td style="width:351px"><span style="color:#000000"><strong>{appals.executor_cores}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">shuffle.partitions</td>
                    <td style="width:351px"><span style="color:#000000"><strong>{(appals.parallelism)}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">batch size</td>
                    <td style="width:351px"><span style="color:#000000"><strong>{(appals.batchsize):,}</strong></span></td>
                </tr>                
                <tr>
                    <td style="width:135px">real executors</td>
                    <td style="width:351px"><span style="color:{errorcolor}"><strong>{(appals.realexecutors)}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">Failed Tasks</td>
                    <td style="width:351px"><span style="color:{errorcolor}"><strong>{(stats['Failed Tasks'])}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">Speculative Tasks</td>
                    <td style="width:351px"><span style="color:#87b00c"><strong>{(appals.speculativetask)}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">Speculative Killed Tasks</td>
                    <td style="width:351px"><span style="color:#87b00c"><strong>{(appals.speculativekilledtask)}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">Speculative Stage</td>
                    <td style="width:351px"><span style="color:#87b00c"><strong>{(appals.speculativestage)}</strong></span></td>
                </tr>
                <tr>
                    <td style="width:135px">runtime</td>
                    <td style="width:351px"><strong>{stats['runtime']}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">disk spilled</td>
                    <td style="width:351px"><strong>{stats['disk spilled']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">memspilled</td>
                    <td style="width:351px"><strong>{stats['memspilled']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">local_read</td>
                    <td style="width:351px"><strong>{stats['local_read']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">remote_read</td>
                    <td style="width:351px"><strong>{stats['remote_read']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">shuffle_write</td>
                    <td style="width:351px"><strong>{stats['shuffle_write']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">task run time</td>
                    <td style="width:351px"><strong>{stats['task run time']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">ser_time</td>
                    <td style="width:351px"><strong>{stats['ser_time']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">f_wait_time</td>
                    <td style="width:351px"><strong>{stats['f_wait_time']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">gc_time</td>
                    <td style="width:351px"><strong>{stats['gc_time']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">input read</td>
                    <td style="width:351px"><strong>{stats['input read']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">storage read</td>
                    <td style="width:351px"><strong>{stats['storage read']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">ram read</td>
                    <td style="width:351px"><strong>{stats['ram read']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">ssd read</td>
                    <td style="width:351px"><strong>{stats['ssd read']:,}</strong></td>
                </tr>
                <tr>
                    <td style="width:135px">acc_task_time</td>
                    <td style="width:351px"><strong>{stats['acc_task_time']:,}</strong></td>
                </tr>
            </tbody>
        </table>

        '''))
        return stats
   
    def generate_trace_view_list(self,id=0,**kwargs):
        Analysis.generate_trace_view_list(self,**kwargs)
        showcpu=kwargs.get('showcpu',False)
        shownodes=kwargs.get("shownodes",None)
        
        showdf=self.df.where(F.col("Host").isin(shownodes)) if shownodes else self.df
        
        showdf=showdf.orderBy(["eventtime", "Finish Time"], ascending=[1, 0])
        
        events=showdf.toPandas()
        coretrack={}
        trace_events=[]
        starttime=self.starttime
        taskend=[]
        trace={"traceEvents":[]}
        exec_hosts={}
        hostsdf=showdf.select("Host").distinct().orderBy("Host")
        hostid=100000
        ended_event=[]
        
        for i,l in hostsdf.toPandas().iterrows():
            exec_hosts[l['Host']]=hostid
            hostid=hostid+100000

        tskmap={}
        for idx,l in events.iterrows():
            if l['Event']=='SparkListenerTaskStart':
                hostid=exec_hosts[l['Host']]

                tsk=l['Task ID']
                pid=int(l['Executor ID'])*100+hostid
                self.pids.append(pid)
                stime=l['Launch Time']
                #the task's starttime and finishtime is the same, ignore it.
                if tsk in ended_event:
                    continue
                if not pid in coretrack:
                    tids={}
                    trace_events.append({
                       "name": "process_name",
                       "ph": "M",
                       "pid":pid,
                       "tid":0,
                       "args":{"name":f"{l['Host']}.{l['Executor ID']}"}
                      })

                else:
                    tids=coretrack[pid]
                for t in tids.keys():
                    if tids[t][0]==-1:
                        tids[t]=[tsk,stime]
                        break
                else:
                    t=len(tids)
                    tids[t]=[tsk,stime]
                #print(f"task {tsk} tid is {pid}.{t}")
                coretrack[pid]=tids

            if l['Event']=='SparkListenerTaskEnd':
                sevt={}
                eevt={}
                hostid=exec_hosts[l['Host']]
                pid=int(l['Executor ID'])*100+hostid
                tsk=l['Task ID']
                fintime=l['Finish Time']
                
                tids=coretrack[pid]
                for t in tids.keys():
                    if tids[t][0]==tsk:
                        tids[t]=[-1,-1]
                        break
                else:
                    ended_event.append(tsk)
                    continue
                for ps in reversed([key for key in tids.keys()]) :
                    if tids[ps][1]-fintime<0 and tids[ps][1]-fintime>=-2:
                        fintime=tids[ps][1]
                        tids[t]=tids[ps]
                        tids[ps]=[-1,-1]
                        break
                if starttime==0:
                    starttime=l['Launch Time']
                    print(f'applog start time: {starttime}')

                sstime=l['Launch Time']-starttime

                trace_events.append({
                       'tid':pid+int(t),
                       'ts':sstime,
                       'dur':fintime-l['Launch Time'],
                       'pid':pid,
                       "ph":'X',
                       'name':"stg{:d}".format(l['Stage ID']),
                       'args':{"job id": l['Job ID'],
                               "stage id": l['Stage ID'],
                               "tskid":tsk,
                               "input":builtins.round(l["Bytes Read"]/1024/1024,2),
                               "spill":builtins.round(l["Memory Bytes Spilled"]/1024/1024,2),
                               "Shuffle Read Metrics": "",
                               "|---Local Read": builtins.round(l["Local Bytes Read"]/1024/1024,2),
                               "|---Remote Read":builtins.round(l["Remote Bytes Read"]/1024/1024,2),
                               "Shuffle Write Metrics": "",
                               "|---Write":builtins.round(l['Shuffle Bytes Written']/1024/1024,2)
                               }
                      })
                tskmap[tsk]={'pid':pid,'tid':pid+int(t)}

        self.starttime=starttime
        self.tskmap=tskmap
        output=[json.dumps(l) for l in trace_events]
        
        df=self.df
        
        if showcpu and self.metricscollect.count()>0:
            metricscollect=self.metricscollect
            metrics_explode=self.metric_df.join(metricscollect,on="ID")
            m1092=df.where("Event='SparkListenerTaskEnd'").select(F.col("Executor ID"),F.col("`Stage ID`"),"`Task ID`",F.col("`Finish Time`"),F.col("`Launch Time`"),(F.col("`Finish Time`")-F.col("`Launch Time`")).alias("elapsedtime"))
            m1092=m1092.join(metrics_explode,on="Task ID")

            met_df=m1092
            met_df=met_df.withColumn("Update",F.when(F.col("type")=='nsTiming',F.col("Update")/1000000).otherwise(F.col("Update")+0))
            met_df=met_df.where("Update>1")

            metdfx=met_df.groupBy("Task ID","elapsedtime").agg(F.sum("Update").alias("totalCnt"))
            taskratio=metdfx.withColumn("ratio",F.when(F.col("totalCnt")<F.col("elapsedtime"),1).otherwise(F.col("elapsedtime")/F.col("totalCnt"))).select("Task ID","ratio")
            met_df=met_df.join(taskratio,on="Task ID")
            met_df=met_df.withColumn("Update",F.col("Update")*F.col("ratio"))

            w = (Window.partitionBy('Task ID').orderBy(F.desc("Update")).rangeBetween(Window.unboundedPreceding, 0))
            met_df=met_df.withColumn('cum_sum', F.sum('Update').over(w))

            met_df=met_df.withColumn("starttime",F.col("Launch Time")+F.col("cum_sum")-F.col("Update"))

            tskmapdf = self.spark.createDataFrame(pandas.DataFrame(self.tskmap).T.reset_index())
            met_df=met_df.join(tskmapdf,on=[met_df["Task ID"]==tskmapdf["index"]])

            rstdf=met_df.select(
                F.col("tid"),
                F.round(F.col("starttime")-self.starttime,0).alias("ts"),
                F.round(F.col("Update"),0).alias("dur"),
                F.col("pid"),
                F.lit("X").alias("ph"),
                F.col("Name").alias("name")
            ).where(F.col("ts").isNotNull()).orderBy('ts')

            output.extend(rstdf.toJSON().collect())

            qtime=df.where("Event='SparkListenerTaskEnd'").groupBy("real_queryid").agg(F.min("Finish Time").alias("time"))
            output.extend(qtime.select(
                F.lit("i").alias("ph"),
                (F.col("time")-starttime).alias('ts'),
                F.lit(0).alias("pid"),
                F.lit(0).alias("tid"),
                F.lit("p").alias("s")
            ).toJSON().collect())
        
        self.starttime=starttime
        
        if kwargs.get("show_criticalshow_time_metric_path",True):
            output.extend(self.generate_critical_patch_traceview(hostid-1))
        
        return output        

    def generate_critical_patch_traceview(self,pid):
        if self.df is None:
            self.load_data()
        traces=[]
        df=self.df.where("Event='SparkListenerTaskEnd' and real_queryid is not null")
        criticaltasks=self.criticaltasks
        cripds=pandas.DataFrame(criticaltasks)
        cripds.columns=['task_id',"launch","finish"]
        cridf=self.spark.createDataFrame(cripds)
        df_ctsk=df.join(cridf,on=[F.col("task_id")==F.col("Task ID")],how="inner")
        traces.extend(df_ctsk.select(F.lit(38).alias("tid"),
                      (F.col("launch")-F.lit(self.starttime)+1).alias("ts"),
                      (F.col("finish")-F.col("launch")-1).alias("dur"),
                      F.lit(pid).alias("pid"),
                      F.lit("X").alias("ph"),
                      F.concat(F.lit("stg"),F.col("Stage ID")).alias("name"),
                      F.struct(
                          F.col("Task ID").alias('taskid'),
                          F.col("Executor ID").astype(IntegerType()).alias('exec_id'),
                          F.col("Host").alias("host"),
                          ).alias("args")
                        ).toJSON().collect())
        traces.extend(df.groupBy("real_queryid").agg(F.max("Finish Time").alias("finish"),F.min("Launch Time").alias("launch")).select(
                        F.lit(38).alias("tid"),
                      (F.col("launch")-F.lit(self.starttime)).alias("ts"),
                      (F.col("finish")-F.col("launch")).alias("dur"),
                      F.lit(pid).alias("pid"),
                      F.lit("X").alias("ph"),
                      F.concat(F.lit("qry"),F.col("real_queryid")).alias("name")).toJSON().collect())


        metricscollect=self.metricscollect.where("Name <> 'time to collect batch' and Name <> 'time of scan'").select("ID","type","nodeName")

        metrics_explode=self.metric_df.join(metricscollect,on="ID").withColumn("Name",
                F.when(
                    F.col("Name") == "exclusive time", 
                    F.concat(F.lit("time of "), F.split(F.col("nodeName"), " ")[0])
                ).otherwise(F.col("Name"))
            )
        m1092=df_ctsk.select(F.col("Executor ID"),F.col("`Stage ID`"),"`Task ID`",F.col("`Finish Time`"),F.col("`Launch Time`"),(F.col("`Finish Time`")-F.col("`Launch Time`")).alias("elapsedtime")).join(metrics_explode,on="Task ID")

        met_df=m1092.withColumn("Update",F.when(F.col("type")=='nsTiming',F.col("Update")/1000000).otherwise(F.col("Update")+0))
        
        @pandas_udf("taskid long, start long, dur long, name string", PandasUDFType.GROUPED_MAP)
        def time_breakdown(pdf):
            ltime=pdf['Launch Time'][0]+2
            pdf['start']=0
            pdf['dur']=0
            outpdf=[]
            ratio=(pdf["Finish Time"][0]-pdf["Launch Time"][0])/pdf["Update"].sum()
            ratio=1 if ratio>1 else ratio
            for idx,l in pdf.iterrows():
                if(l["Update"]*ratio>1):
                    outpdf.append([l["Task ID"],ltime,int(l["Update"]*ratio),l["mname"]])
                    ltime=ltime+int(l["Update"]*ratio)
            if len(outpdf)>0:
                return pandas.DataFrame(outpdf)
            else:
                return pandas.DataFrame({'taskid': pandas.Series([], dtype='long'),
                        'start': pandas.Series([], dtype='long'),
                        'dur': pandas.Series([], dtype='long'),
                        'name': pandas.Series([], dtype='str'),
                                        })

        #pandas UDF doesn't work. hang
        #tmbk=met_df.groupBy('Task ID').apply(time_breakdown)
        
        w=Window.partitionBy('Task ID')
        met_df1=met_df.withColumn("sum_update",F.sum("Update").over(w))
        met_df2=met_df1.withColumn("ratio",(F.col("Finish Time")-F.col("Launch Time")-2)/F.col("sum_update"))
        met_df3=met_df2.withColumn("ratio",F.when(F.col("ratio")>1,1).otherwise(F.col("ratio")))
        met_df4=met_df3.withColumn("update_ratio",F.floor(F.col("ratio")*F.col("Update")))
        met_df5=met_df4.where(F.col("update_ratio")>2)
        w = (Window.partitionBy('Task ID').orderBy(F.desc("update_ratio")).rowsBetween(Window.unboundedPreceding, Window.currentRow))
        met_df6=met_df5.withColumn('ltime_dur', F.sum('update_ratio').over(w))
        met_df8=met_df6.withColumn("ltime",F.col("ltime_dur")+F.col("Launch Time")-F.col("update_ratio"))

        tmbk=met_df8.withColumn("taskid",F.col("Task ID")).withColumn("start",F.col("ltime")+F.lit(1)).withColumn("dur",F.col("update_ratio")-F.lit(1)).withColumn("name",F.col("Name"))
        
        
        traces.extend(tmbk.select(
                        F.lit(38).alias("tid"),
                      (F.col("start")-F.lit(self.starttime)).alias("ts"),
                      (F.col("dur")).alias("dur"),
                      F.lit(pid).alias("pid"),
                      F.lit("X").alias("ph"),
                      F.col("name").alias("name")).toJSON().collect())
        traces.append(json.dumps({
                       "name": "process_name",
                       "ph": "M",
                       "pid":pid,
                       "tid":0,
                       "args":{"name":"critical path"}
                      }))
        return traces    
                            
    def get_table_scan_metrics(self,**kwargs):
        
        if self.df is None:
            self.load_data()
        
        plot = kwargs.get("plot",True)

        df=self.df.where("Event='SparkListenerTaskEnd'").select("real_queryid",'Stage ID','Task ID')
        df=df.join(self.metric_df,on=("Task ID"))
        metricdf=self.allmetrics
        metricdf=metricdf.withColumnRenamed("type","Unit").withColumnRenamed("Name","metricName")
        df=df.join(metricdf,on=["ID"],how="right")
        nodenames=df.select("nodename").distinct().collect()
        tables=[l['nodename'] for l in nodenames if "ScanTransformer parquet" in l['nodename'] or "FileSourceScanExecTransformer" in l['nodename'] or "IcebergScanTransformer" in l['nodename']]

        df.cache()
        df.count()

        veloxdec=self.df.where(f"Event='SparkListenerTaskEnd'").select("real_queryid","Stage ID","Task ID").join(self.metric_df,on="Task ID").where("Name='velox task stats'").select("real_queryid","Stage ID","Task ID","Value")

        return_schema = ArrayType(
            StructType([
                StructField("nodeidx", IntegerType(), True),
                StructField("metric_name", StringType(), True),
                StructField("metric_value", StringType(), True)
            ])
        )

        def parse_scan(s):
            import json
            s=json.loads(s)
            ret=[]
            nodeidx=0
            for k in s:
                if k["operatorType"]=="TableScan":
                    for v in k.keys():
                        if v.endswith("Timing"):
                            wall=k[v].split(",")[1].split(":")[1].strip()
                            if wall.endswith("ns"):
                                wallf=str(float(wall[:-2])/1000000)
                            elif wall.endswith("us"):
                                wallf=str(float(wall[:-2])/1000)
                            elif wall.endswith("ms"):
                                wallf=wall[:-2]
                            elif wall.endswith("s"):
                                wallf=str(float(wall[:-1])*1000)
                            else:
                                wallf=str(wall)
                            ret.append({"nodeidx":nodeidx,"metric_name":v,"metric_value":wallf})
                        elif v=="customStats":
                            for cv in k[v].keys():
                                try:
                                    wall=k[v][cv].split(",")[0].split(":")[1].strip()
                                    if wall.endswith("ns"):
                                        wallf=str(float(wall[:-2])/1000000)
                                    elif wall.endswith("us"):
                                        wallf=str(float(wall[:-2])/1000)
                                    elif wall.endswith("ms"):
                                        wallf=wall[:-2]
                                    elif wall.endswith("s"):
                                        wallf=str(float(wall[:-1])*1000)
                                    elif wall.endswith("GB"):
                                        wallf=str(float(wall[:-2])*1024*1024*1024)
                                    elif wall.endswith("MB"):
                                        wallf=str(float(wall[:-2])*1024*1024)
                                    elif wall.endswith("KB"):
                                        wallf=str(float(wall[:-2])*1024)
                                    elif wall.endswith("B"):
                                        wallf=wall[:-1]
                                    else:
                                        wallf=str(wall)
                                except:
                                    wallf="wrong " + wall
                                ret.append({"nodeidx":nodeidx,"metric_name":cv,"metric_value":wallf})
                        else:
                            ret.append({"nodeidx":nodeidx,"metric_name":v,"metric_value":str(k[v])})
                    nodeidx+=1
            return ret

        my_udf = udf(parse_scan, return_schema)

        veloxdec2=veloxdec.select("real_queryid","Stage ID","Task ID",F.explode(my_udf(F.col("Value"))).alias("stat"))
        tbs=veloxdec2.select("real_queryid","Stage ID","Task ID","stat.*")    

        tbs.cache()
        tbs.count()

        shufflemetric=['number of raw input rows','number of output rows','number of output bytes']
        dfnv=df.where("nodename like 'ScanTransformer%' or nodename like 'FileSourceScanExecTransformer%' or nodename like 'IcebergScanTransformer%' ")
        metricdfs=[dfnv.where(F.col("Name")==l).where("Update is not null").select("nodename","Task ID",F.col("Update").alias(l)) for l in shufflemetric]
        nodemetric=reduce(lambda x,y: x.join(y, on=["Task ID","nodename"]),metricdfs)
        nodemetric=nodemetric.withColumnRenamed("number of raw input rows","rawInputRows").withColumnRenamed("number of output rows","outputRows").withColumnRenamed("number of output bytes","outputBytes")

        shufflemetricx=['rawInputRows','outputRows','outputBytes']
        metricdfsx=[tbs.where(F.col("metric_name")==l).select("Task ID",F.col("nodeidx").alias(l+"_idx"),F.col("metric_value").alias(l)) for l in shufflemetricx]

        nodemetricx=reduce(lambda x,y: x.join(y, on=["Task ID"],how="full"),metricdfsx)
        nodemetricx=nodemetricx.join(nodemetric,on=["Task ID","rawInputRows","outputRows","outputBytes"]).select("nodename","Task ID",F.col("rawInputRows_idx").alias("nodeidx")).distinct()
        tbsv=tbs.join(nodemetricx,on=["Task ID","nodeidx"])

        tbsv.cache()
        tbsv.count()
        tbs.unpersist()

        dfout=dfnv.where("Name is not null").groupBy(F.col("nodename"),F.col("Name")).agg(F.round(F.when(F.col("Name").like('%peak%'),F.max("Update")).otherwise(F.sum("Update")),2).alias("sum"),F.round(F.avg("Update"),2).alias("avg"))
        dftbls=dfout.where("name='number of raw input bytes' and sum > 1000000000").select("nodename").distinct()
        dfout=dfout.join(dftbls,on="nodename")
        pandas.set_option('display.max_colwidth', None) 
        pandas.set_option('display.precision', 2)

        pandas.set_option('display.float_format', '{:,.0f}'.format)
        if plot:
            display(dfout.orderBy("nodename","name").toPandas())

        tbsvout=tbsv.where("metric_name is not null").groupBy(F.col("nodename"),F.col("metric_name")).agg(F.round(F.when(F.col("metric_name").like('%peak%'),F.max("metric_value")).otherwise(F.sum("metric_value")),2).alias("sum"),F.round(F.avg("metric_value"),2).alias("avg"))
        tbsvout=tbsvout.join(dftbls,on="nodename")
        pandas.set_option('display.float_format', '{:,.0f}'.format)
        if plot:
            display(tbsvout.orderBy("nodename","metric_name").toPandas())

            splitdf=dfnv.join(dftbls,on="nodename").where("Name='number of processed splits'").groupBy(F.col("real_queryid"),F.col("nodename")).agg(F.count(F.col('Update')).alias("cnt"),F.round(F.sum("Update"),2).alias("splits"))
            rowgroupdf=dfnv.join(dftbls,on="nodename").where("Name='number of processed row groups'").groupBy(F.col("real_queryid"),F.col("nodename")).agg(F.count(F.col('Update')).alias("cnt"),F.round(F.sum("Update"),2).alias("rowgroups"))
            sizedf=dfnv.join(dftbls,on="nodename").where("Name='number of raw input bytes'").groupBy(F.col("real_queryid"),F.col("nodename")).agg(F.count(F.col('Update')).alias("cnt"),F.round(F.sum("Update"),2).alias("size"))

            query_splits=splitdf.collect()
            query_rowgroups=rowgroupdf.collect()
            query_sizes=sizedf.collect()

            query_splitsdf={}
            query_cntdf={}
            query_rowgroupdf={}
            query_sizedf={}

            tbls=set([l['nodename'] for l in query_splits])

            qrys=list(set([l['real_queryid'] for l in query_splits]))
            qrys.sort()

            for l in qrys:
                query_splitsdf[l]={}
                query_cntdf[l]={}
                query_rowgroupdf[l]={}
                query_sizedf[l]={}
                for t in tbls:
                    query_splitsdf[l][t]=0
                    query_cntdf[l][t]=0
                    query_rowgroupdf[l][t]=0
                    query_sizedf[l][t]=0

            for l in query_splits:
                query_splitsdf[l['real_queryid']][l['nodename']]=l['splits']
                query_cntdf[l['real_queryid']][l['nodename']]=l['cnt']
            for l in query_rowgroups:
                query_rowgroupdf[l['real_queryid']][l['nodename']]=l['rowgroups']
            for l in query_sizes:
                query_sizedf[l['real_queryid']][l['nodename']]=l['size']

            print("splits")
            pandas.set_option('display.float_format', '{:,.0f}'.format)
            display(pandas.DataFrame(query_splitsdf))
            print("partitions")
            pandas.set_option('display.float_format', '{:,.0f}'.format)
            display(pandas.DataFrame(query_cntdf))
            print("rowgroups")
            pandas.set_option('display.float_format', '{:,.0f}'.format)
            display(pandas.DataFrame(query_rowgroupdf))
            print("input size")
            pandas.set_option('display.float_format', '{:,.0f}'.format)
            display(pandas.DataFrame(query_sizedf))

            print("splits per partition")
            pandas.set_option('display.float_format', '{:,.2f}'.format)
            display((pandas.DataFrame(query_splitsdf)/pandas.DataFrame(query_cntdf)).fillna(0))

            print("rowgroups per partition")
            pandas.set_option('display.float_format', '{:,.2f}'.format)
            display((pandas.DataFrame(query_rowgroupdf)/pandas.DataFrame(query_cntdf)).fillna(0))    

            print("rowgroups per split")
            pandas.set_option('display.float_format', '{:,.2f}'.format)
            display((pandas.DataFrame(query_rowgroupdf)/pandas.DataFrame(query_splitsdf)).fillna(0))    

            print("input size per partition")
            pandas.set_option('display.float_format', '{:,.2f}'.format)
            display((pandas.DataFrame(query_sizedf)/pandas.DataFrame(query_cntdf)).fillna(0))    

        df.unpersist()
        tbs.unpersist()
        tbsv.unpersist()
        return dfout.orderBy("nodename","name").toPandas(), tbsvout.orderBy("nodename","metric_name").toPandas()
            
    def show_time_metric(self,**kwargs):
        if self.df is None:
            self.load_data()
        df=self.df.where("queryid is not NULL")
        if "shownodes" in kwargs:
            df=df.where(F.col("Host").isin(kwargs.get("shownodes")))
            
        if "queryid" in kwargs:
            query=kwargs.get("queryid")
            if type(query)==int:
                query = [query,]
            df=df.where(F.col("real_queryid").isin(query))
            queryid = query[0]
        else:
            queryid = 0
            
        if "stageid" in kwargs:
            stage=kwargs.get("stageid")
            if type(stage)==int:
                stage = [stage,]
            df=df.where(F.col("Stage ID").isin(stage))
            
        if "taskids" in kwargs:
            df=df.where(F.col("Task ID").isin(kwargs.get("taskids")))
            showexecutor=False
            exec_cores=1
            execs=1
        else:
            showexecutor=kwargs.get("showexecutor",True)
            exec_cores=self.executor_cores
            execs=self.executor_instances
            
        plot=kwargs.get("plot",True)
        
        metricscollect=self.metricscollect

        metrics_explode=df.where("Event='SparkListenerTaskEnd'")
        m1092=metrics_explode.select(F.col("Executor ID"),F.col("`Stage ID`"),"`Task ID`",F.col("`Finish Time`"),F.col("`Launch Time`"),(F.col("`Finish Time`")-F.col("`Launch Time`")).alias("elapsedtime")).join(self.metric_df.drop("Name"),on="Task ID").join(metricscollect,on="ID")

        runtime=metrics_explode.agg(F.round(F.max("Finish Time")/1000-F.min("Launch Time")/1000,2).alias("runtime")).collect()[0]["runtime"]

        met_df=m1092
        met_df=met_df.withColumn("Update",F.when(F.col("type")=='nsTiming',F.col("Update")/1000000).otherwise(F.col("Update")+0))
        outpdf=met_df.groupBy("`Executor ID`","Name").sum("Update").orderBy("Executor ID").toPandas()

        met_time_cnt=df.where("Event='SparkListenerTaskEnd'")
        exectime=met_time_cnt.groupBy("Executor ID").agg((F.max("Finish Time")-F.min("Launch Time")).alias("totaltime"),F.sum(F.col("`Finish Time`")-F.col("`Launch Time`")).alias("tasktime"))

        totaltime_query=met_time_cnt.groupBy("real_queryid").agg((F.max("Finish Time")-F.min("Launch Time")).alias("totaltime")).agg(F.sum("totaltime").alias("totaltime")).collect()
        totaltime_query=totaltime_query[0]["totaltime"]
        
        pdf=exectime.toPandas()
        exeids=set(outpdf['Executor ID'])
        outpdfs=[outpdf[outpdf["Executor ID"]==l] for l in exeids]
        tasktime=pdf.set_index("Executor ID").to_dict()['tasktime']

        def comb(l,r):
            execid=list(r['Executor ID'])[0]
            lp=r[['Name','sum(Update)']]
            lp.columns=["Name","val_"+execid]
            idle=totaltime_query*exec_cores-tasktime[execid]
            nocount=tasktime[execid]-sum(lp["val_"+execid])
            if idle<0:
                idle=0
            if nocount<0:
                nocount=0
            lp=pandas.concat([lp,pandas.DataFrame([{"Name":"idle","val_"+execid:idle}])], ignore_index=True)
            lp=pandas.concat([lp,pandas.DataFrame([{"Name":"not_counted","val_"+execid:nocount}])], ignore_index=True)
            if l is not None:
                return pandas.merge(lp, l,on=["Name"],how='outer')
            else:
                return lp

        rstpdf=None
        for l in outpdfs[0:]:
            rstpdf=comb(rstpdf,l)
            
        for l in [l for l in rstpdf.columns if l!="Name"]:
            rstpdf[l]=rstpdf[l]/1000/exec_cores
    
        rstpdf=rstpdf.sort_values(by="val_"+list(exeids)[0],axis=0,ascending=False)
        if showexecutor and plot:
            rstpdf.set_index("Name").T.plot.bar(stacked=True,figsize=(30,8))
        pdf_sum=pandas.DataFrame(rstpdf.set_index("Name").T.sum())
        totaltime=totaltime_query/1000
        pdf_sum[0]=pdf_sum[0]/(execs)
        pdf_sum.loc['idle']=[(totaltime_query-sum(tasktime.values())/execs/exec_cores)/1000]
        pdf_sum=pdf_sum.sort_values(by=0,axis=0,ascending=False)
        pdf_sum=pdf_sum.T
        pdf_sum.columns=["{:>2.0f}%_{:s}".format(pdf_sum[l][0]/totaltime*100,l) for l in pdf_sum.columns]
        matplotlib.rcParams['font.sans-serif'] = "monospace"
        matplotlib.rcParams['font.family'] = "monospace"
        import matplotlib.font_manager as font_manager
        if plot:
            ax=pdf_sum.plot.bar(stacked=True,figsize=(30,8))
            font = font_manager.FontProperties(family='monospace',
                                               style='normal', size=14)
            ax.legend(prop=font,loc=4)
            plt.title("{:s} q{:s} executors={:d} cores_per_executor={:d} parallelism={:d} sumtime={:.0f} runtime={:.0f}".format(self.file.split("/")[2],str(queryid),self.executor_instances,self.executor_cores,self.parallelism,totaltime,runtime),fontdict={'fontsize':24})
        return pdf_sum

    def show_critical_path_time_breakdown(self,**kwargs):
        if self.df is None:
            self.load_data()
        return self.show_time_metric(taskids=[l[0].item() for l in self.criticaltasks],**kwargs)
    
    def get_spark_config(self):
        if self.appid is None:
            df=self.read.jason(self.file)
            self._extract_spark_config(df)

        pandas.set_option('display.max_rows', None)
        pandas.set_option('display.max_columns', None)
        pandas.set_option('display.max_colwidth', 100000)
        return pandas.DataFrame([self.config,]).T
    
    def show_app_name(self):
        cfg=self.get_spark_config()
        display(HTML("<font size=5 color=red>" + cfg.loc[cfg.index=='spark.app.name'][0]['spark.app.name']+"</font>"))
        
        
    def get_query_time(self,**kwargs):
        if self.df is None:
            self.load_data()
        queryid=kwargs.get("queryid",None)
        plot=kwargs.get("plot",True)
        
        if queryid and type(queryid)==int:
            queryid = [queryid,]
        
        df=self.df.where(F.col("real_queryid").isin(queryid)) if queryid else self.df.where("queryid is not NULL")
        
            
        stages=df.select("real_queryid","Stage ID").distinct().orderBy("Stage ID").groupBy("real_queryid").agg(F.collect_list("Stage ID").alias("stages")).orderBy("real_queryid")
        runtimeacc=df.where("Event='SparkListenerTaskEnd'") \
                      .groupBy("real_queryid") \
                      .agg(F.round(F.sum(F.col("Finish Time")-F.col("Launch Time"))/1000/self.executor_instances/self.executor_cores*self.taskcpus,2).alias("acc_task_time"))
        
        metric_ds = df.where("Event='SparkListenerTaskEnd'").select("real_queryid","Stage ID","Task ID")\
                        .join(self.metric_df,on="Task ID")

        outputrows = metric_ds\
                        .where("Name='number of output rows'")\
                        .groupBy("real_queryid")\
                        .agg(F.round(F.sum("Update")/1000000000,2).alias("output rows"))
        storage_MB = metric_ds\
                        .where("Name='storage read bytes'")\
                        .groupBy("real_queryid")\
                        .agg(F.round(F.sum("Update")/1024/1024/1024,2).alias("storage read"))
        
        ram_MB = metric_ds\
                .where("Name='ram read bytes'")\
                .groupBy("real_queryid")\
                .agg(F.round(F.sum("Update")/1024/1024/1024,2).alias("ram read"))
        
        ssd_MB = metric_ds\
                .where("Name='local ssd read bytes'")\
                .groupBy("real_queryid")\
                .agg(F.round(F.sum("Update")/1024/1024/1024,2).alias("ssd read"))
        
        stages=runtimeacc.join(stages,on="real_queryid",how="left")
        stages=stages.join(outputrows,on='real_queryid',how="left")
        stages=stages.join(storage_MB,on='real_queryid',how="left")
        stages=stages.join(ram_MB,on='real_queryid',how="left")
        stages=stages.join(ssd_MB,on='real_queryid',how="left")
        
        out=df.groupBy("real_queryid").agg(
            F.round(F.max("query_endtime")/1000-F.min("query_starttime")/1000,2).alias("runtime"),
            F.round(F.sum("Bytes Read")/1024/1024/1024,2).alias("input read"),
            F.round(F.sum("Disk Bytes Spilled")/1024/1024/1024,2).alias("disk spilled"),
            F.round(F.sum("Memory Bytes Spilled")/1024/1024/1024,2).alias("memspilled"),
            F.round(F.sum("Local Bytes Read")/1024/1024/1024,2).alias("local_read"),
            F.round(F.sum("Remote Bytes Read")/1024/1024/1024,2).alias("remote_read"),
            F.round(F.sum("Shuffle Bytes Written")/1024/1024/1024,2).alias("shuffle_write"),
            F.round(F.sum("Executor Deserialize Time")/1000/self.parallelism,2).alias("deser_time"),
            F.round(F.sum("Executor Run Time")/1000/self.parallelism,2).alias("run_time"),
            F.round(F.sum("Result Serialization Time")/1000/self.parallelism,2).alias("ser_time"),
            F.round(F.sum("Fetch Wait Time")/1000/self.parallelism,2).alias("f_wait_time"),
            F.round(F.sum("JVM GC Time")/1000/self.parallelism,2).alias("gc_time"),
            F.round(F.max("Peak Execution Memory")/1000000000*self.executor_cores,2).alias("peak_mem"),
            F.max("queryid").alias("queryid")
            ).join(stages,"real_queryid",how="left").orderBy(F.col("queryid").cast(IntegerType())).toPandas().set_index("real_queryid")
        out["executors"]=self.executor_instances
        out["core/exec"]=self.executor_cores
        out["task.cpus"]=self.taskcpus
        out['parallelism']=self.parallelism
        
        if plot:
            def highlight_greater(x):
                m1 = x['acc_task_time'] / x['runtime'] * 100
                m2 = x['run_time'] / x['runtime'] * 100
                m3 = x['f_wait_time'] / x['runtime'] * 100
                

                df1 = pandas.DataFrame('', index=x.index, columns=x.columns)

                df1['acc_task_time'] = m1.apply(lambda x: 'background-image: linear-gradient(to right,#5fba7d {:f}%,white {:f}%)'.format(x,x))
                df1['run_time'] = m2.apply(lambda x: 'background-image: linear-gradient(to right,#5fba7d {:f}%,white {:f}%)'.format(x,x))
                df1['f_wait_time'] = m3.apply(lambda x: 'background-image: linear-gradient(to right,#d65f5f {:f}%,white {:f}%)'.format(x,x))
                return df1


            cm = sns.light_palette("green", as_cmap=True)
            display(out.style.apply(highlight_greater, axis=None).background_gradient(cmap=cm,subset=['input read', 'shuffle_write']))
        
        return out
                
    def getOperatorCount(self):
        if self.df is None:
            self.load_data()
        
        def get_all_node(s):
            import json
            nodes = {}
            def get_node(node):
                #wholestagetransformer not counted
                if node['nodeName'] is not None and not node['nodeName'].startswith("WholeStageCodegenTransformer"):
                    if node["nodeName"] not in nodes:
                        nodes[node["nodeName"]]=0
                    nodes[node["nodeName"]]=nodes[node["nodeName"]]+1
                if node["children"] is not None:
                    for c in node["children"]:
                        get_node(c)
            
            children=json.loads(s)
            for row in children:
                get_node(row)
            
            return [{"operator":k,"count":v} for k,v in nodes.items()]
        
        return_schema = ArrayType(
        StructType([
                StructField("operator", StringType(), True),
                StructField("count", IntegerType(), True)
            ])
        )
        get_all_node_udf = udf(get_all_node, return_schema)
        allnodes=self.queryplans.select("real_queryid","queryid",get_all_node_udf("query_plan").alias("node_count"))
        allnodescol=allnodes.select("real_queryid","queryid",F.explode("node_count").alias("col")).select("real_queryid","queryid","col.*").orderBy(F.col("queryid").cast(IntegerType())).collect()

        qps=OrderedDict()
        for r in allnodescol:
            if r["operator"] not in qps:
                qps[r['operator']]=OrderedDict()
            if r['real_queryid'] not in qps[r['operator']]:
                qps[r['operator']][r['real_queryid']]=0
            qps[r['operator']][r['real_queryid']]+=r['count']
        pandas.set_option('display.float_format', '{:,.0f}'.format)
        return pandas.DataFrame(qps).fillna(0).T
        
    def get_metric_output_rowcnt(self, **kwargs):
        return self.get_metric_rowcnt("number of output rows",**kwargs)
        
    def get_metric_input_rowcnt(self, **kwargs):
        return self.get_metric_rowcnt("number of input rows",**kwargs)
        
    def get_metric_rowcnt(self,rowname, **kwargs):
        if self.df is None:
            self.load_data()

        queryid=kwargs.get("queryid",None)
        stageid=kwargs.get("stageid",None)
        show_task=kwargs.get("show_task",False)
        
        if queryid and type(queryid)==int:
            queryid = [queryid,]
            
        if stageid and type(stageid)==int:
            stageid = [stageid,]
            
        queryplans = self.queryplans.where(F.col("real_queryid").isin(queryid)).orderBy(F.col("queryid").cast(IntegerType())) if queryid else self.queryplans.orderBy(F.col("queryid").cast(IntegerType()))
        qps=[]

        rownames=rowname if type(rowname)==list else [rowname,]
        def get_all_node(s,rownames):
            import json
            nodes = []
            def get_node(node,rownames):
                rownames_dict=json.loads(rownames)
                if node['metrics'] is not None:
                    outputrows=[x for x in node["metrics"] if "name" in x and x["name"] in rownames_dict]
                    if len(outputrows)>0:
                        nodes.append({"nodeName": node["nodeName"],"accumulatorId": outputrows[0]['accumulatorId']})
                if node["children"] is not None:
                    for c in node["children"]:
                        get_node(c,rownames)
            
            children=json.loads(s)
            for row in children:
                get_node(row, rownames)
            
            return nodes
        
        return_schema = ArrayType(
        StructType([
                StructField("nodeName", StringType(), True),
                StructField("accumulatorId", IntegerType(), True)
            ])
        )
        get_all_node_udf = udf(get_all_node, return_schema)
        allnodes=queryplans.select("real_queryid","queryid",get_all_node_udf("query_plan",F.lit(json.dumps(rownames))).alias("node_info"))
        allnodescol=allnodes.select("real_queryid","queryid",F.explode("node_info").alias("col")).select("col.*")

        stagetime=self.df.where("Event='SparkListenerTaskEnd'").groupBy("Stage ID").agg(F.round(F.sum(F.col("Finish Time")-F.col("Launch Time"))/1000/self.executor_instances/self.executor_cores*self.taskcpus,2).alias("stage time"))
        dfmetric=self.df.where("Event='SparkListenerTaskEnd'").select("queryid","real_queryid","Stage ID","Job ID", "Task ID").join(self.metric_df,on="Task ID")

        dfmetric_rowcnt=dfmetric.join(allnodescol,on=[F.col("accumulatorId")==F.col("ID")],how="right")
        if show_task:
            stagemetric=dfmetric_rowcnt.join(stagetime,"Stage ID")
        else:
            stagemetric=dfmetric_rowcnt.groupBy("queryid","real_queryid","Job ID","Stage ID","accumulatorId").agg(F.round(F.sum("Update")/1000000,2).alias("total_row"),F.max("nodeName").alias("nodename")).join(stagetime,"Stage ID")

        if queryid:
            if stageid:
                return stagemetric.where(F.col("real_queryid").isin(queryid) & F.col("Stage ID").isin(stageid)).orderBy("Stage ID")
            else:
                return stagemetric.where(F.col("real_queryid").isin(queryid)).orderBy("Stage ID")
        else:
            noderow=stagemetric.groupBy("real_queryid","nodename").agg(F.round(F.sum("total_row"),2).alias("total_row")).orderBy("nodename").collect()
            out={}
            qids=set([r.real_queryid for r in noderow])
            for r in noderow:
                if r.nodename not in out:
                    out[r.nodename]={c:0 for c in qids}
                out[r.nodename][r.real_queryid]=r.total_row
            return pandas.DataFrame(out).T.sort_index(axis=0)
            
    def show_app_info(self,**kwargs):
        if self.df is None:
            self.load_data()

        display(HTML(f"<font color=red size=7 face='Courier New'><b> {self.appid} </b></font>",))
        display(HTML(f"<a href=http://{server}:18080/history/{self.appid}>http://{server}:18080/history/{self.appid}</a>"))
        display(HTML("<font color=red size=7 face='Courier New'><b> query time </b></font>",))
        tmp=self.get_query_time(**kwargs)
        display(HTML("<font color=red size=7 face='Courier New'><b> operator count </b></font>",))
        pdf=self.getOperatorCount()
        display(pdf.style.apply(background_gradient,
               cmap='OrRd',
               m=pdf.min().min(),
               M=pdf.max().max(),
               low=0,
               high=1))
        
        display(HTML("<font color=red size=7 face='Courier New'><b> operator input row count </b></font>",))
        pdf=self.get_metric_input_rowcnt(**kwargs)
        if pdf is not None:
            display(pdf.style.apply(background_gradient,
                   cmap='OrRd',
                   m=pdf.min().min(),
                   M=pdf.max().max(),
                   low=0,
                   high=1))
        display(HTML("<font color=red size=7 face='Courier New'><b> operator output row count </b></font>",))
        pdf=self.get_metric_output_rowcnt(**kwargs)
        if pdf is not None:
            display(pdf.style.apply(background_gradient,
                   cmap='OrRd',
                   m=pdf.min().min(),
                   M=pdf.max().max(),
                   low=0,
                   high=1))
        self.show_time_metric(**kwargs)    
        
notlist=['resource.executor.cores',
 'spark.app.id',
 'spark.app.initial.file.urls',
 'spark.app.name',
 'spark.app.startTime',
 'spark.driver.port',
 'spark.job.description',
 'spark.jobGroup.id',
 'spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS',
 'spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES',
 'spark.rdd.scope',
 'spark.sql.execution.id',
 '__fetch_continuous_blocks_in_batch_enabled',
 'spark.driver.appUIAddress'
 'spark.driver.appUIAddress',
 'spark.driver.host',
 'spark.driver.appUIAddress',
 'spark.driver.extraClassPath',
 'spark.eventLog.dir',
 'spark.executorEnv.CC',
 'spark.executorEnv.LD_LIBRARY_PATH',
 'spark.executorEnv.LD_PRELOAD',
 'spark.executorEnv.LIBARROW_DIR',
 'spark.files',
 'spark.history.fs.logDirectory',
 'spark.sql.warehouse.dir',
 'spark.yarn.appMasterEnv.LD_PRELOAD',
 'spark.yarn.dist.files'
]
def comp_spark_conf(app0,app1):   
    pdf_sparkconf_0=app0.get_spark_config()
    pdf_sparkconf_1=app1.get_spark_config()
    pdfc=pdf_sparkconf_0.join(pdf_sparkconf_1,lsuffix=app0.appid[-8:],rsuffix=app1.appid[-8:])
    pdfc["0"+app0.appid[-8:]]=pdfc["0"+app0.appid[-8:]].str.lower()
    pdfc["0"+app1.appid[-8:]]=pdfc["0"+app1.appid[-8:]].str.lower()
    
    pdfc['comp']=(pdfc["0"+app0.appid[-8:]]==pdfc["0"+app1.appid[-8:]])
    return pdfc.loc[(pdfc['comp']==False) & (~pdfc.index.isin(notlist))]


# # Run base

# In[ ]:


class Run:
    def __init__(self,samples):
        self.samples=samples
    
    def generate_trace_view(self,appid,**kwargs):
        traces=[]
        
        for idx, s in enumerate(self.samples):
            traces.extend(s.generate_trace_view_list(idx,**kwargs))        
        output='''
        {
            "traceEvents": [
        
        ''' + \
        ",\n".join(traces)\
       + '''
            ]
        }'''

        with open('/home/sparkuser/trace_result/'+appid+'.json', 'w') as outfile:  
            outfile.write(output)

        print(f"http://{server}:1088/tracing_examples/trace_viewer.html#/tracing/test_data/{appid}.json")


# # Application Run

# In[ ]:


class Application_Run:
    def __init__(self, appid,**kwargs):
        self.appid=appid
        
        basedir=kwargs.get("basedir","")
        self.filedir=basedir+"/"+self.appid+"/"
        self.basedir=basedir
        self.spark = kwargs.get("spark",None)

        eventlogs=[]
        
        if basedir.startswith("s3"):
            client = boto3.client('s3')
            resource = boto3.resource('s3')
            bucket=re.split(r"/+",basedir)[1]
            prefix="/".join(re.split(r"/+",basedir)[2:])
            queries=client.list_objects(Bucket=bucket,Prefix=f"{prefix}/{appid}/result/", Delimiter='/')
            qlist=[]
            for q in queries['Contents']:
                response = client.get_object(Bucket=bucket, Key=q['Key'])
                content = response['Body'].read().decode('utf-8')
                qlist.append(json.loads(content))

            worker_objs=client.list_objects_v2(Bucket=bucket,Prefix=f"{prefix}/{appid}/", Delimiter='/')
            workers = [p['Prefix'].split("/")[-2] for p in worker_objs['CommonPrefixes'] if p['Prefix'].split("/")[-2].startswith("worker") ]
            # Check if 'eventlogs' folder exists in worker_objs
            has_eventlogs_folder = any('eventlogs' in p['Prefix'] for p in worker_objs.get('CommonPrefixes', []))
            
            if has_eventlogs_folder:
                eventlogs = ["eventlogs/"]
            else:
                # Use the original way
                eventlogs=[p['Key'].split("/")[-1] for p in worker_objs['Contents'] if p['Key'].endswith("gz") or p['Key'].endswith("zstd")]
        elif basedir.startswith("/"):
            qlist=[]
            for q in os.listdir(basedir+"/" + appid + "/result"):
                with open(basedir+"/" + appid + "/result"+"/"+q,"r") as f:
                    qlist.append(json.loads(f.read()))
            workers = [l for l in os.listdir(basedir+"/" + appid) if l.startswith("worker")]
            # Check if 'eventlogs' folder exists
            eventlogs_path = basedir + "/" + appid + "/eventlogs"
            if os.path.exists(eventlogs_path) and os.path.isdir(eventlogs_path):
                # Get all files ending with gz or zstd from eventlogs folder
                eventlogs = ["eventlogs/"]
            else:
                # Use the original way
                eventlogs = [l for l in os.listdir(basedir+"/" + appid) if l.endswith("gz") or l.endswith("zstd")]
            
        self.clients=workers
        self.qlist=qlist    
        
        qdf=self.spark.createDataFrame(pandas.DataFrame(self.qlist))
        self.starttime=qdf.agg(F.min("start_time").alias("starttime")).collect()[0]['starttime']
        self.endtime=qdf.agg(F.max(F.col("start_time")+F.col("application_time_taken")).alias("endtime")).collect()[0]['endtime']
        
        jobids=kwargs.get("jobids",None)
                
        sarclnt={}
        for idx,l in enumerate(self.clients):
            telegraffile=self.filedir + l + "/"+"telegraf.out"
            sarclnt[l]={'sar_cpu':{'als':Telegraf_cpu_analysis(telegraffile,self.starttime,self.endtime,spark=self.spark),'pid':idx},
                'sar_disk':{'als':Telegraf_disk_analysis(telegraffile,self.starttime,self.endtime,spark=self.spark),'pid':idx},
                'sar_mem':{'als':Telegraf_mem_analysis(telegraffile,self.starttime,self.endtime,spark=self.spark),'pid':idx},
                'sar_nic':{'als':Telegraf_nic_analysis(telegraffile,self.starttime,self.endtime,spark=self.spark),'pid':idx},
                'sar_page':{'als':Telegraf_PageCache_analysis(telegraffile,self.starttime,self.endtime,spark=self.spark),'pid':idx}
            }
        self.analysis={
            "sar": sarclnt
        }
        
        self.analysis['app']={'als':App_Log_Analysis(self.filedir+eventlogs[0], jobids, self.qlist, spark=self.spark)}
            
    def generate_trace_view(self,showsar=True,**kwargs):
        traces=[]
        shownodes=kwargs.get("shownodes",self.clients)
        for l in shownodes:
            if l not in self.clients:
                print(l,"is not in clients",self.clients)
                return
        self.clients=shownodes
        
        xgbtcks=kwargs.get('xgbtcks',("calltrain",'enter','begin','end'))
        
        if "app" in self.analysis:
            appals=self.analysis['app']['als']
            appals.starttime=self.starttime*1000
            traces.extend(appals.generate_trace_view_list(self.analysis['app'],**kwargs))
        
        counttime=kwargs.get("counttime",False)
        
        pidmap={}
        if showsar:
            for l in self.clients:
                for alskey, sarals in self.analysis["sar"][l].items():
                    t1 = time.time()
                    traces.extend(sarals['als'].generate_trace_view_list(sarals['pid'],node=l, **kwargs))
                    if counttime:
                        print(l,alskey," spend time: ", time.time()-t1)
                        
        for idx,l in enumerate(self.clients):
            traces.append(json.dumps({"name": "process_sort_index","ph": "M","pid":idx,"tid":0,"args":{"sort_index ":idx}}))
            traces.append(json.dumps({"name": "process_sort_index","ph": "M","pid":idx+100,"tid":0,"args":{"sort_index ":idx+100}}))
            traces.append(json.dumps({"name": "process_sort_index","ph": "M","pid":idx+200,"tid":0,"args":{"sort_index ":idx+200}}))
        
        if "app" in self.analysis:
            for pid in self.analysis['app']['als'].pids:
                traces.append(json.dumps({"name": "process_sort_index","ph": "M","pid":pid+200,"tid":0,"args":{"sort_index ":pid+200}}))
        
        output='''
        {
            "traceEvents": [
        
        ''' + \
        ",\n".join(traces)\
       + '''
            ],
            "displayTimeUnit": "ns"
        }'''

        output_dir = '/opt/spark/work-dir/ipython/analysis/'
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_dir + self.appid + '.json', 'w') as outfile:
            outfile.write(output)
        
        traceview_link=f'http://{server}:1088/tracing_examples/trace_viewer.html#/tracing/test_data/{self.appid}.json'
        display(HTML(f"<a href={traceview_link}>{traceview_link}</a>"))
        return traceview_link
    
    def get_sar_stat(app,**kwargs):
        cpustat=[app.analysis["sar"][l]['sar_cpu']['als'].get_stat() for l in app.clients]
        cpustat=reduce(lambda l,r:l.join(r),cpustat)
        diskstat=[app.analysis["sar"][l]['sar_disk']['als'].get_stat() for l in app.clients]
        diskstat=reduce(lambda l,r:l.join(r),diskstat)
        memstat=[app.analysis["sar"][l]['sar_mem']['als'].get_stat() for l in app.clients]
        memstat=reduce(lambda l,r:l.join(r),memstat)
        nicstat=[app.analysis["sar"][l]['sar_nic']['als'].get_stat() for l in app.clients]
        nicstat=reduce(lambda l,r:l.join(r),nicstat)
        pagestat=[app.analysis["sar"][l]['sar_page']['als'].get_stat() for l in app.clients]
        pagestat=reduce(lambda l,r:l.join(r),pagestat)
        pandas.options.display.float_format = '{:,.2f}'.format
        return pandas.concat([cpustat,diskstat,memstat,nicstat,pagestat])
                
    def get_summary(app, **kwargs):
        output=[]
        
        appals=app.analysis["app"]["als"]
        
        out=appals.get_query_time(plot=False)
        
        lrun=app.appid
        
        cmpcolumns=['runtime','disk spilled','shuffle_write','f_wait_time','input read','acc_task_time','output rows']
        outcut=out[cmpcolumns]
        
        pdsout=pandas.DataFrame(outcut.sum(),columns=[lrun])
        pdstime=pdsout  

        print("sar metric")
        if app.analysis["sar"]:
            sardf=app.get_sar_stat(**kwargs)
            
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

            sarsum=sardf[["agg"]]

            sarsum.columns=[lrun]
        
            summary=pandas.concat([pdstime,sarsum])
        else:
            summary=pdstime
        df_sum=app.spark.createDataFrame(summary.T.reset_index())
        for c in df_sum.columns:
            df_sum=df_sum.withColumnRenamed(c,c.replace(" ","_").replace("(","").replace(")",""))
        df_sum.write.mode("overwrite").parquet(app.filedir+"summary.parquet")
        
        return summary
    
    def drawsar(self):

        qdf=self.spark.createDataFrame(pandas.DataFrame(self.qlist))
        starttime=self.starttime
        endtime=self.endtime
        qdf=qdf.withColumn("query_name",F.regexp_replace("query_name",".sql","")).withColumn("query_name",F.regexp_replace("query_name","tpc[hds]+-","")).withColumn("query_name",F.regexp_replace("query_name","query","q"))
        pqdf=qdf.select(F.col('query_name'),(F.col("start_time")-F.lit(starttime)).alias("start_time"),(F.col("start_time")+F.col("application_time_taken")-F.lit(starttime)).alias("endtime"),F.col("application_time_taken")).toPandas()
        pqdf2=qdf.select(F.col('query_name').alias("query"),F.round(F.col("application_time_taken"),2).alias("elapsed")).orderBy("query").toPandas()
        if pqdf2['query'][0].startswith("1-"):
            lastquery=pqdf.loc[pqdf['endtime'].idxmax()]['query_name']
            runtimes=int(re.search(r"^(\d+)-",lastquery).group(1))
            querydfs=[]

            for q in range(1,runtimes+1):
                querydf=duckdb.query(f"select query, elapsed from pqdf2 where query like '{q}-%'").df()
                querydf['query'] = querydf['query'].str.replace(r'^\d+-', '', regex=True)
                querydf.columns=['query',str(q)+'-elapsed']
                querydfs.append(querydf)
            merged_df = querydfs[0]
            for i in range(1, len(querydfs)):
                merged_df = pandas.merge(merged_df, querydfs[i], on='query', how='outer')
            print(merged_df.sum())
            display(merged_df)
        else:
            print("total time: ", pqdf2['elapsed'].sum())
            display(pqdf2)

        schema = StructType(
            [StructField(f"_c{l}", StringType(), True) for l in range(0,13)]
        )
        for w in self.clients:
            tg_cpu = self.analysis["sar"][w]['sar_cpu']['als']
            tg_mem = self.analysis["sar"][w]['sar_mem']['als']
            tg_disk = self.analysis["sar"][w]['sar_disk']['als']
            tg_nic = self.analysis["sar"][w]['sar_nic']['als']
            tg_pg = self.analysis["sar"][w]['sar_page']['als']


            charts=tg_cpu.plot_num() + tg_mem.plot_num() + tg_disk.plot_num() + tg_nic.plot_num() + tg_pg.plot_num()

            qtime=pqdf.set_index("query_name").T.to_dict()

            fig, axs=plt.subplots(charts,1,sharex=True,figsize=(30,charts*6))

            tg_cpu.plot(axs[0],w)
            tg_mem.plot(axs[1],w)        
            axsid=2
            tg_disk.plot(axs[2:],w)
            axsid+=tg_disk.plot_num()
            tg_nic.plot(axs[axsid:],w)
            axsid+=tg_nic.plot_num()
            tg_pg.plot(axs[axsid],w)

            # Add vertical lines and text for qtime, and calculate per query cpu%
            if qtime is not None:
                for ax in axs:
                    for k, v in qtime.items():
                        ax.axvline(x = v['start_time'], color = 'b')
                        ax.axvline(x = v['endtime'], color = 'b')
                    x=endtime-starttime
                    for k, v in qtime.items():
                        if (v['application_time_taken']) / x > 15 / 772:
                            ax.text(v['start_time'] + v['application_time_taken'] / 2 - 6 * x / 772, ax.get_ylim()[1] * 1.05, k)
            plt.show()

m='''1	q01
    2	q02
    3	q03
    4	q04
    5	q05
    6	q06
    7	q07
    8	q08
    9	q09
    10	q10
    11	q11
    12	q12
    13	q13
    14	q14a
    15	q14b
    16	q15
    17	q16
    18	q17
    19	q18
    20	q19
    21	q20
    22	q21
    23	q22
    24	q23a
    25	q23b
    26	q24a
    27	q24b
    28	q25
    29	q26
    30	q27
    31	q28
    32	q29
    33	q30
    34	q31
    35	q32
    36	q33
    37	q34
    38	q35
    39	q36
    40	q37
    41	q38
    42	q39a
    43	q39b
    44	q40
    45	q41
    46	q42
    47	q43
    48	q44
    49	q45
    50	q46
    51	q47
    52	q48
    53	q49
    54	q50
    55	q51
    56	q52
    57	q53
    58	q54
    59	q55
    60	q56
    61	q57
    62	q58
    63	q59
    64	q60
    65	q61
    66	q62
    67	q63
    68	q64
    69	q65
    70	q66
    71	q67
    72	q68
    73	q69
    74	q70
    75	q71
    76	q72
    77	q73
    78	q74
    79	q75
    80	q76
    81	q77
    82	q78
    83	q79
    84	q80
    85	q81
    86	q82
    87	q83
    88	q84
    89	q85
    90	q86
    91	q87
    92	q88
    93	q89
    94	q90
    95	q91
    96	q92
    97	q93
    98	q94
    99	q95
    100	q96
    101	q97
    102	q98
    103	q99'''.split("\n")
tpcds_query_map=[l.strip().split("\t") for l in m]
tpcds_query_map={int(l[0]):l[1] for l in tpcds_query_map}
