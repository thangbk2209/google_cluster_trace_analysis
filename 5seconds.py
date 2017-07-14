
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
folder_path = '/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/analysis/ggcluster/'

dataSchema = StructType([StructField('startTime', StringType(), True),
                         StructField('endTime', StringType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('taskIndex', LongType(), True),
                         StructField('machineId', LongType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('max_mem_usage', FloatType(), True),
                         StructField('mean_diskIO_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True),
                         StructField('max_cpu_usage', FloatType(), True),
                         StructField('max_disk_io_time', FloatType(), True),
                         StructField('cpi', FloatType(), True),
                         StructField('mai', FloatType(), True),
                         StructField('sampling_portion', FloatType(), True),
                         StructField('agg_type', FloatType(), True),
                         StructField('sampled_cpu_usage', FloatType(), True)])
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/analysis/MidJobId.csv")
    # .load("/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv")
    
)
df.createOrReplaceTempView("dataFrame")
maxTime= 2506200000000
minTime= 660000000
extraTime = 300000000
# i = minTime+extraTime*50

for i in range(minTime,maxTime, extraTime):
	newData = sql_context.sql("SELECT startTime+60000000, sum(meanCPUUsage) from dataFrame where startTime < %s and endTime > %s"%(i,i) )
	# newData.withColumn('timeStamp',i)
	newData.toPandas().to_csv('newResults/%s.csv'%(i), index=False, header=None)
sc.stop()