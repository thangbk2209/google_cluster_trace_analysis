
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
    # .load("/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/analysis/resourceTopJobId.csv")
    .load("/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv")
    
)
df.createOrReplaceTempView("dataFrame")
data = sql_context.sql("SELECT * from dataFrame")
# print min(data['startTime'])
# print sql_context.sql("SELECT max(endTime) from dataFrame")
schema_minStartdf = ["startTime"]
schema_maxEnddf = ["endTime"]

minStartTimeSQL = sql_context.sql("SELECT min(startTime) from dataFrame")
maxEndTimeSQL = sql_context.sql("SELECT max(endTime) from dataFrame")

minStartTimeSQL.toPandas().to_csv('results/minStartTime.csv', index=False, header=None)
dataminStart = pd.read_csv('results/minStartTime.csv',names=schema_minStartdf)
minTime= dataminStart['startTime']

maxEndTimeSQL.toPandas().to_csv('results/maxEndTime.csv', index=False, header=None)
datamaxEnd = pd.read_csv('results/maxEndTime.csv',names=schema_maxEnddf)
maxTime= datamaxEnd['endTime']

extraTime = 5000000
# i = minTime+extraTime*50

for i in range(minTime,maxTime, extraTime):
	newData = sql_context.sql("SELECT * from dataFrame where startTime < %s and endTime > %s"%(i,i) )
	# newData.withColumn('timeStamp',i)
	newData.toPandas().to_csv('thangbk2209/5secondsResults/hehe%s.csv'%(i), index=False, header=None)


sc.stop()