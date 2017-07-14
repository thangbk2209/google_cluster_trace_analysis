
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
from pyspark.sql.functions import lit,ntile
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
folder_path = '/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/newResults/'

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
                         StructField('unmap_page_cache_memory_ussage', FloatType(), True),
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
# i=878853000000
for file_name in os.listdir(folder_path):
    # i+=5000000
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(dataSchema)
        .load("%s%s"%(folder_path,file_name))
        
    )
    df.createOrReplaceTempView("dataFrame")
    data = sql_context.sql("SELECT sum(meanCPUUsage),sum(CMU),sum(AssignMem),sum(unmap_page_cache_memory_ussage),sum(page_cache_usage),sum(mean_diskIO_time),sum(mean_local_disk_space) from dataFrame")

    # extraTime = 5000000
    # i = minTime+extraTime*50
    # abc=data.withColumn("timeStamp",lit(i))
    # abc.show()
    # abc.toPandas().to_csv('%s.csv'%(i))
    data.toPandas().to_csv('heheResults/%s'%(file_name), index=False, header=None)


sc.stop()