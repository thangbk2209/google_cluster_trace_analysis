
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
    .load("/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/analysis/resourceTopJobId.csv")
    
)
df.createOrReplaceTempView("dataFrame")
schema_minStartdf = ["startTime"]

minStartTime = sql_context.sql("SELECT min(startTime) from dataFrame")
maxEndTime = sql_context.sql("SELECT max(endTime) from dataFrame")
minStartTime.toPandas().to_csv('results/minStartTime.csv', index=False, header=None)
dat = pd.read_csv('results/minStartTime.csv',names=schema_minStartdf)
print dat['startTime']

maxEndTime.toPandas().to_csv('results/maxEndTime.csv', index=False, header=None)
# print "minStartTime: "
# print minStartTime
# print "maxEndTime: "
# print maxEndTime
# df.printSchema()
# sumCPUUsage = sql_context.sql("SELECT JobId, count(taskIndex) as countTask from dataFrame group by JobId order by countTask")
# # sumCPUUsage.show(5000)

# sumCPUUsage.toPandas().to_csv('thangbk2209/results/listJobId-countTask.csv', index=False, header=None)
# sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()