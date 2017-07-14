import os
import numpy as np
from pandas import Series
from matplotlib import pyplot
from pandas import DataFrame
from pandas import concat

folder_path ='/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/abc'
# for file_name in os.listdir(folder_path):
# 	print file_name +"\n"
series = Series.from_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/abc/part-00000-09243fc6-2d5e-4b7e-a4d7-146ad3399131.csv', header=0)
X = series.values
pyplot.plot(X)
pyplot.show()