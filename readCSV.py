import os
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
from pandas import Series
from matplotlib import pyplot
from pandas import DataFrame
from pandas import concat



folder_path ='/home/nguyen/test/'
# for file_name in os.listdir(folder_path):
# 	print file_name +"\n"
# series = Series.from_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/testpart-00000-of-00500.csv', header=0)
for file_name in os.listdir(folder_path):
	series = Series.from_csv("%s%s"%(folder_path,file_name), header=0)
	X = series.values
	# pyplot.plot(X)
	# pyplot.show()
	fig, ax = pyplot.subplots()
	ax.plot(X)
	fig.savefig('fig%s.pdf'%(file_name))
	pyplot.show()