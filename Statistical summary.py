# Databricks notebook source
from __future__ import print_function
import pyspark
from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics

# COMMAND ----------

#Load the data
df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/praneethsanthosh555@gmail.com/winequality_red-3.csv",header=True)
df1.show(5)

# COMMAND ----------

#Drop the nan values if it is in the data
data_drop_miss=df1.dropna(how="any")

# COMMAND ----------

#Drop the label column
features=data_drop_miss.drop("quality")
features.show(5)

# COMMAND ----------

feature_rdd=features.rdd.map(lambda row: row[0:])

# COMMAND ----------

import numpy as np
summary=Statistics.colStats(feature_rdd)
print('Mean: %s' % summary.mean()) #a dense vector containing the mean value of each colunn
print('\nVariance: %s' % summary.variance()) #Column -wise variance
print('\nNumNonzeros: %s' % summary.numNonzeros()) #return a number of zero values in the data
print('\nNorm1: %s' % summary.normL1()) #return a norml1 summary
print('\nMax values: %s' % summary.max()) #return a maximum values
print('\nMin: %s' % summary.min())

# COMMAND ----------


