#Shagun Kadam
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

import pandas as pd

data_df = pd.read_csv('data/customer-orders.csv')
df = spark.createDataFrame(data_df)


import pandas as pd

data_df = pd.read_csv('data/customer-orders.csv')
df = spark.createDataFrame(data_df)
# Write PySpark script to map each line to key/value pairs of customer ID and the amount.
customer_id_amount = df.rdd.map(lambda x: (x[0], x[2]))
df2 = customer_id_amount.toDF(["CustomerId","Amount"])
# PySpark script to reduceByKey to add up amount spent by customer ID.
reduced = customer_id_amount.reduceByKey(lambda x, y: x + y)
# PySpark script to finally collect the result and display them on the spark shell.
results = reduced.collect()
results