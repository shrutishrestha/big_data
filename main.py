#export SPARK_HOME=/home/users/sshrestha8/spark-3.3.1-bin-hadoop3
import os
import rasterio
import findspark
import numpy as np
import pandas as pd
import tensorflow as tf
import geopyspark as gps
from typing import Iterator
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, BinaryType
from functions import *

sc = SparkContext('local')
spark = SparkSession(sc)

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.11.0-openjdk-amd64"
os.environ["SPARK_HOME"] = "/home/users/sshrestha8/spark-3.3.1-bin-hadoop3"
os.environ["PYTHONPATH"] ="/home/users/sshrestha8/spark-3.3.1-bin-hadoop3/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.stop()

conf = gps.geopyspark_conf(master="local[*]", appName="sentinel-ingest-example")
conf.set("spark.kryoserializer.buffer.max.mb", "512")
conf.set("spark.driver.memory","15g")

pysc = SparkContext(conf=conf)
jp2s = ["2020_01_01__00_00_35_12__SDO_AIA_AIA_94.jp2"]
arrs = []

for jp2 in jp2s:
    with rasterio.open(jp2) as f:
        arrs.append(f.read(1))

data = np.array(arrs, dtype=arrs[0].dtype)

patch_size = PatchSize.FOUR
test_obj = MeanParamCalculator(patch_size)
    
    
rdd_fake=pysc.parallelize(data)
collection_rdd_fake = rdd_fake.map(test_obj.calculate_parameter)
print(datetime.now())
collection_rdd_fake.collect()
print(datetime.now())


# print(datetime.now())
# print("data", data.shape)
# test_obj.calculate_parameter(data[0])
# print(datetime.now())


