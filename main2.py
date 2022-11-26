#export SPARK_HOME=/Users/shruti/environment/jupyter/lib/python3.9/site-packages/pyspark
import os
import rasterio
import findspark
import numpy as np
import pandas as pd
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

def start():
    findspark.init()
    os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home"

    os.environ["SPARK_HOME"] = "/Users/shruti/big_data/spark-3.3.1-bin-hadoop3"
    os.environ["PYTHONPATH"] = "$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
    sc = SparkContext('local')

    spark = SparkSession(sc)
    os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home"
    os.environ["SPARK_HOME"] = "/Users/shruti/environment/jupyter/lib/python3.9/site-packages/pyspark"
    os.environ["PYTHONPATH"] ="/home/users/sshrestha8/spark-3.3.1-bin-hadoop3/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.stop()

    conf = gps.geopyspark_conf(master="local[*]", appName="sentinel-ingest-example")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryoserializer.buffer.max", "512")

    conf.set("spark.driver.memory","15g")
    conf.set("executor-memory","64G")

    pysc = SparkContext(conf=conf)
    
    return pysc
