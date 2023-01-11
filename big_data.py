#export SPARK_HOME=/Users/shruti/environment/jupyter/lib/python3.9/site-packages/pyspark
import os
import rasterio
import findspark
import numpy as np
import pandas as pd
# import geopyspark as gps
from typing import Iterator
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, BinaryType
from functions import *
import cv2

def start():

    conf = SparkConf()
    conf.set("spark.executor.memory", "5g")
    conf.set("spark.executor.cores", 4)
    conf.set("spark.executor.instances", 50)

    pysc = SparkContext(conf=conf)

    return pysc
