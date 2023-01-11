import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

pathJarDir = os.path.join("/Users/shruti/big_data/jars")
listjar = [os.path.join(pathJarDir, x) for x in os.listdir(pathJarDir)]
print(listjar)

kafka_topic = "registered_user"
kafka_bootstrap_servers = 'localhost:9092'
postgresql_hostName = "localhost"
postgresql_portNo = "5432"
postgresql_username = "shruti"
postgresql_password = "  "
postgresql_database = "shruti"
postgresql_driver = "org.postgresql.Driver"

database_properties = {}
database_properties['user'] = postgresql_username
database_properties['password'] = "  "
database_properties['driver'] = postgresql_driver

def write_to_table(current_df, epoc_id, table):
    try:
        jdbc_url = "jdbc:postgresql://" + postgresql_hostName + ":" + str(postgresql_portNo) + "/" + postgresql_database
        #Save the dataframe to the table.
        current_df.write.jdbc(url = jdbc_url,
                              table = table,
                              mode = 'append',
                              properties = database_properties)

        print("in table")
    except Exception as ex:
        print(ex)
    # print("Exit out of write_to_table function")

if __name__ == "__main__":
    print("Welcome !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.jars", ",".join(listjar)) \
        .config("spark.jars", ",".join(listjar)) \
        .config("spark.executor.extraClassPath", "/Users/shruti/big_data/jars/commons-pool2-2.8.1.jar:/Users/shruti/big_data/jars/postgresql-42.2.16.jar:/Users/shruti/big_data/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:/Users/shruti/big_data/jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    items_df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", kafka_topic) \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    items_df.printSchema()

    items_schema = StructType() \
        .add("filename", StringType()) \
        .add("mean", StringType()) \
        .add("skew", StringType()) \
        .add("kurtosis", StringType())\
        .add("relative_smoothness", StringType())\
        .add("uniformity", StringType())\
        .add("entropy", StringType())\
        .add("tcontrast", StringType())


    items_df1 = items_df.selectExpr("CAST(value AS STRING)")
    items_df2 = items_df1.select(from_json(col("value"), items_schema).alias("items"))
    items_df3 = items_df2.select("items.*")

    print("Printing Schema of orders_df3: ")
    items_df3.printSchema()
    try:
        item_df4 = items_df3
        orders_agg_write_stream = item_df4 \
            .writeStream \
            .trigger(processingTime='10 seconds') \
            .outputMode("update") \
            .option("truncate", "false")\
            .format("console") \
            .start()
        item_df4 \
            .writeStream \
            .trigger(processingTime='10 seconds') \
            .outputMode("update") \
            .foreachBatch(
            lambda current_df, epoc_id: write_to_table(current_df, epoc_id, "image_parameters")) \
            .start()

  
        orders_agg_write_stream.awaitTermination()
    except Exception as ex:
        print(ex)

    print("Stream Data Processing Application Completed.")