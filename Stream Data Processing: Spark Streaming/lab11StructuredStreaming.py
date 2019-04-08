from pyspark.sql import SparkSession
import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
import datetime
import time

os.environ['SPARK_HOME'] = "/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7"
sys.path.append("/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7/python/lib")

if __name__ == "__main__":

    # check the number of arguments
    if len(sys.argv) != 3:
        print("Usage:  structStreamExample.py <input_folder> ")
        exit(-1)

    # Set a name for the application
    appName = 'DataFrameExample'

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    schema = StructType().add("last_name", "string").add("first_name", "string").add("balance", "double").add(
        "address", "string").add("city", "string").add("last_transaction", "string").add("bank_name", "string")
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()

    csvDF = spark.readStream.option("sep", ",").schema(schema).csv(input_folder)

    # Compute count aggregation as WordCount
    # In structured streaming it is automatically as a stateful DataFrame,
    # that is stored in memory and continuously updated based on incoming data from the stream
    dfWithTimeStamp = csvDF.withColumn("timestamp", F.lit(
        datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')).cast("timestamp"))
    grouped = dfWithTimeStamp.groupBy("bank_name", F.window("timestamp", "1 minutes", "1 minutes")).agg(
        F.count("bank_name"), F.sum("balance"), F.avg("balance"))

    # Define Streaming query.
    # output mode complete returns whole result in memory
    # console prints out the result to console similar to df.show()
    query = grouped\
            .writeStream \
            .format("console") \
            .trigger(processingTime='20 seconds') \
            .outputMode("update") \
            .start()

    query.awaitTermination()
    spark.stop()
