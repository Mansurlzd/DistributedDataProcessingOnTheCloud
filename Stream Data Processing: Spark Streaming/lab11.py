from pyspark.sql import SparkSession
import sys
import os
from pyspark.streaming import StreamingContext

os.environ['SPARK_HOME'] = "/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7"
sys.path.append("/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7/python/lib")

if __name__ == "__main__":
    # check the number of arguments
    if len(sys.argv) != 3:
        print("Usage: streamFileExample <input folder> <output folder>")
        exit(-1)

    # input directory. Should exist and be empty
    dataDirectory = sys.argv[1]
    output_folder = sys.argv[2]
    # Micro-batch interval
    interval = 5
    # application name
    appName = "SparkStreamingExample"

    # Create spark session
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()

    # spark context
    sc = spark.sparkContext
    # Spark streaming context
    ssc = StreamingContext(sc, interval)
    ssc.checkpoint("checkpoint")

    # use dataDirectory folder as a text file stream source
    lines = ssc.textFileStream(dataDirectory)

    # Compute WordCount
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .updateStateByKey(lambda a, b: sum(a) + (b or 0))

    # Print out results of each micro-batch to the console
    counts.pprint()
    counts.saveAsTextFiles(output_folder)
    # start the streaming application
    ssc.start()
    # wait until application is shut down
    ssc.awaitTermination()
    # terminate spark session
    spark.stop()
