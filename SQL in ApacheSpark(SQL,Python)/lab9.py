import sys
from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

os.environ['SPARK_HOME'] = "/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7"
sys.path.append("/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7/python/lib")



def wordLenAvg(review_text):
    words = review_text.split(" ")
    avgl = sum(len(word) for word in words) / len(words)
    return avgl


if __name__ == "__main__":

    # check the number of arguments
    if len(sys.argv) != 3:
        print("Usage: sql_example <input folder> <output folder>")
        exit(-1)
    # Set a name for the application
    appName = "SQL Example"
    # Set the input folder location to the first argument of the application
    # NB! sys.argv[0] is the path/name of the script file
    input_folder = sys.argv[1]
    # Set the output folder location to the second argument of the application
    output_folder = sys.argv[2]
    # create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.udf.register("wordLenAvg_udf", wordLenAvg, DoubleType())
    # spark.udf.register("funniness_udf", funniness_udf, DoubleType)
    # read in the JSON dataset as a DataFrame
    # inferSchema option forces Spark to automatically specify data column types
    business = spark.read.option("inferSchema", True).json(input_folder + 'small_business')
    tip = spark.read.option("inferSchema", True).json(input_folder + 'small_tip')
    photo = spark.read.option("inferSchema", True).json(input_folder + 'small_photo')
    checkin = spark.read.option("inferSchema", True).json(input_folder + 'small_checkin')
    review = spark.read.option("inferSchema", True).json(input_folder + 'small_review')
    user = spark.read.option("inferSchema", True).json(input_folder + 'small_user')
    tableNames = ["business", "tip", "review", "user", "photo", "checking"]
    tables = [business, tip, review, user, photo, checkin]

    for (table, tableName) in zip(tables, tableNames):
        table.registerTempTable(tableName)
        # table.printSchema()

    # Run an SQL statement. Result will be a DataFrame.
    # counts = spark.sql("Select stars, count(*) FROM dataset GROUP BY stars")
    # counts = spark.sql("Select stars,count(*), avg(funny) collect_list(user_id) FROM review GROUP BY stars")
    # joined = spark.sql("Select * FROM review as r , business as b WHERE r.business_id = b.business_id")
    # Register the resulting DataFreame as an SQL table so it can be addressed in following SQL querries.
    # joined.registerTempTable("joined")
    # select r.busness_id, r.starts/r.funny as funniness From review as r, business as b where r.business_id = b.business_id order by funniness DESC
    # select r.busness_id, funniness_udf(r.stars,r.funny) From review as r, business as b where r.business_id = b.business_id order by funniness DESC

    ##### Lab 9.2.1 #####
    ##### Processing data using Spark SQL #####
    joined = spark.sql("Select r.stars, u.user_id FROM review as r, user as u WHERE r.user_id = u.user_id")
    joined.registerTempTable("joined")
    joined.printSchema()
    grouped = spark.sql(
        "Select j.user_id,j.stars, count(*) as starcount FROM joined as j WHERE j.stars=5  GROUP BY j.user_id,j.stars ORDER BY j.stars DESC")
    grouped.printSchema()
    grouped.show(5)

    ##Lab 9.2.2 #####
    grouped2 = spark.sql(
        "Select j.user_id,j.stars, count(*) as starcount, SUM(j.stars) OVER (PARTITION by j.stars) as total  FROM joined as j WHERE j.stars=5 GROUP BY j.user_id,j.stars")
    grouped2.registerTempTable("grouped2")
    grouped2_percent = spark.sql(
        "Select g.user_id,g.stars,g.starcount,g.total, g.starcount*100/g.total as percent FROM grouped2 as g")

    grouped2_percent.printSchema()
    grouped2_percent.show(5)

    ###Lab9.2.3 ####
    joined_RB = spark.sql("Select r.business_id,b.attributes, b.stars  FROM review as r, business as b WHERE r.business_id = b.business_id")
    joined_RB.registerTempTable("joined_RB")
    joined_RBP = spark.sql("Select j.business_id,j.attributes, j.stars FROM joined_RB as j, photo as p WHERE j.business_id = p.business_id GROUP BY j.business_id, j.stars, j.attributes")
    joined_RBP.registerTempTable("joined_RBP")
    joined_RBP.printSchema()
    grouped_RBP = spark.sql("Select j.business_id,avg(j.stars) as avgstars FROM joined_RBP as j WHERE j.attributes['RestaurantsTakeOut'] = 'True' GROUP BY j.business_id")
    grouped_RBP.show(5,False)


    #grouped_PBR = spark.sql("Select j.business_id, j.stars FROM joined_PBR as j")
    #grouped_PBR.show(5)

    ###Lab9.3###
    joined_rev = spark.sql("Select r.text, u.user_id FROM review as r, user as u WHERE r.user_id = u.user_id")
    joined_rev.registerTempTable("joined_rev")
    grouped_rev = spark.sql("Select j.user_id, wordLenAvg_udf(j.text) as avgW FROM joined_rev as j ORDER BY  avgW DESC")
    grouped_rev.printSchema()
    grouped_rev.show(5)

    business.printSchema()


    # Write results into the output folder
    # counts.write.format("json").save(output_folder)

    # To force Spark write output as a single file, you can use:
    # counts.coalesce(1).write.format("json").save(output_folder)
    # coalesce(N) repartitions DataFrame or RDD into N partitions.
    # NB! But be careful when using coalesce(N), your program will crash if the whole DataFrame does not fit into memory of N processes.

    # Stop Spark session
    spark.stop()
