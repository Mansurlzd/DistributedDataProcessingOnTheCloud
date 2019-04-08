import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType, StructField

os.environ['SPARK_HOME'] = "/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7"
sys.path.append("/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7/python/lib")

def myudf(review_text):
    words = review_text.split(" ")
    avgl = sum(len(word) for word in words) / len(words)
    medl = med(list(len(word) for word in words))
    return avgl, medl

def med(list1):

    list1.sort()
    if len(list1)%2>0:
            x = list1[int((len(list1)/2))]
    else:
            x = ((list1[int((len(list1)/2))-1])+(list1[int(((len(list1)/2)))]))/2
    return x

if __name__ == "__main__":

    # check the number of arguments
    if len(sys.argv) != 3:
        print("Usage: dataframe_example <input folder> <output folder> ")
        exit(-1)

    # Set a name for the application
    appName = "DataFrame Example"

    # Set the input folder location to the first argument of the application
    # NB! sys.argv[0] is the path/name of the script file
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    # create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    schema = StructType([
        StructField("avgl",LongType(),True),
        StructField("medl",LongType(),True)
    ])
    my_udf = F.udf(myudf,schema)


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
    # Show 10 rows without truncating lines.
    # review content might be a multi-line string.
    # review.show(10, False)

    # Show dataset schema/structure with filed names and types
    # review.printSchema()

    # Group reviews by stars (1,2,3,4 or 5) and count how many there were inside each group. Result will be a DataFrame.
    # counts = review.groupBy(review.stars).count()

    # Alternative way to aggregate values inside a group using the count(column) function from the pyspark.sql.functions library
    # counts_alternative = review.groupBy("stars").agg(F.count("business_id"))

    # counting without grouping simply returns a global count (NB! output is no longer DataFrame)
    # review_count = review.count()
    # print("Total number of reviews:", review_count)

    # Display counts
    # counts.show(5, False)
    # counts_alternative.show(5, False)

    # agg(sum(),avg(),max(),min()) ###############

    ####10.1.1####
    grouped_review = review.select(review.user_id,review.stars).groupBy(review.user_id, review.stars).agg(F.count("*").alias("starcount"))
    filtered_review = grouped_review.filter("stars=5")
    grouped_review.show(5, False)
    filtered_review.show(5, False)

    #####10.1.2####
    joined_PBR = review.join(business,["business_id"]).join(photo,["business_id"]).drop(business.stars)  #drop business starts in order to distinguish stars column in joined table
    joined_PBR.registerTempTable("joined_PBR")
    joined_PBR.printSchema()
    grouped_PBR = joined_PBR.select(joined_PBR.business_id,joined_PBR.stars).filter(joined_PBR.attributes['RestaurantsTakeOut'] == 'True').filter(joined_PBR.label == 'menu').groupBy(joined_PBR.business_id).agg(F.avg("stars").alias("avgstars"))
    grouped_PBR.show(5,False)

    #### 10.1.3####
    joined = review.join(business,["business_id"]).drop(business.stars)
    joined.printSchema()
    joined_shortened_date = joined.select(joined.stars,joined.city,(F.substring(joined.date, 0, 7).alias("date")))
    joined_shortened_date.printSchema()
    joined_shortened_date.show(5,False)
    joined_shortened_date.groupBy("city","date").pivot("date").avg("stars").show()

    ###### 10.2 ####
    selected = review.select(review.text,review.useful)
    udf_applied = selected.select(selected.text,selected.useful, my_udf(selected['text']).alias("my_udf"))
    nested = udf_applied.select(udf_applied.useful,udf_applied.text,"my_udf.avgl","my_udf.medl")
    nested.show(5,True)
    corr_avgl_useful = nested.select(nested.useful,nested.avgl).agg(F.corr("useful","avgl")) # corr_avgl_useful is also a dataframe
    corr_avgl_useful.show()
    corr_medl_useful = nested.select(nested.useful,nested.medl).agg(F.corr("useful","medl")) # this is also a dataframe
    corr_medl_useful.show()

    filtered_review.write.format("json").save(output_folder + "/10.1.1")
    grouped_PBR.write.format("json").save(output_folder + "/10.1.2")
    joined_shortened_date.write.format("json").save(output_folder + "/10.1.3")
    corr_medl_useful.write.format("json").save(output_folder+"/10.2-corr_AVG_USEFUL")
    corr_avgl_useful.write.format("json").save(output_folder+"/10.2-corr_MED_USEFUL")



    # Write results into the output folder
    # counts.write.format("json").save(output_folder)

    # To force Spark write output as a single file, you can use:
    # counts.coalesce(1).write.format("json").save(output_folder
    # coalesce(N) repartitions DataFrame or RDD into N partitions.
    # NB! But be careful when using coalesce(N), your program will crash if the whole DataFrame does not fit into memory of N processes.

    # Stop Spark session
    spark.stop()
