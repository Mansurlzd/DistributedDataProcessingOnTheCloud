from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F
import os

os.environ['SPARK_HOME'] = "/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7"
sys.path.append("/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7/python/lib")


from graphframes import GraphFrame

if __name__ == "__main__":

    # Specify GraphFrames Spark package to be downloaded when PySpark executes the Python script
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell"
    )

    # Verify the number of arguments
    if len(sys.argv) != 2:
        print("Usage:  structStreamExample.py <input_folder> ")
        exit(-1)

    # Set a name for the application
    appName = 'GraphFrameExample'

    # specify input folder location
    input_folder = sys.argv[1]

    # Create (or fetch existing) a Spark session
    # If you are having memory issues, you can  increase the memory given to the local process by adding:
    # .config("spark.driver.memory", "2g") \
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()

    # Load in YELP dataset tables as JSON documents.
    business = spark.read.option("inferSchema", True).json(input_folder + "/business")
    review = spark.read.option("inferSchema", True).json(input_folder + "/review")
    user = spark.read.option("inferSchema", True).json(input_folder + "/user")

    # Prepare user vertices
    # id, name, type, city
    userV = user.withColumn("type", F.lit("user")) \
        .withColumn("city", F.lit("")) \
        .select(F.col("user_id").alias("id"),
                "name", "type", "city", "review_count")

    print("User Vertices: ")
    userV.show(8, False)

    # Prepare Business vertices
    # id, name, type, city
    businessV = business.withColumn("type", F.lit("company")) \
        .select(F.col("business_id").alias("id"),
                "name", "type", "city", "review_count")

    print("Business Vertices: ")
    businessV.show(8, False)

    # Prepare review Edges
    # src, dst, relationship, stars
    # User reviewed business
    reviewE = review.withColumn("relationship", F.lit("reviewed")) \
        .select(F.col("user_id").alias("src"),
                F.col("business_id").alias("dst"),
                "relationship", "stars", "useful")

    print("Review Edges: ")
    reviewE.show(8, False)

    # Prepare friendship edges by extracting  user_id's from the user dataset "friend" column
    # src, dst, relationship, stars
    # User src is a friend with User dst
    friendE = user.withColumn("relationship", F.lit("friend")) \
        .withColumn("stars", F.lit("")).withColumn("useful", F.lit("")) \
        .select(F.col("user_id").alias("src"),
                F.explode(F.split("friends", ", ")).alias("dst"),
                "relationship", "stars", "useful")

    print("Friendship Edges: ")
    friendE.show(8, False)

    #union user vertices together with business vertices.
    all_vertices = userV.union(businessV)

    #union friend Edges together with review edges.
    all_edges = friendE.union(reviewE)

    # Create the GraphFrame object
    g = GraphFrame(all_vertices, all_edges)

    # Make sure GraphFrame is cached in memory in case we want to query/manipulate it multiple times in a row.
    g.cache()

    ### 12.2 #####
    # Find shortest paths between users named Eric and Restaurants with 'Taco Bell' in their name
    paths = g.bfs(" name = 'Eva'", " type = 'company' and review_count >=10",
                 " stars='5' or relationship = 'friend'")
    # Get list of columns
    cols = paths.columns

    # Get the label/name of the last Edge in the path
    last_edge = cols[len(cols)-2]

    # The resulting paths can be manipulated as normal DataFrames
    # order by the stars of the last edge (which must be reviewd type edge)
    print("BFS :: ")
    paths.orderBy(last_edge+".stars", ascending = False).show(5, False)

    ###### 12.3.1   ###
    query = "(u)-[e1]->(b1); (u)-[e2] -> (b2)"
    results = g.find(query)

    out = results.filter(" b1.type != 'user' and b2.type != 'user' and e1.stars = '5' and  e2.stars = '5' and b1.city != b2.city ")
    grouped = out.groupBy("u").count().orderBy("count", ascending = False).limit(5)
    #print(grouped.count())
    print("12.3.1 --------------------------------12.3.1--------------------------------------------- ")
    grouped.show(10, False)

    ##### 12.3.2 ####
    query2 = "(u1)-[e1]->(b); (u2)-[e2] -> (b); (u1)-[e3]->(u2)"
    results2 = g.find(query2)
    out2 = results2.filter("b.type != 'user' and e1.stars <= '2' and e2.stars >= '4' and e3.relationship= 'friend' ")
    grouped2 = out2.groupBy("b").count().orderBy("count", ascending = False).limit(5)
    print("12.3.2 --------------------------------12.3.2--------------------------------------------- ")
    grouped2.show(5,False)


    #### 12.3.3 ####
    query3 = "(u1) - [e1] -> (b); (u2) - [e2] -> (b); !(u1) - [] -> (u2)"
    results3 = g.find(query3)
    out3 = results3.filter(" b.type != 'user' and e1.stars = e2.stars")
    grouped3 = out3.groupBy("u1","u2").count().orderBy("count", ascending = False).limit(5)
    print("12.3.3 --------------------------------12.3.3--------------------------------------------- ")
    grouped3.show(5, False)


    spark.stop()
