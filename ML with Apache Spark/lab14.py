from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StringIndexer, ChiSqSelector
from pyspark.ml import Pipeline


os.environ['SPARK_HOME'] = "/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7"
sys.path.append("/home/mansur/DDPC/spark-2.3.2-bin-hadoop2.7/python/lib")


if __name__ == "__main__":

    # Check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: ML_lib_example <input file> ")
        exit(-1)

    input_file = sys.argv[1]  # input file
    spark = SparkSession.builder.appName("ML_lib_example").getOrCreate()

    dataset_testing = spark.read.option("inferSchema", True).csv(input_file + "/census-income.test").drop("_c24")
    dataset_training = spark.read.option("inferSchema", True).csv(input_file + "/census-income.data").drop("_c24")

    ## drop column 24
    dataset_testing = dataset_testing.drop("_c24")
    dataset_training = dataset_training.drop("_c24")
    dataset_testing.show(6, False)



    cols = dataset_training.columns
    coltypes = dataset_training.dtypes
    print(coltypes)
    feature_cols = cols[0:-1]

    # assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    # training_set = assembler.transform(dataset_training)
    # training_set.show(6,False)

    string_cols = [c for (c, t) in coltypes if t == "string"]
    numerical_cols = [c for (c, t) in coltypes if t != "string"]

    indexers = []
    for col in string_cols:
        indexer = StringIndexer(inputCol=col, outputCol="s" + col).setHandleInvalid("skip")
        indexers.append(indexer)

    #pipeline = Pipeline(stages=indexers)
    #model_pipeline = pipeline.fit(dataset_training)
    #dataset_training_converted = model_pipeline.transform(dataset_training)
    #dataset_training_converted.show(10, False)


    ## 14.2 ##

    class1 = dataset_training.filter("_c41 LIKE '%+%'")
    class2 = dataset_training.filter(dataset_training._c41.contains("-"))
    class1_num = class1.count()
    class2_num = class2.count()
    fraction = 1.0 * class1_num / class2_num
    class2 = class2.sample(fraction)
    training_dataset_balanced = class1.union(class2)
    training_dataset_balanced.groupBy("_c41").count().show()

    ####### 14.1 ###
    converted_cols = ["s" + col for col in string_cols]
    assembler = VectorAssembler(inputCols=converted_cols + numerical_cols, outputCol="features")
    labelIndexer = StringIndexer(inputCol="_c41", outputCol="label")
    #classifier = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10, maxBins=64, maxDepth= 5, subsamplingRate= 1.0  )  ## 14.2
    #classifier = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxBins=64)
    selector = ChiSqSelector(numTopFeatures=35, featuresCol="features",
                             outputCol="selectedFeatures")
    classifier = NaiveBayes(smoothing=1.0) ## modelType="multinomial" we have binomial here so it doesn't make change when we apply this parameter
    pipeline = Pipeline(stages=indexers + [assembler, labelIndexer,selector ,classifier])
    model = pipeline.fit(training_dataset_balanced)
    #predictions = model.transform(dataset_testing) ##14.1
    predictions = model.transform(dataset_testing) ## 14.2
    predictions.show(10, False)

    ###### 14.2 ####
    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="prediction")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))
    predictions.crosstab("prediction", "label").show(30, False)






    spark.stop()
