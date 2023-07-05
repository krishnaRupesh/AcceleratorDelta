import pyspark
from delta import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

location = "s3a://raw-data-source-athena-crud/stack-overflow-developer-survey-2020/survey_results_public.csv"

data = spark.read.option("header","true").option("inferSchema","true").csv(location)

data.show()

data.write.mode("overwrite").format("delta").save("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
