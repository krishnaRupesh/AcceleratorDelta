# own account mumbai region

import pyspark
from delta import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

AWS_ACCESS_KEY_ID="AKIAV3QP7XCB6WEBBJVX"
AWS_SECRET_ACCESS_KEY="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"
s3_region_end_point = "s3-ap-south-1.amazonaws.com"

builder = pyspark.sql.SparkSession.builder.appName("MyApp").master("local") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", AWS_ACCESS_KEY_ID)
hadoop_conf.set("fs.s3a.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
hadoop_conf.set("fs.s3a.endpoint", "s3-ap-southeast-1.amazonaws.com")


location = "s3a://raw-data-source-athena-crud/stack-overflow-developer-survey-2020/survey_results_public.csv"

data = spark.read.option("header","true").csv(location)

data.show()

data.write.mode("overwrite").format("delta").save("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
