import findspark

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3,net.java.dev.jets3t:jets3t:0.9.4 pyspark-shell'


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3,net.java.dev.jets3t:jets3t:0.9.4,io.delta:delta-core_2.12:1.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pyspark-shell'

#spark configuration

conf = SparkConf().set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true"). \
 set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true"). \
 setAppName("pyspark_aws").setMaster("local[*]")

sc=SparkContext(conf=conf)
# sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
print("modules imported")

accessKeyId="AKIAV3QP7XCB6WEBBJVX"
secretAccessKey="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"

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", accessKeyId)
hadoopConf.set("fs.s3a.secret.key", secretAccessKey)
# hadoopConf.set("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com")
# hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoopConf.set("fs.s3a.multipart.size", "104857600")
sc.setLogLevel("ERROR")

spark=SparkSession(sc)

dbname = "dbname"
tablename = "table_name"
emp_df=spark.read.csv("C:\\Users\\krishnan\\Downloads\\tripdetails_20210603_081845.csv",header=True,inferSchema=True)
emp_df.write.saveAsTable(dbname+"."+tablename)

# s3_df=spark.read.csv("s3a://raw-data-source-athena-crud1/stack-overflow-developer-survey-2020/survey_results_public.csv",header=True,inferSchema=True)
# s3_df.show(5)

s3_df=spark.read.csv("s3a://bugpics/survey_results_public.csv", header=True, inferSchema=True)
s3_df.show(5)
