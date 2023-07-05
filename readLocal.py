import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import regex as re
import json
spark = SparkSession.builder.appName("MyApp").master("local").getOrCreate()

#emp_df = spark.read.csv("C:\\Users\\krishnan\\Downloads\\tripdetails_20210603_081845.csv", header=True,
#                        inferSchema=True)

emp_df = spark.read.csv("C:\\Users\\krishnan\\Downloads\\stack-overflow-developer-survey-2020\\survey_results_public.csv", header=True,
                        inferSchema=True)


emp_df.show(5)


def sparkTypeToAthenaType(sparkType):
    if sparkType == "StringType":
        a = "STRING"
    elif sparkType == "IntegerType":
        a = "INT"
    elif sparkType == "DateType":
        a = "Date"
    elif sparkType == "DecimalType":
        a = "Decimal"
    elif sparkType == "FloatType":
        a = "Float"
    elif sparkType == "LongType":
        a = "BIGINT"
    elif sparkType == "TimestampType":
        a = "TIMESTAMP"
    else:
        a = "STRING"

    return a


a = json.loads(emp_df.schema.json())

fields = []

for i in a["fields"]:
    fields.append(i["name"] + " " + (i["type"]))

field_string = ",\n".join(fields)

athenaTableName = "stack_overflow_developer_survey.new_survey"
s3location = "s3a://raw-data-source-athena-crud/delta-stack-survey/"
output = "CREATE EXTERNAL TABLE IF NOT EXISTS {} \n(".format(athenaTableName) + field_string + ")\n" \
         + "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'" + "\n" \
         + "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'" + "\n" \
         + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" + "\n" \
         + "LOCATION '{}_symlink_format_manifest/'".format(s3location)

print(output)
# for i in a["fields"]:
#     print(i)




# s3_delta_path = r"C:\Users\krishnan\Downloads\delta-stack-overflow-developer"
#
#
# def delta_athena_ddl_generator(s3_delta_path, athena_table_name):
#     partition_by = ""
#
#     table_statement = spark.sql("SHOW CREATE TABLE delta.`{0}`".format(s3_delta_path)).collect()[0][0]
#     columns = re.search(r'\(([^()]|(?R))*\)', table_statement).group(0)[1:-1]
#
#     # table_details = \
#     # spark.sql("DESCRIBE DETAIL delta.`{0}`".format(s3_delta_path)).select("location", "partitionColumns").collect()[0]
#     # location = table_details[0].replace("s3a://", "s3://").replace("s3n://", "s3://").replace("dbfs://", "s3://")
#     # partitions = table_details[1]
#     # num_partitions = len(partitions)
#     #
#     # # If partitions then additional logic
#     # if num_partitions > 0:
#     #     columns = columns.split(",")
#     #     new_columns = []
#     #     partitions_w_types = {}
#     #
#     #     for col in columns:
#     #         col = col.replace("\n", "").replace("`", "").strip()
#     #         if any(partition in col for partition in partitions):
#     #             #split_c
#     #             pass
#     #
#
#
# delta_athena_ddl_generator(s3_delta_path, "namer")
