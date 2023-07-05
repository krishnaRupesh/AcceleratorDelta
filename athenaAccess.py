import boto3

# emr = boto3.client(
#     'emr',
#     region_name='ap-south-1'
# )

clusterID = "rupesh"
ExecutionEngine={'Id': clusterID}
print(ExecutionEngine)
print(type(ExecutionEngine))

athena_db_name = "stack_overflow_developer_survey"
delta_s3_location = "s3://delta-data-athena-crud1/stack-overflow-developer-survey-2020"
delta_s3a_location = (
    "s3a://delta-data-athena-crud1/stack-overflow-developer-survey-2020"
)
raw_s3a_location = "s3a://raw-data-source-athena-crud1/stack-overflow-developer-survey-2020/survey_results_public.csv"
spark_db_name = "delta_database"
table_name = "survey_2020_new"
print(type(delta_s3a_location))

# start_resp = emr.start_notebook_execution(
#     EditorId='e-6XSKZMAN8B0EA360MX23HUC34',
#     RelativePath='athena_connector.ipynb',
#     ExecutionEngine={'Id': clusterID},
#     ServiceRole='EMR_Notebooks_DefaultRole',
#     NotebookParams=json.dumps(notebookvalues)
# )

# client = boto3.client('athena')
#
#
#
#
# def sparkTypeToAthenaType(sparkType):
#     if sparkType == "StringType":
#         a = "STRING"
#     elif sparkType == "IntegerType":
#         a = "INT"
#     elif sparkType == "DateType":
#         a = "Date"
#     elif sparkType == "DecimalType":
#         a = "Decimal"
#     elif sparkType == "FloatType":
#         a = "Float"
#     elif sparkType == "LongType":
#         a = "BIGINT"
#     elif sparkType == "TimestampType":
#         a = "TIMESTAMP"
#     else:
#         a = "STRING"
#
#     return a
#
# # response = client.create_named_query(
# #     Name='TestQuery123q',
# #     Database='rainfall_data',
# #     QueryString='select * from "rainfall_data"."rainfall_source_s3_folder" limit 2;',
# # )
# # print(response)
# # print(response["NamedQueryId"])
# #
# # response1 = client.get_named_query(
# #     NamedQueryId=response["NamedQueryId"]
# # )
# # print(response1)
#
# response = client.start_query_execution(
#     QueryString='select * from "rainfall_data"."rainfall_source_s3_folder" limit 3;',
#     QueryExecutionContext={
#         'Database': 'rainfall_data'
#     },
#      ResultConfiguration={
#         'OutputLocation': 's3://athena-s3-log-setup/saved_queries',
#         },
#     WorkGroup='primary'
# )
# print(response)
#
# import regex as re
# import botocore
#
#
# # # Copied this from https://tfsedp.cloud.databricks.com/#notebook/2521911/command/2521915 and enhanced
# # def delta_athena_ddl_generator(s3_delta_path, athena_table_name):
# #     partition_by = ""
# #
# #     table_statement = spark.sql("SHOW CREATE TABLE delta.`{0}`".format(s3_delta_path)).collect()[0][0]
# #     columns = re.search(r'\(([^()]|(?R))*\)', table_statement).group(0)[1:-1]
# #
# #     table_details = \
# #     spark.sql("DESCRIBE DETAIL delta.`{0}`".format(s3_delta_path)).select("location", "partitionColumns").collect()[0]
# #     location = table_details[0].replace("s3a://", "s3://").replace("s3n://", "s3://").replace("dbfs://", "s3://")
# #     partitions = table_details[1]
# #     num_partitions = len(partitions)
# #
# #     # If partitions then additional logic
# #     if num_partitions > 0:
# #         columns = columns.split(",")
# #         new_columns = []
# #         partitions_w_types = {}
# #
#         for col in columns:
#             col = col.replace("\n", "").replace("`", "").strip()
#             if any(partition in col for partition in partitions):
#                 split_c


