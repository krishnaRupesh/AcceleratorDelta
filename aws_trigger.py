import boto3
import time

emr = boto3.client('emr', region_name='ap-south-1')

start_resp = emr.start_notebook_execution(
    EditorId='e-19GRIUPNYQLXPKXTCF1K76BSI',
    RelativePath='first_notebook.ipynb',
    ExecutionEngine={'Id': 'j-19YQXBN8D0YM7'},
    ServiceRole='EMR_Notebooks_DefaultRole',
    NotebookParams='{"raw_s3a_location" : "s3a://raw-data-source-athena-crud/stack-overflow-developer-survey-2020/survey_results_public.csv","delta_s3a_location" : "s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020", "delta_s3_location" : "s3://delta-data-athena-crud/stack-overflow-developer-survey-2020","athena_db_name" : "stack_overflow_developer_survey","athena_table_name" : "survey_2020_cli"}'
)

execution_id = start_resp["NotebookExecutionId"]
print(execution_id)
print("\n")

describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)

status = describe_response["NotebookExecution"]["Status"]

while status != "FAILED":
    describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)
    status = describe_response["NotebookExecution"]["Status"]
    print(status)
    print(describe_response)
    print("\n")
    time.sleep(60)



# list_response = emr.list_notebook_executions()
# print("Existing notebook executions:\n")
# for execution in list_response['NotebookExecutions']:
#     print(execution)
#     print("\n")
#
# print("Sleeping for 5 sec...")
# time.sleep(5)

# print("Stop execution " + execution_id)
# emr.stop_notebook_execution(NotebookExecutionId=execution_id)
# describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)
# print(describe_response)
# print("\n")
