import time
import boto3

emr = boto3.client(
    'emr',
    region_name='ap-southeast-1'
)
execution_id="ex-J00HI2FYGFF2MWCOLIGH7DQHQZM0F"
while (True):
    describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)
    print(describe_response["NotebookExecution"]["Status"])

    print(describe_response)
    print("\n")
    time.sleep(60)