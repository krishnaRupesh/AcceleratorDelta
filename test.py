# import boto3
#
# responseObject = {}
# responseObject["statusCode"] = 200
# responseObject["headers"] = {}
# responseObject["headers"]["Content-Type"] = "application/json"
#
# print(responseObject)
#
# event = {}
# clusterID = event["pathParameters"].get("clusterID")
# notebookvalues = event["queryStringParameters"]
#
# print(event)
# emr = boto3.client(
#     'emr',
#     region_name='ap-south-1'
# )
#
# start_resp = emr.start_notebook_execution(
#     EditorId='e-6XSKZMAN8B0EA360MX23HUC34',
#     RelativePath='athena_connector.ipynb',
#     ExecutionEngine={'Id': clusterID},
#     ServiceRole='EMR_Notebooks_DefaultRole',
#     NotebookParams=notebookvalues
# )

event = {}
event["pathParameters"] ={}
event["pathParameters"]["clusterID"] = "j-12345"
clusterID = event["pathParameters"].get("clusterID")

ExecutionEngine={'Id': clusterID}

print(ExecutionEngine)
print(type(ExecutionEngine))

