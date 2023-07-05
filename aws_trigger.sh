aws emr --region ap-southeast-1 \
start-notebook-execution \
--editor-id e-19GRIUPNYQLXPKXTCF1K76BSI \
--notebook-params '{"raw_s3a_location" : "s3a://raw-data-source-athena-crud/stack-overflow-developer-survey-2020/survey_results_public.csv","delta_s3a_location" : "s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020", "delta_s3_location" : "s3://delta-data-athena-crud/stack-overflow-developer-survey-2020","athena_db_name" : "stack_overflow_developer_survey","athena_table_name" : "survey_2020_cli"}' \
--relative-path first_notebook.ipynb \
--notebook-execution-name my-execution-cli \
--execution-engine '{"Id" : "j-16V486VP2GD9O"}' \
--service-role EMR_Notebooks_DefaultRole