# **Example DAG Triggers**

# About
Examples on how to Trigger a [Google Cloud Composer](https://cloud.google.com/composer) DAG.

# Go Cloud Function v1
An example Google Cloud Function v1, written in the Go language, which can be used to trigger a DAG within a Cloud Composer v2 Instance.

## Environment Variables:
- ```AIRFLOW_URI```: The Airflow Web Server URI, e.g. https://xxxxx-dot-region.composer.googleusercontent.com
- ```DAG_ID```: Name of the Cloud Composer DAG that you wish to trigger, e.g. trigger_response_dag
- ```VERBOSE```(Optional): TRUE / FALSE - If true will output additional debug information to the log.

