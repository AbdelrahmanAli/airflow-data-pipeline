# Data Pipelines with Airflow

## Introduction
This project is one of Udacitys Data Engineering Nano Degree projects. It is requested in the 4th course: Data Pipelines with Airflow.

You are required to build an Airflow DAG to schedule data movement from AWS S3 to Redshift and perform data quality check. The source data resides in S3 and needs to be processed in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Description
>A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

>They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Project Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

## Project Datasets
There are two datasets residing in S3:

- **Song data:** `s3://udacity-dend/song_data`
- **Log data:** `s3://udacity-dend/log_data`

## Technologies
- Python 3
- Airflow
- AWS S3
- AWS Redshift

## Required DAG
![Resulted DAG](res_dag.png)

## Setup
- In your Airflow, create a new AWS `Connection` 
  - **Conn Id:** Enter aws_credentials.
  - **Conn Type:** Enter Amazon Web Services.
  - **Login:** Enter your Access key ID from the IAM User credentials you downloaded earlier.
  - **Password:** Enter your Secret access key from the IAM User credentials you downloaded earlier.
- Create a new Redshift `Connection`
  - **Conn Id:** Enter redshift.
  - **Conn Type:** Enter Postgres.
  - **Host:** Enter the endpoint of your Redshift cluster, excluding the port at the end.
  - **Schema:** Enter dev. This is the Redshift database you want to connect to.
  - **Login:** Enter awsuser.
  - **Password:** Enter the password you created when launching your Redshift cluster.
  - **Port:** Enter 5439.
- Add the DAG to your Airflow

## Files
- `dags/udac_example_dag.py`: Contains the main DAG.
  - The DAG does not have dependencies on past runs
  - On failure, the task are retried 3 times
  - Retries happen every 5 minutes
  - Catchup is turned off
  - Do not email on retry
- `plugins/operators/stage_redshift.py`: Custome Operator for loading staging data from S3 into Redshift.
- `plugins/operators/load_dimension.py`: Custom Operator for performing insertion query into dimension tables.
- `plugins/operators/load_fact.py`: Custom Operator for performing insertion query into fact table.
- `plugins/operators/data_quality.py`: Custom Operator for data quality queries.

