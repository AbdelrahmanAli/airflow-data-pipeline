from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 11, 1),
    'email_on_retry': False,
    'email_on_failure': False,
    'depends_on_past': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          #schedule_interval=None,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    #s3_key = "log_data/{execution_date.year}/{execution_date.month}/" # there is no folder for year 2020
    s3_key = "log_data",
    json_path="auto"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    # s3_key = "song_data" # too many files, it takes forever
    s3_key = "song_data/A/A/A",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "songplays",
    sql = SqlQueries.songplay_table_insert,
    overwrite = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "users",
    sql = SqlQueries.user_table_insert,
    overwrite = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "songs",
    sql = SqlQueries.song_table_insert,
    overwrite = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "artists",
    sql = SqlQueries.artist_table_insert,
    overwrite = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "time",
    sql = SqlQueries.time_table_insert,
    overwrite = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    tests=[
            {"test":"SELECT COUNT(*) FROM songplays","expected":"100"},
            {"test":"SELECT COUNT(*) FROM users","expected":"100"},
            {"test":"SELECT COUNT(*) FROM songs","expected":"100"},
            {"test":"SELECT COUNT(*) FROM artists","expected":"100"},
            {"test":"SELECT COUNT(*) FROM time","expected":"100"}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [
                    stage_events_to_redshift,
                    stage_songs_to_redshift
                  ] >> load_songplays_table >> [
                                                 load_user_dimension_table,
                                                 load_song_dimension_table,
                                                 load_artist_dimension_table,
                                                 load_time_dimension_table
                                               ] >> run_quality_checks >> end_operator
