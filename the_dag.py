from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),		
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Rename the DAG variable
udac_example_dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=udac_example_dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=udac_example_dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="s3://udacity-dend/log-data/",
    region="us-west-2",
    file_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=udac_example_dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path="s3://udacity-dend/song-data/A/A/A",
    region="us-west-2",
    file_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=udac_example_dag,
    table="songplays",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=udac_example_dag,
    table="users",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=udac_example_dag,
    table="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=udac_example_dag,
    table="artists",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=udac_example_dag,
    table="time",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=udac_example_dag,
    redshift_conn_id="redshift",
    checks_list=[
        {'test_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0, 'comparison': '>'}, 
        {'test_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0, 'comparison': '>'}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=udac_example_dag)

							 

# Define dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
