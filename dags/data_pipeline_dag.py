from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 8, 6),
    'depends_on_past':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'catchup':False,
}

dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables=PostgresOperator(
    task_id='create_tables',
    dag=dag,    
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_parameter='s3://udacity-dend/log_json_path.json'   
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_parameter='auto'    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    append_data=True,
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    append_data=False,
    sql_statement=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    append_data=False,
    sql_statement=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    append_data=False,
    sql_statement=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    append_data=False,
    sql_statement=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
            {'table':'users',
             'check_sql':'SELECT COUNT(*) FROM users WHERE userid IS NULL',
             'expected_result':0},
            {'table':'songs',
             'check_sql':'SELECT COUNT(*) FROM songs WHERE songid IS NULL',
             'expected_result':0},
            {'table':'artists',
             'check_sql':'SELECT COUNT(*) FROM artists WHERE artistid IS NULL',
             'expected_result':0}
        ]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator>>create_tables
create_tables>>stage_events_to_redshift
create_tables>>stage_songs_to_redshift
stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table
load_user_dimension_table>>run_quality_checks
load_song_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks
run_quality_checks>>end_operator