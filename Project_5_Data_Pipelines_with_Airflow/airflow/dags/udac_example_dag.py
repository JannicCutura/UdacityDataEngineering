from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(year=2019, month=1, day=1),
    'end_date': datetime(year=2019, month=3, day=1),
    'retries':0, #3
    'retry_delay':timedelta(minutes=1), #minutes=5
    'catchup':False,
    'depends_on_past':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_key="log_data",
    table="staging_events",
    json_path="s3://udacity-dend/log_json_path.json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_key="song_data",
    table="staging_songs",
    provide_context=True
)





start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift


#
#load_songplays_table = LoadFactOperator(
#    task_id='Load_songplays_fact_table',
#    dag=dag
#)
#
#load_user_dimension_table = LoadDimensionOperator(
#    task_id='Load_user_dim_table',
#    dag=dag
#)
#
#load_song_dimension_table = LoadDimensionOperator(
#    task_id='Load_song_dim_table',
#    dag=dag
#)
#
#load_artist_dimension_table = LoadDimensionOperator(
#    task_id='Load_artist_dim_table',
#    dag=dag
#)
#
#load_time_dimension_table = LoadDimensionOperator(
#    task_id='Load_time_dim_table',
#    dag=dag
#)
#
#run_quality_checks = DataQualityOperator(
#    task_id='Run_data_quality_checks',
#    dag=dag
#)
#
#end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
