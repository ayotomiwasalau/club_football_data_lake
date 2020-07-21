from datetime import datetime, timedelta
import os
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from helper.sql_queries import HelperQueries

from operators.create_tables import Create_Table_in_Redshift
from operators.load_table_s3_redhsift import Load_s3_to_redshift
from operators.check_table_content import Check_table_quality 

# parameters
default_args = {
    'owner': 'tomiwa_salau',
    'start_date': datetime.now() - timedelta(seconds=1),
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False    
}

# create Dag
dag = DAG(
    'club_football_data',
    default_args = default_args,
    description='Load spark processed data from S3 to Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# operator to create table for player
create_player_table = Create_Table_in_Redshift(
    task_id="create_player_table",
    dag=dag,
    redshift_conn="redshift",
    table="player",
    postgres_conn_id="redshift",
    sql=HelperQueries.create_player_tbl
)

# # operator to create table for manager 
# create_manager_table = Create_Table_in_Redshift(
#     task_id="create_manager_table",
#     dag=dag,
#     table="manager",
#     postgres_conn_id="redshift",
#     sql=HelperQueries.create_manager_tbl
# )

# operator to create table fo referee
create_referee_table = Create_Table_in_Redshift(
    task_id="create_referee_table",
    dag=dag,
    redshift_conn="redshift",
    table="referee",
    postgres_conn_id="redshift",
    sql=HelperQueries.create_referee_tbl
)

# operator to create table for club
create_club_table = Create_Table_in_Redshift(
    task_id="create_club_table",
    dag=dag,
    redshift_conn="redshift",
    table="club",
    postgres_conn_id="redshift",
    sql=HelperQueries.create_club_tbl
)

# operator to create table for match
create_match_table = Create_Table_in_Redshift(
    task_id="create_match_table",
    dag=dag,
    redshift_conn="redshift",
    table="match",
    postgres_conn_id="redshift",
    sql=HelperQueries.create_match_tbl
)

# operator to create table for match_event
create_match_event_table = Create_Table_in_Redshift(
    task_id="create_match_event_table",
    dag=dag,
    redshift_conn="redshift",
    table="match_event",
    postgres_conn_id="redshift",
    sql=HelperQueries.create_match_event_tbl
)

#------------------------------------

# operator to load player data from s3 into redshift
load_player_data = Load_s3_to_redshift(
    task_id='load_player_from_s3_to_redshift',
    dag=dag,
    redshift_conn="redshift",
    aws_credential_id="aws_credentials",
    table="player",
    s3_bucket="football-club-data",
    s3_key="player_data"
   
)

# # operator to load manager data from s3 into redshift
# load_manager_data = Load_s3_to_redshift(
#     task_id='load_manager_from_s3_to_redshift',
#     dag=dag,
#     redshift_conn="redshift_conn",
#     aws_credential_id="aws_credential_id",
#     table="",
#     s3_bucket="",
#     s3_key="",
    
# )

# operator to load referee data from s3 into redshift
load_referee_data = Load_s3_to_redshift(
    task_id='load_referee_from_s3_to_redshift',
    dag=dag,
    redshift_conn="redshift",
    aws_credential_id="aws_credentials",
    table="referee",
    s3_bucket="football-club-data",
    s3_key="referee_data"  
)

# operator to load club data from s3 into redshift
load_club_data = Load_s3_to_redshift(
    task_id='load_club_from_s3_to_redshift',
    dag=dag,
    redshift_conn="redshift",
    aws_credential_id="aws_credentials",
    table="club",
    s3_bucket="football-club-data",
    s3_key="club_data" 
)
# operator to load match data from s3 into redshift
load_match_data = Load_s3_to_redshift(
    task_id='load_match_from_s3_to_redshift',
    dag=dag,
    redshift_conn="redshift",
    aws_credential_id="aws_credentials",
    table="match",
    s3_bucket="football-club-data",
    s3_key="match_data"  
)
# operator to load match_event data from s3 into redshift
load_match_event_data = Load_s3_to_redshift(
    task_id='load_match_event_from_s3_to_redshift',
    dag=dag,
    redshift_conn="redshift",
    aws_credential_id="aws_credentials",
    table="match_event",
    s3_bucket="football-club-data",
    s3_key="match_events_data"
)
#----------------------------------------------------

# Run check on the content data
# Integrity constraints on the relational database (e.g., unique key, data type, etc.)
# Unit tests for the scripts to ensure they are doing the right thing
# Source/count checks to ensure completeness


run_quality_checks = Check_table_quality(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    provide_context=True,
    tables=["player", "referee", "club", "match", "match_event"]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
# ------------------------

#Design flow

start_operator >> create_player_table
# start_operator >> create_manager_table
start_operator >> create_club_table
start_operator >> create_referee_table


create_player_table >> create_match_table 
create_club_table >> create_match_table 
create_referee_table >> create_match_table 

create_match_table >> create_match_event_table

create_match_event_table >> load_player_data
# create_manager_table >> load_manager_data
create_match_event_table >>  load_club_data
create_match_event_table >>  load_referee_data
create_match_event_table >> load_match_data
create_match_event_table >> load_match_event_data

load_player_data >> run_quality_checks
# load_manager_data >> run_quality_checks
load_club_data >> run_quality_checks
load_referee_data >> run_quality_checks
load_match_data >> run_quality_checks
load_match_event_data >> run_quality_checks

run_quality_checks >> end_operator