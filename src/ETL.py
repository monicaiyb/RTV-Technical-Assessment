from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import xmltodict
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id = 'podcast_summary',
    start_date = datetime(2024,4,4),
    schedule_interval='@daily',
    catchup = False
)

def podcast_summary():
    
    create_database = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres_local',
        
    )