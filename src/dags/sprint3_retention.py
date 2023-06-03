import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
       'customer_retention',
        default_args=args,
        description='Calculate customer retention mart',
        catchup=True,
        start_date=datetime.today() - timedelta(days=1),
        schedule_interval = "0 11 * * MON"
) as dag:

    del_mart_f_customer_retention = PostgresOperator(
        task_id='del_mart_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/trn_mart_f_customer_retention.sql")

    mart_f_customer_retention = PostgresOperator(
        task_id='mart_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql")


(
    del_mart_f_customer_retention >> mart_f_customer_retention
)