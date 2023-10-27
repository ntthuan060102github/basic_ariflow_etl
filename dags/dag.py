from datetime import timedelta
from config import CET
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from etl.extract.extract_ss1 import extract_data_from_ss1
from etl.extract.extract_ss2 import extract_data_from_ss2
from etl.extract.extract_ss3 import extract_data_from_ss3

from etl.transform.transform_data_from_ss1 import transform_data_from_ss1
from etl.transform.transform_data_from_ss2 import transform_data_from_ss2
from etl.transform.transform_data_from_ss3 import transform_data_from_ss3

from etl.load.load_ss1_to_stage import load_ss1_to_stage
from etl.load.load_ss2_to_stage import load_ss2_to_stage
from etl.load.load_ss3_to_stage import load_ss3_to_stage

default_args = {
    'owner': 'trist',
    'start_date': CET,
    'email': ['ntthuan060102.work@gmail.com'],
    'email_on_failure': ['ntthuan060102.work@gmail.com'],
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False
}

with DAG(dag_id='airflow_dag', default_args=default_args, schedule_interval='* * * * *') as dag:
    task_start = DummyOperator(
        task_id="start",
        dag=dag,
    )

    extract_ss1 = PythonOperator(
	    task_id='extract_data_from_ss1',
	    python_callable=extract_data_from_ss1
    )
    
    extract_ss2 = PythonOperator(
	    task_id='extract_data_from_ss2',
	    python_callable=extract_data_from_ss2
    )

    extract_ss3 = PythonOperator(
	    task_id='extract_data_from_ss3',
	    python_callable=extract_data_from_ss3
    )

    transform_ss1 = PythonOperator(
	    task_id='transform_data_from_ss1',
	    python_callable=transform_data_from_ss1
    )

    transform_ss2 = PythonOperator(
	    task_id='transform_data_from_ss2',
	    python_callable=transform_data_from_ss2
    )

    transform_ss3 = PythonOperator(
	    task_id='transform_data_from_ss3',
	    python_callable=transform_data_from_ss3
    )

    truncate_stage_table = PostgresOperator(
        task_id='truncate_stage_table',
        postgres_conn_id='postgres_stage',
        sql="""
            select * from information_schema.tables;
        """
    )

    load_ss1 = PythonOperator(
	    task_id='load_data_from_ss1_to_stage',
	    python_callable=load_ss1_to_stage
    )

    load_ss2 = PythonOperator(
	    task_id='load_data_from_ss2_to_stage',
	    python_callable=load_ss2_to_stage
    )

    load_ss3 = PythonOperator(
	    task_id='load_data_from_ss3_to_stage',
	    python_callable=load_ss3_to_stage
    )

    task_end = DummyOperator(
        task_id="end",
        dag=dag,
    )

task_start >> [extract_ss1, extract_ss2, extract_ss3]

extract_ss1 >> transform_ss1
extract_ss2 >> transform_ss2
extract_ss3 >> transform_ss3

[transform_ss1, transform_ss2, transform_ss3] >> truncate_stage_table

truncate_stage_table >> load_ss1 >> load_ss2 >> load_ss3 >> task_end