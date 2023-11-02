import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_ss1_to_stage(ti):
    dataset = ti.xcom_pull(task_ids=["transform_data_from_ss1"])

    if not dataset:
        raise Exception("Empty data set!")
    
    df = pd.DataFrame(json.loads(dataset[0]), columns=["nk", "full_name", "gender", "arrival_time", "departure_time", "source_system"])
    pg_hook = PostgresHook(
        postgres_conn_id='pg_stage',
        # database='stage'
    )
    pg_hook.insert_rows("CustomerAttendanceLog", rows=df.values, target_fields=list(df.columns))

    return True