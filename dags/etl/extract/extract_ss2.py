import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.custom_json_encoder import CustomJSONEncoder

def extract_data_from_ss2():
    sql_stmt = 'SELECT * FROM CustomerAttendanceLog'
    pg_hook = PostgresHook(
        postgres_conn_id='pg_source_system_2',
        # database='source_system_2'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return json.dumps(cursor.fetchall(), cls=CustomJSONEncoder)