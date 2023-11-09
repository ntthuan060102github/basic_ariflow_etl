import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.custom_json_encoder import CustomJSONEncoder
from helpers.lset import get_lset, update_lset
from config import CET

def extract_data_from_ss3():
    LSET = get_lset("CustomerVisit", 3).strftime("%Y-%m-%d %H:%M:%S")
    sql_stmt = f'''
        SELECT * 
        FROM public.CustomerVisit
        WHERE (
            "created_at" >= '{LSET}'
            AND "created_at" < '{CET}'
        )
        OR (
            "last_updated" >= '{LSET}'
            AND "last_updated" < '{CET}'
        )
    '''
    pg_hook = PostgresHook(
        postgres_conn_id='pg_source_system_3',
        # database='source_system_3'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    update_lset("CustomerVisit", 3, CET)
    return json.dumps(cursor.fetchall(), cls=CustomJSONEncoder)