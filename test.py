import datetime
import psycopg2
from dags.helpers.custom_json_encoder import CustomJSONEncoder

db_params = {
    "dbname": "metadata",
    "user": "airflow",
    "password": "airflow",
    "host": "127.0.0.1",
    "port": "5432"
}

def update_lset(table_name, source_system, cet):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(
        f'''
            UPDATE RDBExtractData 
            SET "LSET" = '{cet}'
            WHERE table_name = '{table_name}' and source_system = {source_system}
        '''
    )
    conn.commit()
    cursor.close()
