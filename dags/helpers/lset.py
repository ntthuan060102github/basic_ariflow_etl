import psycopg2
from datetime import datetime

db_params = {
    "dbname": "metadata",
    "user": "airflow",
    "password": "airflow",
    "host": "172.18.0.3",
    "port": "5432"
}

def get_lset(table_name, source_system):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(
        f'''
            SELECT "LSET" 
            FROM RDBExtractData 
            WHERE table_name = '{table_name}' and source_system = {source_system}
        '''
    )
    data = cursor.fetchall()

    if data[0][0]:
        return data[0][0]
    return datetime(1, 1, 1, 0, 0, 0)

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