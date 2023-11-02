# import pandas as pd

# df = pd.read_csv("source_system/source_system_1.csv", sep=";")
# df.rename(
#     columns={
#         "customer_name": "full_name",
#         "gender": "gender",
#         "arrival_time": "arrival_time",
#         "departure_time": "departure_time"
#     }
# )
# df['gender'] = df['gender'].map({'M': True, 'F': False})

# print(df.to_dict())

import json
import pandas as pd
import psycopg2

from dags.helpers.custom_json_encoder import CustomJSONEncoder


db_params = {
    'dbname': 'source_system_1',
    'user': 'airflow',
    'password': 'airflow',
    'host': '127.0.0.1',  # default is 'localhost'
    'port': '5432'   # default is 5432
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()
cursor.execute("SELECT * FROM CustomerVisit")
result = json.dumps(cursor.fetchall(), cls=CustomJSONEncoder)
df = pd.DataFrame(json.loads(result), columns=["id", "full_name", "gender", "arrival_time", "departure_time"])
df = df.assign(source_system=1)
print(df.to_sql("CustomerAttendanceLog", conn, index=False, method="multi"))