import json
import pandas as pd

from helpers.custom_json_encoder import CustomJSONEncoder

def transform_data_from_ss2(ti):
    dataset = ti.xcom_pull(task_ids=["extract_data_from_ss2"])

    if not dataset:
        raise Exception("Empty data set!")
    
    df = pd.DataFrame(json.loads(dataset[0]), columns=["nk", "full_name", "gender", "arrival_time", "departure_time"])
    df['gender'] = df['gender'].map({'male': True, 'female': False})
    df = df.assign(source_system=2)

    return json.dumps(df.to_dict(), cls=CustomJSONEncoder)