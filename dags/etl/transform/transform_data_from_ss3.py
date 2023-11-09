import json
import pandas as pd

from helpers.custom_json_encoder import CustomJSONEncoder

def transform_data_from_ss3(ti):
    dataset = ti.xcom_pull(task_ids=["extract_data_from_ss3"])

    if not dataset:
        raise Exception("Empty data set!")
    
    df = pd.DataFrame(json.loads(dataset[0]), columns=["nk", "full_name", "gender", "arrival_time", "departure_time", "created_at", "last_updated"])
    df = df.assign(source_system=3)
    df.drop(['created_at', 'last_updated'], axis=1)
    
    return json.dumps(df.to_dict(), cls=CustomJSONEncoder)