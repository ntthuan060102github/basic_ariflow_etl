import json
import pandas as pd

def load_ss1_to_stage(ti):
    dataset = ti.xcom_pull(task_ids=["extract_data_from_ss1"])

    if not dataset:
        raise Exception("Empty data set!")
    
    df = pd.DataFrame(json.loads(dataset[0]), columns=["nk", "full_name", "gender", "arrival_time", "departure_time"])