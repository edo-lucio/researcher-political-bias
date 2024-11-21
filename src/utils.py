import json
import os
import pandas as pd

def read_json(path: str) -> dict:
    with open(path) as json_file:
        json_data = json.load(json_file)   
        return json_data
    
def write_to_file(df, path: str):
    def make_hashable(obj):
        if isinstance(obj, list):
            return tuple(make_hashable(item) for item in obj)
        elif isinstance(obj, dict):
            return frozenset((make_hashable(k), make_hashable(v)) for k, v in obj.items())
        else:
            return obj  # Return as-is if it's already hashable

    def convert_unhashable_to_hashable(df):
        for col in df.columns:
            df[col] = df[col].apply(make_hashable)  # Apply the recursive hashable conversion to each column
        return df

    df = convert_unhashable_to_hashable(df)

    if os.path.exists(path):
        existing_df = pd.read_csv(path)
        existing_df = convert_unhashable_to_hashable(existing_df)
        combined_df = pd.concat([existing_df, df])
        combined_df = combined_df.drop_duplicates(keep='first')
        combined_df.to_csv(path, mode='w', header=True, index=False)
    else:
        df.to_csv(path, mode='w', header=True, index=False)

def remove_duplicates(path: str):
    df = pd.read_csv(path)
    df = df.drop_duplicates()
    df.to_csv(path)
    return df

def clean_text(text: str):
    return text.replace("\n", "")

        