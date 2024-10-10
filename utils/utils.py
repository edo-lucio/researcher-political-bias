import json
import os

class Utils:
    @staticmethod
    def read_json(path: str) -> dict:
        with open(path) as json_file:
            json_data = json.load(json_file)   
            return json_data
        
    @staticmethod
    def write_to_file(df, path: str):
        if os.path.exists(path):
            df.to_csv(path, mode='a', header=False, index=False)
        else:
            df.to_csv(path, mode='w', header=True, index=False)

    @staticmethod
    def clean_text(text: str):
        return text.replace("\n", "")
        