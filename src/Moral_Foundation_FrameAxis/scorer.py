import os
import pandas as pd

from gensim.models import KeyedVectors

from frameAxis import FrameAxis
from utils import read_json

class MoralFoundationScorer:
    def __init__(
            self, 
            input_file: str, 
            dict_type: str, 
            output_file: str, 
            docs_col: str, 
            model_path: str, 
            tfidf: bool=False, 
            format: str="virtue_vice") -> None:
        
        self.input_file = f"./data/{input_file}"
        self.output_file = f"./data/{output_file}"
        self.dict_type = dict_type # if DICT_TYPE not in ["emfd", "mfd", "mfd2", "customized"]:
        self.docs_col = docs_col
        self.model = self.setup_model(model_path)
        self.tfidf = tfidf
        self.format = format

    def setup_model(self, model_path: str='word2vec-google-news-300.bin'):
        model = model_path.split(".")[0]

        if os.path.isfile(model_path):
            model = KeyedVectors.load_word2vec_format(model_path, binary=True)
        else:
            print(f'Downloading word embedding model: {model}')
            import gensim.downloader
            model = gensim.downloader.load(model)
            model.save_word2vec_format(model_path, binary=True)
            print(f"Model downloaded and saved at {model_path}")
        
        return model
    
    def score(self) -> pd.DataFrame:
        if self.dict_type not in ["emfd", "mfd", "mfd2", "customized"]:
            raise ValueError(
                f'Invalid dictionary type received: {self.dict_type}, dict_type must be one of \"emfd\", \"mfd\", \"mfd2\", \"customized\"')

        data = pd.read_csv(self.input_file, on_bad_lines='skip', encoding='utf-8').drop_duplicates()
        print(data.head())

        fa = FrameAxis(mfd=self.dict_type, w2v_model=self.model)
        mf_scores = fa.get_fa_scores(
            df=data, 
            doc_colname=self.docs_col, 
            tfidf=self.tfidf, 
            format=self.format,
            save_path=self.output_file)
        
        return mf_scores

if __name__ == "__main__":
    config = read_json("./config/scoring_config.json")

    scorer = MoralFoundationScorer(
        input_file=config["input_file"],
        dict_type=config["dict_type"],
        output_file=config["output_file"], 
        docs_col=config["docs_col"],
        model_path=config["model_path"],
        tfidf=eval(config["tfidf"]),
        format=config["format"])
    
    scores = scorer.score()