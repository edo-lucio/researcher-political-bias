import json
import os

from utils.utils import Utils
from api import RedditApi, ApiInterface
from dotenv import load_dotenv

import datetime
import time
import pandas as pd

load_dotenv()

class DataCollector:
    def __init__(self, api_client: ApiInterface) -> None:
        self.api_client = api_client

    def collect_reddit_users(
            self, 
            subreddit: str, 
            number_of_users: int = 1000, 
            output_file: str = "./data/users_karma.csv", 
            label: str = ""):
        
        users_karma = self.api_client.get_top_users_by_karma(subreddit, number_of_users)
        users_dict = {
            "users": [user[0] for user in users_karma], 
            "karma": [user[1] for user in users_karma],
            "label": [label] * len(users_karma)
            }
        
        users_df = pd.DataFrame(users_dict)
        Utils.write_to_file(users_df, output_file)

        return users_df

    def collect_user_posts(
        self,
        users: list[str],
        number_of_messages: int = 1000,
        start_time: datetime = None,
        end_time: datetime = None,
        posts: bool = True,
        output_file: str = "./data/users_posts.csv",
        label: str = ""
    ) -> pd.DataFrame:
        
        if start_time is None:
            start_time = 0.0
        if end_time is None:
            end_time = time.time()
        
        users_posts_list = []

        for user in users:
            users_posts = self.api_client.get_user_posts_within_timeframe(user, number_of_messages, start_time, end_time, posts)
            for user_post in users_posts:
                if "selftext" in user_post.keys():
                    user_post["selftext"] = Utils.clean_text(user_post["selftext"])
                    
                user_post_data = {
                    **user_post,
                    "user": user,
                    "label": label,
                    "is_post": 1 if posts else 0
                }
                users_posts_list.append(user_post_data)

        users_posts_df = pd.DataFrame(users_posts_list)

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        Utils.write_to_file(users_posts_df, output_file)
        
        return users_posts_df


if __name__ == "__main__":
    reddit_config = Utils.read_json("./config/collection_config.json")["reddit"]
    credentials = json.loads(os.getenv('REDDIT_API_CREDENTIALS'))[0]
    
    # auth configurations
    auth_keys = [credentials["client_id"], credentials["client_secret"]]
    username = credentials["username"]
    password = credentials["password"]
    auth_url = reddit_config["auth_url"]

    # collection configurations
    collection_configs = reddit_config["collection_configs"]

    for config in collection_configs:
        subreddit = config["subreddit"]
        karma_threshold = config["karma_threshold"]
        number_of_users = config["number_of_users"]
        number_of_posts_per_users = config["number_of_posts_per_users"]
        label = config["label"]

        # set reddit api client and collector
        reddit_client = RedditApi(auth_keys=auth_keys, username=username, password=password, auth_url=auth_url)
        reddit_collector = DataCollector(reddit_client)

        # users karma
        users_karma_df = reddit_collector.collect_reddit_users(subreddit=subreddit, number_of_users=number_of_users, label=label)
        users = users_karma_df["users"]

        # get text
        posts = reddit_collector.collect_user_posts(users, number_of_posts_per_users)
        comments = reddit_collector.collect_user_posts(users, number_of_posts_per_users, posts=False, label= label)