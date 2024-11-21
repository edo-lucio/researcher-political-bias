import json
import os
import logging
from utils import Utils
from data_collection.api import RedditApi
from dotenv import load_dotenv

import datetime
import time
import functools
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format of log messages
    handlers=[
        logging.StreamHandler()  # Log to console
    ]
)

load_dotenv()
config = Utils.read_json("./config/collection_config.json")["reddit"]
AUTH_URL = config["auth_url"]
COLLECTION_CONFIGS = config["collection_configs"]

# Define the decorator for handling errors
def handle_reddit_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]  # Get the instance (self) of the class
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e):  # Handle rate limit errors
                logging.error(f"Rate limit error encountered: {e}. Switching credentials and retrying...")
                time.sleep(8)
                return func(*args, **kwargs)  # Retry the function after switching credentials
            else:
                logging.error(f"An error occurred: {e}")
                raise  # Re-raise the error if it's not a rate limit issue
    return wrapper

class DataCollector:
    def __init__(
            self, 
            reddit_credentials_list: list = [], 
            headers: dict = {"User-Agent": "ChangeMeClient/0.1 by YourUsername"}, 
            timeout: float = 4) -> None:
        
        self.index = 0
        self.credentials_list = reddit_credentials_list
        self.headers = headers
        self.timeout = timeout
        self.credentials = self.credentials_list[self.index]
        self.auth_keys = [self.credentials["client_id"], self.credentials["client_secret"]]
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]

        self.reddit_client = RedditApi(
            self.auth_keys,
            self.username,
            self.password,
            AUTH_URL, 
            self.headers,
            self.timeout
        )

    def change_credentials(self):
        if len(self.credentials_list) == 1:
            logging.info("Only one set of credentials available. Waiting to avoid rate-limiting...")
            time.sleep(8)  # wait some seconds to avoid flooding
        elif self.index < len(self.credentials_list) - 2:
            self.index += 1
        else:
            self.index -= 1

        self.credentials = self.credentials_list[self.index]
        self.auth_keys = [self.credentials["client_id"], self.credentials["client_secret"]]
        self.username = self.credentials["username"]
        self.password = self.credentials["password"]

        self.reddit_client.auth_keys = self.auth_keys
        self.reddit_client.username = self.username
        self.reddit_client.password = self.password

        logging.info(f"Switched to credentials: {self.username}")

    def clean_posts(self, user_post):
        if "selftext" in user_post.keys():
            user_post["selftext"] = Utils.clean_text(user_post["selftext"])
            if "selftext_html" in user_post.keys():
                user_post.pop('selftext_html', None)
        return user_post

    @handle_reddit_errors
    def collect_reddit_users(
            self, 
            subreddit: str, 
            number_of_users: int = 100000,
            output_file: str = "./data/users_karma.csv", 
            karma_threshold: int = 0,
            label: str = ""):
        
        if number_of_users == 0:
            number_of_users = 100000
        logging.info(f"Collecting top {number_of_users} users from subreddit '{subreddit}' with karma threshold {karma_threshold}.")
        
        users_karma = self.reddit_client.get_top_users_by_karma(subreddit, number_of_users)
        users_dict = {
            "users": [user[0] for user in users_karma], 
            "karma": [user[1] for user in users_karma],
            "label": [label] * len(users_karma)
        }
        
        users_df = pd.DataFrame(users_dict)
        Utils.write_to_file(users_df, output_file)

        logging.info(f"Collected {len(users_df)} users from subreddit '{subreddit}'. Data saved to {output_file}.")
        
        return users_df

    @handle_reddit_errors
    def collect_user_posts(
        self,
        users: list[str],
        number_of_messages: int = 1000,
        start_time: datetime = None,
        end_time: datetime = None,
        posts: bool = True,
        output_file: str = "./data/users_posts.csv",
    ) -> pd.DataFrame:
        
        logging.info(f"Collecting posts for {len(users)} users. Collecting posts: {posts}")
        
        if start_time is None:
            start_time = 0.0
        if end_time is None:
            end_time = time.time()
        
        users_posts_list = []

        for user in users:
            logging.debug(f"Collecting posts for user: {user}")
            users_posts = self.reddit_client.get_user_posts_within_timeframe(
                user["users"], number_of_messages, start_time, end_time, posts)

            for user_post in users_posts:
                user_post = self.clean_posts(user_post)
                user_post_data = {
                    **user_post,
                    "label": user["label"],
                    "karma": user["karma"],
                    "is_post": 1 if posts else 0
                }
                users_posts_list.append(user_post_data)

        users_posts_df = pd.DataFrame(users_posts_list)
        Utils.write_to_file(users_posts_df, output_file)

        logging.info(f"Collected {len(users_posts_df)} posts/comments for users. Data saved to {output_file}.")
        return users_posts_df

    def collect_reddit_data(self):
        for config in COLLECTION_CONFIGS:
            subreddit = config["subreddit"]
            label = config["label"]
            karma_threshold = config["karma_threshold"]
            number_of_posts_per_users = config["number_of_posts_per_users"]
            users_sample_size = (self.reddit_client.get_subreddit_member_count(subreddit)) * 0.1

            logging.info(f"Starting data collection for subreddit: {subreddit} \n ======== \n")
            
            users_karma_df = self.collect_reddit_users(
                subreddit=subreddit,
                number_of_users=users_sample_size, 
                label=label, 
                karma_threshold=karma_threshold)
            
            users_karma = users_karma_df.to_dict(orient="records")
            
            posts = self.collect_user_posts(users_karma, number_of_posts_per_users)
            comments = self.collect_user_posts(users_karma, number_of_posts_per_users, posts=False)

def main():
    credentials_json = os.getenv('REDDIT_API_CREDENTIALS')
    credentials = json.loads(credentials_json)
    data_collector = DataCollector(reddit_credentials_list=credentials)
    data_collector.collect_reddit_data()

if __name__ == "__main__":
    main()
