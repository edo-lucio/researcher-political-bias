import praw

from datetime import datetime
import pandas as pd
import json
import os

from utils import read_json
import logging

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

credentials_json = os.getenv('REDDIT_API_CREDENTIALS')
credentials = json.loads(credentials_json)[0]

config = read_json("./config/collection_config.json")["reddit"]
COLLECTION_CONFIGS = config["collection_configs"]

def _convert_columns_to_lowercase(df, columns):
    """
    Convert specified columns to lowercase in both DataFrames.

    Parameters:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    columns (list): List of column names to convert to lowercase.

    Returns:
    pd.DataFrame, pd.DataFrame: The modified DataFrames with specified columns in lowercase.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.lower()
            df[col] = df[col].astype(str)

    return df

class DataCollector:
    def __init__(
            self, 
            client_id,
            client_secret,
            username,
            password,
            user_agent="User-Agent: Mozilla/5.0 (<system-information>) <platform> (<platform-details>) <extensions>"
            ):

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password
        )

    def get_user_karma(self, subreddit_name, label, limit=10, output_file="./data/user_karma.csv"):
        def get_or_create_user(username, users):
            if username not in users:
                users[username] = {"username": username, "karma": 0, "label": label}
                logger.info(f"Created new user: {username}")

        subreddit = self.reddit.subreddit(subreddit_name)
        users = {}
        submission_types = ['top', 'controversial', 'new', 'hot', 'rising']
        user_data = []

        for submission_type in submission_types:
            for submission in getattr(subreddit, submission_type)(limit=1000):
                if len(users) >= limit:
                    break
                if not submission.author or not submission.author.name:
                    continue
                username = submission.author.name
                get_or_create_user(username, users)

                users[username]["karma"] += submission.score
                logger.info(f"Updated karma for user {username} from submission '{submission.title}'")

                # Fetch comments and add karma
                submission.comments.replace_more(limit=0)  # To fetch all comments
                for comment in submission.comments.list():
                    if len(users) >= limit:
                        break
                    if not comment.author or not comment.author.name:
                        continue
                    user = comment.author.name
                    get_or_create_user(user, users)

                    users[user]["karma"] += comment.score
                    logger.info(f"Updated karma for user {user} from comment in submission '{submission.title}'")

        for username, user_data_dict in users.items():
            user_data.append([user_data_dict["username"], user_data_dict["karma"], user_data_dict["label"], subreddit_name])

        df = pd.DataFrame(user_data, columns=["username", "karma", "label", "subreddit"])
        df.to_csv(output_file, index=False, mode='a', header=not pd.io.common.file_exists(output_file))

        df = _convert_columns_to_lowercase(df, ["username", "subreddit"])

        logger.info(f"User karma data saved to {output_file}")

        return df


    def get_user_posts(self, usernames, limit=10, output_file="./data/user_posts.csv"):
        """
        Retrieve recent posts for a list of users and write them to a CSV file.
        
        Args:
        usernames (list): A list of Reddit usernames to retrieve posts from.
        limit (int): Number of posts to retrieve per user.
        csv_file (str): The name of the CSV file to write to.
        """
        posts = []

        for username in usernames:
            try:
                user = self.reddit.redditor(username)
                for submission in user.submissions.new(limit=limit):
                    posted_time = datetime.fromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                    post = {
                        "username": username,
                        "title": submission.title.replace("\n", ""),
                        "selftext": submission.selftext.replace("\n", ""),
                        "subreddit": str(submission.subreddit),
                        "score": submission.score,
                        "num_comments": submission.num_comments,
                        "posted_time": posted_time
                    }
                    posts.append(post)
                    print(f"Added post by {username}: {submission.title} {submission.subreddit}")
            except Exception as e:
                print(f"Could not fetch posts for user {username}: {e}")
            
        df = pd.DataFrame(posts)
        df = _convert_columns_to_lowercase(df, ["username", "subreddit"])

        df.to_csv(output_file, index=False, mode='a', header=not pd.io.common.file_exists(output_file))

        return df
    
    def get_subreddit_member_count(self, subreddit_name):
        """
        Retrieve the number of members (subscribers) in a subreddit.
        
        Args:
        subreddit_name (str): The name of the subreddit.
        
        Returns:
        int: The number of members (subscribers) of the subreddit.
        """
        subreddit = self.reddit.subreddit(subreddit_name)
        return subreddit.subscribers


def main():
    collector = DataCollector(**credentials)

    for config in COLLECTION_CONFIGS:
        subreddit_name = config["subreddit"]
        label = config["label"]
        posts_per_users = config["number_of_posts_per_users"]
        # users_sample_size = int(collector.get_subreddit_member_count(subreddit_name) * 0.00005)
        users_sample_size = 1000

        # COllect users' karma
        users_karma = collector.get_user_karma(subreddit_name, label, limit=users_sample_size)

        # Collect users' posts
        posts = collector.get_user_posts(users_karma["username"], limit=1000)

        # Combine the data
        final_posts = pd.merge(users_karma, posts, how="right", on=["username", "subreddit"])
        final_posts.to_csv("./data/data.csv", index=False, mode='a', header=not pd.io.common.file_exists("./data/data.csv"))


if __name__ == "__main__":
    main()