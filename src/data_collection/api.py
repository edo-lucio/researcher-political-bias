import time
import requests
import requests.auth
import logging
import collections

from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ApiInterface:
    def __init__(
            self, 
            auth_keys: list,
            headers: dict = {"User-Agent": "ChangeMeClient/0.1 by YourUsername"},
            timeout: float = 4) -> None:
        
        self.headers = headers
        self.auth_keys = auth_keys
        self.timeout = timeout
        self.client_auth = requests.auth.HTTPBasicAuth(*auth_keys)

    def post_request(self, url: str, post_data: dict):
        response = requests.post(url=url, auth=self.client_auth, data=post_data, headers=self.headers, timeout=self.timeout)
        if response.status_code != 200: 
            raise Exception(f"Failed to get access token: {response.status_code} {response.text}")
        return response.json()
    
    def get_request(self, url: str):
        response = requests.get(url=url, headers=self.headers)
        if response.status_code != 200: 
            raise Exception(f"Failed to get access token: {response.status_code} {response.text}")
        return response.json()

class RedditApi(ApiInterface):
    def __init__(
            self, 
            auth_keys: list,
            username: str,
            password: str,
            auth_url: str,
            headers: dict = {"User-Agent": "ChangeMeClient/0.1 by YourUsername"},
            timeout: float = 4) -> None:
        
        super().__init__(auth_keys, headers, timeout)

        self.username = username
        self.password = password
        self.auth_url = auth_url
        self.post_data = {"scope": "read identity history", "grant_type": "password", "username": username, "password": password}
        self._update_token()
        
    def _token_expired(self):
        if time.time() - self.token_start_time >= self.token_expires_in:
            return True

    def _update_token(self):
        token = super().post_request(url=self.auth_url, post_data=self.post_data)  # Refresh the token
        self.token_expires_in = token["expires_in"]
        self.token_start_time = time.time() 
        self.headers["Authorization"] = f"{token['token_type']} {token['access_token']}"
        
    def get_request(self, url):
        try:
            if self._token_expired():
                self._update_token()
            return super().get_request(url)
        except Exception as e:
            logging.error(f"First attempt failed with error: {str(e)}")

            if "expired" in str(e).lower():
                logging.info("Refreshing access token and retrying...")
                self._update_token()
                try:
                    return super().get_request(url)
                except Exception as retry_error:
                    logging.error(f"Retry failed with error: {str(retry_error)}")
                    raise retry_error
            else:
                raise e

    def get_top_users_by_karma(self, subreddit: str, limit: int = 1000):
        user_karma = collections.defaultdict(int)
        
        after = None
        total_fetched = 0

        while total_fetched < limit:
            url = f"https://oauth.reddit.com/r/{subreddit}/top?limit=100&t=all"
            if after:
                url += f"&after={after}"
            logging.info(f"Fetching posts from: {url}")
        
            response = self.get_request(url)
            posts = response.get('data', {}).get('children', [])

            for post in posts:
                post_data = post['data']
                author = post_data['author']
                karma = post_data['score']
                if author and karma:
                    user_karma[author] += karma

            total_fetched += len(posts)
            after = response.get('data', {}).get('after')

            if not after:
                break 

        sorted_users = sorted(user_karma.items(), key=lambda item: item[1], reverse=True)
        return sorted_users[:limit] 

    def get_user_posts_within_timeframe(
            self, 
            username: str,
            number_of_messages: int,
            start_time: datetime=float(0),
            end_time: datetime=time.time(),
            posts: bool=True):
        
        content_type = "submitted" if posts else "comments"
        url = f"https://oauth.reddit.com/user/{username}/{content_type}?limit=100"

        user_posts = []
        after = None

        if not isinstance(start_time, float):
            start_time = int(start_time.replace(tzinfo=timezone.utc).timestamp())

        if not isinstance(end_time, float):
            end_time = int(end_time.replace(tzinfo=timezone.utc).timestamp())

        while len(user_posts) < number_of_messages:
            url = url.format({"username": username})
            if after:
                url += f"&after={after}"
            logging.info(f"Fetching posts from: {url}")
            
            try:
                response = self.get_request(url)
            except Exception as e:
                if "404" in str(e):
                    logging.error(f"User {username} does not exists")
                if "403" in str(e):
                    logging.error(f"User {username} cannot be accessed")
                return []

            posts = response.get('data', {}).get('children', [])

            if not posts:
                break

            for post in posts:
                post_data = post['data']
                post_time = post_data['created_utc']  

                if start_time <= post_time <= end_time:
                    user_posts.append(post_data)
                    if len(user_posts) >= number_of_messages:
                        break

            after = response.get('data', {}).get('after')

            if not after:
                break

        return user_posts
    