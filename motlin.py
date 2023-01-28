from __future__ import annotations
import requests
from redis import Redis
from datetime import datetime


class Motlin:
    EXPIRED_SPARE_TIME = 300  # seconds

    client_id = str()
    client_secret = str()
    
    token = str()
    token_expired = str()

    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 redis_host: str = 'localhost',
                 redis_port: int = 6379,
                 redis_password: str = None,
                 redis_db: int = 0) -> Motlin:

        self.redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            decode_responses=True
        )
        self.client_id = client_id
        self.client_secret = client_secret
        self.token, self.token_expired = self.get_token(
            client_id=client_id,
            client_secret=client_secret
        )

    def get_token(self,
                  client_id: str = client_id,
                  client_secret: str = client_secret) -> tuple[str]:
        access_token_url = 'https://api.moltin.com/oauth/access_token'
        access_token_data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
        }
        response = requests.get(access_token_url, data=access_token_data)
        response.raise_for_status()
        token_meta = response.json()
        return token_meta['access_token'], int(token_meta['expires'])

    def _refresh_token_if_expired(func, **kwargs):
        def wrapper(self, **kwargs):
            if datetime.now().timestamp() + self.EXPIRED_SPARE_TIME > self.token_expired:
                self.token, self.token_expired = self.get_token()
            return func(self, **kwargs)
        return wrapper

    @_refresh_token_if_expired
    def add_product(self, product_data: dict) -> str:
        url = 'https://api.moltin.com/pcm/products'
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json={"data": product_data})
        response.raise_for_status()
        return response.json()['data']['id']

    @_refresh_token_if_expired
    def add_file(self, image_url: str) -> dict:
        url = 'https://api.moltin.com/v2/files'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        files = {
            "file_location": (None, image_url)
        }
        response = requests.post(url, headers=headers, files=files)
        response.raise_for_status()
        return response.json()['data']['id']

    @_refresh_token_if_expired
    def link_prod_and_image(self, product_id: str, image_id: str) -> None:
        url = f'https://api.moltin.com/pcm/products/{product_id}/relationships/main_image'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        request_data = {
            "data": {
                "type": "file",
                "id": image_id
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()

