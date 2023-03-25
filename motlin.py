from __future__ import annotations
from contextlib import suppress
from itertools import product
import json

import requests
from redis import Redis
from datetime import datetime


class Motlin:
    EXPIRED_SPARE_TIME = 300  # seconds
    
    client_id = str()
    client_secret = str()
    
    token = str()
    token_expired = str()

    products = list()

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
                with suppress(requests.exceptions.HTTPError):
                    self.token, self.token_expired = self.get_token()
            return func(self, **kwargs)
        return wrapper

    @_refresh_token_if_expired
    def create_catalog(self,
                       name: str,
                       description: str,
                       hierarchy_ids: list[str],
                       pricebook_id: str) -> dict:
        url = 'https://api.moltin.com/pcm/catalogs'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "type": "catalog",
                "attributes": {
                    "name": name,
                    "description": description,
                    "hierarchy_ids": hierarchy_ids,
                    "pricebook_id": pricebook_id
                }
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def publish_catalog(self, catalog_id: str) -> dict:
        url = f'https://api.moltin.com/pcm/catalogs/{catalog_id}/releases'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "export_full_delta": True
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def get_catalog(self, catalog_id: str) -> dict:
        url = f'https://api.moltin.com/pcm/catalogs/{catalog_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def create_hierarchy(self, hierarchy_name: str) -> dict:
        url = 'https://api.moltin.com/pcm/hierarchies/'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "type": "hierarchy",
                "attributes": {
                    "name": hierarchy_name
                }
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def create_node(self, hierarchy_id: str, node_name:str) -> dict:
        url = f'https://api.moltin.com/pcm/hierarchies/{hierarchy_id}/nodes'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "type": "node",
                "attributes": {
                    "name": node_name
                }
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def get_product(self, product_id: str) -> dict:
        url = f'https://api.moltin.com/pcm/products/{product_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        params = {
            "include": "main_image",
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def create_product(self, product_data: dict) -> str:
        url = 'https://api.moltin.com/pcm/products'
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json={"data": product_data})
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def create_product_node_relationship(self,
                                        hierarchy_id: str,
                                        node_id: str,
                                        products_ids: list|tuple) -> dict:
        url = f'https://api.moltin.com/pcm/hierarchies/{hierarchy_id}/nodes/{node_id}/relationships/products'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": [
                {
                    "type": "product",
                    "id": product_id
                }
                for product_id in products_ids
            ]
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()

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
        return response.json()

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

    @_refresh_token_if_expired
    def get_flow(self, flow_id: str) -> dict:
        url = f'https://api.moltin.com/v2/flows/{flow_id}'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
        
    @_refresh_token_if_expired
    def create_flow(self,
                    name: str,
                    description: str,
                    slug: str,
                    enabled: bool = True):
        url = 'https://api.moltin.com/v2/flows'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        request_data = {
            "data": {
                "type": "flow",
                "name": name,
                "description": description,
                "slug": slug,
                "enabled": enabled
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def get_flow_fields(self, flow_slug: str) -> set:
        url = f'https://api.moltin.com/v2/flows/{flow_slug}/fields'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def create_field(self,
                     name: str,
                     slug: str,
                     field_type: str,
                     description: str,
                     flow_id: str,
                     required: bool = True,
                     enabled: bool = True):
        url = 'https://api.moltin.com/v2/fields'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        request_data = {
            "data": {
                "type": "field",
                "name": name,
                "slug": slug,
                "field_type": field_type,
                "description": description,
                "required": required,
                "enabled": enabled,
                "relationships": {
                    "flow": {
                        "data": {
                            "type": "flow",
                            "id": flow_id
                        }
                    }
                }
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def get_entries(self,
                     flow_slug: str) -> list:
        url = f'https://api.moltin.com/v2/flows/{flow_slug}/entries'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        entries = list()
        while True:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            entries_meta = response.json()
            entries += entries_meta['data']
            if not entries_meta['data'] or not entries_meta['links']['next']:
                break
            else:
                url = entries_meta['links']['next']
        return entries
    

    @_refresh_token_if_expired
    def get_entry(self,
                     flow_slug: str,
                     entry_id: str) -> dict:
        url = f'https://api.moltin.com/v2/flows/{flow_slug}/entries/{entry_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    

    @_refresh_token_if_expired
    def create_entry(self,
                     flow_slug: str,
                     address: str,
                     alias: str,
                     longitude: float,
                     latitude: float) -> dict:
        url = f'https://api.moltin.com/v2/flows/{flow_slug}/entries'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "type": "entry",
                "address": address,
                "alias": alias,
                "longitude": longitude,
                "latitude": latitude
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def update_entry(self,
                     flow_slug: str,
                     entry_id: str,
                     field_slug: str,
                     field_value: str):
        url = f'https://api.moltin.com/v2/flows/{flow_slug}/entries/{entry_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "type": "entry",
                "id": entry_id,
                field_slug: field_value,
            }
        }
        response = requests.put(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()


    @_refresh_token_if_expired
    def get_products(self):
        url = 'https://api.moltin.com/pcm/products'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def get_products_in_release(self,
                                catalog_id: str,
                                node_id: str,
                                release_id: str = 'latest') -> dict:
        url = f'https://api.moltin.com/pcm/catalogs/{catalog_id}/releases/{release_id}/nodes/{node_id}/relationships/products'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    @_refresh_token_if_expired
    def get_pricebook(self, pricebook_id: str, include_prices: bool = True) -> dict:
        url = f'https://api.moltin.com/pcm/pricebooks/{pricebook_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        params = {"include": "prices"} if include_prices else {}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def create_pricebook(self, pricebook_name: str, pricebook_description: str = '') -> dict:
        url = 'https://api.moltin.com/pcm/pricebooks'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        request_data = {
            "data": {
                "type": "pricebook",
                "attributes": {
                    "name": pricebook_name,
                    "description": pricebook_description
                }
            }
        }
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def create_product_price(self, pricebook_id: str, price_meta: dict) -> dict:
        url = f'https://api.moltin.com/pcm/pricebooks/{pricebook_id}/prices'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.post(url, headers=headers, json=price_meta)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def create_cart(self,
                    name: str = f'{int(datetime.now().timestamp())}_cart') -> dict:
        url = 'https://api.moltin.com/v2/carts'
        headers = {
            "Authorization": f"Bearer {self.token}",
            'Content-Type': 'application/json',
        }
        post_data = {
            "data": {
                "name": name,
            }
        }
        response = requests.post(url, headers=headers, json=post_data)
        response.raise_for_status()
        return response.json()

    def _create_or_refresh_cart(func, **kwargs):
        def wrapper(self, **kwargs):
            user_telegram_id = kwargs.get('user_telegram_id')
            cart_id = self.redis.get(f'{user_telegram_id}_cart_id')
            cart_expired = self.redis.get(f'{user_telegram_id}_cart_expired')
            if not (cart_id and cart_expired) or \
                datetime.now().timestamp() + self.EXPIRED_SPARE_TIME > int(cart_expired):
                new_cart = self.create_cart()

                expired_at = new_cart['data']['meta']['timestamps']['expires_at']
                datetime_obj = datetime.fromisoformat(expired_at + '+03:00')

                self.redis.set(f'{user_telegram_id}_cart_id', new_cart['data']['id'])
                self.redis.set(f'{user_telegram_id}_cart_expired', int(datetime_obj.timestamp()))
            return func(self, **kwargs)
        return wrapper

    @_refresh_token_if_expired
    def delete_cart(self, cart_id: str) -> None:
        url = f'https://api.moltin.com/v2/carts/{cart_id}'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        response = requests.delete(url, headers=headers)
        response.raise_for_status()

    @_refresh_token_if_expired
    @_create_or_refresh_cart
    def add_product_to_cart(self,
                            user_telegram_id: int,
                            product_id: str,
                            quantity: int):
        cart_id = self.redis.get(f'{user_telegram_id}_cart_id')
        url = f'https://api.moltin.com/v2/carts/{cart_id}/items'
        headers = {
            "Authorization": f"Bearer {self.token}",
            'Content-Type': 'application/json',
        }
        post_data = {
            "data": {
                "id": product_id,
                "type": 'cart_item',
                'quantity': quantity
            }
        }
        response = requests.post(url, headers=headers, json=post_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    @_create_or_refresh_cart
    def get_cart(self,
                 user_telegram_id: int) -> dict:
        cart_id = self.redis.get(f'{user_telegram_id}_cart_id')
        url = f'https://api.moltin.com/v2/carts/{cart_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        params = {
            "include": "items",
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    @_create_or_refresh_cart
    def remove_product_from_cart(self,
                                 user_telegram_id: int,
                                 item_id: str) -> dict:
        cart_id = self.redis.get(f'{user_telegram_id}_cart_id')
        url = f'https://api.moltin.com/v2/carts/{cart_id}/items/{item_id}'
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def create_customer(self,
                        name: str,
                        email: str,
                        user_telegram_id: int) -> dict:
        url = 'https://api.moltin.com/v2/customers'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        post_data = {
            "data": {
                "type": "customer",
                "name": name,
                "email": email,
            }
        }
        response = requests.post(url, headers=headers, json=post_data)
        response.raise_for_status()
        response_meta = response.json()
        self.redis.set(f'{user_telegram_id}_customer_id', response_meta['data']['id'])
        return response_meta
    
    @_refresh_token_if_expired
    def update_customer_address(self,
                                customer_id: str,
                                longitude: float,
                                latitude: float) -> dict:
        url = f'https://api.moltin.com/v2/customers/{customer_id}'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        put_data = {
            "data": {
                "type": "customer",
                "longitude": longitude,
                "latitude": latitude
            }
        }
        response = requests.put(url, headers=headers, json=put_data)
        response.raise_for_status()
        return response.json()
    
    @_refresh_token_if_expired
    def get_customer(self, customer_id: str) -> dict:
        url = f'https://api.moltin.com/v2/customers/{customer_id}'
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
