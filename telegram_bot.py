import os

from datetime import datetime

from environs import Env
from redis import Redis

import elastic_management

DEFAULT_REDIS_HOST = 'localhost'

DEFAULT_REDIS_PORT = 6379

DEFAULT_REDIS_PASSWORD = None

DEFAULT_REDIS_DB = 0

SPARE_TOKEN_CART_TIME = 300
# SPARE_TOKEN_CART_TIME is used to prevent token or cart death while current script processing.
# 300 - random value ensuring that there is enough time for script processing

def get_or_create_elastic_token(redis: Redis) -> str:
    token = redis.get('ELASTIC_AUTH_TOKEN')
    expired_at = redis.get('ELASTIC_AUTH_TOKEN_expires')
    
    if token is None or \
       expired_at is None or \
       datetime.now().timestamp() + SPARE_TOKEN_CART_TIME > int(expired_at):

        token, expired_at = elastic_management.get_token(os.getenv('CLIENT_ID'))
        redis.set('ELASTIC_AUTH_TOKEN', token)
        redis.set('ELASTIC_AUTH_TOKEN_expires', expired_at)
        
    return token


if __name__ == '__main__':
    env = Env()
    env.read_env()

    from motlin import Motlin

    elastic = Motlin()
    print(elastic.get_token())
