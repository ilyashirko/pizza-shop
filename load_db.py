import argparse
import json
import os
import re
import sys
import requests

from hashlib import md5

from environs import Env
from redis import Redis
from transliterate import translit
from tqdm import tqdm

from motlin import Motlin


APP_DESCRIPTION = 'Script for adding items to Moltin database'

SLUG_EXISTS_ERROR_CODE = 422

def create_parser():
    parser = argparse.ArgumentParser(description=APP_DESCRIPTION)

    default_menu_path = os.path.join(os.getcwd(), 'menu.json')
    parser.add_argument(
        '-m'
        '--menu',
        type=str,
        help=f'Путь к файлу с товарами (напр. {default_menu_path})'
    )

    return parser


def make_slug(prod_name: str) -> str:
    translited = translit(prod_name, reversed=True)
    slug = '_'.join(translited.lower().split(' '))
    slug = ''.join(re.split(r"\W", slug))
    return slug


if __name__ == '__main__':
    env = Env()
    env.read_env()

    parser = create_parser()
    args = parser.parse_args()
    
    motlin_api = Motlin(
        env.str('CLIENT_ID'),
        env.str('CLIENT_SECRET')
    )

    menu_filepath = args.m__menu

    if not menu_filepath:
        sys.stdout.write('Enter menu filepath!\n')
        sys.exit(os.EX_USAGE)

    try:
        with open(menu_filepath, 'r') as menu_file:
            menu = json.load(menu_file)
    except FileNotFoundError:
        sys.stdout.write('File not found!\n')
        sys.exit(os.EX_NOINPUT)
    except json.decoder.JSONDecodeError:
        sys.stdout.write('Looks like its not a JSON-object!\n')
        sys.exit(os.EX_DATAERR)
    except:
        sys.stdout.write('Unknown error!\n')
        sys.exit(os.EX_IOERR)
    
 

    for product in tqdm(menu, desc='add products'):
        product_meta = {
            "type": "product",
            "attributes": {
                "name": product['name'],
                "slug": make_slug(product['name']),
                "sku": md5(product['name'].encode('utf-8')).hexdigest(),
                "manage_stock": False,
                "description": product['description'],
                "price": [
                    {
                        "amount": product['price'],
                        "currency": "RUB",
                        "includes_tax": True
                    }
                ],
                "status": "live",
                "commodity_type": "physical"
            }
        }
        try:
            new_product_id = motlin_api.add_product(product_data=product_meta)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == SLUG_EXISTS_ERROR_CODE:
                continue
            else:
                sys.stdout.write(json.dumps(error.response.json(), indent=4))
                sys.exit(os.EX_IOERR)
        
        try:
            image_url = product['product_image']['url']
            image_id = motlin_api.add_file(image_url=image_url)

            motlin_api.link_prod_and_image(product_id=new_product_id, image_id=image_id)
        except requests.exceptions.HTTPError as error:
            sys.stdout.write(json.dumps(error.response.json(), indent=4))
            sys.exit(os.EX_IOERR)
