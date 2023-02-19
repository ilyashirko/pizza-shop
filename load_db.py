import argparse
import json
import os
import re
import sys
import requests

from datetime import datetime
from hashlib import md5

from environs import Env
from redis import Redis
from transliterate import translit
from transliterate.exceptions import LanguageDetectionError
from tqdm import tqdm

from motlin import Motlin

APP_DESCRIPTION = 'Script for adding items to Moltin database'

SLUG_EXISTS_ERROR_CODE = 422

EXAMPLE_MENU_FILENAME = 'menu.json'

EXAMPLE_ADDRESSES_FILENAME = 'addresses.json'

DEFAULT_FLOW_FIELDS = set(
    (
        ('address', 'string'),
        ('alias', 'string'),
        ('longitude', 'float'),
        ('latitude', 'float')
    )
)

def create_parser():
    parser = argparse.ArgumentParser(description=APP_DESCRIPTION)

    parser.add_argument(
        '--menu',
        type=str,
        help=f'Путь к файлу с товарами (напр. {os.path.join(os.getcwd(), EXAMPLE_MENU_FILENAME)})'
    )
    parser.add_argument(
        '--catalog_id',
        type=str,
        help=f'ID существующего каталога'
    )
    parser.add_argument(
        '--new_catalog_name',
        type=str,
        help=f'Название нового каталога'
    )
    parser.add_argument(
        '--hierarchy_id',
        type=str,
        help=f'ID существующей товарной группы'
    )
    parser.add_argument(
        '--new_hierarchy_name',
        type=str,
        help=f'Название новой товарной группы'
    )
    parser.add_argument(
        '--node_id',
        type=str,
        help=f'ID существующего подраздела товарной группы'
    )
    parser.add_argument(
        '--new_node_name',
        type=str,
        help=f'Название нового подраздела товарной группы'
    )
    parser.add_argument(
        '--pricebook_id',
        type=str,
        help=f'ID существующего прайс-листа'
    )
    parser.add_argument(
        '--new_pricebook_name',
        type=str,
        help=f'Название нового прайс-листа'
    )
    parser.add_argument(
        '--addresses',
        type=str,
        help=f'Путь к файлу с адресами пицерий (напр. {os.path.join(os.getcwd(), EXAMPLE_ADDRESSES_FILENAME)})'
    )
    parser.add_argument(
        '--new_flow_name',
        type=str,
        help=f'Название новой группы офисов / точек продаж'
    )
    parser.add_argument(
        '--new_flow_description',
        type=str,
        help=f'Описание новой группы офисов / точек продаж'
    )
    parser.add_argument(
        '--flow_id',
        type=str,
        help=f'ID группы офисов / точек продаж'
    )
    parser.add_argument(
        '--new_field_name',
        type=str,
        help=f'Название нового поля'
    )
    parser.add_argument(
        '--new_field_type',
        type=str,
        help=f'Тип нового поля'
    )
    parser.add_argument(
        '--default_value',
        type=str,
        help=f'Значение по умолчанию'
    )
    return parser


def make_slug(prod_name: str) -> str:
    try:
        translited = translit(prod_name.lower(), reversed=True)
    except LanguageDetectionError:
        translited = prod_name.lower()
    slug = '_'.join(translited.lower().split(' '))
    slug = ''.join(re.split(r"\W", slug))
    return slug


def get_file_content(filepath: str) -> dict:
    try:
        with open(filepath, 'r') as fileout:
            file_content = json.load(fileout)
    except FileNotFoundError:
        sys.stdout.write('File not found!\n')
        sys.exit(os.EX_NOINPUT)
    except json.decoder.JSONDecodeError:
        sys.stdout.write('Looks like its not a JSON-object!\n')
        sys.exit(os.EX_DATAERR)
    except:
        sys.stdout.write('Unknown error!\n')
        sys.exit(os.EX_IOERR)
    return file_content


if __name__ == '__main__':
    env = Env()
    env.read_env()

    parser = create_parser()
    args = parser.parse_args()
    
    motlin_api = Motlin(
        env.str('CLIENT_ID'),
        env.str('CLIENT_SECRET')
    )
    
    menu_filepath = args.menu
    addresses_filepath = args.addresses
    
    if not menu_filepath and not addresses_filepath and not args.new_field_name:
        sys.stdout.write('Enter menu, addresses or new field meta !\n')
        sys.exit(os.EX_USAGE)
    
    if menu_filepath:
        menu = get_file_content(filepath=menu_filepath)

        catalog_id = args.catalog_id or env.str('CATALOG_ID', None)
        hierarchy_id = args.hierarchy_id or env.str('HIERARCHY_ID', None)
        node_id = args.node_id or env.str('NODE_ID', None)
        pricebook_id = args.pricebook_id or env.str('PRICEBOOK_ID', None)

        is_enought_args = (
            (catalog_id or args.new_catalog_name) and 
            (hierarchy_id or args.new_hierarchy_name) and
            (node_id or args.new_node_name) and
            (pricebook_id or args.new_pricebook_name)
        )
        if not is_enought_args:
            sys.stdout.write('You should input catalog, hierarchy, node and price_boook id or name!\n')
            sys.exit(os.EX_USAGE)

        if not pricebook_id:
            new_pricebook_meta = motlin_api.create_pricebook(pricebook_name=args.new_pricebook_name)
            pricebook_id = new_pricebook_meta['data']['id']
            with open('.env', 'a') as env_file:
                env_file.write(f'\nPRICEBOOK_ID={pricebook_id}')
        if not hierarchy_id:
            new_hierarchy_meta = motlin_api.create_hierarchy(hierarchy_name=args.new_hierarchy_name)
            hierarchy_id = new_hierarchy_meta['data']['id']
            with open('.env', 'a') as env_file:
                env_file.write(f'\nHIERARCHY_ID={hierarchy_id}')
        if not node_id:
            new_node_meta = motlin_api.create_node(hierarchy_id=hierarchy_id, node_name=args.new_node_name)
            node_id = new_node_meta['data']['id']
            with open('.env', 'a') as env_file:
                env_file.write(f'\nNODE_ID={node_id}')
        if not catalog_id:
            new_catalog_meta = motlin_api.create_catalog(
                name=args.new_catalog_name,
                description='',
                hierarchy_ids=[hierarchy_id],
                pricebook_id=pricebook_id
            )
            catalog_id = new_catalog_meta['data']['id']
            with open('.env', 'a') as env_file:
                env_file.write(f'\nCATALOG_ID={catalog_id}')
            
            

        
        
        for product in tqdm(menu, desc='add products'):
            product_meta = {
                "type": "product",
                "attributes": {
                    "name": product['name'],
                    "slug": make_slug(product['name']),
                    "sku": md5(product['name'].encode('utf-8')).hexdigest()+'test12354',
                    "manage_stock": False,
                    "description": product['description'],
                    "status": "live",
                    "commodity_type": "physical"
                }
            }
            price_meta = {
                "data": {
                    "type": "product-price",
                    "attributes": {
                        "sku": product_meta['attributes']['sku'],
                        "currencies": {
                            "RUB": {
                                "amount": product['price'],
                                "includes_tax": True
                            }
                        }
                    }
                }
            }
            try:
                new_product_response = motlin_api.create_product(product_data=product_meta)
                motlin_api.create_product_price(pricebook_id=pricebook_id, price_meta=price_meta)
                new_product_id = new_product_response['data']['id']
                motlin_api.create_product_node_relationship(
                    hierarchy_id=hierarchy_id,
                    node_id=node_id,
                    products_ids=[new_product_id]
                )
            except requests.exceptions.HTTPError as error:
                if 'sku must be unique amongst products' in error.response.json()['errors'][0]['detail']:
                    continue
                
                elif error.response.status_code:
                    input(json.dumps(error.response.json(), indent=4))
                    continue
                else:
                    sys.stdout.write(json.dumps(error.response.json(), indent=4))
                    sys.exit(os.EX_IOERR)
            
            try:
                image_url = product['product_image']['url']
                image_response = motlin_api.add_file(image_url=image_url)
                image_id = image_response['data']['id']
                motlin_api.link_prod_and_image(product_id=new_product_id, image_id=image_id)
            except requests.exceptions.HTTPError as error:
                sys.stdout.write(json.dumps(error.response.json(), indent=4))
                sys.exit(os.EX_IOERR)

    if addresses_filepath:
        addresses = get_file_content(filepath=addresses_filepath)
        flow_id = args.flow_id or os.getenv('FLOW_ID', None)
        if addresses_filepath and not (flow_id or args.new_flow_name):
            sys.stdout.write('If you dont enter flow_id - you should enter new unique flow name!\n')
            sys.exit(os.EX_USAGE)
        
        if not flow_id:
            flow_name = args.new_flow_name
            flow_description = args.new_flow_description or ''
            flow_meta = motlin_api.create_flow(
                name=flow_name,
                description=flow_description,
                slug=make_slug(flow_name)
            )
            flow_id = flow_meta['data']['id']
            with open('.env', 'a') as env_file:
                env_file.write(f'\nFLOW_ID={flow_id}')
        else:
            flow_meta = motlin_api.get_flow(flow_id=flow_id)
        
        flow_slug = flow_meta['data']['slug']
        
        fields_metas = motlin_api.get_flow_fields(flow_slug=flow_slug)
        current_fields = set((field['name'], field['field_type']) for field in fields_metas['data'])
        
        for field in tqdm(DEFAULT_FLOW_FIELDS.difference(current_fields), desc='creating fields'):
            motlin_api.create_field(
                name=field[0],
                slug=make_slug(field[0]),
                field_type=field[1],
                description='',
                flow_id=flow_id
            )
        
        current_entities = motlin_api.get_entries(flow_slug=flow_slug)
        current_addresses = [address['alias'] for address in current_entities]
        
        for address in tqdm(addresses, desc='adding addresses'):
            if address['alias'] in current_addresses:
                continue
            motlin_api.create_entry(
                flow_slug=flow_slug,
                address=address['address']['full'],
                alias=address['alias'],
                longitude=float(address['coordinates']['lon']),
                latitude=float(address['coordinates']['lat'])
            )

    if args.new_field_name:
        new_field_name = args.new_field_name
        default_value = args.default_value
        new_field_type = args.new_field_type
        flow_id = os.getenv('PIZZERIAS_FLOW_ID', None) or args.flow_id
        
        if not (new_field_name and new_field_type and default_value and flow_id):
            sys.stdout.write('Enter new field params a!\n')
            sys.exit(os.EX_USAGE)
        if new_field_type.lower() not in ('string', 'integer', 'boolean', 'float', 'date', 'relationship'):
            sys.stdout.write('Incorrect field type!\n')
            sys.exit(os.EX_USAGE)
        new_field_slug = make_slug(new_field_name)
        try:
            motlin_api.create_field(
                name=new_field_name,
                slug=new_field_slug,
                field_type=new_field_type.lower(),
                description='',
                flow_id=flow_id
            )
        except requests.exceptions.HTTPError as error:
            pass
        flow_meta = motlin_api.get_flow(flow_id=os.getenv('PIZZERIAS_FLOW_ID'))
        entries = motlin_api.get_entries(flow_slug=flow_meta['data']['slug'])
        for entry in tqdm(entries, desc='adding field value'):
            try:
                motlin_api.update_entry(
                    flow_slug=flow_meta['data']['slug'],
                    entry_id=entry['id'],
                    field_slug=new_field_slug,
                    field_value=default_value
                )
            except requests.exceptions.HTTPError as error:
                sys.stdout.write(json.dumps(error.response.json(), indent=4))
                sys.exit(os.EX_IOERR)