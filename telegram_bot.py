import json
import os
import re

from functools import partial
from environs import Env
from textwrap import dedent

from more_itertools import chunked

import requests

from geopy import distance as geopy_distance
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    CallbackContext,
    CallbackQueryHandler,
    ConversationHandler,
    Filters,
)
from telegram.ext.jobqueue import JobQueue

from motlin import Motlin

PRODUCTS_PER_MESSAGE = 10

ONE_HOUR = 60 * 60

CUSTOMER_ALREADY_EXISTS_ERROR_CODE = 409


def make_products_inline(motlin_api: Motlin,
                         items_in_row: int = 2,
                         left_border: int = 0,
                         right_border: int = PRODUCTS_PER_MESSAGE) -> InlineKeyboardMarkup:
    assert left_border >= 0
    assert right_border >= 0
    products = motlin_api.get_products_in_release(
        catalog_id=os.getenv("CATALOG_ID"),
        node_id=os.getenv('NODE_ID')
    )['data']
    chunked_products = list(chunked(products[left_border:right_border], items_in_row))
    if not chunked_products:
        # perhaps DB was refactored / became shorter or something else.
        # this is emergency option
        chunked_products = list(chunked(products[:PRODUCTS_PER_MESSAGE], items_in_row))
    buttons = [
        [
            InlineKeyboardButton(
                text=product['attributes']['name'],
                callback_data=f'product:{product["id"]}'
            )
            
            for product in row
        ]
        for row in chunked_products 
    ]
    
    navigation_buttons = list()
    if left_border > 0:
        assert left_border - 10 >= 0
        navigation_buttons.append(InlineKeyboardButton(
            text='⬅️ предыдущие ⬅️', callback_data=f'other_products:{left_border - 10}-{left_border}'
        ))
    if len(products) > right_border:
        navigation_buttons.append(InlineKeyboardButton(
            text='➡️ следующие ➡️', callback_data=f'other_products:{right_border}-{right_border + 10}'
        ))
    buttons.append(navigation_buttons)
    buttons.append([InlineKeyboardButton(text='🛒 Моя корзина 🛒', callback_data='show_cart')])
    return InlineKeyboardMarkup(buttons)


def make_current_product_inline(product_id: str, quantity: int = 1) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(text='-', callback_data=f'reduce_quantity:{product_id}'),
                InlineKeyboardButton(text=f'{quantity}', callback_data=f'add_to_cart:{product_id}'),
                InlineKeyboardButton(text='+', callback_data=f'increase_quantity:{product_id}')
            ],
            [InlineKeyboardButton(text='Добавить в корзину', callback_data=f'add_to_cart:{product_id}')],
            [InlineKeyboardButton(text='🛒 Моя корзина 🛒', callback_data='show_cart')],
            [InlineKeyboardButton(text='Назад', callback_data='main_menu')]
        ]
    )


def display_products(motlin_api: Motlin,
                     update: Update,
                     context: CallbackContext) -> str:
    context.bot.send_message(
        update.effective_chat.id,
        'Выберите пиццу',
        reply_markup=make_products_inline(
            motlin_api=motlin_api,
            left_border=0,
            right_border=PRODUCTS_PER_MESSAGE
        )
    )
    return 'HANDLE_MENU'


def display_other_products(motlin_api: Motlin,
                           update: Update,
                           context: CallbackContext) -> str:
    _, left_border, right_border = re.split(r':|-', update.callback_query.data)
    left_border, right_border = int(left_border), int(right_border)
    context.bot.send_message(
        update.effective_chat.id,
        'Выберите пиццу',
        reply_markup=make_products_inline(
            motlin_api=motlin_api,
            left_border=left_border,
            right_border=right_border
        )
    )
    context.bot.delete_message(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
    )
    return 'HANDLE_MENU'


def show_product(motlin_api: Motlin,
                 update: Update,
                 context: CallbackContext) -> str:
    _, product_id = re.split(r':', update.callback_query.data)
    product = motlin_api.get_product(product_id=product_id)
    
    pricebook = motlin_api.get_pricebook(pricebook_id=os.getenv('PRICEBOOK_ID'))
    price = [
        price['attributes']['currencies']['RUB']['amount']
        for price in pricebook["included"]
        if price['attributes']['sku'] == product['data']['attributes']['sku']
    ][0]
    
    main_image_url = product['included']['main_images'][0]['link']['href']
    image_response = requests.get(main_image_url)
    if image_response.ok:
        photo=image_response.content
    else:
        photo=open(os.getenv('LOGO_IMAGE'), 'rb')
    
    context.bot.delete_message(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
    )
    context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=photo,
        caption=dedent(
            f"""
            {product['data']['attributes']['name']}
            {product['data']['attributes']['description']}
            {price} RUB
            """
        ),
        reply_markup=make_current_product_inline(product_id=product_id)
    )
    return 'HANDLE_DESCRIPTION'


def increase_quantity(update: Update,
                      context: CallbackContext) -> str:
    _, product_id = update.callback_query.data.split(':')
    current_quantity = int(update.callback_query.message.reply_markup['inline_keyboard'][0][1]['text'])
    context.bot.edit_message_reply_markup(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
        reply_markup=make_current_product_inline(product_id=product_id, quantity=(current_quantity + 1))
    )
    return 'HANDLE_DESCRIPTION'


def reduce_quantity(update: Update,
                    context: CallbackContext) -> str:
    _, product_id = update.callback_query.data.split(':')
    current_quantity = int(update.callback_query.message.reply_markup['inline_keyboard'][0][1]['text'])
    if current_quantity > 1:
        context.bot.edit_message_reply_markup(
            chat_id=update.effective_chat.id,
            message_id=update.callback_query.message.message_id,
            reply_markup=make_current_product_inline(product_id=product_id, quantity=(current_quantity - 1))
        )
    return 'HANDLE_DESCRIPTION'


def add_to_cart(motlin_api: Motlin,
                update: Update,
                context: CallbackContext) -> str:

    _, product_id = update.callback_query.data.split(':')
    quantity = int(update.callback_query.message.reply_markup['inline_keyboard'][0][1]['text'])
    
    try:
        motlin_api.add_product_to_cart(
            user_telegram_id=update.effective_chat.id,
            product_id=product_id,
            quantity=quantity
        )
        context.bot.delete_message(
            chat_id=update.effective_chat.id,
            message_id=update.callback_query.message.message_id,
        )
        context.bot.send_message(
            update.effective_chat.id,
            dedent(
                '''
                Товар успешно добавлен в корзину.
                Добавим что-то еще?
                '''
            )
        )
    except requests.exceptions.HTTPError as error:
        print(json.dumps(error.response.json(), indent=4))
        context.bot.send_message(
            update.effective_chat.id,
            'Sorry, cant add this good to your cart.'
        )
    
    return display_products(motlin_api, update, context)


def show_cart(motlin_api: Motlin, update: Update, context: CallbackContext) -> str:
    user_cart = motlin_api.get_cart(user_telegram_id=update.effective_chat.id)
    context.bot.delete_message(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
    )
    if 'included' not in user_cart or not user_cart['included']['items']:
        context.bot.send_message(
            update.effective_chat.id,
            'Ваша корзина пуста'
        )
        return display_products(motlin_api, update, context)
    
    cart_message = dedent(
        """
        Ваша корзина:
        """
    )
    keyboard_buttons = [[InlineKeyboardButton(
        text='💰 ОФОРМИТЬ ЗАКАЗ 💰',
        callback_data='make_order'
    )]]
    for item in user_cart['included']['items']:
        cart_message += dedent(
            f"""
            {item['name']}
            В корзине: {item['quantity']} шт.
            На сумму: {item['meta']['display_price']['with_tax']['value']['formatted']}
            """
        )
        keyboard_buttons.append([InlineKeyboardButton(
            text=f'Удалить \"{item["name"]}\"',
            callback_data=f'remove_from_cart:{item["id"]}'
        )])

    cart_message += dedent(
        f"""
        Итого к оплате: {user_cart['data']['meta']['display_price']['with_tax']['formatted']}
        """
    )
    keyboard_buttons.append([InlineKeyboardButton(
        text='В меню',
        callback_data='main_menu'
    )])
    
    context.bot.send_message(
        update.effective_chat.id,
        cart_message,
        reply_markup=InlineKeyboardMarkup(keyboard_buttons)
    )
    return 'HANDLE_DESCRIPTION'


def remove_from_cart(motlin_api: Motlin,
                     update: Update,
                     context: CallbackContext) -> str:
    _, item_id = update.callback_query.data.split(':')
    try:
        motlin_api.remove_product_from_cart(
            user_telegram_id=update.effective_chat.id,
            item_id=item_id
        )
        context.bot.send_message(
            update.effective_chat.id,
            'Товар успешно удален из корзины.'
        )
    except requests.exceptions.HTTPError:
        pass
    return show_cart(motlin_api, update, context)


def make_order(update: Update, context: CallbackContext) -> str:

    context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=open('privacy_policy.pdf', 'rb'),
        filename='Политика конфиденциальности.pdf',
        caption=dedent(
            '''
            Для оформления заказа напишите нам свою почту ✉️
            Отправляя почту вы принимаете условия политики конфиденциальности
            '''
        )
    )
    return 'WAITING_EMAIL'


def enter_email(motlin_api: Motlin, update: Update, context: CallbackContext) -> str:
    if not re.search(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", update.message.text):
        context.bot.send_message(
            update.effective_chat.id,
            'Некорректная почта, попробуйте еще раз'
        )
        return 'WAITING_EMAIL'
    try:
        customer = motlin_api.create_customer(
            name=update.effective_chat.first_name or \
                 update.effective_chat.username or \
                 str(update.effective_chat.id),
            email=update.message.text,
            user_telegram_id=update.effective_chat.id
        )
        motlin_api.redis.set(f"{update.effective_chat.id}_customer_id", customer['data']['id'])
    except requests.exceptions.HTTPError as error:
        if error.response.status_code == CUSTOMER_ALREADY_EXISTS_ERROR_CODE:
            pass
    context.bot.send_message(
        update.effective_chat.id,
        dedent(
            '''
            Ваш заказ оформлен!
            Займемся вопросом доставки.
            Введите адрес доставки, ваши координаты или отправьте свою геопозицию.
            '''
        ),
    )
    return 'WAITING_GEO'


def enter_location(motlin_api: Motlin, update: Update, context: CallbackContext) -> str:
    if update.message.location:
        customer_coords = update.message.location.longitude, update.message.location.latitude
    elif re.match(r'[?-]+[0-9]+[.|,]+[0-9]+[ ]+[?-]+[0-9]+[.|,]+[0-9]+', update.message.text):
        input_coordinates = '.'.join(update.message.text.split(','))
        customer_coords = tuple(float(coord) for coord in input_coordinates.split())
    else:
        customer_coords = fetch_coordinates(os.getenv('YANDEX_GEO_API_KEY'), update.message.text)
    
    if None in customer_coords:
        context.bot.send_message(
            update.effective_chat.id,
            dedent(
                '''
                Не могу распознать этот адрес!
                Вы можете просто отправить нам геопозицию.
                '''
            )
        )
        return 'WAITING_GEO'
    motlin_api.update_customer_address(
        customer_id=motlin_api.redis.get(f"{update.effective_chat.id}_customer_id"),
        longitude=customer_coords[0],
        latitude=customer_coords[1]
    )
    motlin_api.redis.set(f'{update.effective_chat.id}_cordinates', ':'.join(map(str, customer_coords)))

    flow_meta = motlin_api.get_flow(flow_id=os.getenv('PIZZERIAS_FLOW_ID'))
    pizzerias = motlin_api.get_entries(flow_slug=flow_meta['data']['slug'])

    for pizzeria in pizzerias:
        pizzeria.update({'distance': geopy_distance.distance(
            customer_coords,
            (pizzeria['longitude'], pizzeria['latitude'])
        ).km})
    pizzerias = sorted(pizzerias, key=lambda item: item['distance'])
    nearest_pizzeria = pizzerias[0]
    motlin_api.redis.set(
        f'{update.effective_chat.id}_nearest_pizzeria_id',
        f'{nearest_pizzeria["id"]}'
    )
    motlin_api.redis.set(
        f'pizerria_{nearest_pizzeria["id"]}_admin_id',
        nearest_pizzeria['admin_tg_id']
    )

    distance = int(nearest_pizzeria['distance'] * 1000)
    if nearest_pizzeria['distance'] <= 0.5:
        context.bot.send_message(
            update.effective_chat.id,
            text=dedent(
                f'''
                Может, заберете пиццу из нашей пиццерии неподалеку?
                Она всего в {distance} метрах от вас!
                Адрес: {nearest_pizzeria['address']}.

                А можем и бесплатно доставить, нам не сложно!
                '''
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(text='Оформить доставку', callback_data='delivery:0'),
                InlineKeyboardButton(text='Заберу сам.', callback_data='pickup')
            ]])
        )
    elif nearest_pizzeria['distance'] <= 5:
        context.bot.send_message(
            update.effective_chat.id,
            text=dedent(
                f'''
                Ваша пицца всего в {distance} метрах от вас!
                Адрес: {nearest_pizzeria['address']}.
                Стоимость доставки - 100 рублей.

                Или можете забрать ваш заказ самостоятельно!
                '''
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(text='Оформить доставку', callback_data='delivery:100'),
                InlineKeyboardButton(text='Заберу сам.', callback_data='pickup')
            ]])
        )
    elif nearest_pizzeria['distance'] <= 20:
        context.bot.send_message(
            update.effective_chat.id,
            text=dedent(
                f'''
                Ваша пицца всего в {distance} метрах от вас!
                Адрес: {nearest_pizzeria['address']}.
                Стоимость доставки - 200 рублей.

                Или можете забрать ваш заказ самостоятельно!
                '''
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(text='Оформить доставку', callback_data='delivery:200'),
                InlineKeyboardButton(text='Заберу сам.', callback_data='pickup')
            ]])
        )
    else:
        context.bot.send_message(
            update.effective_chat.id,
            text=dedent(
                f'''
                Вы можете забрать заказ по адресу:

                {nearest_pizzeria['address']}
                '''
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(text='Вернуться в меню', callback_data='back_to_store')
            ]])
        )
        return pickup(motlin_api, update, context)
    return 'DELIVERY'

def fetch_coordinates(apikey, address):
    base_url = "https://geocode-maps.yandex.ru/1.x"
    response = requests.get(base_url, params={
        "geocode": address,
        "apikey": apikey,
        "format": "json",
    })
    response.raise_for_status()
    found_places = response.json()['response']['GeoObjectCollection']['featureMember']

    if not found_places:
        return None, None

    most_relevant = found_places[0]
    lon, lat = most_relevant['GeoObject']['Point']['pos'].split(" ")
    return lon, lat

def delete_cart(motlin_api: Motlin, update: Update, context: CallbackContext) -> None:
    cart_id = motlin_api.redis.get(f'{update.effective_chat.id}_cart_id')
    motlin_api.delete_cart(cart_id=cart_id)
    motlin_api.redis.delete(f'{update.effective_chat.id}_cart_id')


def pickup(motlin_api: Motlin, update: Update, context: CallbackContext) -> str:
    context.bot.send_message(
        update.effective_chat.id,
        text='Спасибо за заказ! Мы вас ждем!'
    )
    update.callback_query = None
    delete_cart(motlin_api=motlin_api, update=update, context=context)
    return display_products(motlin_api, update, context)

def delivery(motlin_api: Motlin, update: Update, context: CallbackContext) -> str:
    customer_id = motlin_api.redis.get(f"{update.effective_chat.id}_customer_id")
    customer_meta = motlin_api.get_customer(customer_id=customer_id)

    nearest_pizerria_id = motlin_api.redis.get(f'{update.effective_chat.id}_nearest_pizzeria_id')
    admin_tg_id = int(motlin_api.redis.get(f'pizerria_{nearest_pizerria_id}_admin_id'))
    user_cart = motlin_api.get_cart(user_telegram_id=update.effective_chat.id)

    if 'included' not in user_cart or not user_cart['included']['items']:
        # it can be if with any reason delivery button was pressed after cart was expired, for example
        context.bot.send_message(
            update.effective_chat.id,
            'Ваша корзина пуста'
        )
        return display_products(motlin_api, update, context)
    
    cart_message = dedent(
        """
        Новый заказ:
        """
    )
    for item in user_cart['included']['items']:
        cart_message += dedent(
            f"""
            {item['name']} ({item['quantity']} шт.)
            """
        )
    context.bot.send_message(
        chat_id=admin_tg_id,
        text=cart_message
    )
    context.bot.send_location(
        chat_id=admin_tg_id,
        longitude=customer_meta['data']['longitude'],
        latitude=customer_meta['data']['latitude']
    )
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text='Ваш заказ передан в доставку'
    )
    delete_cart(motlin_api=motlin_api, update=update, context=context)
    return display_products(motlin_api, update, context)

def scheduled_message(callback: CallbackContext):
    message_meta = json.loads(callback.job.context)
    inline_components = message_meta.get('inline_reply_markup')
    inline_keyboard = None
    if inline_components and inline_components[0]:
        inline_keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text=button['text'],
                        callback_data=button['callback_data']
                    )
                    for button in line
                ]
                for line in inline_components
            ]
        )
    callback.bot.send_message(
        chat_id=message_meta.get('chat_id'),
        text=message_meta.get('text'),
        reply_markup=inline_keyboard
    )


def launch_timer(job_queue: JobQueue, update: Update, context: CallbackContext):
    print(type(job_queue))
    context.bot.send_message(
        chat_id=update.message.chat_id,
        text='Setting a timer for 1 minute!')
    message_meta = {
        "chat_id": update.effective_chat.id,
        "text": 'Минута прошла',
        "inline_reply_markup": None
    }
    job_queue.run_once(scheduled_message, 5, context=json.dumps(message_meta, ensure_ascii=False))
    return display_products(motlin_api, update, context)


if __name__ == '__main__':
    env = Env()
    env.read_env()

    motlin_api = Motlin(
        env.str('CLIENT_ID'),
        env.str('CLIENT_SECRET')
    )
    
    updater = Updater(token=env.str('TELEGRAM_BOT_TOKEN'), use_context=True)
    job_queue = updater.job_queue
    updater.dispatcher.add_handler(
        ConversationHandler(
            entry_points = [
                CommandHandler('start', partial(launch_timer, job_queue), pass_job_queue=True),
                CommandHandler('start', partial(display_products, motlin_api))
            ],
            states = {
                'HANDLE_MENU': [
                    CallbackQueryHandler(callback=partial(show_product, motlin_api), pattern='product'),
                    CallbackQueryHandler(callback=partial(display_other_products, motlin_api), pattern='other_products'),
                    CallbackQueryHandler(callback=partial(show_cart, motlin_api), pattern='show_cart'),
                ],
                'HANDLE_DESCRIPTION': [
                    CallbackQueryHandler(callback=partial(display_products, motlin_api), pattern='main_menu'),
                    CallbackQueryHandler(callback=increase_quantity, pattern='increase_quantity'),
                    CallbackQueryHandler(callback=reduce_quantity, pattern='reduce_quantity'),
                    CallbackQueryHandler(callback=partial(add_to_cart, motlin_api), pattern='add_to_cart'),
                    CallbackQueryHandler(callback=partial(remove_from_cart, motlin_api), pattern='remove_from_cart'),
                    CallbackQueryHandler(callback=partial(show_cart, motlin_api), pattern='show_cart'),
                    CallbackQueryHandler(callback=make_order, pattern='make_order')
                ],
                'WAITING_EMAIL': [
                    MessageHandler(filters=Filters.text, callback=partial(enter_email, motlin_api))
                ],
                'WAITING_GEO': [
                    MessageHandler(filters=Filters.all, callback=partial(enter_location, motlin_api)),
                    CallbackQueryHandler(callback=partial(pickup, motlin_api), pattern='pickup'),
                ],
                'DELIVERY': [
                    CallbackQueryHandler(callback=partial(display_products, motlin_api), pattern='back_to_store'),
                    CallbackQueryHandler(callback=partial(pickup, motlin_api), pattern='pickup'),
                    CallbackQueryHandler(callback=partial(delivery, motlin_api), pattern='delivery'),
                ]
            },
            fallbacks=[
            ]
        )
    )
    
    updater.start_polling()
    updater.idle()

