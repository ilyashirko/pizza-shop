import json
import os
import re

from datetime import datetime
from functools import partial
from urllib import request
from environs import Env
from textwrap import dedent

from more_itertools import chunked

import requests

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
    ConversationHandler, Filters
)

from motlin import Motlin

PRODUCTS_PER_MESSAGE = 10

CUSTOMER_ALREADY_EXISTS_ERROR_CODE = 409


def make_inline_products(motlin_api: Motlin,
                         items_in_row: int = 2,
                         left_border: int = 0,
                         right_border: int = PRODUCTS_PER_MESSAGE):
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


def make_prod_inline(product_id: str, quantity: int = 1):
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
        reply_markup=make_inline_products(
            motlin_api=motlin_api,
            left_border=0,
            right_border=PRODUCTS_PER_MESSAGE
        )
    )
    if update.callback_query:
        context.bot.delete_message(
            chat_id=update.effective_chat.id,
            message_id=update.callback_query.message.message_id,
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
        reply_markup=make_inline_products(
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
    with open('pricebook.json', 'w') as pb:
        json.dump(pricebook, pb, indent=4, ensure_ascii=True)
    main_image_url = product['included']['main_images'][0]['link']['href']
    print(json.dumps(pricebook, indent=4))
    price = [
        price['attributes']['currencies']['RUB']['amount']
        for price in pricebook["included"]
        if price['attributes']['sku'] == product['data']['attributes']['sku']
    ][0]
    image_response = requests.get(main_image_url)
    if image_response.ok:
        photo=image_response.content
    else:
        photo=open(os.getenv('LOGO_IMAGE'), 'rb')
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
        reply_markup=make_prod_inline(product_id=product_id)
    )
    context.bot.delete_message(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
    )
    return 'HANDLE_DESCRIPTION'


def increase_quantity(update: Update,
                      context: CallbackContext) -> str:
    _, product_id = update.callback_query.data.split(':')
    current_quantity = int(update.callback_query.message.reply_markup['inline_keyboard'][0][1]['text'])
    context.bot.edit_message_reply_markup(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
        reply_markup=make_prod_inline(product_id=product_id, quantity=(current_quantity + 1))
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
            reply_markup=make_prod_inline(product_id=product_id, quantity=(current_quantity - 1))
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
    #print(json.dumps(user_cart, indent=4))
    if not user_cart['included']['items']:
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
    context.bot.delete_message(
        chat_id=update.effective_chat.id,
        message_id=update.callback_query.message.message_id,
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
            email=update.message.text
        )
    except requests.exceptions.HTTPError as error:
        if error.response.status_code == CUSTOMER_ALREADY_EXISTS_ERROR_CODE:
            pass
    context.bot.send_message(
        update.effective_chat.id,
        'Ваш заказ оформлен!'
    )
    motlin_api.redis.delete(f'{update.effective_chat.id}_cart_id')
    motlin_api.redis.delete(f'{update.effective_chat.id}_cart_expires')
    return display_products(motlin_api, update, context)


if __name__ == '__main__':
    env = Env()
    env.read_env()

    motlin_api = Motlin(
        env.str('CLIENT_ID'),
        env.str('CLIENT_SECRET')
    )
    
    updater = Updater(token=env.str('TELEGRAM_BOT_TOKEN'), use_context=True)

    updater.dispatcher.add_handler(
        ConversationHandler(
            entry_points = [
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
                ]
            },
            fallbacks=[
            ]
        )
    )
    
    updater.start_polling()
    updater.idle()

