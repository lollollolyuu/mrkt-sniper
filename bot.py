import asyncio
import re
from decimal import Decimal

import aiohttp
import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    BotCommand,
    BotCommandScopeChat,
)
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PRICE_FILE = Path(__file__).with_name("subscription_prices.json")

DEFAULT_PRICES = {
    "1 час": 0,
    "3 часа": 0,
    "6 часов": 0,
    "1 день": 0,
    "1 месяц": 0,
}
PLAN_SECONDS = dict(zip(DEFAULT_PRICES, (3600, 10800, 21600, 86400, 2592000), strict=True))
PAYMENT_DB = Path(__file__).with_name("subscriptions.sqlite3")
STARS = "XTR"


def load_prices():
    if PRICE_FILE.exists():
        try:
            saved = json.loads(PRICE_FILE.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                prices = DEFAULT_PRICES.copy()
                for name in prices:
                    value = saved.get(name)
                    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                        prices[name] = value
                return prices
        except (OSError, json.JSONDecodeError):
            pass
    return DEFAULT_PRICES.copy()


SUBSCRIPTIONS = load_prices()


def save_prices():
    PRICE_FILE.write_text(
        json.dumps(SUBSCRIPTIONS, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class PriceState(StatesGroup):
    waiting = State()


dp = Dispatcher(storage=MemoryStorage())


def init_payments():
    with closing(sqlite3.connect(PAYMENT_DB)) as db, db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                duration INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                checkout_id TEXT,
                charge_id TEXT UNIQUE,
                active_until INTEGER
            );
            CREATE INDEX IF NOT EXISTS orders_user_expiry
                ON orders(user_id, active_until);
            CREATE TABLE IF NOT EXISTS mrkt_accounts (
                telegram_user_id INTEGER PRIMARY KEY,
                mrkt_token TEXT NOT NULL,
                connected_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
        """)


def save_mrkt_account(user_id, token):
    """Create or update the MRKT connection for one Telegram user."""
    now = int(time.time())
    with closing(sqlite3.connect(PAYMENT_DB)) as db, db:
        db.execute(
            """INSERT INTO mrkt_accounts
               (telegram_user_id, mrkt_token, connected_at, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(telegram_user_id) DO UPDATE SET
                 mrkt_token = excluded.mrkt_token,
                 updated_at = excluded.updated_at""",
            (user_id, token, now, now),
        )


def get_mrkt_token(user_id):
    with closing(sqlite3.connect(PAYMENT_DB)) as db:
        row = db.execute(
            "SELECT mrkt_token FROM mrkt_accounts WHERE telegram_user_id = ?",
            (user_id,),
        ).fetchone()
    return row[0] if row else None


def subscription_expiry(db, user_id):
    return db.execute(
        "SELECT COALESCE(MAX(active_until), 0) FROM orders WHERE user_id = ?",
        (user_id,),
    ).fetchone()[0]


def format_expiry(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


MRKT_LISTINGS_URL = "https://api.tgmrkt.io/api/v1/gifts/saling"
market_lock = asyncio.Lock()
market_reply = ""
market_refresh_at = 0.0


def format_lots(payload):
    gifts = payload.get("gifts") if isinstance(payload, dict) else None
    if not isinstance(gifts, list):
        raise ValueError("Unexpected MRKT response")
    if not gifts:
        return "Сейчас нет выставленных подарков."
    lines = []
    for gift in gifts[:5]:
        if not isinstance(gift, dict):
            continue
        name = gift.get("collectionName") or gift.get("title") or gift.get("name")
        amount = gift.get("salePrice")
        if not isinstance(name, str) or type(amount) is not int or amount < 0:
            continue
        number = gift.get("number")
        label = f"{name[:100]} #{number}" if type(number) is int else name[:100]
        price = format(Decimal(amount).scaleb(-9), "f").rstrip("0").rstrip(".") or "0"
        gift_id = gift.get("id")
        if not isinstance(gift_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", gift_id):
            continue
        link = f"https://t.me/mrkt/app?startapp={gift_id}"
        lines.append(f"{label} — {price} TON\n{link}")
    if not lines:
        raise ValueError("Unexpected MRKT gift fields")
    return "Лоты MRKT по возрастанию цены:" + chr(10) + chr(10).join(lines)


async def market_listings():
    global market_reply, market_refresh_at
    async with market_lock:
        if time.monotonic() < market_refresh_at:
            return market_reply
        load_dotenv(Path(__file__).with_name(".env"), override=True)
        token = (os.getenv("MRKT_TOKEN") or os.getenv("MRKT_ACCESS_TOKEN") or "").strip()
        if not token:
            return "Доступ к MRKT ещё не настроен: администратору нужно добавить MRKT_TOKEN в .env."
        headers = {"Authorization": token, "Referer": "https://cdn.tgmrkt.io/"}
        filters = {"count": 5, "cursor": "", "ordering": "Price", "lowToHigh": True,
                   "collectionNames": [], "modelNames": [], "backdropNames": [], "symbolNames": []}
        ttl = 30
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(MRKT_LISTINGS_URL, json=filters, headers=headers,
                                        allow_redirects=False) as response:
                    if response.status == 429:
                        delay = response.headers.get("Retry-After", "")
                        ttl = max(60, int(delay)) if delay.isascii() and delay.isdigit() and len(delay) < 9 else 60
                        reply = "MRKT временно ограничил запросы. Попробуй позже."
                    elif response.status in (401, 403):
                        reply = "MRKT отклонил доступ к лотам. Администратору нужно проверить доступ к API."
                    elif response.status != 200:
                        reply = "Сервис MRKT временно недоступен."
                    else:
                        reply = format_lots(await response.json())
        except (aiohttp.ClientError, TimeoutError, ValueError):
            reply = "Не удалось получить список лотов MRKT. Попробуй позже."
        market_reply = reply
        market_refresh_at = time.monotonic() + ttl
        return reply


@dp.message(Command("lots"), F.chat.type == "private")
async def show_market_listings(message: Message):
    await message.answer(await market_listings())


@dp.message(Command("subscribe"), F.chat.type == "private")
async def subscribe(message: Message):
    rows = [
        [InlineKeyboardButton(text=f"{name} — {price} ⭐", callback_data=f"buy:{name}")]
        for name, price in SUBSCRIPTIONS.items()
        if price > 0
    ]
    if not rows:
        await message.answer("Оплата пока недоступна. Попробуй позже.")
        return
    await message.answer(
        "Выбери срок подписки. Оплата разовая.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@dp.callback_query(F.data.startswith("buy:"))
async def buy_subscription(callback: CallbackQuery, bot: Bot):
    if not isinstance(callback.message, Message) or callback.message.chat.type != "private":
        await callback.answer("Открой личный чат с ботом.", show_alert=True)
        return

    name = callback.data.split(":", 1)[1]
    price = SUBSCRIPTIONS.get(name, 0)
    if price <= 0:
        await callback.answer("Тариф сейчас недоступен.", show_alert=True)
        return

    user_id = callback.from_user.id
    order_id = uuid4().hex
    with closing(sqlite3.connect(PAYMENT_DB)) as db, db:
        db.execute(
            """INSERT INTO orders (id, user_id, amount, duration, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            (order_id, user_id, price, PLAN_SECONDS[name], int(time.time()) + 1800),
        )
    await callback.answer()
    duration = "30 дней" if name == "1 месяц" else name
    await bot.send_invoice(
        chat_id=user_id,
        title=f"Подписка — {duration}",
        description=f"Срок: {duration}. Разовый платёж без автопродления.",
        payload=order_id,
        currency=STARS,
        prices=[LabeledPrice(label=name, amount=price)],
        provider_token="",
        start_parameter="subscribe",
    )


@dp.pre_checkout_query()
async def check_subscription_payment(query: PreCheckoutQuery):
    accepted = False
    if query.currency == STARS:
        with closing(sqlite3.connect(PAYMENT_DB)) as db, db:
            accepted = db.execute(
                """UPDATE orders SET checkout_id = ?
                   WHERE id = ? AND user_id = ? AND amount = ?
                   AND expires_at >= ? AND charge_id IS NULL
                   AND (checkout_id IS NULL OR checkout_id = ?)""",
                (query.id, query.invoice_payload, query.from_user.id,
                 query.total_amount, int(time.time()), query.id),
            ).rowcount == 1
    if accepted:
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Открой /subscribe и создай новый счёт.")


@dp.message(F.successful_payment)
async def complete_subscription_payment(message: Message):
    payment = message.successful_payment
    if not message.from_user or payment.currency != STARS:
        return

    user_id = message.from_user.id
    with closing(sqlite3.connect(PAYMENT_DB)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        order = db.execute(
            """SELECT duration, charge_id FROM orders
               WHERE id = ? AND user_id = ? AND amount = ? AND checkout_id IS NOT NULL""",
            (payment.invoice_payload, user_id, payment.total_amount),
        ).fetchone()
        if order and order[1] == payment.telegram_payment_charge_id:
            return
        if order is None or order[1] is not None or db.execute(
            "SELECT 1 FROM orders WHERE charge_id = ?",
            (payment.telegram_payment_charge_id,),
        ).fetchone():
            expiry = None
        else:
            current_expiry = subscription_expiry(db, user_id)
            expiry = max(int(message.date.timestamp()), current_expiry) + order[0]
            db.execute(
                "UPDATE orders SET charge_id = ?, active_until = ? WHERE id = ?",
                (payment.telegram_payment_charge_id, expiry, payment.invoice_payload),
            )

    if expiry is None:
        await message.answer("Для сверки платежа напиши в /paysupport.")
        return
    await message.answer(f"Оплата получена. Подписка активна до {format_expiry(expiry)}.")


@dp.message(Command("subscription"), F.chat.type == "private")
async def subscription_status(message: Message):
    with closing(sqlite3.connect(PAYMENT_DB)) as db:
        expiry = subscription_expiry(db, message.from_user.id)
    if expiry > time.time():
        await message.answer(f"Подписка активна до {format_expiry(expiry)}.")
    else:
        await message.answer("Активной подписки нет. Выбрать тариф можно в /subscribe.")


@dp.message(Command("paysupport"), F.chat.type == "private")
async def payment_support(message: Message, command: CommandObject, bot: Bot):
    if not command.args:
        await message.answer("Напиши /paysupport, затем опиши проблему с оплатой.")
        return
    user_id = message.from_user.id
    await bot.send_message(
        ADMIN_ID,
        f"Вопрос об оплате от {user_id}:\n{command.args[:3000]}\n\n"
        f"Ответить: /payreply {user_id} текст ответа",
    )
    await message.answer("Вопрос отправлен администратору.")


@dp.message(Command("payreply"), F.chat.type == "private")
async def reply_to_payment_support(message: Message, command: CommandObject, bot: Bot):
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    user_id, _, reply = (command.args or "").partition(" ")
    if not user_id.isascii() or not user_id.isdecimal() or len(user_id) > 16 or not reply.strip():
        await message.answer("Формат: /payreply ID текст ответа")
        return
    await bot.send_message(int(user_id), f"Поддержка оплаты: {reply}")
    await message.answer("Ответ отправлен.")


def admin_keyboard():
    rows = []
    for name, price in SUBSCRIPTIONS.items():
        rows.append([
            InlineKeyboardButton(
                text=f"{name} — {price} ⭐",
                callback_data=f"price:{name}",
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_menu_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="⚙️ Управление подписками",
                callback_data="admin",
            )
        ]]
    )


@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("MRKT Sniper запущен! Посмотреть лоты маркетплейса: /lots")
    if message.chat.type == "private":
        await subscribe(message)
    if message.from_user and message.from_user.id == ADMIN_ID and ADMIN_ID != 0:
        await message.answer(
            "Тебе доступна админ-панель.",
            reply_markup=admin_menu_keyboard(),
        )


@dp.message(Command("admin"))
async def admin(message: Message):
    if not message.from_user or message.from_user.id != ADMIN_ID or ADMIN_ID == 0:
        await message.answer("У тебя нет доступа.")
        return
    await message.answer("Выбери тариф, цену которого хочешь изменить:", reply_markup=admin_keyboard())


@dp.callback_query(F.data == "admin")
async def open_admin(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID or ADMIN_ID == 0:
        await callback.answer("Нет доступа", show_alert=True)
        return
    if callback.message:
        await callback.message.answer(
            "Выбери тариф, цену которого хочешь изменить:",
            reply_markup=admin_keyboard(),
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("price:"))
async def choose_price(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID or ADMIN_ID == 0:
        await callback.answer("Нет доступа", show_alert=True)
        return

    name = callback.data.split(":", 1)[1]
    if name not in SUBSCRIPTIONS:
        await callback.answer("Неизвестный тариф", show_alert=True)
        return

    await state.update_data(subscription_name=name)
    await state.set_state(PriceState.waiting)
    if callback.message:
        await callback.message.answer(
            f"Введи новую цену для тарифа «{name}» в Telegram Stars. "
            "Отправь целое число больше нуля."
        )
    await callback.answer()


@dp.message(PriceState.waiting)
async def set_price(message: Message, state: FSMContext):
    if not message.from_user or message.from_user.id != ADMIN_ID or ADMIN_ID == 0:
        await state.clear()
        return

    text = (message.text or "").strip()
    try:
        price = int(text)
        if price < 1:
            raise ValueError
    except ValueError:
        await message.answer("Нужно отправить целое число больше нуля. Например: 50")
        return

    data = await state.get_data()
    name = data.get("subscription_name")
    if name not in SUBSCRIPTIONS:
        await state.clear()
        await message.answer("Не удалось определить тариф. Открой /admin и попробуй снова.")
        return

    SUBSCRIPTIONS[name] = price
    try:
        save_prices()
    except OSError:
        await message.answer("Не удалось сохранить цену. Проверь доступ к папке проекта.")
        return

    await state.clear()
    await message.answer(
        f"Цена тарифа «{name}» сохранена: {price} ⭐",
        reply_markup=admin_keyboard(),
    )


async def main():
    if not TOKEN:
        print("Нет BOT_TOKEN в файле .env")
        return
    if ADMIN_ID == 0:
        print("Нет ADMIN_ID в файле .env. Добавь строку ADMIN_ID=твой_числовой_ID")
        return

    init_payments()
    bot = Bot(token=TOKEN)
    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть бота"),
        BotCommand(command="lots", description="Показать лоты MRKT"),
        BotCommand(command="subscribe", description="Выбрать подписку"),
        BotCommand(command="subscription", description="Статус подписки"),
        BotCommand(command="paysupport", description="Помощь с оплатой"),
    ])
    if ADMIN_ID:
        await bot.set_my_commands([
            BotCommand(command="start", description="Открыть бота"),
            BotCommand(command="lots", description="Показать лоты MRKT"),
            BotCommand(command="subscribe", description="Выбрать подписку"),
            BotCommand(command="subscription", description="Статус подписки"),
            BotCommand(command="admin", description="Управление тарифами"),
            BotCommand(command="paysupport", description="Помощь с оплатой"),
        ], scope=BotCommandScopeChat(chat_id=ADMIN_ID))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
