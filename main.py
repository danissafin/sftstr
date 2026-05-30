import asyncio
import base64
import csv
import io
import logging
import os
import re
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
import asyncpg
import cv2
import numpy as np
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TOKEN")
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "5"))
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()}

CIDMS_API_URL = "https://pidkey.com/ajax/cidms_api"
CIDMS_IMAGE_API_URL = "https://pidkey.com/ajax/cidms_via_image_base64_string_api"
PIDMS_API_URL = "https://pidkey.com/ajax/pidms_api"

CHECK_KEYS_ADMINS_ONLY = True

FLOOD_LIMIT = 5
FLOOD_SECONDS = 10
user_messages = defaultdict(deque)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not TOKEN:
    raise RuntimeError("Не задан TOKEN")
if not API_KEY:
    raise RuntimeError("Не задан API_KEY")
if not DATABASE_URL:
    raise RuntimeError("Не задан DATABASE_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

db_pool: Optional[asyncpg.Pool] = None


def is_admin(user_id: int) -> bool:
    return user_id in ADMINS


def check_flood(user_id: int) -> bool:
    if is_admin(user_id):
        return True

    now = time.time()
    messages = user_messages[user_id]

    while messages and now - messages[0] > FLOOD_SECONDS:
        messages.popleft()

    if len(messages) >= FLOOD_LIMIT:
        return False

    messages.append(now)
    return True


def extract_product_keys(text: str) -> list[str]:
    pattern = r"[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}"
    return re.findall(pattern, text.upper())


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
                InlineKeyboardButton(text="💾 Бэкап БД", callback_data="admin_backup"),
            ],
        ]
    )


async def init_db():
    global db_pool

    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_limits (
                user_id BIGINT PRIMARY KEY,
                limit_value INTEGER NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_activations (
                user_id BIGINT NOT NULL,
                confirmation_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, confirmation_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS activation_logs (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                iid TEXT,
                confirmation_id TEXT NOT NULL,
                source TEXT DEFAULT 'unknown',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, confirmation_id)
            )
        """)

        await conn.execute("""
            INSERT INTO activation_logs (user_id, confirmation_id, source)
            SELECT user_id, confirmation_id, 'legacy'
            FROM user_activations
            ON CONFLICT DO NOTHING
        """)


async def get_user_limit(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT limit_value FROM user_limits WHERE user_id = $1",
            user_id,
        )
        return row["limit_value"] if row else DEFAULT_LIMIT


async def set_user_limit(user_id: int, limit_value: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_limits (user_id, limit_value)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET limit_value = EXCLUDED.limit_value
            """,
            user_id,
            limit_value,
        )


async def count_activations(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS count FROM activation_logs WHERE user_id = $1",
            user_id,
        )
        return row["count"] if row else 0


async def add_activation(user_id: int, confirmation_id: str, iid: str | None = None, source: str = "unknown"):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_activations (user_id, confirmation_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            """,
            user_id,
            confirmation_id,
        )

        await conn.execute(
            """
            INSERT INTO activation_logs (user_id, iid, confirmation_id, source)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, confirmation_id) DO NOTHING
            """,
            user_id,
            iid,
            confirmation_id,
            source,
        )


async def check_limit(user_id: int) -> bool:
    limit = await get_user_limit(user_id)
    activations = await count_activations(user_id)
    return activations < limit


async def get_stats() -> dict:
    async with db_pool.acquire() as conn:
        users = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM activation_logs")
        activations = await conn.fetchval("SELECT COUNT(*) FROM activation_logs")
        users_with_limits = await conn.fetchval("SELECT COUNT(*) FROM user_limits")

    return {
        "users": users or 0,
        "activations": activations or 0,
        "users_with_limits": users_with_limits or 0,
    }


async def get_user_info(user_id: int) -> dict:
    limit = await get_user_limit(user_id)
    used = await count_activations(user_id)

    return {
        "user_id": user_id,
        "limit": limit,
        "used": used,
        "left": max(limit - used, 0),
    }


async def get_user_history(user_id: int, limit: int = 10):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT iid, confirmation_id, source, created_at
            FROM activation_logs
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            user_id,
            limit,
        )

    return rows


async def request_cid_by_iid(iid: str) -> dict:
    params = {
        "iids": iid,
        "justforcheck": 0,
        "apikey": API_KEY,
    }

    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(CIDMS_API_URL, params=params) as response:
            if response.status != 200:
                raise RuntimeError(f"CIDMS status: {response.status}")
            return await response.json(content_type=None)


def improve_image_with_opencv(image_bytes: bytes) -> bytes:
    arr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)

    if img is None:
        return image_bytes

    scale = 2
    img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    gray = cv2.bilateralFilter(gray, 9, 75, 75)

    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    gray = clahe.apply(gray)

    _, encoded = cv2.imencode(".png", gray)
    return encoded.tobytes()


async def request_cid_via_image(image_bytes: bytes) -> dict:
    improved_bytes = improve_image_with_opencv(image_bytes)
    image_base64 = base64.b64encode(improved_bytes).decode("utf-8")

    payload = {
        "apikey": API_KEY,
        "imagebase64string": image_base64,
    }

    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(CIDMS_IMAGE_API_URL, json=payload) as response:
            if response.status != 200:
                raise RuntimeError(f"CIDMS image status: {response.status}")
            return await response.json(content_type=None)


async def check_product_keys(keys: list[str]) -> dict:
    keys_text = "\r\n".join(keys)

    params = {
        "keys": keys_text,
        "justgetdescription": 0,
        "apikey": API_KEY,
    }

    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(PIDMS_API_URL, params=params) as response:
            if response.status != 200:
                raise RuntimeError(f"PIDMS status: {response.status}")
            return await response.json(content_type=None)


def format_pidms_response(data) -> str:
    def get_value(item: dict, *names, default=""):
        for name in names:
            if name in item and item[name] not in (None, ""):
                return item[name]
        return default

    if isinstance(data, dict):
        for key in ("result", "data", "keys", "products"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
        else:
            data = [data]

    if not isinstance(data, list):
        return str(data)

    result_blocks = []

    for item in data:
        if not isinstance(item, dict):
            result_blocks.append(str(item))
            continue

        key = get_value(item, "key", "pid", "product_key", default="Не найден")
        description = get_value(item, "description", "Description", "desc", default="Не найдено")
        subtype = get_value(item, "subtype", "sub_type", "SubType", "sku", default="Не найдено")
        error_code = get_value(item, "errorcode", "error_code", "ErrorCode", "error", default="Не найдено")
        time_value = get_value(item, "time", "Time", "date", "checked_time", default="Не найдено")

        block = (
            f"Key: {key}\n"
            f"Description: {description}\n"
            f"Sub type: {subtype}\n"
            f"Error code: {error_code}\n"
            f"Time: {time_value}"
        )

        result_blocks.append(block)

    return "\n\n".join(result_blocks)


async def process_activation(message: Message, iid: str, source: str):
    user_id = message.from_user.id

    if not await check_limit(user_id):
        await message.answer("Вы достигли лимита успешных активаций. Свяжитесь с нами для увеличения лимита.")
        return

    await message.answer("Обрабатываю запрос...")

    try:
        data = await request_cid_by_iid(iid)
        logger.info(f"CIDMS response: {data}")
    except Exception:
        logger.exception("Ошибка обращения к CIDMS API")
        await message.answer("Ошибка при обращении к API. Попробуйте позже.")
        return

    short_result = data.get("short_result")

    if short_result == "IID is not correct!!":
        await message.answer("Проверьте код установки на корректность ввода, я подозреваю, что Вы где-то ошиблись")
        return

    if short_result == "Key blocked!":
        await message.answer(
            "К сожалению, мы не смогли проверить Ваш код установки в автоматическом режиме. "
            "Пожалуйста, свяжитесь с нами и мы обязательно Вам поможем."
        )
        return

    confirmation_id = data.get("confirmationid") or data.get("confirmation_id_with_dash")

    if not confirmation_id:
        await message.answer("CID не найден. Попробуйте позже или свяжитесь с нами.")
        return

    await add_activation(user_id, confirmation_id, iid=iid, source=source)
    await message.answer(f"Ваш Confirmation ID:\n{confirmation_id}")


async def process_activation_image(message: Message, image_bytes: bytes):
    user_id = message.from_user.id

    if not await check_limit(user_id):
        await message.answer("Вы достигли лимита успешных активаций. Свяжитесь с нами для увеличения лимита.")
        return

    await message.answer("Обрабатываю фото через PIDKey...")

    try:
        data = await request_cid_via_image(image_bytes)
        logger.info(f"CIDMS image response: {data}")
    except Exception:
        logger.exception("Ошибка обращения к CIDMS image API")
        await message.answer("Ошибка при обработке изображения через API. Попробуйте позже.")
        return

    short_result = data.get("short_result")

    if short_result == "IID is not correct!!":
        await message.answer(
            "Я где-то некорректно прочитал код установки, пожалуйста, предоставьте скриншот четче "
            "или введите код установки вручную"
        )
        return

    if short_result == "Key blocked!":
        await message.answer(
            "К сожалению, мы не смогли проверить Ваш код установки в автоматическом режиме. "
            "Пожалуйста, свяжитесь с нами и мы обязательно Вам поможем."
        )
        return

    confirmation_id = data.get("confirmationid") or data.get("confirmation_id_with_dash")
    iid_detected = data.get("iid_detected")

    if not confirmation_id:
        await message.answer("Не удалось получить Confirmation ID по фото. Попробуйте отправить скриншот чётче.")
        return

    await add_activation(user_id, confirmation_id, iid=iid_detected, source="photo")

    if iid_detected:
        await message.answer(
            f"Распознанный IID:\n{iid_detected}\n\n"
            f"Ваш Confirmation ID:\n{confirmation_id}"
        )
    else:
        await message.answer(f"Ваш Confirmation ID:\n{confirmation_id}")


async def create_backup_file() -> BufferedInputFile:
    async with db_pool.acquire() as conn:
        limits = await conn.fetch("SELECT * FROM user_limits ORDER BY user_id")
        activations = await conn.fetch("SELECT * FROM activation_logs ORDER BY created_at DESC")

    output = io.StringIO()

    output.write("user_limits\n")
    writer = csv.writer(output)
    writer.writerow(["user_id", "limit_value"])
    for row in limits:
        writer.writerow([row["user_id"], row["limit_value"]])

    output.write("\nactivation_logs\n")
    writer.writerow(["id", "user_id", "iid", "confirmation_id", "source", "created_at"])
    for row in activations:
        writer.writerow([
            row["id"],
            row["user_id"],
            row["iid"],
            row["confirmation_id"],
            row["source"],
            row["created_at"],
        ])

    data = output.getvalue().encode("utf-8-sig")
    filename = f"backup_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"

    return BufferedInputFile(data, filename=filename)


async def send_backup_to_admins():
    file = await create_backup_file()

    for admin_id in ADMINS:
        try:
            await bot.send_document(admin_id, file, caption="Ежедневная резервная копия БД")
        except Exception:
            logger.exception(f"Не удалось отправить бэкап админу {admin_id}")


async def daily_backup_task():
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)

        if next_run <= now:
            next_run += timedelta(days=1)

        await asyncio.sleep((next_run - now).total_seconds())

        try:
            await send_backup_to_admins()
        except Exception:
            logger.exception("Ошибка ежедневного бэкапа")


@router.message(F.text == "/start")
async def start_command(message: Message):
    await message.answer(
        "Привет! Отправь мне код IID, и я проверю его через CIDMS API. "
        "Код должен содержать 63 или 48 цифр."
    )


@router.message(F.text == "/help")
async def help_command(message: Message):
    await message.answer(
        "Можно отправить:\n"
        "1. Код установки IID — 63 или 48 цифр.\n"
        "2. Фото окна активации.\n"
        "3. Ключи Windows/Office для проверки, если вы администратор.\n\n"
        "Админ-команды:\n"
        "/admin — админ-панель\n"
        "/stats — статистика\n"
        "/user USER_ID — информация о пользователе\n"
        "/history USER_ID — история активаций\n"
        "/backup — резервная копия БД\n"
        "/setlimit USER_ID LIMIT — изменить лимит"
    )


@router.message(F.text == "/admin")
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав администратора.")
        return

    await message.answer("Админ-панель:", reply_markup=admin_keyboard())


@router.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    stats = await get_stats()

    await callback.message.answer(
        f"📊 Статистика:\n\n"
        f"Количество пользователей: {stats['users']}\n"
        f"Количество активаций: {stats['activations']}\n"
        f"Пользователей с индивидуальными лимитами: {stats['users_with_limits']}"
    )

    await callback.answer()


@router.callback_query(F.data == "admin_backup")
async def admin_backup_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    file = await create_backup_file()
    await callback.message.answer_document(file, caption="Резервная копия БД")
    await callback.answer()


@router.message(F.text == "/stats")
async def stats_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав администратора.")
        return

    stats = await get_stats()

    await message.answer(
        f"📊 Статистика:\n\n"
        f"Количество пользователей: {stats['users']}\n"
        f"Количество активаций: {stats['activations']}\n"
        f"Пользователей с индивидуальными лимитами: {stats['users_with_limits']}"
    )


@router.message(F.text.startswith("/user"))
async def user_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав администратора.")
        return

    parts = message.text.split()

    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /user USER_ID")
        return

    user_id = int(parts[1])
    info = await get_user_info(user_id)

    await message.answer(
        f"👤 Пользователь: {info['user_id']}\n"
        f"Лимит: {info['limit']}\n"
        f"Использовано: {info['used']}\n"
        f"Осталось: {info['left']}"
    )


@router.message(F.text.startswith("/history"))
async def history_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав администратора.")
        return

    parts = message.text.split()

    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /history USER_ID")
        return

    user_id = int(parts[1])
    rows = await get_user_history(user_id, limit=10)

    if not rows:
        await message.answer("История активаций пуста.")
        return

    blocks = []

    for row in rows:
        blocks.append(
            f"Дата: {row['created_at']}\n"
            f"Источник: {row['source']}\n"
            f"IID: {row['iid'] or 'не сохранён'}\n"
            f"CID: {row['confirmation_id']}"
        )

    text = "\n\n".join(blocks)

    if len(text) > 3900:
        text = text[:3900] + "\n\nИстория обрезана."

    await message.answer(text)


@router.message(F.text == "/backup")
async def backup_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав администратора.")
        return

    file = await create_backup_file()
    await message.answer_document(file, caption="Резервная копия БД")


@router.message(F.text.startswith("/setlimit"))
async def set_limit_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для изменения лимитов.")
        return

    parts = message.text.split()

    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Использование: /setlimit USER_ID LIMIT")
        return

    target_user_id = int(parts[1])
    new_limit = int(parts[2])

    await set_user_limit(target_user_id, new_limit)
    await message.answer(f"Лимит для пользователя {target_user_id} изменён на {new_limit} активаций.")


@router.message(F.photo)
async def process_photo(message: Message):
    if not check_flood(message.from_user.id):
        await message.answer("Слишком много запросов. Подождите немного и попробуйте снова.")
        return

    photo = message.photo[-1]

    try:
        file_info = await bot.get_file(photo.file_id)
        content = await bot.download_file(file_info.file_path)

        image_bytes = content.getvalue() if isinstance(content, io.BytesIO) else content

    except Exception:
        logger.exception("Ошибка скачивания изображения")
        await message.answer("Не удалось скачать изображение. Попробуйте отправить фото ещё раз.")
        return

    await process_activation_image(message, image_bytes)


@router.message(F.text)
async def process_text(message: Message):
    if not check_flood(message.from_user.id):
        await message.answer("Слишком много запросов. Подождите немного и попробуйте снова.")
        return

    text = message.text.strip()
    product_keys = extract_product_keys(text)

    if product_keys:
        if CHECK_KEYS_ADMINS_ONLY and not is_admin(message.from_user.id):
            await message.answer("Проверка ключей доступна только администратору.")
            return

        await message.answer(f"Проверяю ключи: {len(product_keys)} шт...")

        try:
            data = await check_product_keys(product_keys)
            logger.info(f"PIDMS response: {data}")
        except Exception:
            logger.exception("Ошибка обращения к PIDMS API")
            await message.answer("Ошибка при проверке ключей через PIDKey. Попробуйте позже.")
            return

        result_text = format_pidms_response(data)

        if len(result_text) > 3900:
            result_text = result_text[:3900] + "\n\nОтвет слишком длинный, часть результата обрезана."

        await message.answer(result_text)
        return

    iid = re.sub(r"\D", "", text)

    if len(iid) not in (63, 48):
        await message.answer("Ошибка: код должен содержать 63 или 48 цифр. Проверьте ввод и попробуйте снова.")
        return

    await process_activation(message, iid, source="text")


async def main():
    await init_db()
    asyncio.create_task(daily_backup_task())

    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
