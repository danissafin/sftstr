import asyncio
import base64
import io
import logging
import os
import re
from typing import Optional

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TOKEN")
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

ADMINS = {
    int(x)
    for x in os.getenv("ADMINS", "").split(",")
    if x.strip().isdigit()
}

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "5"))

CIDMS_API_URL = "https://pidkey.com/ajax/cidms_api"
CIDMS_IMAGE_API_URL = "https://pidkey.com/ajax/cidms_via_image_base64_string_api"
PIDMS_API_URL = "https://pidkey.com/ajax/pidms_api"

CHECK_KEYS_ADMINS_ONLY = True

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


def extract_product_keys(text: str) -> list[str]:
    pattern = r"[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}"
    return re.findall(pattern, text.upper())


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
            "SELECT COUNT(*) AS count FROM user_activations WHERE user_id = $1",
            user_id,
        )
        return row["count"] if row else 0


async def add_activation(user_id: int, confirmation_id: str):
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


async def check_limit(user_id: int) -> bool:
    limit = await get_user_limit(user_id)
    activations = await count_activations(user_id)
    return activations < limit


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


async def request_cid_via_image(image_bytes: bytes) -> dict:
    image_base64 = base64.b64encode(image_bytes).decode("utf-8")

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
    if isinstance(data, list):
        lines = []

        for item in data:
            key = item.get("key") or item.get("pid") or "Ключ"
            description = item.get("description") or item.get("desc") or "Описание не найдено"
            error = item.get("error") or item.get("errorcode") or ""
            remaining = item.get("remaining") or item.get("remain") or ""

            line = f"{key}\n{description}"

            if remaining != "":
                line += f"\nОстаток: {remaining}"

            if error:
                line += f"\nОшибка: {error}"

            lines.append(line)

        return "\n\n".join(lines)

    if isinstance(data, dict):
        return str(data)

    return str(data)


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
        if source == "photo":
            await message.answer(
                "Я где-то некорректно прочитал код установки, пожалуйста, предоставьте скриншот четче "
                "или введите код установки вручную"
            )
        else:
            await message.answer(
                "Проверьте код установки на корректность ввода, я подозреваю, что Вы где-то ошиблись"
            )
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

    await add_activation(user_id, confirmation_id)
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

    if not confirmation_id:
        await message.answer("Не удалось получить Confirmation ID по фото. Попробуйте отправить скриншот чётче.")
        return

    await add_activation(user_id, confirmation_id)

    iid_detected = data.get("iid_detected")

    if iid_detected:
        await message.answer(
            f"Распознанный IID:\n{iid_detected}\n\n"
            f"Ваш Confirmation ID:\n{confirmation_id}"
        )
    else:
        await message.answer(f"Ваш Confirmation ID:\n{confirmation_id}")


@router.message(F.text == "/start")
async def start_command(message: Message):
    await message.answer(
        "Привет! Отправь мне код IID текстом или фото окна активации — я пришлю Confirmation ID.\n\n"
        "Также администратор может отправить ключи Windows/Office для проверки через PIDKey."
    )


@router.message(F.text == "/help")
async def help_command(message: Message):
    await message.answer(
        "Что можно отправить:\n\n"
        "1. Код установки IID текстом — 63 или 48 цифр.\n"
        "2. Фото окна активации.\n"
        "3. Ключи формата XXXXX-XXXXX-XXXXX-XXXXX-XXXXX для проверки через PIDKey.\n\n"
        "Команды администратора:\n"
        "/setlimit USER_ID LIMIT"
    )


@router.message(F.text.startswith("/setlimit"))
async def set_limit_command(message: Message):
    admin_id = message.from_user.id

    if admin_id not in ADMINS:
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
    photo = message.photo[-1]

    try:
        file_info = await bot.get_file(photo.file_id)
        content = await bot.download_file(file_info.file_path)

        if isinstance(content, io.BytesIO):
            image_bytes = content.getvalue()
        else:
            image_bytes = content

    except Exception:
        logger.exception("Ошибка скачивания изображения")
        await message.answer("Не удалось скачать изображение. Попробуйте отправить фото ещё раз.")
        return

    await process_activation_image(message, image_bytes)


@router.message(F.text)
async def process_text(message: Message):
    text = message.text.strip()

    product_keys = extract_product_keys(text)

    if product_keys:
        if CHECK_KEYS_ADMINS_ONLY and message.from_user.id not in ADMINS:
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

        await message.answer(f"Ответ PIDKey:\n\n{result_text}")
        return

    iid = re.sub(r"\D", "", text)

    if len(iid) not in (63, 48):
        await message.answer("Ошибка: код должен содержать 63 или 48 цифр. Проверьте ввод и попробуйте снова.")
        return

    await process_activation(message, iid, source="text")


async def main():
    await init_db()

    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
