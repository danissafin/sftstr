import asyncio
import io
import logging
import os
import platform
import re
from typing import Optional

import aiohttp
import asyncpg
import pytesseract
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message
from dotenv import load_dotenv
from PIL import Image, ImageEnhance, ImageStat

load_dotenv()

TOKEN = os.getenv("TOKEN")
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMINS = {int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()}

API_URL = "https://pidkey.com/ajax/cidms_api"
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "5"))

if platform.system() == "Windows":
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
else:
    os.environ.setdefault("TESSDATA_PREFIX", "/usr/share/tesseract-ocr/5/tessdata/")

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


def extract_installation_code(text: str) -> Optional[str]:
    sequences = re.findall(r"\d+", text)

    seq7 = [s for s in sequences if len(s) == 7]
    if len(seq7) >= 9:
        code = "".join(seq7[:9])
        if len(code) == 63:
            return code

    seq9 = [s for s in sequences if len(s) == 9]
    if len(seq9) >= 7:
        code = "".join(seq9[:7])
        if len(code) == 63:
            return code

    for s in sequences:
        if len(s) in (63, 48):
            return s

    all_digits = re.sub(r"\D", "", text)
    if len(all_digits) in (63, 48):
        return all_digits

    return None


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


async def request_cid(iid: str) -> dict:
    params = {
        "iids": iid,
        "justforcheck": 0,
        "apikey": API_KEY,
    }

    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(API_URL, params=params) as response:
            if response.status != 200:
                raise RuntimeError(f"API status: {response.status}")

            return await response.json(content_type=None)


async def process_activation(message: Message, iid: str, source: str):
    user_id = message.from_user.id

    if not await check_limit(user_id):
        await message.answer("Вы достигли лимита успешных активаций. Свяжитесь с нами для увеличения лимита.")
        return

    await message.answer("Обрабатываю запрос...")

    try:
        data = await request_cid(iid)
        logger.info(f"API response: {data}")
    except Exception:
        logger.exception("Ошибка обращения к API")
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

    confirmation_id = data.get("confirmationid")

    if not confirmation_id:
        await message.answer("CID не найден. Попробуйте позже или свяжитесь с нами.")
        return

    await add_activation(user_id, confirmation_id)
    await message.answer(f"Ваш Confirmation ID: {confirmation_id}")


def preprocess_images(image: Image.Image) -> list[Image.Image]:
    image = image.convert("L")

    stat = ImageStat.Stat(image)
    avg_brightness = stat.mean[0]
    logger.info(f"Средняя яркость изображения: {avg_brightness:.2f}")

    images = []

    contrast = ImageEnhance.Contrast(image).enhance(2.0)
    images.append(contrast)

    binary = contrast.point(lambda x: 0 if x < 150 else 255, "1")
    images.append(binary)

    inverted = Image.eval(contrast, lambda x: 255 - x)
    images.append(inverted)

    return images


def ocr_image(image: Image.Image) -> str:
    configs = [
        "--psm 6",
        "--psm 7",
        "--psm 11",
        "--psm 12",
    ]

    langs = ["rus+eng", "eng", "rus"]

    best_text = ""

    for prepared_image in preprocess_images(image):
        for lang in langs:
            for config in configs:
                try:
                    text = pytesseract.image_to_string(prepared_image, lang=lang, config=config)
                    logger.info(f"OCR lang={lang}, config={config}:\n{text}")

                    if len(text) > len(best_text):
                        best_text = text

                    code = extract_installation_code(text)
                    if code:
                        return text

                except Exception:
                    logger.exception(f"Ошибка OCR lang={lang}, config={config}")

    return best_text


@router.message(F.text == "/start")
async def start_command(message: Message):
    await message.answer(
        "Привет! Отправь мне код IID текстом или пришли фото окна активации. "
        "Я проверю код и пришлю Confirmation ID."
    )


@router.message(F.text == "/help")
async def help_command(message: Message):
    await message.answer(
        "Можно отправить:\n"
        "1. Код установки текстом — 63 или 48 цифр.\n"
        "2. Фото окна активации.\n\n"
        "Команда администратора:\n"
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


@router.message(F.text)
async def process_text(message: Message):
    iid = re.sub(r"\D", "", message.text)

    if len(iid) not in (63, 48):
        await message.answer("Ошибка: код должен содержать 63 или 48 цифр. Проверьте ввод и попробуйте снова.")
        return

    await process_activation(message, iid, source="text")


@router.message(F.photo)
async def process_photo(message: Message):
    photo = message.photo[-1]

    try:
        file_info = await bot.get_file(photo.file_id)
        content = await bot.download_file(file_info.file_path)
        photo_bytes = content if isinstance(content, io.BytesIO) else io.BytesIO(content)
        photo_bytes.seek(0)
        image = Image.open(photo_bytes)
    except Exception:
        logger.exception("Ошибка скачивания или открытия изображения")
        await message.answer("Не удалось открыть изображение. Попробуйте отправить фото ещё раз.")
        return

    try:
        ocr_text = await asyncio.to_thread(ocr_image, image)
        logger.info(f"Лучший OCR текст:\n{ocr_text}")
    except Exception:
        logger.exception("Ошибка распознавания изображения")
        await message.answer("Ошибка при распознавании изображения. Попробуйте позже.")
        return

    iid = extract_installation_code(ocr_text)

    if not iid:
        await message.answer(
            "Не удалось распознать корректный код установки из изображения. "
            "Попробуйте отправить более чёткий скриншот или введите код вручную."
        )
        return

    logger.info(f"Распознанный IID: {iid}")
    await process_activation(message, iid, source="photo")


async def main():
    await init_db()

    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
