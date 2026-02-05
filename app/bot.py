from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart

from app.config import load_settings
from app.db import DB
from app.metrics.executor import execute_metric
from app.nlp.parser import parse_query, LLMParseError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer("Salom! Rus tilida savol bering. Masalan: «Сколько всего видео есть в системе?»")

@dp.message()
async def handle(m: types.Message):
    text = (m.text or "").strip()
    if not text:
        return

    s = load_settings()

    try:
        pr = await parse_query(s.ollama_url, s.ollama_model, text)
    except LLMParseError as e:
        await m.answer(f"Не понял запрос: {e}")
        return
    except Exception as e:
        logger.exception("LLM error")
        await m.answer(f"LLM error: {type(e).__name__}: {e}")
        return

    db = DB(s.database_url)
    await db.connect()
    try:
        val = await execute_metric(db, pr)
    except Exception as e:
        logger.exception("DB/metric error")
        await m.answer(f"DB error: {type(e).__name__}: {e}")
        return
    finally:
        await db.close()

    await m.answer(str(int(val)))

async def main():
    s = load_settings()
    bot = Bot(token=s.bot_token)
    logger.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
