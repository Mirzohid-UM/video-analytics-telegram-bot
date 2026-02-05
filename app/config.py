import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()

@dataclass(frozen=True)
class Settings:
    bot_token: str
    database_url: str
    ollama_url: str
    ollama_model: str

def load_settings() -> Settings:
    bot_token = os.environ["BOT_TOKEN"]
    database_url = os.environ["DATABASE_URL"]
    ollama_url = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434")
    ollama_model = os.environ.get("OLLAMA_MODEL", "qwen2.5:7b-instruct")
    return Settings(
        bot_token=bot_token,
        database_url=database_url,
        ollama_url=ollama_url,
        ollama_model=ollama_model,
    )
