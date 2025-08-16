from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()

def _to_bool(v, default=False):
    if v is None:
        return default
    return str(v).strip().lower() in {"1","true","yes","on"}

class Settings:
    def __init__(self):
        self.provider = os.getenv("PROVIDER", "gemini")
        self.google_api_key = os.getenv("GOOGLE_API_KEY")
        # offline by default; only touch cloud when explicitly configured
        self.offline = _to_bool(os.getenv("SMARTSQL_OFFLINE"), True)

@lru_cache
def get_settings() -> Settings:
    return Settings()
