from typing import List, Dict, Any
from .config import get_settings

class LLM:
    def __init__(self):
        self.settings = get_settings()
        if self.settings.provider.lower() != "gemini":
            raise NotImplementedError(f"Provider {self.settings.provider} not supported yet.")
        if not self.settings.google_api_key:
            raise RuntimeError("GOOGLE_API_KEY missing. Add it to .env and re-run.")
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.settings.google_api_key)
            # Fast, cheap model for bootstrap; we can swap later via env.
            self.model = genai.GenerativeModel("gemini-1.5-flash")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Gemini client: {e}") from e

    def chat(self, messages: List[Dict[str, Any]]) -> str:
        """
        messages: list of {"role": "user"|"system"|"assistant", "content": str}
        Returns plain text response.
        """
        prompt = "\n".join(m.get("content","") for m in messages if m.get("content"))
        try:
            resp = self.model.generate_content(prompt)
            return (resp.text or "").strip()
        except Exception as e:
            raise RuntimeError(f"LLM call failed: {e}") from e

def get_llm() -> LLM:
    return LLM()
