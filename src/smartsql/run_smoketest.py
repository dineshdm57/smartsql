from smartsql.llm import get_llm
text = get_llm().chat([{"role":"user","content":"Reply with the single word: pong"}]).strip()
print(f"LLM ok: {text}")
