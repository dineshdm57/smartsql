# SmartSQL (MVP)

Offline-first multi-agent (A/B/C) with LangGraph routing.

API: http://127.0.0.1:8000

Key endpoints:
- /upload — schema ingest (A)
- /verify/compare — contract vs table/catalog (B)
- /ask/draft — NL → SQL (C)
- /ask/execute — confirm gate (offline blocks)
- /chat — one-box router → B/C

Docker:
  docker build -t smartsql:local .
  docker run --rm -p 8000:8000 --env-file .env -v \"$PWD/.smartsql\":/app/.smartsql smartsql:local
