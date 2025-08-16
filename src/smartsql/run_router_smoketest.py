from smartsql.router import detect_intent
tests = [
    "Upload new schema doc",
    "Verify prod dataset columns",
    "Top agents by cost last 30d with success %",
]
for t in tests:
    r = detect_intent(t)
    print(f"{t!r} -> intent={r.intent} target={r.target}")
