from smartsql.graph import invoke_graph

tests = [
    {"text":"verify prod spans", "dataset":"prod", "table":"spans"},
    {"text":"Top agents by total spend in the last 30 days with success rate.", "dataset":"prod", "table":None},
    {"text":"what is this?", "dataset":"prod", "table":None}
]

for t in tests:
    out = invoke_graph(**t)
    print("----")
    print(t["text"])
    print(out.get("intent"), out.get("error"))
    res = out.get("result")
    if res:
        print(res.get("status"), res.get("message"))
        if "sql" in res:
            print("SQL:", res["sql"][:140] + ("..." if len(res["sql"])>140 else ""))
