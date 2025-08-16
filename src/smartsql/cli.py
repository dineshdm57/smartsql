import typer
from rich import print
from .llm import get_llm

app = typer.Typer(add_completion=False)

@app.command()
def smoketest():
    """Checks LLM connectivity & basic response."""
    llm = get_llm()
    text = llm.chat([{"role":"user","content":"Reply with the single word: pong"}]).strip()
    print(f"[bold green]LLM ok:[/bold green] {text}")

if __name__ == "__main__":
    app()
