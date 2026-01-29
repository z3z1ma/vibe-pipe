"""Quality dashboard CLI commands."""

from pathlib import Path

import typer
from rich.console import Console

from vibe_piper.dashboard.api import run_server

console = Console()


def dashboard(
    project_path: Path = typer.Argument(Path("."), help="Path to VibePiper project", exists=True),
    host: str = typer.Option("0.0.0.0", "--host", "-H", help="Host to bind to"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to listen on"),
    reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
) -> None:
    """
    Start the quality dashboard server.

    Launches a FastAPI server with the quality dashboard UI and API.
    The dashboard provides real-time quality metrics, historical trends,
    anomaly detection, and drill-down capabilities.
    """
    console.print(
        f"[bold cyan]Starting Quality Dashboard[/bold cyan]\n"
        f"[dim]Project:[/dim] {project_path}\n"
        f"[dim]Host:[/dim] {host}\n"
        f"[dim]Port:[/dim] {port}\n"
        f"[dim]Reload:[/dim] {reload}"
    )

    if reload:
        console.print(
            "\n[yellow]Auto-reload enabled. The server will restart on file changes.[/yellow]"
        )

    console.print(f"\n[green]Dashboard will be available at:[/green] http://{host}:{port}")
    console.print("[dim]Press Ctrl+C to stop the server.[/dim]\n")

    try:
        run_server(host=host, port=port)
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard stopped.[/yellow]")
    except ImportError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print(
            "\n[dim]Install dashboard dependencies:[/dim]\n"
            "  [cyan]uv pip install 'vibe-piper[dashboard]'[/cyan]"
        )
