"""Test command for VibePiper CLI."""

import subprocess
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

app = typer.Typer(help="Run tests for a VibePiper project")
console = Console()


@app.command()  # type: ignore[misc]
def test(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    coverage: bool = typer.Option(
        False,
        "--coverage",
        "-c",
        help="Generate coverage report",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed test output",
    ),
    marker: str | None = typer.Option(
        None,
        "--marker",
        "-m",
        help="Run tests with specific marker",
    ),
    file: str | None = typer.Option(
        None,
        "--file",
        "-f",
        help="Run specific test file",
    ),
) -> None:
    """Run tests for a VibePiper project.

    Uses pytest to run the test suite.

    Example:
        vibepiper test my-pipeline/ --coverage
    """
    project_path = project_path.resolve()
    tests_path = project_path / "tests"

    if not tests_path.exists():
        console.print(
            f"[bold red]Error:[/bold red] Tests directory not found: {tests_path}"
        )
        console.print(
            "\n[dim]Hint: Make sure your project has a 'tests/' directory[/dim]"
        )
        raise typer.Exit(1)

    console.print(f"\n[bold cyan]Running tests for:[/bold cyan] {project_path.name}\n")

    # Build pytest command
    pytest_args: list[str] = ["pytest", str(tests_path)]

    # Add verbosity
    if verbose:
        pytest_args.append("-v")
    else:
        pytest_args.append("-q")

    # Add coverage
    if coverage:
        pytest_args.extend(
            [
                "--cov=src",
                "--cov-report=term-missing",
                "--cov-report=html",
            ]
        )

    # Add marker filter
    if marker:
        pytest_args.extend(["-m", marker])

    # Add specific file
    if file:
        test_file = tests_path / file
        if not test_file.exists():
            console.print(
                f"[bold red]Error:[/bold red] Test file not found: {test_file}"
            )
            raise typer.Exit(1)
        pytest_args = ["pytest", str(test_file)]
        if verbose:
            pytest_args.append("-v")
        else:
            pytest_args.append("-q")

    # Display command
    console.print(f"[dim]Running:[/dim] {' '.join(pytest_args)}\n")

    # Run pytest
    try:
        result = subprocess.run(
            pytest_args,
            cwd=project_path,
            capture_output=not verbose,
            text=True,
        )

        # Display output
        if not verbose:
            console.print(result.stdout)
            if result.stderr:
                console.print(result.stderr)

        # Display results
        console.print()

        if result.returncode == 0:
            console.print("[bold green]✓[/bold green] All tests passed!")

            if coverage:
                console.print("\n[bold]Coverage report:[/bold]")
                console.print("[dim]HTML report: htmlcov/index.html[/dim]")

            console.print(
                Panel(
                    "[bold green]✓ Test suite passed![/bold green]\n\n"
                    "All tests completed successfully.",
                    title="[bold green]Tests Passed[/bold green]",
                    border_style="green",
                )
            )
            raise typer.Exit(0)
        else:
            console.print("[bold red]✗[/bold red] Some tests failed")

            console.print(
                Panel(
                    "[bold red]✗ Test suite failed[/bold red]\n\n"
                    "Some tests did not pass. Please review the output above.",
                    title="[bold red]Tests Failed[/bold red]",
                    border_style="red",
                )
            )
            raise typer.Exit(1)

    except FileNotFoundError:
        console.print("[bold red]Error:[/bold red] pytest not found")
        console.print("\n[dim]Hint: Install pytest with: pip install pytest[/dim]")
        raise typer.Exit(1) from None
    except KeyboardInterrupt:
        console.print("\n\n[bold yellow]Tests interrupted by user[/bold yellow]")
        raise typer.Exit(130) from None
