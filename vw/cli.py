"""CLI entry point for vw."""

import click

from vw import __version__


@click.command()
@click.option("--version", is_flag=True, help="Show version and exit")
def main(version: bool) -> None:
    """vw CLI application."""
    if version:
        click.echo(f"vw version {__version__}")
        return

    click.echo("Welcome to vw!")


if __name__ == "__main__":
    main()
