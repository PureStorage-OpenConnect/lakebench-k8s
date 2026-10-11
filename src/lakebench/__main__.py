"""Entry point for python -m lakebench."""

from lakebench.cli import app
from lakebench.cli._process_exit import run_and_exit

if __name__ == "__main__":
    run_and_exit(app)
