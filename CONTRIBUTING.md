# Contributing to ExPiDITE
To ensure a smooth process and maintain code quality, please follow these guidelines.

## Development Environment Setup
All commands below assume you are in the root directory of the project and have an active virtual environment.

Clone the Repository:
- git clone https://github.com/Oxford-Bee-Ops/expidite

Create and activate the virtual environment (Linux/macOS)
- `cd expidite`
- `python3 -m venv .venv `
- `source .venv/bin/activate`

Create the environment (Windows)
- `cd expidite`
- `python -m venv .venv`
- `.venv\Scripts\Activate.ps1`

Install Dependencies
- `pip install -e ".[dev]"`

## Quality Checks

Before you commit your changes, you must run the following quality checks to ensure your code meets project standards.

1. `ruff format` Ruff is used to ensure consistent code styling.
2. `ruff check` Linting and Static Analysis (Ruff Check)This command runs all static checks configured in pyproject.toml. This command should return no errors.
3. `uvx ty check` Type Checking. This command should return no errors.
4. `pyright` Type Checking and bug detection. More comprehensive checking that mypy. This command should return no errors.
5. `mypy` Type Checking. Mypy verifies all type hints for correctness and completeness. This command should return no errors.
6. `pytest` Unit Tests. Run the full test suite to ensure your changes did not break existing functionality. All tests must pass.

On Windows, `check.cmd` can be used to run all of the above checks except for `pytest`.

### Optional checks

These tools currently produce a lot of false positives, but you may find them useful.
- `codespell` Spelling Check to catch common spelling errors. 
- `deadcode .` Dead code detection to identify unused code segments. This currently produces a 
  lot of output covering both false positives and genuinely unused code. 
