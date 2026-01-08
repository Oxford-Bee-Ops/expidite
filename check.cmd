:: Run all static analysis checks.
cls
ruff format
ruff check --fix
ty check
pyright
mypy
