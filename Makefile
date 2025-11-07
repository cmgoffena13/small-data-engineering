format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

test:
	uv run -- pytest -v -n auto

install:
	uv sync --frozen --compile-bytecode

upgrade:
	uv sync --upgrade

clean:
	rm -rf __pycache__ logs .pytest_cache .ruff_cache
	uv venv --python 3.12

dev:
	docker compose up -d
	uv run -- dagster dev -m src.definitions