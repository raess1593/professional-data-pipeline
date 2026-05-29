.PHONY: lint

lint:
	uv run isort src
	uv run black src