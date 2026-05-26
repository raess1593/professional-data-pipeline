.PHONY: lint

lint:
	isort src
	black src