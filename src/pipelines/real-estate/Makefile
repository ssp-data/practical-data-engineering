.DEFAULT_GOAL := up

up:
	uv run dagster dev

install:
	uv sync --all-extras

minio:
	minio server ~/Documents/minio/


help: ## Show all Makefile targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

