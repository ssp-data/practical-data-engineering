.DEFAULT_GOAL := run

run: ## Start dagster dev server
	uv run dagster dev

install: ## Install all dependencies
	uv sync --all-extras

s3: ## Start SeaweedFS with S3 API
	weed server -s3 -s3.config=seaweedfs-s3.json -dir=/tmp/seaweedfs

test: ## Run tests
	uv run pytest realestate_tests/ -v

help: ## Show all Makefile targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
