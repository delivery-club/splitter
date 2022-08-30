.PHONY: test ci-lint install-linter lint

test:
	@echo "Running tests..."
	@go test ./... -cover -short -count=1 -race

ci-lint: install-linter lint

install-linter:
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.45

lint:
	@echo "Running golangci-lint..."
	@golangci-lint run --config=.golangci.yml
