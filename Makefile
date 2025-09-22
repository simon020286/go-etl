# Go ETL Pipeline Engine Makefile

.PHONY: build run run-web test test-nobuild test-specific clean deps help

# Build the application
build:
	go build -o etl

# Run with a pipeline file (requires pipeline.yml)
run: build
	./etl -file pipeline.yml

# Run with custom pipeline file
run-file: build
	@if [ -z "$(FILE)" ]; then echo "Usage: make run-file FILE=path/to/pipeline.yml"; exit 1; fi
	./etl -file $(FILE)

# Run with custom pipeline file and log level
run-log: build
	@if [ -z "$(FILE)" ]; then echo "Usage: make run-log FILE=path/to/pipeline.yml LOG=info"; exit 1; fi
	./etl -file $(FILE) -log $(or $(LOG),info)

# Start web server mode
run-web: build
	./etl -web

# Run all tests
test:
	go test ./tests/

# Run tests with plugin builds disabled
test-nobuild:
	NOBUILD=1 go test ./tests/

# Run specific test
test-specific:
	@if [ -z "$(TEST)" ]; then echo "Usage: make test-specific TEST=TestName"; exit 1; fi
	go test ./tests/ -run $(TEST)

# Update dependencies
deps:
	go mod tidy

# Clean build artifacts
clean:
	rm -f etl

# Show help
help:
	@echo "Available targets:"
	@echo "  build        - Build the ETL application"
	@echo "  run          - Build and run with pipeline.yml"
	@echo "  run-file     - Build and run with custom file: make run-file FILE=path/to/pipeline.yml"
	@echo "  run-log      - Build and run with custom file and log level: make run-log FILE=path/to/pipeline.yml LOG=info"
	@echo "  run-web      - Build and start web server mode"
	@echo "  test         - Run all tests"
	@echo "  test-nobuild - Run tests with NOBUILD=1"
	@echo "  test-specific - Run specific test: make test-specific TEST=TestName"
	@echo "  deps         - Update Go dependencies"
	@echo "  clean        - Remove build artifacts"
	@echo "  help         - Show this help message"