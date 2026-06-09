GO ?= go
GOFMT ?= gofmt
PACKAGES ?= ./...
COVERAGE_OUT ?= coverage.out

.PHONY: test
test:
	$(GO) test -v -covermode=count -coverprofile=$(COVERAGE_OUT) $(PACKAGES)

.PHONY: test-race
test-race:
	$(GO) test -v -race $(PACKAGES)

.PHONY: vet
vet:
	$(GO) vet $(PACKAGES)

.PHONY: fmt
fmt:
	$(GOFMT) -w $$(find . -name '*.go' -not -path './tmp/*')

.PHONY: fmt-check
fmt-check:
	@diff="$$( $(GOFMT) -d $$(find . -name '*.go' -not -path './tmp/*') )"; \
	if [ -n "$$diff" ]; then \
		echo "Please run 'make fmt' and commit the result:"; \
		echo "$$diff"; \
		exit 1; \
	fi

.PHONY: lint
lint:
	golangci-lint run

.PHONY: ci
ci: fmt-check vet test
