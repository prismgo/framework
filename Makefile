GO ?= go
GOFMT ?= gofmt
PACKAGES ?= ./...
COVERAGE_OUT ?= coverage.out

.PHONY: test
test:
	$(GO) test -v -covermode=count -coverprofile=$(COVERAGE_OUT) $(PACKAGES)

.PHONY: covdata
covdata:
	./.github/scripts/coverage.sh $(PACKAGES)

.PHONY: test-race
test-race:
	./.github/scripts/test_with_summary.sh $(GO) test -v -race $(PACKAGES)

.PHONY: vet
vet:
	$(GO) vet $(PACKAGES)

.PHONY: fmt
fmt:
	$(GOFMT) -w $$(find . -name '*.go' -not -path './tmp/*')

.PHONY: fmt-check
fmt-check:
	@tmp="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp"' EXIT; \
	status=0; \
	for file in $$(find . -name '*.go' -not -path './tmp/*'); do \
		current="$$tmp/current"; \
		formatted="$$tmp/formatted"; \
		sed 's/\r$$//' "$$file" > "$$current"; \
		$(GOFMT) "$$file" > "$$formatted"; \
		if ! cmp -s "$$current" "$$formatted"; then \
			echo "Please run 'make fmt' and commit the result for $$file:"; \
			diff -u "$$current" "$$formatted" || true; \
			status=1; \
		fi; \
	done; \
	exit "$$status"

.PHONY: lint
lint:
	golangci-lint run

.PHONY: ci
ci: fmt-check vet test
