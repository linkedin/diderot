PACKAGE = github.com/linkedin/diderot
SOURCE_FILES = $(wildcard $(shell git ls-files))
PROFILES = out
COVERAGE = $(PROFILES)/diderot.cov
GOBIN = $(shell go env GOPATH)/bin

# "all" is invoked on a bare "make" call since it's the first recipe. It just formats the code and
# checks that all packages can be compiled
.PHONY: all
all: fmt build

build:
	go build -v ./...
	go test -v -c -o /dev/null $$(go list -f '{{if .TestGoFiles}}{{.ImportPath}}{{end}}' ./...)

tidy:
	go mod tidy

vet:
	go vet ./...

$(GOBIN)/goimports:
	go install golang.org/x/tools/cmd/goimports@latest

.PHONY: fmt
fmt: $(GOBIN)/goimports
	$(GOBIN)/goimports -w .

# Can be used to change the number of tests run, defaults to 1 to prevent caching
TESTCOUNT = 1
# Can be used to change the verobosity of tests: make test TESTVERBOSE=-v
TESTVERBOSE =
# Can be used to generate coverage reports for a specific package
COVERPKG = $(PACKAGE)
# Can be used to change which package gets tested, defaults to all packages.
TESTPKG = ./...

test: $(COVERAGE)
$(COVERAGE):
	@mkdir -p $(@D)
	go test -race -coverprofile=$(COVERAGE) -coverpkg=$(COVERPKG)/... -count=$(TESTCOUNT) $(TESTVERBOSE) $(TESTPKG)

coverage: $(COVERAGE)
	go tool cover -html=$(COVERAGE)

profile_cache:
	$(MAKE) -B $(PROFILES)/BenchmarkCacheThroughput.bench BENCH_PKG=./cache

profile_handlers:
	$(MAKE) -B $(PROFILES)/BenchmarkHandlers.bench BENCH_PKG=./server

BENCHCOUNT = 1
BENCHTIME = 1s

$(PROFILES)/%.bench:
ifdef BENCH_PKG
	$(eval BENCHBIN=$(PROFILES)/$*)
	mkdir -p $(PROFILES)
	go test -c \
		-o $(BENCHBIN) \
		./$(BENCH_PKG)
	cd $(BENCH_PKG) && $(BENCHBIN) \
		-test.count $(BENCHCOUNT) \
		-test.benchmem \
		-test.bench="^$*$$" \
		-test.cpuprofile $(PROFILES)/$*.cpu \
		-test.memprofile $(PROFILES)/$*.mem \
		-test.blockprofile $(PROFILES)/$*.block \
		-test.benchtime $(BENCHTIME) \
		-test.run "^$$" $(BENCHVERBOSE) \
		. | tee $(abspath $@) $(abspath $(BENCHOUT))
else
	$(error BENCH_PKG undefined)
endif
ifdef OPEN_PROFILES
	go tool pprof $(BENCHBIN) $(PROFILES)/$*.cpu <<< web
	go tool pprof $(PROFILES)/$*.mem <<< web
else
	$(info Not opening profiles since OPEN_PROFILES is not set)
endif

$(GOBIN)/pkgsite:
	go install golang.org/x/pkgsite/cmd/pkgsite@latest

docs: $(GOBIN)/pkgsite
	$(GOBIN)/pkgsite -open .

