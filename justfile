set shell := ["bash", "-euo", "pipefail", "-c"]

# govengers/events, from the library template of govengers/standards (templates/library).
# A library pins its tools here with go run, not with tool directives in go.mod (TCH-002).

gotestsum := "go run gotest.tools/gotestsum@v1.13.0"
govulncheck := "go run golang.org/x/vuln/cmd/govulncheck@v1.9.0"
coverage_threshold := "80"
coverage_excluded := "/examples/|/test/|/internal/postgrestest/"

default:
    @just --list

# Vet and lint with the govengers baseline (.golangci.yml, copied from govengers/standards, LIB-005 checks enabled)
lint:
    go vet ./...
    golangci-lint run ./...

# Unit tests of changed and affected packages; the Go test cache replays the rest (TST-014). Fails below the coverage threshold (TST-010)
test *flags:
    mkdir -p sandbox
    {{gotestsum}} --format testname --jsonfile sandbox/unit.json -- -race -coverprofile=sandbox/unit.raw.cover {{flags}} ./...
    grep -v -E '{{coverage_excluded}}' sandbox/unit.raw.cover > sandbox/unit.cover
    go tool cover -func=sandbox/unit.cover > sandbox/unit.func
    awk -v threshold={{coverage_threshold}} '/^total:/ { found = 1; sub("%", "", $3); print "coverage " $3 "%"; if ($3 + 0 < threshold) { print "coverage is below " threshold "%"; exit 1 } } END { if (!found) { print "no coverage total"; exit 1 } }' sandbox/unit.func

# Every unit test, uncached and shuffled: the nightly run and before a release (TST-014, TST-005)
test-all: (test "-count=1" "-shuffle=on")

# Scan for known vulnerabilities in reachable code (TCH-008)
vulncheck:
    {{govulncheck}} ./...

# Everything CI runs on a change
check: lint test vulncheck
