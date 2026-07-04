#!/usr/bin/env sh
# Run the full test matrix and print a summarized report with insights.
#
# Usage: ./scripts/test.sh [suite]
#   suite: all (default) | unit | integration
#
# Suites:
#   unit         go test ./...            (pure, no external deps)
#   race         go test -race ./...      (skipped when cgo/gcc unavailable; CI runs it)
#   vet          go vet ./...             (static analysis)
#   coverage     derived from the unit run (-coverprofile)
#   integration  live end-to-end regression checks against the running stack
set -u

# Anchor to the repo root so the script works from any directory.
cd "$(dirname "$0")/.."

SUITE="${1:-all}"
# Single source for the local API address; override with API_BASE=...
API_BASE="${API_BASE:-http://localhost:8081}"
POLL_SECONDS=10

# --- reporting helpers ------------------------------------------------------

if [ -t 1 ]; then
    ANIMATE=1
    GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; BOLD="\033[1m"; DIM="\033[2m"; RESET="\033[0m"
else
    ANIMATE=0
    GREEN=""; RED=""; YELLOW=""; BOLD=""; DIM=""; RESET=""
fi

REPORT=""
FAILED=0

# record appends to the final table AND prints a live completion line, so
# progress is visible the moment each suite finishes.
record() { # name result duration details
    REPORT="${REPORT}${1}|${2}|${3}|${4}\n"
    case "$2" in
        PASS) printf "  %b✓%b %-12s %b(%s)%b %s\n" "$GREEN" "$RESET" "$1" "$DIM" "$3" "$RESET" "$4" ;;
        FAIL) printf "  %b✗%b %-12s %b(%s)%b %s\n" "$RED" "$RESET" "$1" "$DIM" "$3" "$RESET" "$4"; FAILED=1 ;;
        SKIP) printf "  %b-%b %-12s %bskipped%b %s\n" "$YELLOW" "$RESET" "$1" "$DIM" "$RESET" "$4" ;;
        *)    printf "  %b·%b %-12s %s\n" "$DIM" "$RESET" "$1" "$4" ;;
    esac
}

colorize() {
    case "$1" in
        PASS) printf "%b" "${GREEN}PASS${RESET}" ;;
        FAIL) printf "%b" "${RED}FAIL${RESET}" ;;
        SKIP) printf "%b" "${YELLOW}SKIP${RESET}" ;;
        *)    printf "%s" "$1" ;;
    esac
}

now() { date +%s; }

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# run_step runs a command with its output captured, showing a spinner on
# interactive terminals and a plain progress line otherwise.
run_step() { # label outfile command...
    step_label=$1
    step_outfile=$2
    shift 2

    if [ "$ANIMATE" = "1" ]; then
        "$@" >"$step_outfile" 2>&1 &
        step_pid=$!
        frames='|/-\'
        frame_index=0
        while kill -0 "$step_pid" 2>/dev/null; do
            frame_index=$(( (frame_index % 4) + 1 ))
            frame=$(printf '%s' "$frames" | cut -c"$frame_index")
            printf "\r  %b%s%b %s" "$YELLOW" "$frame" "$RESET" "$step_label"
            sleep 0.2 2>/dev/null || sleep 1
        done
        printf "\r\033[K"
        wait "$step_pid"
    else
        echo "==> $step_label"
        "$@" >"$step_outfile" 2>&1
    fi
}

# --- unit suite ---------------------------------------------------------------

UNIT_TESTS=0
COVER_TOTAL="n/a"
LOWEST_COVER=""

run_unit() {
    started=$(now)
    if run_step "unit: go test ./..." "$WORK_DIR/unit.json" \
            go test -json -coverprofile="$WORK_DIR/cover.out" ./...; then
        UNIT_TESTS=$(grep -c '"Action":"pass".*"Test":"' "$WORK_DIR/unit.json" || true)
        packages=$(grep '"Action":"pass"' "$WORK_DIR/unit.json" | grep -cv '"Test":' || true)
        record "unit" "PASS" "$(($(now) - started))s" "${UNIT_TESTS} tests in ${packages} packages"
    else
        failures=$(grep '"Action":"fail".*"Test":"' "$WORK_DIR/unit.json" | sed 's/.*"Test":"\([^"]*\)".*/\1/' | sort -u | head -5 | tr '\n' ' ')
        record "unit" "FAIL" "$(($(now) - started))s" "failing: ${failures:-see go test output}"
        go test ./... 2>&1 | grep -E "^(---|FAIL|ok)" | head -20
    fi

    if [ -f "$WORK_DIR/cover.out" ]; then
        COVER_TOTAL=$(go tool cover -func="$WORK_DIR/cover.out" 2>/dev/null | awk '/^total:/ {print $3}')
        # Cached second run; prints one "coverage: N% of statements" line per
        # package. Field positions vary (cached, no-test-files), so locate
        # the percentage field instead of assuming a column.
        run_step "coverage: per-package breakdown" "$WORK_DIR/coverpkg.out" go test -cover ./...
        LOWEST_COVER=$(awk '/coverage:/ {
                pkg = ($1 == "ok") ? $2 : $1
                for (i = 1; i <= NF; i++) if ($i ~ /%$/) { gsub("%", "", $i); print $i, pkg }
            }' "$WORK_DIR/coverpkg.out" | sort -n | head -1)
        record "coverage" "-" "-" "${COVER_TOTAL:-n/a} of statements"
    fi
}

run_race() {
    started=$(now)
    if run_step "race: go test -race ./..." "$WORK_DIR/race.out" \
            env CGO_ENABLED=1 go test -race ./...; then
        record "race" "PASS" "$(($(now) - started))s" "no data races detected"
    elif grep -q "requires cgo\|C compiler" "$WORK_DIR/race.out"; then
        record "race" "SKIP" "-" "cgo/gcc unavailable on this machine; CI enforces -race"
    else
        record "race" "FAIL" "$(($(now) - started))s" "$(grep -m1 'DATA RACE\|FAIL' "$WORK_DIR/race.out" || echo 'see race output')"
    fi
}

run_vet() {
    started=$(now)
    if run_step "vet: go vet ./..." "$WORK_DIR/vet.out" go vet ./...; then
        record "vet" "PASS" "$(($(now) - started))s" "no static-analysis issues"
    else
        record "vet" "FAIL" "$(($(now) - started))s" "$(head -1 "$WORK_DIR/vet.out")"
    fi
}

# --- integration / e2e regression suite --------------------------------------

http_code() { curl -s -o "$WORK_DIR/body" -w "%{http_code}" "$@" 2>/dev/null; }

run_integration() {
    started=$(now)

    if [ "$(http_code "$API_BASE/healthz")" != "200" ]; then
        record "integration" "SKIP" "-" "stack not running; start with: ./scripts/deploy.sh local"
        return
    fi

    printf "  %b▶%b integration checks:\n" "$BOLD" "$RESET"
    checks=0; passed=0; failing=""

    check() { # name condition_result
        checks=$((checks + 1))
        if [ "$2" = "0" ]; then
            passed=$((passed + 1))
            printf "      %b✓%b %s\n" "$GREEN" "$RESET" "$1"
        else
            failing="${failing}${1}; "
            printf "      %b✗%b %s\n" "$RED" "$RESET" "$1"
        fi
    }

    # 1. liveness
    [ "$(http_code "$API_BASE/healthz")" = "200" ]; check "healthz 200" "$?"

    # 2. create returns 201 and a queued/pending notification
    code=$(http_code -X POST "$API_BASE/api/v1/notifications" \
        -H "Content-Type: application/json" \
        -d '{"recipient":"+905551234567","channel":"sms","content":"test-suite e2e"}')
    notification_id=$(sed 's/.*"id":"\([^"]*\)".*/\1/' "$WORK_DIR/body")
    [ "$code" = "201" ] && [ -n "$notification_id" ]; check "create 201" "$?"

    # 3. pipeline regression: reaches sent within POLL_SECONDS
    delivered=1
    elapsed=0
    while [ "$elapsed" -lt "$POLL_SECONDS" ]; do
        http_code "$API_BASE/api/v1/notifications/$notification_id" >/dev/null
        if grep -q '"status":"sent"' "$WORK_DIR/body"; then delivered=0; break; fi
        if [ "$ANIMATE" = "1" ]; then
            printf "\r      %b|%b waiting for delivery (%ss)" "$YELLOW" "$RESET" "$elapsed"
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    [ "$ANIMATE" = "1" ] && printf "\r\033[K"
    check "delivered to sent in ${elapsed}s" "$delivered"

    # 4. validation regression: bad recipient → 400 naming the field
    code=$(http_code -X POST "$API_BASE/api/v1/notifications" \
        -H "Content-Type: application/json" \
        -d '{"recipient":"nope","channel":"sms","content":"x"}')
    [ "$code" = "400" ] && grep -q '"field":"recipient"' "$WORK_DIR/body"; check "validation 400" "$?"

    # 5. unknown id → 404
    code=$(http_code "$API_BASE/api/v1/notifications/00000000-0000-0000-0000-000000000000")
    [ "$code" = "404" ]; check "unknown id 404" "$?"

    if [ "$passed" = "$checks" ]; then
        record "integration" "PASS" "$(($(now) - started))s" "${passed}/${checks} e2e regression checks"
    else
        record "integration" "FAIL" "$(($(now) - started))s" "failed: ${failing}"
    fi
}

# --- execute ------------------------------------------------------------------

printf "%b\n" "${BOLD}Running suite: ${SUITE}${RESET}"

case "$SUITE" in
    all)         run_vet; run_unit; run_race; run_integration ;;
    unit)        run_vet; run_unit; run_race ;;
    integration) run_integration ;;
    *) echo "test: unknown suite '$SUITE' (supported: all, unit, integration)" >&2; exit 1 ;;
esac

# --- report -------------------------------------------------------------------

printf "\n%b\n" "${BOLD}============================ TEST RESULTS ============================${RESET}"
printf "%-14s %-6s %-10s %s\n" "SUITE" "RESULT" "DURATION" "DETAILS"
printf "%b" "$REPORT" | while IFS='|' read -r name result duration details; do
    [ -z "$name" ] && continue
    printf "%-14s " "$name"
    colorize "$result"
    printf "%*s" $((7 - ${#result})) ""
    printf "%-10s %s\n" "$duration" "$details"
done
printf "%b\n" "${BOLD}======================================================================${RESET}"

echo ""
echo "Insights:"
[ -n "$COVER_TOTAL" ] && [ "$COVER_TOTAL" != "n/a" ] && echo "  - total statement coverage: ${COVER_TOTAL}"
if [ -n "$LOWEST_COVER" ]; then
    echo "  - lowest-covered package: $(echo "$LOWEST_COVER" | awk '{print $2 " (" $1 "%)"}') — transport/persistence packages are exercised by the integration suite instead"
fi
echo "$REPORT" | grep -q "race|SKIP" && echo "  - race detector skipped locally (no C toolchain); rely on CI's go test -race"
echo "$REPORT" | grep -q "integration|SKIP" && echo "  - integration suite needs the stack: ./scripts/deploy.sh local"
if [ "$FAILED" = "1" ]; then
    printf "%b\n" "  - ${RED}one or more suites failed — fix before committing${RESET}"
else
    printf "%b\n" "  - ${GREEN}all executed suites green${RESET}"
fi

exit "$FAILED"
