#!/usr/bin/env bash
#
# scripts/smoke_cli.sh — smoke-test the Arweave CLI tools.
#
# Verifies each bin/* wrapper still dispatches to a live Erlang entry
# point and that the entry point reaches `arweave_config` without
# crashing. Each check asserts:
#   - the command exits with the expected status
#   - stdout/stderr contains an expected marker string
#   - stdout/stderr does NOT contain Erlang crash signatures
#     (noproc, undef, function_clause, ** exception, …)
#
# Phases:
#   1. No-node tools: help, check, wallet creation, benchmarks, …
#   2. Doctor tools (against throwaway tmp dirs).
#   3. Daemon-required: `arweave config get/set` against a real node
#      booted with scripts/smoke_node_config.json.
#
# Run after a successful `./ar-rebar3 default release` build. Exits
# 0 iff every check passes.

set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

# Normalize env so the smoke behaves the same regardless of how the
# developer has their shell configured. ARWEAVE_DEV=1 in particular
# makes bin/arweave run a full `ar-rebar3 default release` rebuild on
# every invocation — fatal for a multi-tool smoke.
unset ARWEAVE_DEV ARWEAVE_BUILD_TARGET ARWEAVE_NAMESPACE ARWEAVE_NS_SUFFIX
unset ARNODE ARCOOKIE

CONFIG_FIXTURE="$ROOT/scripts/smoke_node_config.json"

# Erlang crash patterns. Any one of these in command output flags the
# check as failed even if exit code and marker matched.
CRASH_SIGS='\*\* exception|exception error:|exception exit:|noproc|=ERROR REPORT|\bundef\b|function_clause|\bbadarg\b|\bbadmatch\b|not_started|init terminating in do_boot'

# Subset of CRASH_SIGS that indicates the VM never reached the tool's
# code — a build/release problem, not a test problem. These get
# flagged separately so a broken release is unmistakable in the
# summary.
LAUNCH_ERROR_SIGS='Runtime terminating during boot|cannot get bootfile|\{load_failed,|init terminating in do_boot|Failed to load module|Crash dump is being written to'

PASS=0
FAIL=0
LAUNCH_ERRORS=0
FAILED_NAMES=()
LAUNCH_ERROR_NAMES=()

# Tempdir tracking — wiped on exit.
TMP_DIRS=()

mktmp() {
	local d
	d=$(mktemp -d "/tmp/smoke-cli-$1.XXXXXX")
	TMP_DIRS+=("$d")
	echo "$d"
}

red()    { printf '\033[31m%s\033[0m' "$1"; }
green()  { printf '\033[32m%s\033[0m' "$1"; }
yellow() { printf '\033[33m%s\033[0m' "$1"; }
bold()   { printf '\033[1m%s\033[0m' "$1"; }

log() { printf '%s\n' "$*"; }

# run_check NAME EXPECTED_EXIT MARKER_REGEX -- CMD...
#   NAME            human-readable label
#   EXPECTED_EXIT   integer exit code we expect
#   MARKER_REGEX    extended regex that must match stdout/stderr
#                   (use '' to skip the marker check)
#   CMD...          the command to run (env vars can be set with `env`
#                   prefix when invoking run_check)
#
# Captures combined stdout+stderr to a per-check tempfile, applies
# every assertion, prints pass/fail, and on fail dumps the last few
# lines of output for debugging.
run_check() {
	local name=$1 expected_exit=$2 marker=$3
	shift 3
	[ "${1-}" = "--" ] && shift

	local out_file
	out_file=$(mktemp "/tmp/smoke-cli-out.XXXXXX")
	TMP_DIRS+=("$out_file")

	# Run via setsid so the command lands in its own session with no
	# controlling tty. BEAM startup touches the tty even with -noshell;
	# under an interactive shell that triggers SIGTTOU and the whole
	# process tree gets SIGSTOP'd, leaving the smoke wedged in `wait`.
	# Stdin is redirected from /dev/null for the same reason (no
	# SIGTTIN on read). $SETSID is set in preflight — falls back to
	# empty when setsid(1) isn't installed.
	local actual_exit=0
	timeout 180s $SETSID "$@" </dev/null >"$out_file" 2>&1 || actual_exit=$?

	local failures=()

	if [ "$actual_exit" != "$expected_exit" ]; then
		failures+=("exit=$actual_exit (expected $expected_exit)")
	fi

	if [ -n "$marker" ]; then
		case "$marker" in
		!*)
			# Leading `!` inverts the assertion: the regex must NOT
			# appear in the output.
			if grep -Eq -- "${marker#!}" "$out_file"; then
				failures+=("forbidden marker /${marker#!}/ present")
			fi
			;;
		*)
			if ! grep -Eq -- "$marker" "$out_file"; then
				failures+=("missing marker /$marker/")
			fi
			;;
		esac
	fi

	local launch_error=""
	if grep -Eq -- "$LAUNCH_ERROR_SIGS" "$out_file"; then
		launch_error=$(grep -Eo -- "$LAUNCH_ERROR_SIGS" "$out_file" | head -1)
		failures+=("LAUNCH ERROR: $launch_error")
	elif grep -Eq -- "$CRASH_SIGS" "$out_file"; then
		local sig
		sig=$(grep -Eo -- "$CRASH_SIGS" "$out_file" | head -1)
		failures+=("crash signature: $sig")
	fi

	if [ ${#failures[@]} -eq 0 ]; then
		PASS=$((PASS + 1))
		log "  $(green PASS) $name"
	else
		FAIL=$((FAIL + 1))
		FAILED_NAMES+=("$name")
		if [ -n "$launch_error" ]; then
			LAUNCH_ERRORS=$((LAUNCH_ERRORS + 1))
			LAUNCH_ERROR_NAMES+=("$name")
			log "  $(red 'LAUNCH ERROR') $name"
		else
			log "  $(red FAIL) $name"
		fi
		local f
		for f in "${failures[@]}"; do
			log "    - $f"
		done
		log "    last output lines:"
		tail -n 20 "$out_file" | sed 's/^/      /'
	fi
}

# --- Daemon helpers --------------------------------------------------

DAEMON_RUNNING=0
DAEMON_ENV=()

daemon_start() {
	local data_dir="$1" log_dir="$2"
	mkdir -p "$data_dir" "$log_dir"

	# Unique node name + cookie so the smoke can't collide with any
	# arweave instance already running on the host. vm.args.src
	# substitutes ARNODE / ARCOOKIE from the environment.
	local node_name="arweave-smoke-$$@127.0.0.1"
	local cookie="smoke-cookie-$$"

	# Non-default port avoids collisions with any other arweave
	# process the runner might already have bound to 1984.
	local port=31984

	# Same env is reused by every later ./bin/arweave call in this
	# phase so erl_call connects to the same node.
	DAEMON_ENV=(
		"ARNODE=$node_name"
		"ARCOOKIE=$cookie"
		"AR_CONFIG_FILE=$CONFIG_FIXTURE"
		"AR_DATA_DIR=$data_dir"
		"AR_LOG_DIR=$log_dir"
		"AR_PORT=$port"
	)

	log "  starting daemon: node=$node_name data=$data_dir"
	# `arweave daemon` waits for erlang:is_alive() before returning,
	# but that fires before any application starts. Cap with timeout
	# in case the node never reaches distribution-up.
	if ! timeout 120s $SETSID env "${DAEMON_ENV[@]}" \
			./bin/arweave daemon </dev/null >"$log_dir/daemon.out" 2>&1; then
		log "  $(red FAIL) daemon start"
		FAIL=$((FAIL + 1))
		FAILED_NAMES+=("daemon-start")
		log "    daemon.out tail:"
		tail -n 40 "$log_dir/daemon.out" | sed 's/^/      /'
		return 1
	fi

	# Daemon is dist-up but arweave_config may not be running yet.
	# Poll a real RPC that needs the app's gen_server alive: a
	# successful `config get debug` against the fixture returns
	# "true" — proves config app + cli module + parser are all wired.
	local deadline=$((SECONDS + 90))
	while [ $SECONDS -lt $deadline ]; do
		if $SETSID env "${DAEMON_ENV[@]}" ./bin/arweave config get debug \
				</dev/null 2>/dev/null | grep -Eq '^"?true"?$'; then
			DAEMON_RUNNING=1
			log "  $(green PASS) daemon ready"
			PASS=$((PASS + 1))
			return 0
		fi
		sleep 2
	done

	log "  $(red FAIL) daemon never became RPC-ready"
	FAIL=$((FAIL + 1))
	FAILED_NAMES+=("daemon-ready")
	log "    daemon.out tail:"
	tail -n 40 "$log_dir/daemon.out" | sed 's/^/      /'
	# Mark as running so cleanup can stop it.
	DAEMON_RUNNING=1
	return 1
}

daemon_stop() {
	[ "$DAEMON_RUNNING" = "1" ] || return 0
	log "  stopping daemon"
	# Bound the stop call — on a wedged node `./bin/arweave stop`
	# loops forever waiting for ping to fail.
	timeout 15s $SETSID env "${DAEMON_ENV[@]}" ./bin/arweave stop \
		</dev/null >/dev/null 2>&1 || true
	DAEMON_RUNNING=0
}

# Kill every descendant of this script — `timeout` wrappers, BEAM VMs
# spawned by ./bin/arweave invocations, rebar3 subshells, anything
# left behind. `pgrep -P` only walks one level, so do an explicit BFS
# to collect every PID in the tree, then signal in
# children-before-parents order so they don't get reaped before we
# can kill them.
kill_descendants() {
	local frontier=($$) all=() i child
	i=0
	while [ $i -lt ${#frontier[@]} ]; do
		for child in $(pgrep -P "${frontier[$i]}" 2>/dev/null); do
			frontier+=("$child")
			all+=("$child")
		done
		i=$((i + 1))
	done
	# Reverse iteration: deepest descendants first.
	local j
	for ((j = ${#all[@]} - 1; j >= 0; j--)); do
		kill -TERM "${all[$j]}" 2>/dev/null || true
	done
	# Brief grace, then SIGKILL anything that ignored TERM (BEAM VMs
	# often do).
	sleep 0.3
	for ((j = ${#all[@]} - 1; j >= 0; j--)); do
		kill -KILL "${all[$j]}" 2>/dev/null || true
	done
}

cleanup() {
	kill_descendants
	daemon_stop || true
	# Stray smoke-daemons from previous (possibly killed) runs.
	pkill -f 'arweave-smoke-' 2>/dev/null || true
	local d
	for d in "${TMP_DIRS[@]}"; do
		[ -e "$d" ] && rm -rf "$d"
	done
	# ar_doctor_inspect:bitmap writes bitmap_<StoreID>.ppm to cwd.
	rm -f "$ROOT"/bitmap_*.ppm 2>/dev/null || true
}

# Foreground signal traps. EXIT covers normal end; INT / TERM make
# Ctrl-C and `kill <pid>` interrupt cleanly. We exit 130 on INT to
# match the conventional Ctrl-C exit status.
on_interrupt() {
	log ""
	log "$(yellow 'INTERRUPTED — cleaning up')"
	cleanup
	exit 130
}
trap on_interrupt INT TERM
trap cleanup EXIT

# --- Preflight -------------------------------------------------------

[ -x ./bin/arweave ] || { log "ERROR: ./bin/arweave not executable"; exit 2; }
[ -f "$CONFIG_FIXTURE" ] || { log "ERROR: fixture missing: $CONFIG_FIXTURE"; exit 2; }
if ! command -v timeout >/dev/null 2>&1; then
	log "ERROR: GNU coreutils 'timeout' required"; exit 2
fi

# `setsid --wait` is what makes the smoke survive being launched from
# an interactive tty. If it's not present we run plain — works in CI
# (always headless) but may hang under a controlling tty.
if command -v setsid >/dev/null 2>&1 && setsid --wait true 2>/dev/null; then
	SETSID="setsid --wait"
else
	SETSID=""
	log "warning: setsid --wait not available; smoke may hang under an interactive tty"
fi

# --- Phase 1: no-node tools -----------------------------------------

log "$(bold '=== Phase 1: no-node tools ===')"

WALLET_DIR=$(mktmp wallet)
ECDSA_WALLET_DIR=$(mktmp ecdsa-wallet)

run_check "arweave check" 0 "" -- \
	./bin/arweave check

run_check "config help" 0 "Available option groups" -- \
	./bin/arweave config help

# Assert an option KEY from the detailed per-group view — the bare
# group name is a useless marker (it also appears in the
# "Available groups" listing of the failure output).
run_check "config help mining" 0 'mining\.enabled' -- \
	./bin/arweave config help mining

# A group name that does not exist — must hit the "Unknown group"
# branch in arweave_config_help.
run_check "config help bogus" 1 "Unknown group" -- \
	./bin/arweave config help xyzzy_smoke_does_not_exist

# Hidden option groups (`hidden => true' in the specs — the gated
# config.http.* HTTP server options) must not render in help, and
# addressing the hidden group directly is "unknown".
run_check "config help hides gated group" 0 '!config\.http' -- \
	./bin/arweave config help

run_check "config help config (hidden group)" 1 "Unknown group" -- \
	./bin/arweave config help config

# create-wallet: no args → usage + exit 1
run_check "create-wallet (no args)" 1 "Usage: ./bin/create-wallet" -- \
	./bin/create-wallet

# create-wallet: positive path — writes a keyfile. ar:create_wallet/1
# calls init:stop(1) on success too, so exit code is 1 regardless.
run_check "create-wallet (writes keyfile)" 1 "Created a wallet" -- \
	./bin/create-wallet "$WALLET_DIR"

if ! ls "$WALLET_DIR"/wallets/*.json >/dev/null 2>&1; then
	log "  $(red FAIL) create-wallet did not produce a keyfile in $WALLET_DIR/wallets/"
	FAIL=$((FAIL + 1))
	FAILED_NAMES+=("create-wallet-keyfile-present")
else
	log "  $(green PASS) create-wallet keyfile present"
	PASS=$((PASS + 1))
fi

# create-ecdsa-wallet: no args → usage + exit 1
run_check "create-ecdsa-wallet (no args)" 1 "Usage: ./bin/create-ecdsa-wallet" -- \
	./bin/create-ecdsa-wallet

# create-ecdsa-wallet: positive path — writes a keyfile. ar:create_wallet/2
# calls init:stop(1) on success too, so exit code is 1 regardless.
run_check "create-ecdsa-wallet (writes keyfile)" 1 "Created a wallet" -- \
	./bin/create-ecdsa-wallet "$ECDSA_WALLET_DIR"

if ! ls "$ECDSA_WALLET_DIR"/wallets/*.json >/dev/null 2>&1; then
	log "  $(red FAIL) create-ecdsa-wallet did not produce a keyfile in $ECDSA_WALLET_DIR/wallets/"
	FAIL=$((FAIL + 1))
	FAILED_NAMES+=("create-ecdsa-wallet-keyfile-present")
else
	log "  $(green PASS) create-ecdsa-wallet keyfile present"
	PASS=$((PASS + 1))
fi

# Light RandomX init (rx512, large_pages=0, hw_aes=0) keeps this in
# the tens of seconds. Exercises ar:benchmark_hash → ar_bench_hash.
run_check "benchmark-hash" 1 "Hashing benchmark" -- \
	./bin/benchmark-hash randomx 512 jit 1 large_pages 0 hw_aes 0

# mode=openssl exercises arweave_config:set([vdf, algorithm], …), the
# integration we most want to smoke. difficulty=1 keeps the VDF step
# tiny.
run_check "benchmark-vdf" 1 "VDF step computed" -- \
	./bin/benchmark-vdf mode openssl difficulty 1 verify false

# Single entropy-only sample (no `dir` arg → skip Phase 2 disk I/O in
# the bench). Calls arweave_config:set([randomx, large_pages], …) —
# the canonical "is arweave_config app started?" failure path. The
# "Initializing" marker prints only after configure_randomx succeeds.
run_check "benchmark-packing" 1 "Initializing" -- \
	./bin/benchmark-packing samples 1 large_pages 0

# --- Phase 2: doctor smokes -----------------------------------------

log "$(bold '=== Phase 2: doctor smokes ===')"

DOC_DATA=$(mktmp doctor-data)
DOC_SRC=$(mktmp doctor-src)

run_check "data-doctor (no args)" 1 "data-doctor merge" -- \
	./bin/data-doctor

# TODO: `data-doctor merge` needs a populated source dir with valid
# RocksDB databases to exercise its end-to-end path. Against an empty
# src dir it crashes on the first ar_kv:open. The arweave_config
# integration is now smoke-covered by data-doctor (no args) +
# data-doctor dump.
#
# TODO: `data-doctor bench` does pmap reads on the data dir and the
# pmap times out after 60s on an empty store. Same arweave_config
# integration is covered by the other doctor smokes.

# Bogus hash → ar_storage:read_block returns unavailable → "Block …
# not found" path. ar_data_doctor:main considers a successful lookup
# (even if it returns "not found") to be the success case and exits 0.
BOGUS_HASH="$(printf 'A%.0s' {1..43})"
run_check "data-doctor dump" 0 "not found" -- \
	./bin/data-doctor dump false "$BOGUS_HASH" 0 "$DOC_DATA" "$DOC_DATA/out"

# TODO: `inspect bitmap` against an empty data_dir hits a
# {badmatch, 262144} in ar_chunk_storage:get_chunk_byte_from_bucket_end/1
# (chunk_visualization assumes chunks above the strict-split threshold).
# Skip in the smoke until ar_doctor_inspect is robust to empty stores.
#
# TODO: `inspect chunks` always tries to fetch from arweave.net via
# HTTP for cross-comparison — a network dependency that's both flaky in
# CI and unrelated to the integration we want to smoke. Skip.

# --- Phase 3: daemon-backed config get/set --------------------------

log "$(bold '=== Phase 3: daemon + config get/set ===')"

DAEMON_DATA=$(mktmp daemon-data)
DAEMON_LOG=$(mktmp daemon-log)

if daemon_start "$DAEMON_DATA" "$DAEMON_LOG"; then
	# `debug` is a runtime-settable bool whose default in the fixture
	# is true (see scripts/smoke_node_config.json). Round-trip it.
	run_check "config get debug" 0 "true" -- \
		env "${DAEMON_ENV[@]}" ./bin/arweave config get debug

	run_check "config set debug=false" 0 "ok" -- \
		env "${DAEMON_ENV[@]}" ./bin/arweave config set debug false

	run_check "config get debug (after set)" 0 "false" -- \
		env "${DAEMON_ENV[@]}" ./bin/arweave config get debug

	run_check "config set unknown key returns error" 0 "error" -- \
		env "${DAEMON_ENV[@]}" ./bin/arweave config set xyzzy_smoke_no_such_key value
fi

# Shell-glue check: with no node running, `arweave config get` should
# exit with the expected "Node is not running!" message. Run this AFTER
# the daemon has been stopped.
daemon_stop

# Use a fresh env so erl_call targets a different (non-existent) node.
run_check "config get (no node)" 1 "Node is not running" -- \
	env ARNODE="arweave-smoke-noop@127.0.0.1" COOKIE="smoke-noop-$$" \
		./bin/arweave config get debug

# --- Summary --------------------------------------------------------

log ""
log "$(bold '=== Summary ===')"
log "  PASS: $PASS"
if [ "$FAIL" = "0" ]; then
	log "  FAIL: 0"
	log "$(green 'All smoke checks passed.')"
	exit 0
fi

if [ "$LAUNCH_ERRORS" -gt 0 ]; then
	log ""
	log "  $(red "LAUNCH ERRORS: $LAUNCH_ERRORS") — the release appears broken."
	log "  These tools couldn't even reach their Erlang entry point:"
	for n in "${LAUNCH_ERROR_NAMES[@]}"; do
		log "    - $n"
	done
	log "  Try: ./ar-rebar3 default release"
fi

log ""
log "  $(red FAIL): $FAIL"
for n in "${FAILED_NAMES[@]}"; do
	log "    - $n"
done
exit 1
