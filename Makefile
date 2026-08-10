# osero-indexer — build / run / test / dashboard
#
# Targets that touch the chain or the dashboard auto-export .env first.
# NOTE: .env uses plain KEY=value lines, so we `set -a` before sourcing it,
# a bare `. ./.env` would set the vars in the shell but NOT export them to
# the binary. Copy .env.example -> .env and drop in your RPC URL first.

.PHONY: help
help: # Show help for each of the Makefile recipes
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

.PHONY: watch
watch: # run check watcher wihtout generating bytecode
	cargo watch -s "cargo check" -c

.PHONY: build
build: # Compile the release binary
	cargo build --release

# .PHONY: test
# test: # 18 offline unit tests. No network, no .env required.
# 	cargo test

.PHONY: smoke
smoke: # Feature-gated live smoke test -- hits the RPC at a pinned block. Needs .env.
	set -a; . ./.env; set +a; cargo test --features live-smoke --test smoke -- --nocapture

.PHONY: index
index: # Build, then run the daemon: backfills from inception, then follows head. Pace RPC calls under a free-tier rate limit by prefixing the knob, e.g.: OSERO_RPC_INTERVAL_MS=1000 make index
	set -a; . ./.env; set +a; ./target/release/osero-indexer

.PHONY: dashboard
dashboard: # Serve the crate root so the dashboard can fetch ../dist/*.json. Then open http://localhost:8091/dashboard/
	python3 -m http.server 8091

.PHONY: clean
clean: # Remove build output, the SQLite store, and exported JSON.
	rm -rf target
	rm -f osero.db osero.db-shm osero.db-wal
	rm -f dist/*.json
