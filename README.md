# Is Osero making money?

**No. It is losing roughly 25 basis points, structurally.**

Osero is a "Star" in the Sky ecosystem. Sky extends it a USDS credit line; Osero draws
against that line, deploys the USDS into a yield strategy, and owes Sky a **base rate of
SSR + 20 bps** on the portion of that capital actually borrowed downstream.

This repo reconstructs that P&L from the chain -- starting from two anchors and nothing
else -- and answers the question in basis points: right now, cumulatively, and over time.

```
                    revenue                         cost
   Osero  ──draws USDS──►  SparkLend USDS  ──owes Sky──►  SSR + 20 bps
          1,001,000        supply 2.056%                  3.720%
                                                          × 61.99% utilization
                                                        = 2.306%

                              net margin = −25 bps
```

The finding underneath the number: Osero borrows from Sky at SSR + 20 bps and lends into
**SparkLend, which is also Sky**, at a supply rate below that base. Both legs sit in the
same ecosystem and the spread between them is negative. It is not a bad entry that a better
one would have avoided -- at current rates the trade does not work at *any* utilization the
venue can reach, and no SPK rewards reach the position to offset it.

|                |                                       |
|----------------|---------------------------------------|
| Position       | 1,001,875 USDS supplied to SparkLend  |
| Debt to Sky    | 1,001,000 USDS (`ALLOCATOR-PRYSM-A`)  |
| Net margin     | **−24.99 bps**                        |
| Cumulative P&L | **−116.67 USDS** over 15.9 days       |
| Break-even     | SSR −40 bps, *or* supply rate +25 bps |

*(Figures from a live run at block 25,715,376, they move)*

---

## Three ways in

| If you want to…                                              | Go to                                                                   | Takes                                  |
|--------------------------------------------------------------|-------------------------------------------------------------------------|----------------------------------------|
| **Read the reasoning**, anchors → answer, with runnable code | [`DISCOVERY.ipynb`](DISCOVERY.ipynb) / [`DISCOVERY.org`](DISCOVERY.org) | Audits how each number was established |
| **Run the production indexer** and watch a live dashboard    | [Quickstart](#quickstart) below                                         | 5 min setup + backfill                 |

---

## Quickstart

### 1. Prerequisites

- **Rust stable ≥ 1.85** (the crate is edition 2024)
- **python3** -- only to serve the static dashboard
- An **HTTPS Ethereum mainnet RPC with archive reads**. Infura/Alchemy free tiers work,
  but will rate-limit the archive-heavy backfill -- see [notes](#a-note-on-rpc-rate-limits).

### 2. Configure

The indexer reads its RPC URL from `.env` file:

```sh
cd osero-indexer
cp .env.example .env
$EDITOR .env          # set ETH_RPC_URL to your HTTPS mainnet RPC
```

### 3. Run the indexer

```sh
make build          # cargo build --release
make index          # run the daemon (needs `make build` first)
```

`make index` backfills from the position's inception block (25,601,435), then keeps running
and follows the chain head, appending new events and re-exporting as blocks finalize.
**Leave it running.**

> Because the position is small and young, most of the wall-clock time goes to **historical
> archive state reads**. Expect the first backfill to take a while.

### 4. Get the outputs

The daemon writes two JSON files into `osero-indexer/dist/`, regenerated on every advance:

**`headline.json`** -- the answer, and the inputs that produced it:

```json
{
  "as_of_block": 25682893,
  "making_money": false,
  "net_margin_bps": -24.98485801434646,
  "cum_revenue": 632.86,
  "cum_cost": 716.83,
  "cum_net": -83.97,
  "inputs":     { "supply_apy": 0.02056, "util": 0.61981,
                  "ssr": 0.03520, "base_rate": 0.03720 },
  "break_even": { "ssr_bps": 40.31, "supply_bps": 24.98, "util_pct": 55.27 }
}
```

**`series.json`** -- one row per sample over the position's life, for the charts:

```json
{ "ts": 1784882159, "block": 25601439, "position": 1000000.0,
  "supply_apy": 0.0205, "util": 0.6189, "base_rate": 0.0372,
  "inst_margin_bps": -25.24,
  "cum_revenue": 0.0, "cum_cost": 0.035, "cum_net": -0.035 }
```

Both are plain static files -- pipe them into `jq`, load them in a notebook, or serve them
to the dashboard.

### 5. View the dashboard

In a **second terminal**, with the indexer still running:

```sh
cd osero-indexer
make dashboard      # python3 -m http.server 8091
```

Then open **<http://localhost:8091/dashboard/>**.

The dashboard is dependency-free static HTML/JS. It fetches `../dist/headline.json` and
`../dist/series.json`, so it **must be served from the crate root** -- which is exactly what
`make dashboard` does. Opening `index.html` directly off the filesystem will fail on CORS.

It shows the verdict and margin headline, the current rate inputs, cumulative P&L, net
margin over time, and the break-even conditions.

### 6. Smoke Test

```sh
make smoke          # feature-gated live test against a pinned block (needs .env)
```

The smoke test pins block 25,682,000 and asserts the pipeline reproduces −24.98 bps from
real chain state.

### Other targets

```sh
make help           # list every target with its description
make clean          # remove target/, the SQLite store, and dist/*.json
```

### A note on RPC rate limits

The backfill does two archive `totalSupply` reads per SparkLend `ReserveDataUpdated` event,
which **will** trip HTTP 429 on a free tier. The daemon paces and retries rather than
dying, but you can slow it further:

```sh
OSERO_RPC_INTERVAL_MS=1000 make index    # default is 400 ms between calls
```

> The real fix is a dedicated archive endpoint via `ETH_ARCHIVE_URL`.

### Configuration reference

Everything is `clap`-driven and settable by **env var or flag**; flags override.

| Env var / flag                      | Default                     | Purpose                                    |
|-------------------------------------|-----------------------------|--------------------------------------------|
| `ETH_RPC_URL` / `--rpc-url`         | *(required)*                | HTTPS mainnet RPC (reads + archive)        |
| `ETH_ARCHIVE_URL` / `--archive-url` | falls back to `ETH_RPC_URL` | Dedicated archive endpoint                 |
| `POSITION_PATH` / `--position-path` | `position.toml`             | Position / contract registry               |
| `DB_PATH` / `--db-path`             | `osero.db`                  | SQLite event-store path                    |
| `EXPORT_DIR` / `--export-dir`       | `dist`                      | Output dir for dashboard JSON              |
| `SYNC_STEP` / `--sync-step`         | `5000`                      | Blocks per `getLogs` chunk during backfill |
| `CONFIRMATIONS` / `--confirmations` | `12`                        | Confirmations before a block is final      |
| `OSERO_RPC_INTERVAL_MS`             | `400`                       | Min ms between RPC calls (env only)        |

---

## Running the notebooks

`DISCOVERY.org` and `DISCOVERY.ipynb` are the same analysis in two formats. They depend on
only `requests` and `matplotlib` -- no web3, no eth-abi, no Foundry, with Keccak-256 and ABI
coding done by hand so nothing can quietly supply an answer. They need `ETH_RPC_URL` in the
**root** `.env`.

Outputs are already embedded from a live run, so both are readable without executing
anything. To re-run: open the `.ipynb` in Jupyter, or evaluate the `.org` blocks in Emacs
with `jupyter-python` (`C-c C-v C-b`). Both use the `llms_kernel` kernelspec -- change it in
the header if yours is named differently.

---

## Repo map

```
.
├── README.md                  ← you are here
├── DISCOVERY.org              Executable notebook (Emacs org + jupyter-python)
├── DISCOVERY.ipynb            Same analysis, terse, as a Jupyter notebook
├── images/                    Generated charts (osero_pnl.png)
│
├── osero-indexer/             Layer 2 -- production indexer (Rust)
    ├── README.md                Indexer-specific detail
    ├── Makefile                 build / test / smoke / index / dashboard / clean
    ├── position.toml            The position + contract registry
    ├── .env.example             Copy to .env, add your RPC URL
    ├── migrations/              SQLite schema
    ├── src/
    │   ├── main.rs                Supervisor: JoinSet + circuit breaker + backoff
    │   ├── cursor.rs              Where to resume
    │   ├── listener.rs            getLogs + archive reads → decoded events
    │   ├── ingest.rs              Transactional writes to the event store
    │   ├── exporter.rs            events → accrual fold → dist/*.json
    │   ├── accrual.rs             The pure fold -- most unit tests live here
    │   ├── model.rs               SQLite store: schema, inserts, reads
    │   ├── rpc.rs                 Pacing + retry-with-backoff (survives 429s)
    │   └── …                      config, connections, contracts, position, types, error
    ├── tests/smoke.rs           Live pinned-block test (--features live-smoke)
    ├── dashboard/               Static index.html + app.js + style.css
    └── dist/                    Generated: headline.json + series.json

```

---

## Caveats worth knowing

- **Single position, single venue, by design.** Where it would need work at 100× -- reflexivity,
  per-venue revenue folds, per-chain parameterization.
- **SSR is modelled as a time series but has not moved** over this position's life. The last
  change (block 25,596,101) preceded the first deposit. The machinery is built and general;
  it simply does not bite yet.
- **The position is young and small.** ~16 days old, 1.001M USDS, 0.14% of the SparkLend
  USDS reserve. Cumulative P&L is a thin, early sample -- the sign is driven by the current
  spot spread rather than a long history, though that spread has not crossed zero once.  
- **The base-rate compounding convention is unspecified** by the brief. Simple vs. compounded
  moves the margin by ~4 bps (−23 to −27 bps). Negligible over 16 days, material over a
  year. Every convention is negative; the sign is robust, the magnitude carries a band.
