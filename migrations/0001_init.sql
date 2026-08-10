CREATE TABLE IF NOT EXISTS positions (
  id INTEGER PRIMARY KEY,
  ilk TEXT NOT NULL, vault TEXT NOT NULL, buffer TEXT NOT NULL, proxy TEXT NOT NULL,
  venue_pool TEXT NOT NULL, venue_atoken TEXT NOT NULL, venue_vdebt TEXT NOT NULL,
  inception_block INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS cursor (
  position_id INTEGER PRIMARY KEY REFERENCES positions(id),
  last_block INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS debt_events (
  position_id INTEGER NOT NULL REFERENCES positions(id),
  block INTEGER NOT NULL, ts INTEGER NOT NULL, log_index INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('Draw','Wipe')),
  wad TEXT NOT NULL,
  UNIQUE(position_id, block, log_index)
);
CREATE TABLE IF NOT EXISTS supply_events (
  position_id INTEGER NOT NULL REFERENCES positions(id),
  block INTEGER NOT NULL, ts INTEGER NOT NULL, log_index INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('Deposit','Withdraw')),
  amount TEXT NOT NULL, index_ray TEXT NOT NULL,
  UNIQUE(position_id, block, log_index)
);
CREATE TABLE IF NOT EXISTS ssr_changes (
  block INTEGER NOT NULL, ts INTEGER NOT NULL, log_index INTEGER NOT NULL,
  ssr_ray TEXT NOT NULL, ssr_apy REAL NOT NULL,
  UNIQUE(block, log_index)
);
CREATE TABLE IF NOT EXISTS reserve_updates (
  position_id INTEGER NOT NULL REFERENCES positions(id),
  block INTEGER NOT NULL, ts INTEGER NOT NULL, log_index INTEGER NOT NULL,
  liq_rate_ray TEXT NOT NULL, borrow_rate_ray TEXT NOT NULL,
  total_supplied TEXT NOT NULL, total_borrowed TEXT NOT NULL, util_ray TEXT NOT NULL,
  UNIQUE(position_id, block, log_index)
);
-- No `series` table: the accrual series is a derived projection recomputed by the
-- Exporter from the event tables and emitted as dist/series.json, not persisted.
