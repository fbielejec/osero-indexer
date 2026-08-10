use crate::types::*;

/// Scaled amount in USDS token units = (amount / WAD) * RAY / index_ray.
///
/// `amount` is RAW wei (u128, exact); dividing by WAD converts it to whole-token
/// units. `index_ray` is a RAY-scaled ratio and is NOT divided by WAD.
pub fn scaled(amount: u128, index_ray: u128) -> f64 {
    amount as f64 / WAD * RAY / index_ray as f64
}

/// cum_revenue = position − net_principal, both in USDS token units.
pub fn revenue(events: &[SupplyEvent], live_index_ray: u128) -> f64 {
    let mut scaled_bal = 0.0;
    let mut net_principal = 0.0;
    for e in events {
        let s = scaled(e.amount, e.index_ray);
        // Money terms enter f64 as USDS tokens (raw wei ÷ WAD).
        let principal = e.amount as f64 / WAD;
        match e.kind {
            SupplyKind::Deposit => {
                scaled_bal += s;
                net_principal += principal;
            }
            SupplyKind::Withdraw => {
                scaled_bal -= s;
                net_principal -= principal;
            }
        }
    }
    scaled_bal * live_index_ray as f64 / RAY - net_principal
}

/// One constant-parameter time slice of the position.
pub struct Segment {
    pub dt: f64,
    /// USDS token units (the fold converts raw wei → tokens by dividing by WAD).
    pub principal: f64,
    pub util: f64,
    pub base_rate: f64,
}

/// Borrow cost integrated over the segments: principal * util * base_rate * dt / year.
pub fn cost(segments: &[Segment]) -> f64 {
    segments
        .iter()
        .map(|s| s.principal * s.util * s.base_rate * s.dt / SECONDS_PER_YEAR)
        .sum()
}

/// Net principal (USDS token units) from supply events whose ts ≤ `t` (deposits
/// add, withdrawals subtract). Raw wei amounts are converted by dividing by WAD.
fn net_principal_at(supply: &[SupplyEvent], t: u64) -> f64 {
    let mut np = 0.0;
    for e in supply.iter().filter(|e| e.ts <= t) {
        match e.kind {
            SupplyKind::Deposit => np += e.amount as f64 / WAD,
            SupplyKind::Withdraw => np -= e.amount as f64 / WAD,
        }
    }
    np
}

/// Latest reserve update with ts ≤ `t` (tie-break on log_index).
fn latest_reserve_at(reserves: &[ReserveUpdate], t: u64) -> Option<&ReserveUpdate> {
    reserves
        .iter()
        .filter(|r| r.ts <= t)
        .max_by_key(|r| (r.ts, r.log_index))
}

/// Latest ssr change with ts ≤ `t`.
fn latest_ssr_at(ssr: &[SsrChange], t: u64) -> Option<&SsrChange> {
    ssr.iter().filter(|s| s.ts <= t).max_by_key(|s| s.ts)
}

/// Max block among all events (any kind) with ts ≤ `t`; 0 if none.
fn block_at(supply: &[SupplyEvent], ssr: &[SsrChange], reserves: &[ReserveUpdate], t: u64) -> u64 {
    let a = supply.iter().filter(|e| e.ts <= t).map(|e| e.block);
    let b = ssr.iter().filter(|e| e.ts <= t).map(|e| e.block);
    let c = reserves.iter().filter(|e| e.ts <= t).map(|e| e.block);
    a.chain(b).chain(c).max().unwrap_or(0)
}

/// Fold raw event streams into a cumulative accrual series (one point per segment).
///
/// Index-selection assumption (documented per plan §3): `revenue` values the scaled
/// aToken balance at a "current index". We only observe the liquidity index at supply
/// events (embedded `index_ray`) and — for the present moment — via `live_index_ray`.
/// For the FINAL breakpoint we use `live_index_ray`; for INTERMEDIATE breakpoints we use
/// the most recent supply event's `index_ray` at/before that breakpoint (fallback:
/// `live_index_ray` if the position opened with no prior index observation). This makes
/// intermediate `cum_revenue` a lower bound between supply actions; the constant golden
/// scenario is unaffected because there is a single final segment. Exact intra-history
/// index reconstruction is refined later against live archive reads.
pub fn fold(
    supply: &[SupplyEvent],
    ssr: &[SsrChange],
    reserves: &[ReserveUpdate],
    now_ts: u64,
    live_index_ray: u128,
) -> Vec<SeriesPoint> {
    // Inception = first supply event ts. No supply → no position → empty series.
    let inception = match supply.iter().map(|e| e.ts).min() {
        Some(t) => t,
        None => return Vec::new(),
    };

    // Sorted, deduped breakpoints ≥ inception, including now_ts.
    let mut breakpoints: Vec<u64> = supply
        .iter()
        .map(|e| e.ts)
        .chain(ssr.iter().map(|e| e.ts))
        .chain(reserves.iter().map(|e| e.ts))
        .chain(std::iter::once(now_ts))
        // Only breakpoints within the observed window [inception, now_ts]: an event ts
        // beyond now_ts must not become the final segment valued at live_index_ray.
        .filter(|&t| t >= inception && t <= now_ts)
        .collect();
    breakpoints.sort_unstable();
    breakpoints.dedup();

    let last_bp = *breakpoints
        .last()
        .expect("inception guarantees ≥1 breakpoint");

    let mut out = Vec::new();
    let mut cum_cost = 0.0;

    for w in breakpoints.windows(2) {
        let (t_i, t_next) = (w[0], w[1]);
        let dt = (t_next - t_i) as f64;

        // Segment parameters snapshot at t_i (left edge, held constant over [t_i, t_next)).
        let principal = net_principal_at(supply, t_i);
        let (util, supply_apy) = match latest_reserve_at(reserves, t_i) {
            Some(r) => (r.util_ray as f64 / RAY, apr_ray_to_apy(r.liq_rate_ray)),
            None => (0.0, 0.0),
        };
        let base_rate =
            latest_ssr_at(ssr, t_i).map(|s| s.ssr_apy).unwrap_or(0.0) + BASE_RATE_SPREAD;

        cum_cost += cost(&[Segment {
            dt,
            principal,
            util,
            base_rate,
        }]);

        // Revenue valued at t_next (right edge = breakpoint we emit at).
        let is_final = t_next == last_bp;
        let index_at = if is_final {
            live_index_ray
        } else {
            supply
                .iter()
                .filter(|e| e.ts <= t_next)
                .max_by_key(|e| (e.ts, e.log_index))
                .map(|e| e.index_ray)
                .unwrap_or(live_index_ray)
        };
        let events_upto: Vec<SupplyEvent> =
            supply.iter().filter(|e| e.ts <= t_next).cloned().collect();
        let cum_revenue = revenue(&events_upto, index_at);

        let inst_margin_bps = (supply_apy - util * base_rate) * 1e4;
        let net_principal_next = net_principal_at(supply, t_next);

        out.push(SeriesPoint {
            ts: t_next,
            block: block_at(supply, ssr, reserves, t_next),
            position: net_principal_next + cum_revenue,
            supply_apy,
            util,
            base_rate,
            inst_margin_bps,
            cum_revenue,
            cum_cost,
            cum_net: cum_revenue - cum_cost,
        });
    }

    out
}
