"use strict";

// Osero P&L dashboard — vanilla JS, no dependencies.
// Reads ../dist/headline.json and ../dist/series.json (written by the Rust indexer).
// All JSON content is treated as untrusted DATA: rendered via textContent only,
// numbers are coerced with Number() and guarded before use.

const HEADLINE_URL = "../dist/headline.json";
const SERIES_URL = "../dist/series.json";
const REFRESH_MS = 60000; // re-fetch once a minute so a running daemon shows through

// ---------- helpers ----------

const $ = (id) => document.getElementById(id);

// Safe finite number or null.
function num(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function fmtBps(x, dp = 1) {
  const n = num(x);
  return n === null ? "—" : n.toFixed(dp) + " bps";
}

// USDS with 2 decimals and explicit sign.
function fmtUsds(x) {
  const n = num(x);
  if (n === null) return "—";
  const sign = n > 0 ? "+" : n < 0 ? "−" : "";
  return sign + Math.abs(n).toFixed(2) + " USDS";
}

// Fraction (0.0206) -> percent string "2.06%".
function fmtPctFrac(x, dp = 2) {
  const n = num(x);
  return n === null ? "—" : (n * 100).toFixed(dp) + "%";
}

// Already-percent value (55.262) -> "55.26%".
function fmtPctVal(x, dp = 2) {
  const n = num(x);
  return n === null ? "—" : n.toFixed(dp) + "%";
}

function fmtTime(ts) {
  const n = num(ts);
  if (n === null) return "—";
  const d = new Date(n * 1000);
  if (isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, {
    year: "numeric", month: "short", day: "2-digit",
    hour: "2-digit", minute: "2-digit"
  });
}

function signClass(x) {
  const n = num(x);
  if (n === null) return "";
  return n > 0 ? "pos" : n < 0 ? "neg" : "";
}

function show(el) { el.classList.remove("hidden"); }
function hide(el) { el.classList.add("hidden"); }

// Build a small labelled card via DOM (no innerHTML with data).
function card(label, value, sub) {
  const c = document.createElement("div");
  c.className = "card";
  const l = document.createElement("div");
  l.className = "c-label"; l.textContent = label;
  const v = document.createElement("div");
  v.className = "c-val"; v.textContent = value;
  c.appendChild(l); c.appendChild(v);
  if (sub) {
    const s = document.createElement("div");
    s.className = "c-sub"; s.textContent = sub;
    c.appendChild(s);
  }
  return c;
}

// ---------- notices ----------

function showNotice(title, htmlSafeBody, isError) {
  const n = $("notice");
  n.className = "notice" + (isError ? " error" : "");
  n.replaceChildren();
  const h = document.createElement("h2");
  h.textContent = title;
  n.appendChild(h);
  const p = document.createElement("p");
  p.append(...htmlSafeBody); // array of nodes/strings (all app-controlled)
  n.appendChild(p);
  show(n);
  hide($("content"));
}

function codeEl(t) { const c = document.createElement("code"); c.textContent = t; return c; }

// ---------- rendering ----------

function renderHeadline(h) {
  const makingMoney = h.making_money === true;
  const tile = $("headline");
  tile.classList.toggle("win", makingMoney);
  tile.classList.toggle("lose", !makingMoney);

  $("verdict").textContent = makingMoney ? "YES" : "NO";

  const marginEl = $("net-margin-bps");
  marginEl.textContent = fmtBps(h.net_margin_bps);
  marginEl.className = "fig-val " + signClass(h.net_margin_bps);

  const netEl = $("cum-net-big");
  netEl.textContent = fmtUsds(h.cum_net);
  netEl.className = "fig-val " + signClass(h.cum_net);

  // As-of line
  const block = num(h.as_of_block);
  const parts = [];
  if (block !== null) parts.push("block " + block.toLocaleString());
  parts.push(fmtTime(h.as_of_ts));
  $("asof").textContent = "as of " + parts.join(" · ");
}

function renderInputs(h) {
  const inp = (h && h.inputs) || {};
  const box = $("inputs");
  box.replaceChildren();
  box.appendChild(card("Supply APY", fmtPctFrac(inp.supply_apy)));
  box.appendChild(card("Utilization", fmtPctFrac(inp.util)));
  box.appendChild(card("SSR", fmtPctFrac(inp.ssr)));
  box.appendChild(card("Base rate", fmtPctFrac(inp.base_rate)));
}

function renderPnl(h) {
  const rows = [
    ["Revenue", fmtUsds(h.cum_revenue), signClass(h.cum_revenue)],
    ["Cost", fmtUsds(h.cum_cost), ""],
    ["Net", fmtUsds(h.cum_net), signClass(h.cum_net)],
  ];
  const t = $("pnl");
  t.replaceChildren();
  for (const [k, v, cls] of rows) {
    const tr = document.createElement("tr");
    const tdk = document.createElement("td");
    tdk.className = "k"; tdk.textContent = k;
    const tdv = document.createElement("td");
    tdv.className = "v " + cls; tdv.textContent = v;
    tr.appendChild(tdk); tr.appendChild(tdv);
    t.appendChild(tr);
  }
}

function renderBreakeven(h) {
  const be = (h && h.break_even) || {};
  const box = $("breakeven");
  box.replaceChildren();
  // SSR needs to fall, venue supply needs to rise, util needs to reach target.
  box.appendChild(card("SSR", "− " + fmtBps(be.ssr_bps), "SSR drops by this much"));
  box.appendChild(card("Venue supply APY", "+ " + fmtBps(be.supply_bps), "supply rises by this much"));
  box.appendChild(card("Utilization", "→ " + fmtPctVal(be.util_pct), "util reaches this level"));
}

// ---------- chart (canvas, zero baseline, axis ticks) ----------

function drawChart(series) {
  const canvas = $("chart");
  const emptyEl = $("chart-empty");

  // Pick metric: inst_margin_bps preferred, else cum_net.
  let metricKey = "inst_margin_bps";
  let usable = series
    .map((p) => ({ ts: num(p && p.ts), y: num(p && p.inst_margin_bps) }))
    .filter((p) => p.ts !== null && p.y !== null);

  if (usable.length < 2) {
    metricKey = "cum_net";
    usable = series
      .map((p) => ({ ts: num(p && p.ts), y: num(p && p.cum_net) }))
      .filter((p) => p.ts !== null && p.y !== null);
  }

  $("chart-metric-label").textContent =
    "(" + (metricKey === "inst_margin_bps" ? "inst_margin_bps" : "cum_net USDS") + ")";

  if (usable.length < 2) {
    hide(canvas);
    show(emptyEl);
    return;
  }
  hide(emptyEl);
  show(canvas);

  usable.sort((a, b) => a.ts - b.ts);

  const ctx = canvas.getContext("2d");
  // Render at 2x for crispness on hidpi; CSS scales width down.
  const cssW = 900, cssH = 320, scale = 2;
  canvas.width = cssW * scale;
  canvas.height = cssH * scale;
  ctx.scale(scale, scale);
  ctx.clearRect(0, 0, cssW, cssH);

  const padL = 64, padR = 16, padT = 16, padB = 34;
  const plotW = cssW - padL - padR;
  const plotH = cssH - padT - padB;

  const xs = usable.map((p) => p.ts);
  const ys = usable.map((p) => p.y);
  let xMin = Math.min(...xs), xMax = Math.max(...xs);
  let yMin = Math.min(...ys, 0), yMax = Math.max(...ys, 0); // always include zero
  if (xMax === xMin) xMax = xMin + 1;
  // Pad y range a touch.
  const yPad = (yMax - yMin) * 0.08 || 1;
  yMin -= yPad; yMax += yPad;

  const xToPx = (t) => padL + ((t - xMin) / (xMax - xMin)) * plotW;
  const yToPx = (v) => padT + (1 - (v - yMin) / (yMax - yMin)) * plotH;

  const css = getComputedStyle(document.documentElement);
  const cLine = css.getPropertyValue("--line").trim() || "#2d3742";
  const cMuted = css.getPropertyValue("--muted").trim() || "#8b98a6";
  const cGreen = css.getPropertyValue("--green").trim() || "#2ec26a";
  const cRed = css.getPropertyValue("--red").trim() || "#ff5a5f";
  const lastY = ys[ys.length - 1];
  const lineColor = lastY >= 0 ? cGreen : cRed;

  ctx.font = "11px system-ui, sans-serif";
  ctx.textBaseline = "middle";

  // Horizontal gridlines + y ticks (5 divisions).
  ctx.strokeStyle = cLine;
  ctx.fillStyle = cMuted;
  ctx.lineWidth = 1;
  const yTicks = 5;
  for (let i = 0; i <= yTicks; i++) {
    const v = yMin + (i / yTicks) * (yMax - yMin);
    const py = yToPx(v);
    ctx.beginPath();
    ctx.moveTo(padL, py); ctx.lineTo(padL + plotW, py);
    ctx.stroke();
    ctx.textAlign = "right";
    ctx.fillText(v.toFixed(1), padL - 8, py);
  }

  // Zero baseline — emphasised.
  const zeroPx = yToPx(0);
  ctx.strokeStyle = cMuted;
  ctx.lineWidth = 1.5;
  ctx.setLineDash([4, 3]);
  ctx.beginPath();
  ctx.moveTo(padL, zeroPx); ctx.lineTo(padL + plotW, zeroPx);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.textAlign = "left";
  ctx.fillStyle = cMuted;
  ctx.fillText("0", padL + plotW - 12, zeroPx - 8);

  // X ticks (dates) — up to 5.
  ctx.textAlign = "center";
  const xTicks = Math.min(5, usable.length);
  for (let i = 0; i < xTicks; i++) {
    const t = xMin + (i / (xTicks - 1 || 1)) * (xMax - xMin);
    const px = xToPx(t);
    const d = new Date(t * 1000);
    const label = isNaN(d.getTime())
      ? "" : d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
    ctx.fillText(label, px, cssH - padB + 14);
  }

  // Fill under/over relative to zero (subtle).
  ctx.beginPath();
  ctx.moveTo(xToPx(usable[0].ts), zeroPx);
  for (const p of usable) ctx.lineTo(xToPx(p.ts), yToPx(p.y));
  ctx.lineTo(xToPx(usable[usable.length - 1].ts), zeroPx);
  ctx.closePath();
  ctx.fillStyle = (lineColor === cGreen ? "rgba(46,194,106,0.10)" : "rgba(255,90,95,0.10)");
  ctx.fill();

  // The line.
  ctx.strokeStyle = lineColor;
  ctx.lineWidth = 2;
  ctx.lineJoin = "round";
  ctx.beginPath();
  usable.forEach((p, i) => {
    const px = xToPx(p.ts), py = yToPx(p.y);
    if (i === 0) ctx.moveTo(px, py); else ctx.lineTo(px, py);
  });
  ctx.stroke();

  // Last-point marker.
  const last = usable[usable.length - 1];
  ctx.fillStyle = lineColor;
  ctx.beginPath();
  ctx.arc(xToPx(last.ts), yToPx(last.y), 3.5, 0, Math.PI * 2);
  ctx.fill();
}

// ---------- load ----------

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error("HTTP " + res.status + " for " + url);
  return res.json();
}

async function load() {
  let headline, series;

  // Headline is required for the page to mean anything.
  try {
    headline = await fetchJson(HEADLINE_URL);
  } catch (err) {
    showNotice(
      "No data yet — run the indexer first",
      [
        "Couldn't read ", codeEl("dist/headline.json"),
        ". Start the indexer to generate it, then reload. (",
        codeEl(String(err.message || err)), ")",
      ],
      false
    );
    return;
  }

  if (!headline || typeof headline !== "object") {
    showNotice("Unexpected data", ["headline.json did not contain an object."], true);
    return;
  }

  // Series is optional — chart degrades gracefully if absent.
  try {
    series = await fetchJson(SERIES_URL);
  } catch {
    series = [];
  }
  if (!Array.isArray(series)) series = [];

  hide($("notice"));
  show($("content"));

  renderHeadline(headline);
  renderInputs(headline);
  renderPnl(headline);
  renderBreakeven(headline);
  drawChart(series);

  $("refresh-note").textContent = "auto-refreshes every " + (REFRESH_MS / 1000) + "s";
}

load();
setInterval(load, REFRESH_MS);
