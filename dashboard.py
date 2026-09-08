"""
Read-only dashboard voor de signal-bot. Leest alleen uit bot_history.db
(geschreven door main.py/executor.py via db.py) en doet live, read-only
info-calls naar Hyperliquid (open posities, PnL) -- doet zelf nooit iets op
Telegram of Hyperliquid, plaatst of wijzigt geen orders.

Draait BEWUST alleen op met naam genoemde interfaces (127.0.0.1 voor de
bestaande SSH-tunnel-route, plus het Tailscale-IP voor toegang via het
tailnet) -- NOOIT op 0.0.0.0, want dat zou 'm ook voor het publieke internet
openzetten. Geen auth, toont trade-details, dus dit mag nooit publiek
bereikbaar zijn.

    python dashboard.py
"""
import subprocess
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template_string, request

import db
from executor import _get_info, get_owner_address

app = Flask(__name__)

PORT = 8787

# Dag dat de huidige 5-staps TP-ladder (elk target sluit z'n eigen cumulatieve
# %) live ging -- zie tp_events in bot_history.db: vanaf hier verschijnen
# tp1_event_closed_be_moved/tp2/tp3/tp4_event_closed i.p.v. het oude
# tp1_filled_be_moved-only-gedrag. Gebruikt als sneltoets in het periode-filter.
LADDER_STRATEGY_START = "2026-08-17"


def get_open_positions_and_pnl() -> dict:
    """Open live posities + ongerealiseerd resultaat, rechtstreeks uit
    Hyperliquid's clearinghouseState (niet de lokale db, die kent geen
    closes/fills)."""
    result = {"positions": [], "unrealized_total": 0.0, "error": None}
    try:
        owner = get_owner_address()
        state = _get_info().user_state(owner)
        for ap in state.get("assetPositions", []):
            p = ap["position"]
            pnl = float(p["unrealizedPnl"])
            result["positions"].append({
                "coin": p["coin"],
                "side": "Long" if float(p["szi"]) > 0 else "Short",
                "size": abs(float(p["szi"])),
                "entry_px": float(p["entryPx"]) if p.get("entryPx") else None,
                "liq_px": float(p["liquidationPx"]) if p.get("liquidationPx") else None,
                "leverage": p.get("leverage", {}).get("value"),
                "unrealized_pnl": pnl,
            })
            result["unrealized_total"] += pnl
    except Exception as e:
        result["error"] = str(e)
    return result


PNL_NEAR_ZERO_USD = 1.0  # zie get_pnl_cutoff()


def get_pnl_cutoff() -> dict:
    """
    Bepaalt vanaf welk moment gerealiseerde PnL relevant is voor "huidig
    gebruik": het laatste moment dat de account-waarde (bijna) nul was, vlak
    vóór de recentste storting(en).

    Nodig gebleken (2026-08-10, zelf ontdekt via een gebruikersvraag over een
    inconsistentie): dit account heeft oudere, allang afgewikkelde
    trading-historie van vóór de huidige stortingen ($5.81 + $23.66). Zonder
    filter telde "laatste 20 closes" grotendeels fills van 17 juni-8 juli mee
    -- weken oud, en al lang niet meer terug te vinden in het huidige saldo
    (het account stond vlak voor de storting op $0.0013). Nagerekend:
    saldo nu ($29.37) = stortingen ($29.47) - fees/PnL van ALLEEN de fills ná
    dit cutoff-moment. Dat klopte, dus dit is de juiste afbakening.

    Gebruikt Hyperliquid's portfolio()-endpoint (accountValueHistory, "day"-
    bucket = fijnste resolutie) i.p.v. een hardcoded datum, zodat dit blijft
    kloppen als het account ooit weer drooggelegd en opnieuw gefund wordt.
    """
    try:
        owner = get_owner_address()
        portfolio = _get_info().portfolio(owner)
        day_bucket = next((bucket for label, bucket in portfolio if label == "day"), None)
        history = day_bucket.get("accountValueHistory", []) if day_bucket else []
        cutoff_ms = None
        for ts, value in history:
            if float(value) < PNL_NEAR_ZERO_USD:
                cutoff_ms = ts
        return {"cutoff_ms": cutoff_ms, "error": None}
    except Exception as e:
        return {"cutoff_ms": None, "error": str(e)}


def _parse_date_range(from_str: str, to_str: str) -> tuple:
    """Zet from/to (YYYY-MM-DD uit de datepickers) om naar UTC-ms-grenzen.
    `from` = 00:00:00 van die dag, `to` = 23:59:59 van die dag (inclusief)."""
    start_ms = end_ms = None
    error = None
    try:
        if from_str:
            start_ms = int(
                datetime.strptime(from_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000
            )
        if to_str:
            end_ms = int(
                (datetime.strptime(to_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp()
                * 1000
            ) - 1
    except ValueError:
        error = f"ongeldige datum ({from_str!r} / {to_str!r}), verwacht YYYY-MM-DD"
        start_ms = end_ms = None
    return start_ms, end_ms, error


TRADE_LEG_GROUP_WINDOW_MS = 3000  # zie get_realized_trades()


def get_realized_trades(cutoff_ms, end_ms=None, limit: int = 60) -> dict:
    """
    Groepeert Hyperliquid's losse fills (user_fills()) tot herkenbare
    'trades': 1 of meer 'Open <coin>'-fills (de entry, evt. in meerdere
    prijs-fills), gevolgd door de losse cash-out-momenten (TP-groepsbericht,
    break-even-SL, cancel) tot de positie weer plat is. Close-fills die
    binnen TRADE_LEG_GROUP_WINDOW_MS van elkaar vallen worden als 1 "leg"
    samengevoegd -- 1 TP-groepsbericht kan in meerdere prijs-fills uiteenvallen
    (zie de PENGU/HYPE-fills die zelf onderzocht zijn: meerdere fills binnen
    dezelfde seconde per TP-event).

    De trade-STRUCTUUR (welke fill bij welke trade hoort, en de entry-prijs)
    wordt op de VOLLEDIGE historie opgebouwd, ongeacht cutoff_ms/end_ms --
    anders klopt de entry-prijs niet meer voor een trade die vóór de gekozen
    periode begon maar er middenin een TP raakte. cutoff_ms/end_ms filteren
    alleen welke LEGS getoond en opgeteld worden; een trade zonder legs in de
    gekozen periode wordt overgeslagen (dit is bewust een "winsten"-overzicht,
    geen trade-log -- een net geopende trade zonder cash-out hoort hier niet
    in thuis, zie tab Open posities daarvoor).

    Fee-conventie: alleen fees van CLOSE-fills tellen mee (net als de vorige,
    niet-gegroepeerde versie van dit dashboard) -- entry-fees niet, zodat het
    "netto verdiend"-totaal bovenaan niet stilzwijgend verandert t.o.v. eerder.
    """
    result = {"trades": [], "realized_total": 0.0, "fees_total": 0.0, "leg_count": 0, "error": None}
    try:
        owner = get_owner_address()
        fills = sorted(_get_info().user_fills(owner), key=lambda f: f["time"])

        open_trades = {}   # coin -> trade-in-opbouw
        all_trades = []    # chronologische volgorde van start

        for f in fills:
            coin = f.get("coin")
            direction = f.get("dir", "") or ""
            sz = float(f["sz"])
            px = float(f["px"])
            pnl = float(f.get("closedPnl", 0) or 0)
            fee = float(f.get("fee", 0) or 0)
            ts = f["time"]

            if direction.startswith("Open"):
                side = "Long" if "Long" in direction else "Short"
                t = open_trades.get(coin)
                if t is None or t["side"] != side:
                    t = {"coin": coin, "side": side, "entry_qty": 0.0,
                         "entry_notional": 0.0, "entry_time": ts, "legs": [], "closed_qty": 0.0}
                    open_trades[coin] = t
                    all_trades.append(t)
                t["entry_qty"] += sz
                t["entry_notional"] += sz * px
            elif direction.startswith("Close") and coin in open_trades:
                t = open_trades[coin]
                if t["legs"] and (ts - t["legs"][-1]["time"]) <= TRADE_LEG_GROUP_WINDOW_MS:
                    leg = t["legs"][-1]
                    leg["qty"] += sz
                    leg["notional"] += sz * px
                    leg["pnl"] += pnl
                    leg["fee"] += fee
                    leg["time"] = ts
                else:
                    t["legs"].append({"time": ts, "qty": sz, "notional": sz * px, "pnl": pnl, "fee": fee})
                t["closed_qty"] += sz
                if t["closed_qty"] >= t["entry_qty"] - 1e-9:
                    del open_trades[coin]

        for t in all_trades:
            for idx, leg in enumerate(t["legs"], start=1):
                leg["idx"] = idx

        for t in reversed(all_trades):  # nieuwste trade eerst
            entry_avg = t["entry_notional"] / t["entry_qty"] if t["entry_qty"] else 0.0
            legs_in_range = [
                leg for leg in t["legs"]
                if (cutoff_ms is None or leg["time"] >= cutoff_ms)
                and (end_ms is None or leg["time"] <= end_ms)
            ]
            if not legs_in_range:
                continue

            trade_out = {
                "coin": t["coin"], "side": t["side"], "entry_px": entry_avg,
                "entry_time": datetime.fromtimestamp(t["entry_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "still_open": t["closed_qty"] < t["entry_qty"] - 1e-9,
                "remaining_qty": max(t["entry_qty"] - t["closed_qty"], 0.0),
                "legs": [], "total_pnl": 0.0, "total_fee": 0.0,
            }
            for leg in legs_in_range:
                avg_px = leg["notional"] / leg["qty"] if leg["qty"] else 0.0
                net = leg["pnl"] - leg["fee"]
                trade_out["legs"].append({
                    "idx": leg["idx"],
                    "time": datetime.fromtimestamp(leg["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "qty": leg["qty"], "avg_px": avg_px, "pnl": leg["pnl"], "fee": leg["fee"], "net": net,
                })
                trade_out["total_pnl"] += leg["pnl"]
                trade_out["total_fee"] += leg["fee"]

            result["trades"].append(trade_out)
            result["realized_total"] += trade_out["total_pnl"]
            result["fees_total"] += trade_out["total_fee"]
            result["leg_count"] += len(trade_out["legs"])

            if len(result["trades"]) >= limit:
                break
    except Exception as e:
        result["error"] = str(e)
    return result


PAGE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="12">
<title>Signal Bot Dashboard</title>
<style>
  :root {
    --bg: #0c0d0f;
    --surface: #16171b;
    --surface-alt: #1c1e23;
    --border: #2a2c32;
    --text: #e4e4e7;
    --text-secondary: #9a9ba5;
    --text-muted: #6b6c76;
    --green: #4ade80;
    --green-bg: rgba(74, 222, 128, 0.12);
    --red: #f87171;
    --red-bg: rgba(248, 113, 113, 0.12);
    --amber: #fbbf24;
    --amber-bg: rgba(251, 191, 36, 0.12);
    --blue: #7aa2f7;
    --blue-bg: rgba(122, 162, 247, 0.12);
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    --font-mono: ui-monospace, "SF Mono", "Cascadia Mono", "JetBrains Mono", Consolas, monospace;
  }
  * { box-sizing: border-box; }
  body {
    font-family: var(--font);
    background: var(--bg);
    color: var(--text);
    margin: 0;
    padding: 0;
    line-height: 1.5;
  }
  .container { max-width: 1320px; margin: 0 auto; padding: 2.5rem 1.75rem 4rem; }

  header { margin-bottom: 2rem; }
  h1 { font-size: 1.375rem; font-weight: 600; margin: 0 0 0.25rem; letter-spacing: -0.01em; }
  .subtitle { color: var(--text-muted); font-size: 0.8rem; }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.5rem 1.75rem;
    margin-bottom: 1.75rem;
  }
  .card h2 {
    font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em;
    color: var(--text-secondary); margin: 0 0 1.25rem;
  }
  .card h2 .count { color: var(--text-muted); font-weight: 400; text-transform: none; letter-spacing: normal; }

  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1.75rem; }
  .stat .label { font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.35rem; }
  .stat .value { font-size: 1.05rem; font-weight: 500; }
  .stat .value.mono { font-family: var(--font-mono); }
  .stat .value.small { font-size: 0.82rem; font-family: var(--font-mono); word-break: break-all; color: var(--text-secondary); }
  .stat .sub { font-size: 0.72rem; color: var(--text-muted); margin-top: 0.2rem; }

  .pnl-big { font-size: 1.6rem; font-weight: 600; font-family: var(--font-mono); }
  .pnl-pos { color: var(--green); }
  .pnl-neg { color: var(--red); }
  .pnl-zero { color: var(--text-secondary); }

  /* --- Pills --- */
  .pill {
    display: inline-flex; align-items: center; gap: 0.35rem;
    padding: 0.2rem 0.65rem; border-radius: 999px;
    font-size: 0.72rem; font-weight: 600; letter-spacing: 0.02em;
    border: 1px solid transparent; white-space: nowrap;
  }
  .pill-dot { width: 6px; height: 6px; border-radius: 50%; flex: none; }

  /* Neutrale/grijstinten pills: richting + live/dry-run (geen fel accent, alleen tekst-verschil) */
  .pill-neutral { background: var(--surface-alt); border-color: var(--border); color: var(--text-secondary); }
  .pill-neutral.emph { color: var(--text); }

  /* Statuspills: hier WEL kleur (groen/rood/amber) */
  .pill-green { background: var(--green-bg); color: var(--green); }
  .pill-green .pill-dot { background: var(--green); }
  .pill-red { background: var(--red-bg); color: var(--red); }
  .pill-red .pill-dot { background: var(--red); }
  .pill-amber { background: var(--amber-bg); color: var(--amber); }
  .pill-amber .pill-dot { background: var(--amber); }
  .pill-blue { background: var(--blue-bg); color: var(--blue); }
  .pill-blue .pill-dot { background: var(--blue); }
  .pill-gray { background: var(--surface-alt); border-color: var(--border); color: var(--text-muted); }

  /* --- Tabellen --- */
  .table-wrap { overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; font-size: 0.84rem; }
  th {
    text-align: left; font-weight: 600; font-size: 0.7rem; text-transform: uppercase;
    letter-spacing: 0.05em; color: var(--text-muted); padding: 0 0.85rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  td { padding: 0.7rem 0.85rem; border-bottom: 1px solid var(--border); vertical-align: middle; }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: var(--surface-alt); }
  td.mono, .mono { font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
  td.dim { color: var(--text-muted); }
  td.truncate { max-width: 380px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-secondary); }

  details.card { padding: 0; }
  details.card summary {
    cursor: pointer; padding: 1.5rem 1.75rem; list-style: none; user-select: none;
    display: flex; align-items: center; justify-content: space-between;
  }
  details.card summary::-webkit-details-marker { display: none; }
  details.card summary h2 { margin: 0; }
  details.card summary .chevron { color: var(--text-muted); font-size: 0.75rem; transition: transform 0.15s; }
  details.card[open] summary .chevron { transform: rotate(90deg); }
  details.card .table-wrap { padding: 0 1.75rem 1.5rem; }

  .empty-state { color: var(--text-muted); font-size: 0.85rem; padding: 0.5rem 0; }

  /* --- Periode-filter --- */
  .range-form { display: flex; flex-wrap: wrap; align-items: end; gap: 0.85rem; margin-bottom: 1rem; }
  .range-form label { display: block; font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
  .range-form input[type=date] {
    background: var(--surface-alt); border: 1px solid var(--border); border-radius: 6px;
    color: var(--text); padding: 0.4rem 0.6rem; font-family: var(--font-mono); font-size: 0.82rem;
  }
  .range-form button {
    background: var(--blue-bg); border: 1px solid transparent; color: var(--blue);
    border-radius: 6px; padding: 0.45rem 0.9rem; font-size: 0.82rem; font-weight: 600; cursor: pointer;
  }
  .range-form button:hover { filter: brightness(1.15); }
  .range-presets { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 1.25rem; }
  .range-presets a {
    text-decoration: none; font-size: 0.75rem; color: var(--text-secondary);
    background: var(--surface-alt); border: 1px solid var(--border); border-radius: 999px;
    padding: 0.3rem 0.75rem;
  }
  .range-presets a:hover { color: var(--text); border-color: var(--text-muted); }
  .range-presets a.active { color: var(--blue); border-color: var(--blue); background: var(--blue-bg); }

  .range-details { margin-top: 0.9rem; }
  .range-details summary {
    cursor: pointer; font-size: 0.78rem; color: var(--text-muted);
    padding: 0.2rem 0; user-select: none;
  }
  .range-details summary:hover { color: var(--text-secondary); }
  .range-details .range-form { margin-top: 1rem; }
  .range-details .sub { margin-top: 0.9rem; }

  /* --- Tabs --- */
  .tab-nav {
    display: flex; flex-wrap: wrap; gap: 0.25rem; margin-bottom: 1.75rem;
    border-bottom: 1px solid var(--border);
  }
  .tab-btn {
    background: none; border: none; color: var(--text-muted); cursor: pointer;
    font-family: var(--font); font-size: 0.85rem; font-weight: 600;
    padding: 0.8rem 1.1rem; border-bottom: 2px solid transparent; margin-bottom: -1px;
  }
  .tab-btn .count { color: var(--text-muted); font-weight: 400; margin-left: 0.3rem; }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--text); border-bottom-color: var(--blue); }
  .tab-btn.has-alert:not(.active) { color: var(--red); }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* --- Winsten: trade -> coin -> TP's -> uitgecasht --- */
  .trade-list { display: flex; flex-direction: column; gap: 0.85rem; }
  .trade-card {
    background: var(--surface-alt); border: 1px solid var(--border); border-radius: 10px;
    padding: 1.1rem 1.25rem;
  }
  .trade-head { display: flex; align-items: center; justify-content: space-between; gap: 0.75rem; flex-wrap: wrap; }
  .trade-title { display: flex; align-items: center; gap: 0.6rem; }
  .trade-coin { font-size: 1.05rem; font-weight: 700; }
  .trade-total { font-size: 1.2rem; font-weight: 700; }
  .trade-meta { font-size: 0.78rem; color: var(--text-muted); margin-top: 0.35rem; font-family: var(--font-mono); display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }
  .trade-legs {
    margin-top: 0.95rem; padding-top: 0.9rem; border-top: 1px solid var(--border);
    display: flex; flex-direction: column; gap: 0.65rem;
  }
  .leg-top { display: flex; align-items: baseline; justify-content: space-between; gap: 0.75rem; }
  .leg-label { font-size: 0.72rem; font-weight: 700; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em; }
  .leg-pnl { font-size: 0.95rem; font-weight: 600; }
  .leg-sub { font-size: 0.74rem; color: var(--text-muted); margin-top: 0.15rem; }

  /* --- Netto-verdiend hero (het antwoord op "hoeveel heb ik verdiend", altijd zichtbaar, geen tab) --- */
  .hero-card { padding: 1.75rem 1.75rem 1.5rem; }
  .hero-label { font-size: 0.78rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.5rem; }
  .hero-number { font-size: 2.9rem; font-weight: 700; font-family: var(--font-mono); line-height: 1.1; }
  .hero-sub { font-size: 0.82rem; color: var(--text-secondary); margin-top: 0.4rem; font-family: var(--font-mono); }
  .hero-note { font-size: 0.82rem; color: var(--text-muted); margin-top: 0.3rem; }
  .hero-card .range-form { margin-top: 1.5rem; }

  /* --- Mobiel (telefoon) --- */
  @media (max-width: 720px) {
    .container { padding: 1.1rem 0.9rem 3rem; }
    h1 { font-size: 1.2rem; }
    .subtitle { font-size: 0.74rem; }

    .card { padding: 1.1rem 1rem; margin-bottom: 1.1rem; border-radius: 10px; }
    .card h2 { margin-bottom: 1rem; }
    details.card summary { padding: 1.1rem 1rem; }
    details.card .table-wrap { padding: 0 1rem 1.1rem; }

    .hero-card { padding: 1.35rem 1rem 1.1rem; }
    .hero-number { font-size: 2.15rem; }
    .hero-sub, .hero-note { font-size: 0.78rem; }

    .stat-grid { grid-template-columns: repeat(2, 1fr); gap: 1rem 0.85rem; }
    .stat .value.small { word-break: break-all; }
    .stat.stat-wide { grid-column: 1 / -1; }

    /* Datumfilter: knop en velden vol breed, netjes onder elkaar */
    .range-form { flex-direction: column; align-items: stretch; gap: 0.7rem; }
    .range-form > div { width: 100%; }
    .range-form input[type=date] { width: 100%; padding: 0.6rem 0.6rem; font-size: 0.9rem; }
    .range-form button { width: 100%; padding: 0.65rem 0.9rem; font-size: 0.88rem; }
    .range-presets a { padding: 0.4rem 0.8rem; font-size: 0.78rem; }

    /* Tab-bar: horizontaal scrollen i.p.v. wrappen, grotere taps */
    .tab-nav { flex-wrap: nowrap; overflow-x: auto; -webkit-overflow-scrolling: touch; scrollbar-width: none; margin: 0 -0.9rem 1.25rem; padding: 0 0.9rem 0; }
    .tab-nav::-webkit-scrollbar { display: none; }
    .tab-btn { flex: none; padding: 0.85rem 0.9rem; font-size: 0.82rem; }

    /* Tabellen -> gestapelde kaarten (geen horizontaal scrollen/knijpen meer) */
    .table-wrap { overflow-x: visible; }
    table { display: block; width: 100%; }
    thead { display: none; }
    tbody { display: block; }
    tbody tr {
      display: block; border: 1px solid var(--border); border-radius: 10px;
      background: var(--surface-alt); padding: 0.9rem 1rem; margin-bottom: 0.6rem;
    }
    tbody tr:last-child { margin-bottom: 0; }
    td {
      display: flex; justify-content: space-between; align-items: baseline; gap: 0.9rem;
      padding: 0.42rem 0; border-bottom: 1px dashed var(--border);
      text-align: right; word-break: break-word;
    }
    td:last-child { border-bottom: none; }
    td::before {
      content: attr(data-label); flex: none; text-align: left;
      color: var(--text-muted); font-size: 0.68rem; font-weight: 600;
      text-transform: uppercase; letter-spacing: 0.05em;
    }
    /* Eerste kolom (meestal tijd/coin) fungeert als kaart-titel, geen label nodig */
    td:first-child {
      display: block; text-align: left; font-weight: 600; font-size: 0.9rem;
      border-bottom: 1px solid var(--border); padding: 0 0 0.55rem; margin-bottom: 0.45rem;
    }
    td:first-child::before { content: none; }
    /* Lange vrije tekst (bericht/detail/fout): label boven, tekst leesbaar uitgevouwen */
    td.truncate {
      display: block; text-align: left; white-space: normal; overflow: visible; text-overflow: clip; max-width: 100%;
    }
    td.truncate::before { display: block; text-align: left; margin-bottom: 0.3rem; }
  }
</style>
</head>
<body>
<div class="container">

<header>
  <h1>Signal Bot Dashboard</h1>
  <div class="subtitle">auto-refresh elke 12s</div>
</header>

<div class="card hero-card">
  <div class="hero-label">Netto verdiend</div>
  {% if realized_data.error %}
    <span class="pill pill-red" title="{{ realized_data.error }}">fout bij ophalen</span>
  {% else %}
    <div class="hero-number {{ 'pnl-pos' if (realized_data.realized_total - realized_data.fees_total) > 0 else ('pnl-neg' if (realized_data.realized_total - realized_data.fees_total) < 0 else 'pnl-zero') }}">
      {{ "%+.2f"|format(realized_data.realized_total - realized_data.fees_total) }}
    </div>
    <div class="hero-sub">bruto {{ "%+.2f"|format(realized_data.realized_total) }} &minus; fees {{ "%.2f"|format(realized_data.fees_total) }} &middot; {{ realized_data.leg_count }} closes in {{ realized_data.trades|length }} trade(s)</div>
  {% endif %}
  {% if not open_data.error and open_data.positions %}
    <div class="hero-note">
      plus <span class="mono {{ 'pnl-pos' if open_data.unrealized_total > 0 else ('pnl-neg' if open_data.unrealized_total < 0 else '') }}">{{ "%+.2f"|format(open_data.unrealized_total) }}</span>
      ongerealiseerd in {{ open_data.positions|length }} open positie(s) &mdash; nog niet definitief, zie tab &laquo;Open posities&raquo;.
    </div>
  {% endif %}

  <div class="range-presets">
    <a href="/" class="{{ 'active' if active_preset == 'ladder' else '' }}">Sinds ladder-strategie ({{ ladder_start }})</a>
    <a href="/?from={{ today }}" class="{{ 'active' if active_preset == 'today' else '' }}">Vandaag</a>
    <a href="/?from={{ week_ago }}" class="{{ 'active' if active_preset == 'week' else '' }}">Laatste 7 dagen</a>
    <a href="/?range=deposit" class="{{ 'active' if active_preset == 'deposit' else '' }}">Sinds storting</a>
    <a href="/?range=all" class="{{ 'active' if active_preset == 'all' else '' }}">Alles</a>
  </div>

  <details class="range-details"{{ ' open' if active_preset == 'custom' else '' }}>
    <summary>Aangepaste periode &amp; details</summary>

    <form class="range-form" method="get" action="/">
      <div>
        <label for="from">Van</label>
        <input type="date" id="from" name="from" value="{{ range_from }}">
      </div>
      <div>
        <label for="to">T/m</label>
        <input type="date" id="to" name="to" value="{{ range_to }}">
      </div>
      <button type="submit">Filter</button>
    </form>

    <div class="sub">
      Let op: funding-betalingen zitten niet in "netto verdiend".
      {% if pnl_cutoff.error %}
        <span class="pill pill-amber" title="{{ pnl_cutoff.error }}">{{ pnl_cutoff.error }}</span>
      {% elif pnl_cutoff.custom and active_preset != 'all' %}
        Getoond: <span class="mono">{{ pnl_cutoff.cutoff_dt or 'begin' }}</span> t/m <span class="mono">{{ pnl_cutoff.end_dt or 'nu' }}</span>{% if active_preset == 'ladder' %} (standaardweergave -- de dag dat de huidige 5-staps TP-ladder live ging){% endif %}.
      {% elif active_preset == 'all' %}
        Getoond: alle historie op dit account, inclusief eventuele oude/allang afgewikkelde trades van vóór de huidige strategie.
      {% elif pnl_cutoff.cutoff_ms %}
        Fills van vóór <span class="mono">{{ pnl_cutoff.cutoff_dt }}</span> (laatste moment dat dit account bijna leeg was, vlak vóór de huidige storting) tellen bewust niet mee.
      {% endif %}
    </div>
  </details>
</div>

<nav class="tab-nav">
  <button class="tab-btn" data-tab="winsten">Winsten <span class="count">({{ realized_data.trades|length }})</span></button>
  <button class="tab-btn" data-tab="posities">Open posities <span class="count">({{ open_data.positions|length }})</span></button>
</nav>

<section class="tab-panel" data-tab="winsten">
<div class="card">
  <h2>Winsten per trade <span class="count">({{ realized_data.trades|length }})</span></h2>
  {% if realized_data.error %}
    <div class="empty-state">Fout bij ophalen: {{ realized_data.error }}</div>
  {% elif realized_data.trades %}
  <div class="trade-list">
    {% for t in realized_data.trades %}
    <div class="trade-card">
      <div class="trade-head">
        <div class="trade-title">
          <span class="trade-coin mono">{{ t.coin }}</span>
          <span class="pill pill-neutral emph">{{ t.side }}</span>
        </div>
        <div class="trade-total mono {{ 'pnl-pos' if (t.total_pnl - t.total_fee) > 0 else ('pnl-neg' if (t.total_pnl - t.total_fee) < 0 else '') }}">
          {{ "%+.2f"|format(t.total_pnl - t.total_fee) }}
        </div>
      </div>
      <div class="trade-meta">
        entry {{ "%.6g"|format(t.entry_px) }} &middot; {{ t.entry_time }} UTC
        {% if t.still_open %}&middot; <span class="pill pill-blue">nog {{ "%.6g"|format(t.remaining_qty) }} open</span>{% endif %}
      </div>
      <div class="trade-legs">
        {% for leg in t.legs %}
        <div class="leg-row">
          <div class="leg-top">
            <span class="leg-label">TP{{ leg.idx }}</span>
            <span class="leg-pnl mono {{ 'pnl-pos' if leg.net > 0 else ('pnl-neg' if leg.net < 0 else '') }}">{{ "%+.4f"|format(leg.net) }}</span>
          </div>
          <div class="leg-sub mono dim">{{ leg.time }} &middot; {{ "%.6g"|format(leg.qty) }} @ {{ "%.6g"|format(leg.avg_px) }} &middot; fee {{ "%.4f"|format(leg.fee) }}</div>
        </div>
        {% endfor %}
      </div>
    </div>
    {% endfor %}
  </div>
  {% else %}
    <div class="empty-state">Geen winsten gevonden in deze periode.</div>
  {% endif %}
</div>
</section>

<section class="tab-panel" data-tab="posities">
<div class="card">
  <h2>Open live posities <span class="count">({{ open_data.positions|length }} open, ongerealiseerd)</span></h2>
  {% if open_data.positions %}
  <div class="table-wrap">
  <table>
    <thead><tr><th>coin</th><th>richting</th><th>size</th><th>entry</th><th>liq. prijs</th><th>leverage</th><th>ongerealiseerd</th></tr></thead>
    <tbody>
    {% for p in open_data.positions %}
    <tr>
      <td class="mono" data-label="Coin">{{ p.coin }}</td>
      <td data-label="Richting"><span class="pill pill-neutral emph">{{ p.side }}</span></td>
      <td class="mono" data-label="Size">{{ p.size }}</td>
      <td class="mono" data-label="Entry">{{ p.entry_px if p.entry_px is not none else '-' }}</td>
      <td class="mono dim" data-label="Liq. prijs">{{ p.liq_px if p.liq_px is not none else '-' }}</td>
      <td class="mono" data-label="Leverage">{{ p.leverage if p.leverage is not none else '-' }}x</td>
      <td class="mono {{ 'pnl-pos' if p.unrealized_pnl > 0 else ('pnl-neg' if p.unrealized_pnl < 0 else '') }}" data-label="Ongerealiseerd">{{ "%+.4f"|format(p.unrealized_pnl) }}</td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
  </div>
  {% else %}
    <div class="empty-state">Geen open live posities.</div>
  {% endif %}
</div>
</section>

</div>
<script>
(function() {
  var tabs = Array.prototype.slice.call(document.querySelectorAll('.tab-btn'));
  var panels = Array.prototype.slice.call(document.querySelectorAll('.tab-panel'));
  var names = tabs.map(function(t) { return t.dataset.tab; });

  function activate(name) {
    tabs.forEach(function(t) { t.classList.toggle('active', t.dataset.tab === name); });
    panels.forEach(function(p) { p.classList.toggle('active', p.dataset.tab === name); });
    try { localStorage.setItem('dashboardTab', name); } catch (e) {}
  }

  tabs.forEach(function(t) {
    t.addEventListener('click', function() { activate(t.dataset.tab); });
  });

  var saved = null;
  try { saved = localStorage.getItem('dashboardTab'); } catch (e) {}
  activate(names.indexOf(saved) !== -1 ? saved : 'winsten');
})();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    range_from = request.args.get("from", "").strip()
    range_to = request.args.get("to", "").strip()
    range_param = request.args.get("range", "").strip()

    # Welke preset is actief? Bepaalt zowel welke knop oplicht als welke
    # cutoff-logica gebruikt wordt. Standaard (geen query-params) = sinds
    # ladder-strategie, NIET "sinds storting": die laatste leunt op
    # get_pnl_cutoff()'s account-bijna-leeg-detectie, en Hyperliquid's
    # portfolio()-endpoint bewaart die geschiedenis niet lang genoeg -- na
    # verloop van tijd valt het stortingsmoment buiten bereik en levert
    # get_pnl_cutoff() cutoff_ms=None terug, waarna hier stilzwijgend ALLE
    # historie (incl. weken oude, allang afgewikkelde trades) meetelde. Voor
    # een cijfer dat "hoeveel heb ik verdiend" moet beantwoorden is dat
    # ronduit misleidend, dus "sinds storting" is nu een expliciete keuze
    # (?range=deposit) i.p.v. de default.
    if range_param == "all":
        active_preset = "all"
    elif range_param == "deposit":
        active_preset = "deposit"
    elif not range_from and not range_to:
        active_preset = "ladder"
    elif range_from == LADDER_STRATEGY_START and not range_to:
        active_preset = "ladder"
    elif range_from == today and not range_to:
        active_preset = "today"
    elif range_from == week_ago and not range_to:
        active_preset = "week"
    else:
        active_preset = "custom"

    end_ms = None
    if active_preset == "all":
        pnl_cutoff = {"cutoff_ms": None, "error": None, "custom": True}
    elif active_preset == "deposit":
        pnl_cutoff = get_pnl_cutoff()
        pnl_cutoff["custom"] = False
        if pnl_cutoff["cutoff_ms"] is None and pnl_cutoff["error"] is None:
            pnl_cutoff["error"] = (
                "kon geen stortingsmoment bepalen (buiten Hyperliquid's bewaarde historie) "
                "-- toont nu ALLE historie, incl. mogelijk weken oude, allang afgewikkelde trades"
            )
    else:
        effective_from = range_from or (LADDER_STRATEGY_START if active_preset == "ladder" else "")
        start_ms, end_ms, range_error = _parse_date_range(effective_from, range_to)
        pnl_cutoff = {"cutoff_ms": start_ms, "error": range_error, "custom": True}

    if pnl_cutoff.get("cutoff_ms") is not None:
        pnl_cutoff["cutoff_dt"] = datetime.fromtimestamp(
            pnl_cutoff["cutoff_ms"] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
    if end_ms is not None:
        pnl_cutoff["end_dt"] = datetime.fromtimestamp(
            (end_ms + 1) / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

    return render_template_string(
        PAGE,
        open_data=get_open_positions_and_pnl(),
        pnl_cutoff=pnl_cutoff,
        realized_data=get_realized_trades(pnl_cutoff.get("cutoff_ms"), end_ms),
        range_from=range_from,
        range_to=range_to,
        active_preset=active_preset,
        ladder_start=LADDER_STRATEGY_START,
        today=today,
        week_ago=week_ago,
    )


def _get_tailscale_ip():
    try:
        result = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5)
        lines = result.stdout.strip().splitlines()
        return lines[0].strip() if lines else None
    except Exception:
        return None


if __name__ == "__main__":
    db.init_db()
    import threading
    from werkzeug.serving import make_server

    # Losse binds op 127.0.0.1 (bestaande SSH-tunnel-route) EN het
    # Tailscale-IP (toegang via het tailnet, bv. vanaf de iPhone) --
    # BEWUST NIET 0.0.0.0. Elke bind accepteert alleen verkeer bestemd voor
    # dat specifieke IP, dus verkeer via de publieke interface wordt sowieso
    # geweigerd (connection refused), los van eventuele firewall-regels.
    hosts = ["127.0.0.1"]
    tailscale_ip = _get_tailscale_ip()
    if tailscale_ip:
        hosts.append(tailscale_ip)
        print(f"Dashboard bereikbaar via: 127.0.0.1:{PORT} en {tailscale_ip}:{PORT} (tailscale0)")
    else:
        print("WAARSCHUWING: kon Tailscale-IP niet bepalen (is tailscaled actief/ingelogd?) "
              "-- dashboard draait nu alleen op 127.0.0.1")

    servers = [make_server(h, PORT, app) for h in hosts]
    for s in servers[1:]:
        threading.Thread(target=s.serve_forever, daemon=True).start()
    servers[0].serve_forever()
