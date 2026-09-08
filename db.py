"""
Structured logging van bot-gebeurtenissen naar een lokale SQLite-database
(bot_history.db), zodat dashboard.py hier read-only uit kan lezen. Dit
vervangt geen van de bestaande logging.info(...) calls -- het is puur een
extra, doorzoekbare geschiedenis voor het dashboard.

Drie tabellen:
- signals   : elk binnengekomen Telegram-bericht, herkend of niet
- orders    : elke (poging tot) orderplaatsing, DRY_RUN of live
- tp_events : TP1/TP3/cancel-verwerking uit executor.py's v2-exitlogica
"""
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

DB_PATH = "bot_history.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    recognized INTEGER NOT NULL,
    symbol TEXT,
    side TEXT,
    entry_low REAL,
    entry_high REAL,
    leverage INTEGER,
    stop_loss REAL,
    targets TEXT,
    raw_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL,
    leverage REAL,
    qty REAL,
    stop_loss REAL,
    tp1 REAL,
    dry_run INTEGER NOT NULL,
    status TEXT NOT NULL,
    tx_sig TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS tp_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event TEXT NOT NULL,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS heartbeat (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    ts TEXT NOT NULL,
    connected INTEGER NOT NULL
);

-- Telegram message-ID's die de live listener (of de reconciliatie-taak
-- eronder) al heeft afgehandeld. Los van de tekst-gebaseerde dedup in
-- main.py (die alleen exact-dezelfde-tekst binnen 300s vangt) -- dit voorkomt
-- dat de reconciliatie-taak een bericht dubbel verwerkt dat de live listener
-- wel degelijk al kreeg. Zie incident 2026-08-27: twee kanaalberichten
-- binnen 3s van elkaar resulteerden erin dat de live listener het tweede
-- (een nieuw entry-signal) nooit ontving -- vermoedelijk een Telethon
-- pts-gap-recovery die geen NewMessage-event vuurde.
CREATE TABLE IF NOT EXISTS processed_messages (
    msg_id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with _conn() as conn:
        conn.executescript(_SCHEMA)


def log_signal(text: str, signal=None):
    """signal is een signal_parser.Signal of None (niet herkend)."""
    with _conn() as conn:
        if signal is None:
            conn.execute(
                "INSERT INTO signals (ts, recognized, raw_text) VALUES (?, 0, ?)",
                (_now(), text),
            )
        else:
            conn.execute(
                """INSERT INTO signals
                   (ts, recognized, symbol, side, entry_low, entry_high,
                    leverage, stop_loss, targets, raw_text)
                   VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    _now(), signal.symbol, signal.side, signal.entry_low,
                    signal.entry_high, signal.leverage, signal.stop_loss,
                    ",".join(str(t) for t in signal.targets), text,
                ),
            )


def log_order(
    symbol: str,
    side: str,
    dry_run: bool,
    status: str,
    entry_price: Optional[float] = None,
    leverage: Optional[float] = None,
    qty: Optional[float] = None,
    stop_loss: Optional[float] = None,
    tp1: Optional[float] = None,
    tx_sig: Optional[str] = None,
    error: Optional[str] = None,
):
    with _conn() as conn:
        conn.execute(
            """INSERT INTO orders
               (ts, symbol, side, entry_price, leverage, qty, stop_loss,
                tp1, dry_run, status, tx_sig, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                _now(), symbol, side, entry_price, leverage, qty, stop_loss,
                tp1, int(dry_run), status, tx_sig, error,
            ),
        )


def log_tp_event(symbol: str, event: str, detail: str = ""):
    with _conn() as conn:
        conn.execute(
            "INSERT INTO tp_events (ts, symbol, event, detail) VALUES (?, ?, ?, ?)",
            (_now(), symbol, event, detail),
        )


def recent_signals(limit: int = 50):
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()


def recent_orders(limit: int = 50, dry_run: Optional[bool] = None):
    with _conn() as conn:
        if dry_run is None:
            return conn.execute(
                "SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return conn.execute(
            "SELECT * FROM orders WHERE dry_run = ? ORDER BY id DESC LIMIT ?",
            (int(dry_run), limit),
        ).fetchall()


def recent_tp_events(limit: int = 50):
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tp_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()


def set_heartbeat(connected: bool):
    with _conn() as conn:
        conn.execute(
            """INSERT INTO heartbeat (id, ts, connected) VALUES (1, ?, ?)
               ON CONFLICT(id) DO UPDATE SET ts=excluded.ts, connected=excluded.connected""",
            (_now(), int(connected)),
        )


def get_heartbeat():
    with _conn() as conn:
        row = conn.execute("SELECT ts, connected FROM heartbeat WHERE id = 1").fetchone()
        return dict(row) if row else None


def is_message_processed(msg_id: int) -> bool:
    with _conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM processed_messages WHERE msg_id = ?", (msg_id,)
        ).fetchone()
        return row is not None


def mark_message_processed(msg_id: int):
    with _conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_messages (msg_id, ts) VALUES (?, ?)",
            (msg_id, _now()),
        )
