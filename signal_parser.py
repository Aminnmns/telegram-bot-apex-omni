"""
Parser die ruwe Telegram-berichten omzet naar een Signal object.

BELANGRIJK: dit is getuned op het voorbeeld dat je gaf:

    Open SHORT at price between $191.8 - $193.9 with X25 leverage.
    TARGETS
    1. Close the order at the price $190.3
    ...
    Stop loss: $200.3

Je zei dat de coin ook ergens IN het bericht staat, maar dat stond niet
in het voorbeeld dat je me gaf. Ik heb daarom een flexibele detectie
gebouwd (zoekt naar iets als $SOL, #SOL, SOL/USDT of SOLUSDT), maar dit
moet je verifiëren met een ECHT bericht (inclusief coin) via
`test_parser.py` voordat je live gaat. Pas SYMBOL_PATTERN/BLACKLIST hieronder
aan indien nodig.
"""
import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Signal:
    symbol: str            # bv. "SOLUSDT"
    side: str               # "Buy" of "Sell" (Bybit conventie)
    entry_low: float
    entry_high: float
    leverage: int
    targets: List[float]
    stop_loss: float
    raw_text: str


@dataclass
class TPEvent:
    """Groepsbericht dat een target geraakt is, bv.
    '#PENGU/USDT Take-Profit target 1 ✅'. Geen nieuw entry-signal -- puur
    een melding die de v2-executie triggert (zie executor.handle_tp_event)."""
    symbol: str            # zelfde format als Signal.symbol, bv. "PENGUUSDT"
    target_number: int
    raw_text: str


@dataclass
class CancelEvent:
    """Groepsbericht dat een eerder signal wordt ingetrokken. Komt in de
    praktijk als TWEE losse berichten binnen, bv.:
        'Close HYPE/USDT'
        '#HYPE/USDT  Cancelled'
    Beide worden hieronder herkend (elk apart voldoende) zodat het niet
    uitmaakt welk van de twee als eerste binnenkomt of als enige aankomt.
    Voorheen werd GEEN van beide herkend (incident 2026-08-12: een
    HYPE-cancel werd genegeerd, de bijbehorende positie bleef live open
    staan totdat 'ie handmatig gesloten werd) -- zie executor.handle_cancel_event."""
    symbol: str            # zelfde format als Signal.symbol, bv. "HYPEUSDT"
    raw_text: str


SIDE_PATTERN = re.compile(r"Open\s+(LONG|SHORT)", re.IGNORECASE)
ENTRY_PATTERN = re.compile(r"between\s+\$?([\d.,]+)\s*-\s*\$?([\d.,]+)")
LEVERAGE_PATTERN = re.compile(r"[Xx]\s?(\d+)\s*leverage", re.IGNORECASE)
TARGET_PATTERN = re.compile(r"the price\s+\$?([\d.,]+)")
STOPLOSS_PATTERN = re.compile(r"Stop\s*loss:?\s*\$?([\d.,]+)", re.IGNORECASE)

# Bv. "#PENGU/USDT Take-Profit target 1 ✅" -- de coin staat hier in een
# "#COIN/USDT"-header net als bij entry-signals, maar zonder blokhaken.
TP_EVENT_PATTERN = re.compile(
    r"#([A-Z0-9]{2,10})/USDT\s+Take-Profit target\s+(\d+)", re.IGNORECASE
)

# Het kanaal stuurt voor de laatste stap GEEN "Take-Profit target 5", maar
# "#ETH/USDT All targets achieved ✈️" -- ontdekt 2026-08-20 doordat de
# ETH/USDT-trade van 2026-08-19 na target 4 bleef hangen: dit bericht werd
# nergens door herkend (recognized=0 in de signals-tabel), dus de laatste
# ~10% restpositie werd nooit door de bot gesloten en moest kennelijk
# handmatig dicht. Behandelen als target 5 (zie parse_tp_event) zodat
# _handle_tp5_event (volledige restpositie sluiten) alsnog gewoon triggert.
ALL_TARGETS_PATTERN = re.compile(
    r"#([A-Z0-9]{2,10})/USDT\s+All targets achieved", re.IGNORECASE
)

# "#HYPE/USDT  Cancelled" (hashtag-header, zoals TP_EVENT_PATTERN) en
# "Close HYPE/USDT" (los "Close"-bericht) -- allebei komen in de praktijk
# als apart bericht binnen bij eenzelfde annulering, elk voldoende om te
# herkennen (zie CancelEvent hierboven).
CANCEL_HASH_PATTERN = re.compile(r"#([A-Z0-9]{2,10})/USDT\s+Cancelled", re.IGNORECASE)
CANCEL_CLOSE_PATTERN = re.compile(r"\bClose\s+([A-Z0-9]{2,10})/USDT\b", re.IGNORECASE)

# "#PENGU/USDT Closed due to opposite direction signal ⚠" -- het kanaal stuurt
# dit zelf wanneer het een NIEUW signal voor dezelfde coin in de tegenovergestelde
# richting gaat sturen en de oude positie daarom intrekt (geobserveerd
# 2026-08-27: deze melding kwam 11 min vóór een nieuw Buy PENGU/USDT-signal).
# Werd voorheen NERGENS door herkend (recognized=0), waardoor de oude positie
# in de lokale state bleef staan en het latere tegengestelde signal een TWEEDE
# v2-entry voor dezelfde coin opende -- precies de "meerdere open v2-posities"
# ambiguïteit die TP/cancel-events daarna herhaaldelijk deed negeren (incident
# 2026-08-26/28). Behandelen als een gewoon cancel-event: handle_cancel_event()
# matcht toch al alleen op symbol (niet op side), dus dit sluit exact de ene
# positie die er op dat moment nog is, ongeacht of dat een Buy of Sell was.
CANCEL_OPPOSITE_DIRECTION_PATTERN = re.compile(
    r"#([A-Z0-9]{2,10})/USDT\s+Closed due to opposite direction signal", re.IGNORECASE
)

# Primair patroon: coin staat in de header als "[TAO/USDT]"
SYMBOL_HEADER_PATTERN = re.compile(r"\[([A-Z0-9]{2,10})/USDT\]")

# Fallback als de header ooit ontbreekt: zoek een los ticker-achtig token
SYMBOL_FALLBACK_PATTERN = re.compile(r"[$#]?\b([A-Z]{2,10})(?:/?USDT)?\b")
BLACKLIST = {
    "OPEN", "SHORT", "LONG", "CLOSE", "STOP", "LOSS", "TARGETS", "PRICE",
    "USDT", "THE", "AT", "WITH", "ORDER", "TARGET", "LEVERAGE", "X", "SIGNAL",
}


def parse_signal(text: str) -> Optional[Signal]:
    side_match = SIDE_PATTERN.search(text)
    entry_match = ENTRY_PATTERN.search(text)
    lev_match = LEVERAGE_PATTERN.search(text)
    sl_match = STOPLOSS_PATTERN.search(text)
    targets = [float(t.replace(",", "")) for t in TARGET_PATTERN.findall(text)]

    if not (side_match and entry_match and sl_match and targets):
        return None  # geen (compleet) signal, negeren

    header_match = SYMBOL_HEADER_PATTERN.search(text)
    if header_match:
        symbol = header_match.group(1).upper()
    else:
        symbol = None
        for m in SYMBOL_FALLBACK_PATTERN.finditer(text):
            candidate = m.group(1).upper()
            if candidate not in BLACKLIST and len(candidate) >= 2:
                symbol = candidate
                break

    if not symbol:
        return None

    side = "Sell" if side_match.group(1).upper() == "SHORT" else "Buy"

    return Signal(
        symbol=f"{symbol}USDT",
        side=side,
        entry_low=float(entry_match.group(1).replace(",", "")),
        entry_high=float(entry_match.group(2).replace(",", "")),
        leverage=int(lev_match.group(1)) if lev_match else 1,
        targets=targets,
        stop_loss=float(sl_match.group(1).replace(",", "")),
        raw_text=text,
    )


def parse_tp_event(text: str) -> Optional[TPEvent]:
    m = TP_EVENT_PATTERN.search(text)
    if m:
        return TPEvent(
            symbol=f"{m.group(1).upper()}USDT",
            target_number=int(m.group(2)),
            raw_text=text,
        )
    m = ALL_TARGETS_PATTERN.search(text)
    if m:
        return TPEvent(
            symbol=f"{m.group(1).upper()}USDT",
            target_number=5,
            raw_text=text,
        )
    return None


def parse_cancel_event(text: str) -> Optional[CancelEvent]:
    m = (
        CANCEL_HASH_PATTERN.search(text)
        or CANCEL_CLOSE_PATTERN.search(text)
        or CANCEL_OPPOSITE_DIRECTION_PATTERN.search(text)
    )
    if not m:
        return None
    return CancelEvent(symbol=f"{m.group(1).upper()}USDT", raw_text=text)
