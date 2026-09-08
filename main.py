"""
Luistert live naar de Telegram-groep en zet elk herkend signal direct door
naar de executor. Geen filtering, geen validatie — blind volgen.
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient, events

import config
import db
import executor
from signal_parser import parse_signal, parse_tp_event, parse_cancel_event
from executor import execute_signal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("telegram_listener")

client = TelegramClient(
    config.TELEGRAM_SESSION_NAME,
    config.TELEGRAM_API_ID,
    config.TELEGRAM_API_HASH,
)

# Het kanaal stuurt af en toe (glitch, geen apart signal) exact hetzelfde
# bericht dubbel binnen een paar seconden -- incident 2026-08-14: een
# duplicaat-HYPE-signal resulteerde in 2 losse orders die elkaars tracking
# overschreven, waardoor een cancel later maar de helft sloot. Zelfde tekst
# binnen dit venster is dus 1 signal, niet 2 -- de tweede wordt hier al
# stilletjes genegeerd, vóór parsing/uitvoering. (executor.place_entry_order
# heeft daarnaast nog een losse guard tegen dezelfde-symbol+side-al-open als
# extra vangnet voor near-duplicates die hier niet exact matchen.)
DEDUP_WINDOW_SECONDS = 300
_recent_message_texts: dict[str, float] = {}


def _is_duplicate_message(text: str) -> bool:
    now = time.monotonic()
    for seen_text, seen_at in list(_recent_message_texts.items()):
        if now - seen_at > DEDUP_WINDOW_SECONDS:
            del _recent_message_texts[seen_text]
    if text in _recent_message_texts:
        return True
    _recent_message_texts[text] = now
    return False


@client.on(events.NewMessage(chats=config.TELEGRAM_GROUP))
async def handle_new_message(event):
    db.mark_message_processed(event.id)
    await process_message(event.raw_text)


# Vangnet naast de live listener hierboven -- zie incident 2026-08-27: twee
# kanaalberichten binnen 3s van elkaar resulteerden erin dat Telethon voor
# het tweede (een nieuw entry-signal) geen NewMessage-event vuurde
# (vermoedelijk pts-gap-recovery), waardoor dat signal simpelweg nooit
# binnenkwam -- niet "genegeerd na parsing" zoals normaal, maar écht gemist.
# reconcile_missed_messages() haalt periodiek de laatste berichten op en
# verwerkt alles wat nog niet in processed_messages staat alsnog, begrensd
# tot recente berichten zodat er nooit oude/stale signals worden opgepikt.
RECONCILE_INTERVAL_SECONDS = 60
RECONCILE_LOOKBACK_MINUTES = 10
RECONCILE_FETCH_LIMIT = 30


async def process_message(text: str):
    log.info("Nieuw bericht ontvangen (%s tekens)", len(text))

    if _is_duplicate_message(text):
        log.info("Duplicaat-bericht (zelfde tekst al verwerkt binnen %ss) -- genegeerd.", DEDUP_WINDOW_SECONDS)
        return

    # Incident 2026-09-08 (bug-audit): dit is het ENIGE stuk van de live
    # message-handler dat niet in een try/except zat -- event.id wordt al
    # (hierboven, in handle_new_message) als verwerkt gemarkeerd vóórdat dit
    # loopt, dus een crash hier (bv. een sqlite-lock-timeout in log_signal)
    # zou het bericht stil laten verdwijnen: geen retry via
    # reconcile_missed_messages() (denkt dat het al verwerkt is) EN geen
    # notify(). Elke andere tak hieronder (execute_signal/handle_tp_event/
    # handle_cancel_event) heeft al zo'n vangnet -- dit stukje miste 'm.
    try:
        signal = parse_signal(text)
        db.log_signal(text, signal)
    except Exception as e:
        log.exception("Parsen/loggen van binnengekomen bericht mislukt")
        await notify(f"❌ Fout bij verwerken van binnengekomen bericht: {e}")
        return
    if signal is not None:
        log.info("Signal herkend: %s %s | entry %s-%s | %sx | SL %s | TP's %s",
                  signal.side, signal.symbol, signal.entry_low, signal.entry_high,
                  signal.leverage, signal.stop_loss, signal.targets)

        try:
            result = await execute_signal(signal)
            await notify(f"✅ Uitgevoerd: {signal.side} {signal.symbol}\n{result}")
        except Exception as e:
            log.exception("Uitvoeren van signal mislukt")
            db.log_order(
                symbol=signal.symbol, side=signal.side, dry_run=config.DRY_RUN,
                status="failed", leverage=signal.leverage, stop_loss=signal.stop_loss,
                tp1=signal.targets[0] if signal.targets else None, error=str(e),
            )
            await notify(f"❌ Fout bij uitvoeren {signal.symbol}: {e}")
        return

    tp_event = parse_tp_event(text)
    if tp_event is not None:
        log.info("TP-event herkend: %s target %d", tp_event.symbol, tp_event.target_number)
        try:
            await executor.handle_tp_event(tp_event)
        except Exception as e:
            log.exception("Verwerken van TP-event mislukt")
            await notify(f"❌ Fout bij verwerken TP-event {tp_event.symbol} target {tp_event.target_number}: {e}")
        return

    cancel_event = parse_cancel_event(text)
    if cancel_event is not None:
        log.info("Cancel-event herkend: %s", cancel_event.symbol)
        try:
            await executor.handle_cancel_event(cancel_event)
        except Exception as e:
            log.exception("Verwerken van cancel-event mislukt")
            await notify(f"❌ Fout bij verwerken cancel-event {cancel_event.symbol}: {e}")
        return

    log.info("Geen (compleet) signal, TP-event of cancel-event herkend in dit bericht, wordt genegeerd.")


async def notify(message: str):
    if config.TELEGRAM_NOTIFY_CHAT:
        try:
            await client.send_message(config.TELEGRAM_NOTIFY_CHAT, message)
        except Exception:
            log.exception("Kon notificatie niet versturen")


executor.notify_callback = notify


async def send_heartbeat():
    """Achtergrondtaak: schrijft elke 15s de live Telegram-verbindingsstatus
    weg, zodat het dashboard kan tonen of de bot écht verbonden is (i.p.v.
    alleen dat het proces draait -- systemd weet niet of telethon intern de
    verbinding is kwijtgeraakt en aan het reconnecten is)."""
    while True:
        try:
            db.set_heartbeat(client.is_connected())
        except Exception:
            log.exception("Kon heartbeat niet wegschrijven")
        await asyncio.sleep(15)


async def reconcile_missed_messages():
    """Zie comment bij RECONCILE_INTERVAL_SECONDS hierboven. Draait los van de
    live listener: mist deze taak zelf een cyclus door een trage/hangende
    Hyperliquid-call, dan pakt de volgende cyclus (60s later) het gewoon
    weer op -- de lookback-window is ruim genoeg (10 min) om dat te overleven."""
    await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)
    while True:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=RECONCILE_LOOKBACK_MINUTES)
            messages = await client.get_messages(config.TELEGRAM_GROUP, limit=RECONCILE_FETCH_LIMIT)
            missed = [
                m for m in messages
                if m.date >= cutoff and not db.is_message_processed(m.id)
            ]
            missed.sort(key=lambda m: m.id)  # chronologisch, oudste eerst
            for m in missed:
                log.warning(
                    "Reconciliatie: bericht %s (verstuurd %s) is nooit door de live listener ontvangen -- alsnog verwerken.",
                    m.id, m.date.isoformat(),
                )
                db.mark_message_processed(m.id)
                try:
                    await process_message(m.raw_text or "")
                except Exception as e:
                    log.exception("Reconciliatie: verwerken van gemist bericht %s mislukt", m.id)
                    await notify(f"❌ Fout bij alsnog verwerken van gemist bericht {m.id}: {e}")
        except Exception:
            log.exception("Reconciliatie-cyclus mislukt")
        await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)


async def reconcile_positions_loop():
    """Zie config.POSITION_RECONCILE_INTERVAL_SECONDS en
    executor.reconcile_positions() -- vangt stille state-drift op wanneer een
    positie buiten de bot om sluit (resting SL, of een TP/cancel-event dat
    ambigu was en dus genegeerd werd)."""
    await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)
    while True:
        try:
            await executor.reconcile_positions()
        except Exception:
            log.exception("Positie-reconciliatie-cyclus mislukt")
        await asyncio.sleep(config.POSITION_RECONCILE_INTERVAL_SECONDS)


def main():
    db.init_db()
    log.info("Bot start | HYPERLIQUID_ENV=%s | DRY_RUN=%s", config.HYPERLIQUID_ENV, config.DRY_RUN)
    with client:
        client.loop.create_task(send_heartbeat())
        client.loop.create_task(reconcile_missed_messages())
        client.loop.create_task(reconcile_positions_loop())
        client.run_until_disconnected()


if __name__ == "__main__":
    main()
