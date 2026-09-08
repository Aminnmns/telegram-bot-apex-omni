"""
Handmatige, losse testtrade OP ECHT GELD, buiten de Telegram-signal-flow om
-- voor het verifiëren van de volledige executie-pipeline (market-lookup,
leverage-cap, qty-berekening, order-plaatsing, TP/SL) zonder op een
Telegram-signal te hoeven wachten.

Onafhankelijk van config.DRY_RUN (dat regelt alleen automatische
signal-verwerking door main.py) -- de expliciete "typ ja"-bevestiging
hieronder is de veiligheidsklep voor DIT script, net als bij
setup_flash_account.py.

    python manual_test_trade.py <COIN> <MARGIN_USDC>
    python manual_test_trade.py SOL 1

Plaatst:
- Market LONG entry, margin=<MARGIN_USDC>, leverage = min(20, max
  toegestane leverage voor deze coin -- live opgevraagd via
  executor.get_market_info(), niet hardcoded, zelfde aanpak als de bot).
- EEN take-profit op +1% vanaf de WERKELIJKE fill-prijs (reduce-only, volle qty).
- EEN stop-loss op -2% vanaf de WERKELIJKE fill-prijs (reduce-only, volle qty),
  puur als veiligheidsnet voor deze test.

Dit is BEWUST geen partial-close-plus-break-even zoals de bot's normale v2
exit-strategie (margin-based sizing + TP-event-gedreven sluiten via
executor.handle_tp_event(), zie config.MAX_MARGIN_PCT_OF_FUNDS/TP_EVENT_TARGET1_CLOSE_PCT)
-- een simpele volledige-close-TP en -SL is genoeg om de pipeline te
verifiëren. handle_tp_event() in main.py bemoeit zich er toch niet mee
(die kent alleen posities die via open_positions.json door
place_entry_order zijn weggeschreven, en dat doet dit script bewust niet --
zie de log-regel hieronder). Sizing hier blijft een expliciet
opgegeven vast bedrag (MARGIN_USDC * leverage), niet %-van-saldo zoals de
v2-flow -- dit script test de order-pipeline, niet de sizing-strategie.
"""
import asyncio
import sys

import config
import db
import executor

TEST_LEVERAGE_CAP = 20
TP_PCT = 0.01  # +1%
SL_PCT = 0.02  # -2%


async def main():
    if len(sys.argv) != 3:
        print("Gebruik: python manual_test_trade.py <COIN> <MARGIN_USDC>")
        print("Bijv.:   python manual_test_trade.py SOL 1")
        sys.exit(1)

    coin = sys.argv[1].upper()
    try:
        margin = float(sys.argv[2])
    except ValueError:
        print(f"Ongeldig margin-bedrag: {sys.argv[2]!r}")
        sys.exit(1)
    if margin <= 0:
        print("Margin moet positief zijn.")
        sys.exit(1)

    db.init_db()

    open_count = await executor.count_open_positions()
    if open_count >= config.MAX_CONCURRENT_POSITIONS:
        print(f"⚠️  Al {open_count}/{config.MAX_CONCURRENT_POSITIONS} open live posities "
              f"(zelfde limiet als de bot, live gecheckt via Hyperliquid). "
              f"Sluit eerst iets, of verhoog MAX_CONCURRENT_POSITIONS in .env.")
        sys.exit(1)

    try:
        market = await executor.get_market_info(coin)
    except ValueError as e:
        print(f"Fout: {e}")
        sys.exit(1)

    used_leverage = min(TEST_LEVERAGE_CAP, market["max_leverage"])
    sz_decimals = market["sz_decimals"]
    raw_sz = (margin * used_leverage) / market["mark_px"]
    qty = executor._round_sz(raw_sz, sz_decimals)
    notional = qty * market["mark_px"]

    if qty <= 0 or notional < 10:
        print(f"Berekende orderwaarde (~${notional:.2f}) is te klein -- Hyperliquid weigert "
              f"orders onder $10. Verhoog het margin-bedrag.")
        sys.exit(1)

    owner = executor.get_owner_address()
    print(f"Wallet:                {owner}")
    print(f"Coin:                  {coin}")
    print(f"Margin:                ${margin:.2f} USDC")
    print(f"Leverage:              {used_leverage}x (max toegestaan voor {coin}: {market['max_leverage']}x)")
    print(f"Markprijs nu:          {market['mark_px']}")
    print(f"Geschatte qty:         {qty} (orderwaarde ~${notional:.2f})")
    print(f"Take-profit:           +{TP_PCT * 100:.0f}% vanaf de werkelijke fill-prijs")
    print(f"Stop-loss:             -{SL_PCT * 100:.0f}% vanaf de werkelijke fill-prijs (veiligheidsnet)")
    print(f"Open live posities nu: {open_count}/{config.MAX_CONCURRENT_POSITIONS}")

    confirm = input("\nDit plaatst een ECHTE market-order met echt geld op Hyperliquid mainnet. "
                     "Doorgaan? (typ 'ja'): ")
    if confirm.strip().lower() != "ja":
        print("Geannuleerd.")
        return

    exchange = executor._get_exchange()

    lev_resp = await executor._call(exchange.update_leverage, used_leverage, coin, False)
    executor._check_order_status(lev_resp, "update_leverage")

    open_resp = await executor._call(exchange.market_open, coin, True, qty)
    statuses = executor._check_order_status(open_resp, "market_open")
    if not statuses or "filled" not in statuses[0]:
        raise RuntimeError(f"Entry-order is niet (meteen) gevuld: {open_resp}")

    filled = statuses[0]["filled"]
    filled_qty = float(filled["totalSz"])
    entry_price = float(filled["avgPx"])
    print(f"\n✅ Entry gevuld: qty={filled_qty} @ {entry_price} ({used_leverage}x)")

    # Zelfde "agressieve richting"-logica als executor.py (na de TP1-fix):
    # exit is hier altijd een SELL (long terugverkopen), dus een LAGERE
    # limit dan de trigger is agressiever en vult betrouwbaarder.
    tp_trigger = executor._round_px(entry_price * (1 + TP_PCT), sz_decimals)
    tp_limit = executor._round_px(
        tp_trigger * (1 - executor.TRIGGER_LIMIT_BUFFER_PCT), sz_decimals
    )
    sl_trigger = executor._round_px(entry_price * (1 - SL_PCT), sz_decimals)
    sl_limit = executor._round_px(
        sl_trigger * (1 - executor.TRIGGER_LIMIT_BUFFER_PCT), sz_decimals
    )

    tp_resp = await executor._call(
        exchange.order, coin, False, filled_qty, tp_limit,
        {"trigger": {"triggerPx": tp_trigger, "isMarket": True, "tpsl": "tp"}},
        reduce_only=True,
    )
    executor._check_order_status(tp_resp, "take-profit plaatsen")
    print(f"✅ Take-profit geplaatst: trigger={tp_trigger} (volle qty={filled_qty})")

    sl_resp = await executor._call(
        exchange.order, coin, False, filled_qty, sl_limit,
        {"trigger": {"triggerPx": sl_trigger, "isMarket": True, "tpsl": "sl"}},
        reduce_only=True,
    )
    executor._check_order_status(sl_resp, "stop-loss plaatsen")
    print(f"✅ Stop-loss geplaatst: trigger={sl_trigger} (volle qty={filled_qty})")

    db.log_order(
        symbol=f"{coin}USDT", side="Buy", dry_run=False, status="manual_test",
        entry_price=entry_price, leverage=used_leverage, qty=filled_qty,
        stop_loss=sl_trigger, tp1=tp_trigger,
    )
    print("\nGelogd naar bot_history.db met status='manual_test' -- herkenbaar als aparte "
          "pill-badge op het dashboard, niet te verwarren met een echt Telegram-signal.")
    print("Let op: deze positie staat NIET in open_positions.json, dus handle_tp_event() "
          "beheert 'm niet -- de TP/SL hierboven zijn losse, op zichzelf staande orders. "
          "Sluit 'm zelf in de Hyperliquid-app als TP/SL niet binnen afzienbare tijd raken.")


if __name__ == "__main__":
    asyncio.run(main())
