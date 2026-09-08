"""
Handmatige, losse testtrade OP ECHT GELD (of testnet-nepgeld, zie
config.APEX_ENV), buiten de Telegram-signal-flow om -- voor het verifiëren
van de volledige executie-pipeline (market-lookup, leverage-cap,
qty-berekening, order-plaatsing, TP/SL) zonder op een Telegram-signal te
hoeven wachten.

Onafhankelijk van config.DRY_RUN (dat regelt alleen automatische
signal-verwerking door main.py) -- de expliciete "typ ja"-bevestiging
hieronder is de veiligheidsklep voor DIT script.

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
verifiëren. handle_tp_event() in main.py bemoeit zich er toch niet mee (die
kent alleen posities die via open_positions.json door place_entry_order zijn
weggeschreven, en dat doet dit script bewust niet -- zie de log-regel
hieronder). Sizing hier blijft een expliciet opgegeven vast bedrag
(MARGIN_USDC * leverage), niet %-van-saldo zoals de v2-flow -- dit script
test de order-pipeline, niet de sizing-strategie.

TIP: zet APEX_ENV=test in .env om dit eerst op testnet-nepgeld te draaien
voor je het op main met echt geld probeert.
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

    client = await executor._get_client()

    open_count = await executor.count_open_positions(client)
    if open_count >= config.MAX_CONCURRENT_POSITIONS:
        print(f"⚠️  Al {open_count}/{config.MAX_CONCURRENT_POSITIONS} open live posities "
              f"(zelfde limiet als de bot, live gecheckt via ApeX Omni). "
              f"Sluit eerst iets, of verhoog MAX_CONCURRENT_POSITIONS in .env.")
        sys.exit(1)

    try:
        market = await executor.get_market_info(coin, client=client)
    except ValueError as e:
        print(f"Fout: {e}")
        sys.exit(1)

    used_leverage = min(TEST_LEVERAGE_CAP, market["max_leverage"])
    step_size = market["step_size"]
    tick_size = market["tick_size"]
    raw_sz = (margin * used_leverage) / market["mark_px"]
    qty = executor._round_sz(raw_sz, step_size)
    notional = qty * market["mark_px"]

    if qty <= 0 or notional < config.MIN_NOTIONAL_USD:
        print(f"Berekende orderwaarde (~${notional:.2f}) is te klein -- ApeX Omni's "
              f"minimum-ordergrootte voor {coin} is {market['step_size']}. Verhoog het margin-bedrag.")
        sys.exit(1)

    owner = await executor.get_owner_address(client)
    print(f"Omgeving:              {config.APEX_ENV}")
    print(f"Wallet:                {owner}")
    print(f"Coin:                  {coin} ({market['apex_symbol']})")
    print(f"Margin:                ${margin:.2f} USDC")
    print(f"Leverage:              {used_leverage}x (max toegestaan voor {coin}: {market['max_leverage']}x)")
    print(f"Markprijs nu:          {market['mark_px']}")
    print(f"Geschatte qty:         {qty} (orderwaarde ~${notional:.2f})")
    print(f"Take-profit:           +{TP_PCT * 100:.0f}% vanaf de werkelijke fill-prijs")
    print(f"Stop-loss:             -{SL_PCT * 100:.0f}% vanaf de werkelijke fill-prijs (veiligheidsnet)")
    print(f"Open live posities nu: {open_count}/{config.MAX_CONCURRENT_POSITIONS}")

    confirm = input(f"\nDit plaatst een ECHTE market-order op ApeX Omni {config.APEX_ENV}. "
                     "Doorgaan? (typ 'ja'): ")
    if confirm.strip().lower() != "ja":
        print("Geannuleerd.")
        return

    imr = str(round(1 / used_leverage, 6))
    lev_resp = await executor._call(client.set_initial_margin_rate_v3, symbol=market["apex_symbol"], initialMarginRate=imr)
    executor._check_order_status(lev_resp, "leverage/margin-rate zetten")

    filled_qty, entry_price, _order_id = await executor._place_market_order(
        client, market["apex_symbol"], is_buy=True, qty=qty, reduce_only=False,
    )
    print(f"\n✅ Entry gevuld: qty={filled_qty} @ {entry_price} ({used_leverage}x)")

    # Exit is hier altijd een SELL (long terugverkopen).
    tp_trigger = executor._round_px(entry_price * (1 + TP_PCT), tick_size)
    tp_limit = executor._round_px(tp_trigger * (1 - executor.TRIGGER_LIMIT_BUFFER_PCT), tick_size)
    sl_trigger = executor._round_px(entry_price * (1 - SL_PCT), tick_size)
    sl_limit = executor._round_px(sl_trigger * (1 - executor.TRIGGER_LIMIT_BUFFER_PCT), tick_size)

    tp_oid = await executor._call(
        client.create_order_v3, symbol=market["apex_symbol"], side="SELL", type="TAKE_PROFIT_MARKET",
        size=str(filled_qty), price=str(tp_limit), triggerPrice=str(tp_trigger), triggerPriceType="INDEX",
        reduceOnly=True, isPositionTpsl=True,
    )
    executor._check_order_status(tp_oid, "take-profit plaatsen")
    print(f"✅ Take-profit geplaatst: trigger={tp_trigger} (volle qty={filled_qty})")

    sl_oid = await executor._place_stop_order(client, market["apex_symbol"], False, filled_qty, sl_trigger, sl_limit)
    print(f"✅ Stop-loss geplaatst: trigger={sl_trigger} (volle qty={filled_qty}, order-id={sl_oid})")

    db.log_order(
        symbol=f"{coin}USDT", side="Buy", dry_run=False, status="manual_test",
        entry_price=entry_price, leverage=used_leverage, qty=filled_qty,
        stop_loss=sl_trigger, tp1=tp_trigger,
    )
    print("\nGelogd naar bot_history.db met status='manual_test' -- herkenbaar als aparte "
          "pill-badge op het dashboard, niet te verwarren met een echt Telegram-signal.")
    print("Let op: deze positie staat NIET in open_positions.json, dus handle_tp_event() "
          "beheert 'm niet -- de TP/SL hierboven zijn losse, op zichzelf staande orders. "
          "Sluit 'm zelf in de ApeX Omni-app als TP/SL niet binnen afzienbare tijd raken.")


if __name__ == "__main__":
    asyncio.run(main())
