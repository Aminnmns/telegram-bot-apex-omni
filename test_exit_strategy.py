"""
Simulatietest voor de v2-exit-strategie in executor.py: margin-based sizing
bij entry (config.MAX_MARGIN_PCT_OF_FUNDS% van het beschikbare saldo), alleen
een SL bij entry (geen TP-trigger-order), en sluiten op basis van de groep's
eigen "Take-Profit target N ✅"-berichten via handle_tp_event(). 5-staps
TP-ladder (targets 1 t/m 4 elk hun eigen cumulatieve percentage van de
ORIGINELE qty, target 5 altijd de volledige rest) met automatische
MIN_NOTIONAL_USD-doorschuif-fallback (zie executor._handle_tp_partial_event).
Mockt de Hyperliquid Exchange/Info-objecten volledig -- geen echte private
key, netwerk of geld nodig:

    python test_exit_strategy.py

Isolatie (belangrijk, zie incident 2026-08-10): dit script geeft de fake
exchange/info als EXPLICIETE functie-argumenten mee aan place_entry_order()/
handle_tp_event()/handle_cancel_event() -- geen monkeypatching van
executor._get_exchange()/_get_info() meer. Daarnaast wordt executor.STATE_FILE
en db.DB_PATH allebei omgeleid naar bestanden buiten de projectmap, zodat dit
script het gedeelde open_positions.json/bot_history.db van de live
signal-bot.service nooit kan raken, ongeacht wat er verder misgaat.

Scenario's:
1.  place_entry_order() v2: margin-based qty (MAX_MARGIN_PCT_OF_FUNDS% van
    het beschikbare saldo * toegestane leverage / prijs), ALLEEN een SL
    geplaatst (geen TP-order), state-entry met "version": "v2".
1b. Max-posities-limiet -> overslaan, geen order-calls.
1c. Nagenoeg geen beschikbaar saldo (perps-withdrawable + spot-USDC SAMEN
    te laag) -> qty rondt af naar 0 -> overslaan, geen order-calls.
1c-bis. Perps-withdrawable alléén is te laag, maar vrije spot-USDC dekt het
    gat -> MOET slagen.
1d. qty > 0 na afronding, maar orderwaarde onder MIN_NOTIONAL_USD -> zelfde
    skip-pad als 1c.
1e. Losse unit-check: margin_to_use = available_funds * (MAX_MARGIN_PCT_OF_FUNDS
    / 100), dus nu 0.33 i.p.v. de oude 0.25 (taak 3a).
1f. qty mikt op MAX_MARGIN_PCT_OF_FUNDS% van de TOTALE accountwaarde, niet
    van de (door andere open posities geslonken) withdrawable -> blijft
    constant ongeacht hoeveel marge al vastzit elders.
1g. Diezelfde totale-accountwaarde-qty past niet binnen de werkelijk vrije
    marge -> nette skip (skipped_insufficient_margin), geen Hyperliquid-
    afwijzing.
2.  TP-event target 1 -> TP_EVENT_TARGET1_CLOSE_PCT% (15%, jouw live .env-
    ladder) van de ORIGINELE qty market-sluiten, GEEN SL-aanpassing (BE-shift
    zit sinds 2026-08-25 op config.BREAKEVEN_MOVE_AFTER_TARGET, default
    target 2 -- een BE-shift al bij target 1 gaf te weinig ademruimte en liet
    TP2-5 stelselmatig missen), state bijgewerkt.
2b. Dezelfde TP-event target 1 nogmaals -> genegeerd (tp1_done), geen calls.
3.  TP-event target 2 -> nog eens TP_EVENT_TARGET2_CLOSE_PCT% (cumulatief
    30%) sluiten + oude SL geannuleerd, nieuwe break-even-SL op basis van de
    ECHT gebankte winst uit target 1+2 (config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT
    als fee/slippage-marge) voor de rest.
3b. Dezelfde TP-event target 2 nogmaals -> genegeerd (tp2_done).
4.  TP-event target 3 -> cumulatief 50% sluiten (i.p.v. vroeger: volledige
    rest).
5.  TP-event target 4 -> cumulatief 75% sluiten.
6.  TP-event target 5 -> ALTIJD de volledige resterende qty sluiten (finale
    exit), positie verdwijnt uit state. Bevestigt dat de som van alle
    deel-closes (stap 2+3+4+5+6) exact de originele qty is -- geen dust,
    geen overshoot (taak 3b).
7.  TP-event voor een coin zonder open v2-positie -> genegeerd.
8.  DRY_RUN-veiligheidsnet in handle_tp_event(): staat er tóch een v2-entry
    in state terwijl DRY_RUN=True, dan geen enkele echte cancel/order-call.
9.  MIN_NOTIONAL_USD-doorschuif-edge-case (taak 3c): klein account waarbij
    de eerste paar targets ELK onder de $10-notionalgrens vallen -> worden
    overgeslagen en schuiven automatisch door naar de eerstvolgende target
    die wél boven de grens uitkomt (of naar target 5, de finale fallback,
    die ALTIJD sluit ongeacht notional). Bevestigt dat geen enkele
    daadwerkelijk geplaatste deel-close-order (behalve de finale target-5
    exit) onder MIN_NOTIONAL_USD zit, en dat de som nog steeds exact de
    originele qty is.
10. Cancel-event (signal_parser.parse_cancel_event / executor.handle_cancel_event,
    toegevoegd na het incident van 2026-08-12: een "#HYPE/USDT Cancelled"-
    bericht werd niet herkend, de bijbehorende positie bleef live open staan
    tot 'ie handmatig gesloten werd) -> volledige resterende qty market-
    sluiten, SL eerst geannuleerd, positie uit state.
10b. Dezelfde cancel-event nogmaals (het kanaal stuurt "Close X/USDT" EN
    "#X/USDT Cancelled" als losse berichten) -> genegeerd, geen dubbele close.
10c. BIJNA-gelijktijdig duplicaat cancel-event (i.p.v. sequentieel zoals 10b).
10d. DRY_RUN-veiligheidsnet in handle_cancel_event().
"""
import asyncio
import json
import os
import tempfile
import time

import config
config.MAX_MARGIN_PCT_OF_FUNDS = 33.0
config.MIN_NOTIONAL_USD = 10.0
# Jouw daadwerkelijke live ladder (.env), niet de oude 40/20/15/15-defaults --
# stap 2 t/m 6 hieronder testen dus exact de percentages die ook echt draaien.
config.TP_EVENT_TARGET1_CLOSE_PCT = 15.0
config.TP_EVENT_TARGET2_CLOSE_PCT = 15.0
config.TP_EVENT_TARGET3_CLOSE_PCT = 20.0
config.TP_EVENT_TARGET4_CLOSE_PCT = 25.0
config.MAX_CONCURRENT_POSITIONS = 5
# Stap 1-6/9/10 simuleren de LIVE v2-flow tegen een fake backend, dus DRY_RUN
# hier expliciet False zetten -- onafhankelijk van wat er in .env staat (die
# staat momenteel bewust op True voor de echte live service). Stap 8/10d
# zetten 'm tijdelijk terug op True om juist het DRY_RUN-veiligheidsnet zelf
# te testen.
config.DRY_RUN = False
# Expliciet fout gezet, geen valide 32-byte key -- als er OOIT een pad zou
# zijn dat toch de echte _get_wallet()/_get_exchange() aanspreekt (i.p.v. de
# hieronder geïnjecteerde fake), moet dat hard falen bij het signen, niet
# stilletjes een order op een bestaand account plaatsen.
config.HYPERLIQUID_PRIVATE_KEY = "0x" + "00" * 32

import db
import executor
from signal_parser import Signal, TPEvent, CancelEvent

# Isolatie: BEIDE bestanden buiten de projectmap, nooit hetzelfde pad als de
# live service (executor.STATE_FILE default resp. db.DB_PATH default).
_TMP_DIR = tempfile.mkdtemp(prefix="signalbot_test_")
TEST_STATE_FILE = os.path.join(_TMP_DIR, "open_positions.json")
TEST_DB_PATH = os.path.join(_TMP_DIR, "bot_history.db")
assert TEST_STATE_FILE != executor.STATE_FILE
assert TEST_DB_PATH != db.DB_PATH
executor.STATE_FILE = TEST_STATE_FILE
db.DB_PATH = TEST_DB_PATH

FAKE_OWNER = "0x000000000000000000000000000000deadbeef"


class FakeHyperliquidServer:
    """Houdt bij welke SDK-calls er zijn gedaan en simuleert Hyperliquid's
    responses voor de v2-flow (SL-only entry, reduce-only IOC-close op
    TP-events). `universe_symbol` laat toe om per test-scenario een andere
    market te simuleren (bv. een losse, goedkope "EDGE"-coin voor de
    MIN_NOTIONAL_USD-edge-case, los van de SOL-market die de rest van het
    script gebruikt)."""

    def __init__(self, mark_px: float, max_leverage: int, sz_decimals: int,
                 perp_withdrawable: float = 1000.0, spot_free_usdc: float = 0.0,
                 universe_symbol: str = "SOL",
                 perp_account_value: float = None, spot_total_usdc: float = None):
        self.calls = []
        self.universe_symbol = universe_symbol
        self.mark_px = mark_px
        self.max_leverage = max_leverage
        self.sz_decimals = sz_decimals
        # Twee losse velden, want get_withdrawable() telt ze nu op -- zelf
        # empirisch geverifieerd (2026-08-11): Hyperliquid accepteert nieuwe
        # posities die meer marge vereisen dan clearinghouseState.withdrawable
        # alleen toestaat, gedekt door vrije spot-USDC (spot_user_state's
        # tokenToAvailableAfterMaintenance). Zie get_withdrawable()'s docstring.
        self.perp_withdrawable = perp_withdrawable
        self.spot_free_usdc = spot_free_usdc
        # accountValue/spot-total zijn los van withdrawable/spot_free, want
        # get_total_equity() gebruikt ze als sizing-basis i.p.v. wat er NU nog
        # vrij is (zie _total_equity_from_state). Als property: volgt
        # withdrawable/spot_free automatisch tenzij expliciet overschreven --
        # zo blijven bestaande tests die alleen `server.perp_withdrawable = X`
        # aanpassen ongewijzigd werken (accountwaarde == wat er vrij is), en
        # kan een nieuw scenario ze bewust laten afwijken (marge al vast in
        # andere gelijktijdig open posities).
        self._perp_account_value_override = perp_account_value
        self._spot_total_usdc_override = spot_total_usdc
        self._next_oid = 1000
        self.open_positions_count = 0  # voor count_open_positions()
        # Optioneel: {"coin", "szi", "entryPx"} -- simuleert wat Hyperliquid's
        # ECHTE clearinghouseState rapporteert voor _get_live_position()/
        # reconcile_positions(), los van open_positions_count hierboven (die
        # kent geen szi/entryPx). Alleen gezet in scenario's die dit expliciet
        # testen (re-entry-merge, reconciliatie); overal elders None, dus
        # bestaand gedrag blijft ongewijzigd.
        self.live_position_override = None

    @property
    def perp_account_value(self):
        if self._perp_account_value_override is not None:
            return self._perp_account_value_override
        return self.perp_withdrawable

    @perp_account_value.setter
    def perp_account_value(self, value):
        self._perp_account_value_override = value

    @property
    def spot_total_usdc(self):
        if self._spot_total_usdc_override is not None:
            return self._spot_total_usdc_override
        return self.spot_free_usdc

    @spot_total_usdc.setter
    def spot_total_usdc(self, value):
        self._spot_total_usdc_override = value

    def next_oid(self) -> int:
        self._next_oid += 1
        return self._next_oid

    # --- Info-achtige methodes ---
    def user_state(self, address):
        self.calls.append(("user_state", {"address": address}))
        positions = [{"position": {"coin": "SOL"}}] * self.open_positions_count
        if self.live_position_override is not None:
            positions = positions + [{"position": self.live_position_override}]
        return {
            "assetPositions": positions,
            "withdrawable": self.perp_withdrawable,
            "marginSummary": {"accountValue": self.perp_account_value},
        }

    def spot_user_state(self, address):
        self.calls.append(("spot_user_state", {"address": address}))
        return {
            "balances": [{"coin": "USDC", "token": 0, "total": str(self.spot_total_usdc), "hold": "0.0"}],
            "tokenToAvailableAfterMaintenance": [[0, str(self.spot_free_usdc)]],
        }

    def meta_and_asset_ctxs(self):
        self.calls.append(("meta_and_asset_ctxs", {}))
        universe = [{"name": self.universe_symbol, "szDecimals": self.sz_decimals,
                     "maxLeverage": self.max_leverage, "onlyIsolated": False}]
        ctxs = [{"markPx": str(self.mark_px)}]
        return [{"universe": universe}, ctxs]

    # --- Exchange-achtige methodes ---
    def update_leverage(self, leverage, name, is_cross):
        self.calls.append(("update_leverage", {"leverage": leverage, "name": name, "is_cross": is_cross}))
        return {"status": "ok", "response": {"type": "default"}}

    def market_open(self, name, is_buy, sz, px=None, slippage=0.05, cloid=None, builder=None):
        self.calls.append(("market_open", {"name": name, "is_buy": is_buy, "sz": sz}))
        return {"status": "ok", "response": {"data": {"statuses": [
            {"filled": {"totalSz": str(sz), "avgPx": str(self.mark_px), "oid": self.next_oid()}}
        ]}}}

    def order(self, name, is_buy, sz, limit_px, order_type, reduce_only=False, cloid=None, builder=None):
        self.calls.append(("order", {
            "name": name, "is_buy": is_buy, "sz": sz, "limit_px": limit_px,
            "order_type": order_type, "reduce_only": reduce_only,
        }))
        oid = self.next_oid()
        # Een IOC-order die daadwerkelijk vult rapporteert Hyperliquid als
        # "filled" (net als market_open), niet als "resting" -- dat laatste
        # geldt alleen voor trigger-orders (SL) die op de book blijven staan
        # totdat ze getriggerd worden. executor._handle_tp_partial_event
        # leest avgPx uit deze fill voor de banked_pnl-berekening.
        if order_type == {"limit": {"tif": "Ioc"}}:
            return {"status": "ok", "response": {"data": {"statuses": [
                {"filled": {"totalSz": str(sz), "avgPx": str(self.mark_px), "oid": oid}}
            ]}}}
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": oid}}]}}}

    def cancel(self, name, oid):
        self.calls.append(("cancel", {"name": name, "oid": oid}))
        return {"status": "ok", "response": {"data": {"statuses": []}}}


def expected_cum_close_qty(qty, remaining_before, cum_pct, sz_decimals):
    """Onafhankelijke herimplementatie van executor._target_close_qty (als
    oracle voor de asserts hieronder, niet als vervanging van de eigenlijke
    implementatie): hoeveel er nu dicht moet voor cumulatief percentage
    `cum_pct` van de ORIGINELE qty."""
    total_should_be_closed = round(qty * (cum_pct / 100), sz_decimals)
    already_closed = round(qty - remaining_before, sz_decimals)
    close_qty = round(total_should_be_closed - already_closed, sz_decimals)
    return max(0.0, min(close_qty, remaining_before))


async def main():
    db.init_db()

    # SOL op 192.85, 50x max leverage.
    server = FakeHyperliquidServer(mark_px=192.85, max_leverage=50, sz_decimals=2)

    notifications = []

    async def fake_notify(msg):
        notifications.append(msg)

    executor.notify_callback = fake_notify

    signal = Signal(
        symbol="SOLUSDT",
        side="Sell",
        entry_low=191.8,
        entry_high=193.9,
        leverage=25,
        targets=[190.3, 189.5, 187.8, 185.9, 183.0],
        stop_loss=200.3,
        raw_text="test",
    )

    # --- Stap 1: v2-entry plaatsen (fake exchange/info als argument) ---
    result = await executor.place_entry_order(signal, dry_run=False, exchange=server, info=server)
    assert result is not None and result.startswith("qty="), f"onverwacht return-resultaat: {result}"

    call_names = [name for name, _ in server.calls]
    assert call_names == [
        "user_state", "meta_and_asset_ctxs", "user_state", "spot_user_state",
        "update_leverage", "market_open", "order",
    ], f"onverwachte volgorde van calls: {call_names}"

    state = json.load(open(TEST_STATE_FILE))
    assert len(state) == 1, "verwacht 1 open positie in state"
    pos = state["SOL:Sell"]
    assert pos["version"] == "v2", "nieuwe entries moeten 'version': 'v2' hebben"
    assert pos["tp1_done"] is False
    assert pos["tp2_done"] is False and pos["tp3_done"] is False and pos["tp4_done"] is False
    assert pos["remaining_qty"] == pos["qty"], "vóór enige TP-event moet remaining_qty == volle qty zijn"

    qty = pos["qty"]
    used_leverage_1 = min(signal.leverage, server.max_leverage)
    withdrawable_1 = server.perp_withdrawable + server.spot_free_usdc
    total_equity_1 = server.perp_account_value + server.spot_total_usdc
    expected_qty = executor._calc_margin_based_qty(server.mark_px, used_leverage_1, total_equity_1, server.sz_decimals)
    assert qty == expected_qty, f"margin-based qty klopt niet: {qty} != {expected_qty}"

    sl_call = server.calls[6][1]
    assert sl_call["order_type"]["trigger"]["tpsl"] == "sl" and sl_call["sz"] == qty, \
        "SL moet op de VOLLE qty staan"
    assert sl_call["is_buy"] is True, "SL van een SHORT moet een reduce-only BUY-order zijn"
    assert sl_call["limit_px"] > sl_call["order_type"]["trigger"]["triggerPx"], \
        "SL-limit moet HOGER dan de trigger staan voor een BUY-exit (agressief genoeg om te vullen)"
    assert "tp1_price" not in pos, "v2-entries plaatsen GEEN TP-trigger-order, dus geen tp1_price"
    print(f"OK stap 1: v2-entry geplaatst, qty={qty} ({config.MAX_MARGIN_PCT_OF_FUNDS}% van "
          f"${withdrawable_1:.2f} beschikbaar bij {used_leverage_1}x), alleen SL geplaatst")

    # SOL:Sell (stap 1) tijdelijk opzij: stap 1c t/m 1g testen saldo/marge-
    # logica voor een NIEUW Buy-signaal, los van de tegengestelde-positie-
    # guard in place_entry_order() (die wordt apart getest in stap 13) --
    # anders zou elke Buy-poging hieronder al op die guard stuiten i.p.v. op
    # wat deze stappen willen verifiëren.
    async with executor._state_lock:
        sol_sell_backup = executor._load_state()
        executor._save_state({})

    # --- Stap 1b: max-posities-limiet -- op de limiet -> overslaan ---
    signal2 = Signal(
        symbol="TAOUSDT", side="Buy", entry_low=200, entry_high=205,
        leverage=10, targets=[210, 215, 220], stop_loss=190, raw_text="test2",
    )
    server.open_positions_count = config.MAX_CONCURRENT_POSITIONS
    calls_before = len(server.calls)
    result2 = await executor.place_entry_order(signal2, dry_run=False, exchange=server, info=server)
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert result2 is None, f"had overgeslagen moeten worden, kreeg: {result2}"
    assert new_calls == ["user_state"], f"had alleen de positie-telling mogen doen: {new_calls}"
    server.open_positions_count = 0
    assert len(notifications) == 1 and "max posities" in notifications[0]
    notifications.clear()
    print(f"OK stap 1b: trade overgeslagen op max-posities-limiet ({config.MAX_CONCURRENT_POSITIONS})")

    # --- Stap 1c: nagenoeg geen beschikbaar saldo (perps + spot SAMEN te laag)
    # -> de op MAX_MARGIN_PCT_OF_FUNDS gebaseerde qty rondt af naar 0 -> zelfde
    # skip-pad als "orderwaarde onder minimum". (server kent alleen de
    # SOL-market, dus hier bewust een SOL-signal i.p.v. signal2/TAOUSDT
    # gebruiken -- anders faalt dit al op de market-lookup.) ---
    signal1c = Signal(
        symbol="SOLUSDT", side="Buy", entry_low=190, entry_high=195,
        leverage=10, targets=[200, 205, 210], stop_loss=180, raw_text="margin-test",
    )
    server.perp_withdrawable = 0.01
    server.spot_free_usdc = 0.0
    calls_before = len(server.calls)
    result1c = await executor.place_entry_order(signal1c, dry_run=False, exchange=server, info=server)
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert result1c is None, f"had overgeslagen moeten worden wegens te kleine ordergrootte, kreeg: {result1c}"
    assert new_calls == ["user_state", "meta_and_asset_ctxs", "user_state", "spot_user_state"], \
        f"had geen order-calls mogen doen: {new_calls}"
    assert len(notifications) == 1 and "te kleine ordergrootte" in notifications[0], notifications
    notifications.clear()
    print("OK stap 1c: trade overgeslagen -- perps-withdrawable ($0.01) + spot ($0.00) samen te weinig voor een qty > 0")

    # --- Stap 1c-bis: perps-withdrawable ALLEEN is te laag, maar vrije
    # spot-USDC dekt het gat -- moet WEL slagen. Dit is precies het
    # empirisch geverifieerde gedrag van het echte account (2026-08-11):
    # clearinghouseState.withdrawable=$0.00, maar Hyperliquid accepteerde
    # een test-order gedekt door vrije spot-USDC. Zie get_withdrawable(). ---
    server.perp_withdrawable = 0.0
    server.spot_free_usdc = 1000.0
    calls_before = len(server.calls)
    result1c_bis = await executor.place_entry_order(signal1c, dry_run=False, exchange=server, info=server)
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert result1c_bis is not None and result1c_bis.startswith("qty="), \
        f"had moeten slagen dankzij vrije spot-USDC, kreeg: {result1c_bis}"
    assert "spot_user_state" in new_calls, f"get_withdrawable() had spot_user_state moeten aanroepen: {new_calls}"
    print(f"OK stap 1c-bis: perps-withdrawable=$0 maar spot-USDC dekt het -> entry SLAAGT "
          f"({result1c_bis}), bevestigt dat get_withdrawable() de twee optelt")

    # Opruimen: deze SOL:Buy-entry heeft niets te maken met de rest van het
    # scenario (dat draait verder om de SOL:Sell-entry uit stap 1) -- meteen
    # weer verwijderen zodat stap 2's handle_tp_event() straks niet op twee
    # kandidaten voor "SOL" stuit.
    async with executor._state_lock:
        state = executor._load_state()
        state.pop("SOL:Buy", None)
        executor._save_state(state)
    server.perp_withdrawable = 1000.0
    server.spot_free_usdc = 0.0

    # --- Stap 1d: qty rondt WEL af naar > 0, maar de resulterende orderwaarde
    # blijft onder MIN_NOTIONAL_USD -> zelfde skip-pad, nu via de andere kant
    # van de "qty <= 0 or notional < MIN_NOTIONAL_USD"-check (i.p.v. stap 1c's
    # qty==0). Geen RuntimeError meer (zie executor.place_entry_order): te
    # weinig saldo is een normale, te verwachten toestand, geen
    # configuratiefout, dus overslaan i.p.v. een fout opgooien. ---
    # side="Buy" (niet "Sell"): stap 1's SOL:Sell-positie staat nog open in
    # state (blijft dat t/m stap 2), en sinds de duplicaat-signal-guard (zie
    # place_entry_order) zou een SOL:Sell-signal hier de VERKEERDE skip-reden
    # geven (al open positie i.p.v. orderwaarde onder minimum).
    signal_tiny = Signal(
        symbol="SOLUSDT", side="Buy", entry_low=191.8, entry_high=193.9,
        leverage=10, targets=[190.3], stop_loss=200.3, raw_text="tiny",
    )
    # $2 beschikbaar * 33% * 10x / 192.85 rondt af naar een kleine qty>0 met
    # notional ruim onder MIN_NOTIONAL_USD (bij de oude 25% zou $4 al genoeg
    # zijn geweest, maar bij 33% is dat nu boven de grens -- vandaar $2 i.p.v.
    # het oorspronkelijke $4).
    server.perp_withdrawable = 2.0
    server.spot_free_usdc = 0.0
    calls_before = len(server.calls)
    result_tiny = await executor.place_entry_order(signal_tiny, dry_run=False, exchange=server, info=server)
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert result_tiny is None, f"had overgeslagen moeten worden wegens orderwaarde onder minimum, kreeg: {result_tiny}"
    assert new_calls == ["user_state", "meta_and_asset_ctxs", "user_state", "spot_user_state"], \
        f"had geen order-calls mogen doen: {new_calls}"
    assert len(notifications) == 1 and "te kleine ordergrootte" in notifications[0], notifications
    notifications.clear()
    orders = db.recent_orders(limit=1)
    assert orders[0]["status"] == "skipped_min_notional"
    print("OK stap 1d: qty > 0 na afronding maar orderwaarde onder MIN_NOTIONAL_USD -> overgeslagen, geen RuntimeError")

    # --- Stap 1e: losse unit-check dat margin_to_use = available_funds * 0.33
    # is (i.p.v. de oude 0.25) -- taak 3a. Ronde getallen (leverage=1,
    # entry_px=1.0) zodat er geen afrondingsonzekerheid in de assert zit. ---
    assert config.MAX_MARGIN_PCT_OF_FUNDS == 33.0, "deze check gaat uit van de nieuwe 33%-default"
    check_funds, check_lev, check_px, check_szdec = 100.0, 1, 1.0, 6
    check_qty = executor._calc_margin_based_qty(check_px, check_lev, check_funds, check_szdec)
    implied_margin_to_use = check_qty * check_px / check_lev
    expected_margin_to_use = check_funds * (config.MAX_MARGIN_PCT_OF_FUNDS / 100)
    assert abs(expected_margin_to_use - 33.0) < 1e-9, expected_margin_to_use
    assert abs(implied_margin_to_use - expected_margin_to_use) < 1e-9, \
        f"margin_to_use klopt niet: {implied_margin_to_use} != {expected_margin_to_use} (verwacht available_funds*0.33)"
    print(f"OK stap 1e: margin_to_use = available_funds * {config.MAX_MARGIN_PCT_OF_FUNDS/100} "
          f"(${check_funds:.2f} -> ${implied_margin_to_use:.2f} margin), niet meer *0.25")

    # --- Stap 1f: qty blijft CONSTANT als er al marge vastzit in andere
    # gelijktijdig open posities -- MAX_MARGIN_PCT_OF_FUNDS% wordt genomen van
    # de TOTALE accountwaarde, niet van wat er NU nog vrij is (zie
    # _total_equity_from_state). Zonder deze fix zou de qty hier gebaseerd
    # zijn op de geslonken withdrawable ($500) i.p.v. de volle accountwaarde
    # ($1000), en dus kleiner uitvallen dan bedoeld naarmate meer posities
    # tegelijk openen. ---
    signal_1f = Signal(
        symbol="SOLUSDT", side="Buy", entry_low=191.8, entry_high=193.9,
        leverage=10, targets=[190.3], stop_loss=200.3, raw_text="equity-based-sizing",
    )
    server.perp_withdrawable = 500.0    # al deels vastgezet in andere posities
    server.spot_free_usdc = 0.0
    server.perp_account_value = 1000.0  # totale accountwaarde blijft hoog
    server.spot_total_usdc = 0.0
    result_1f = await executor.place_entry_order(signal_1f, dry_run=False, exchange=server, info=server)
    assert result_1f is not None and result_1f.startswith("qty="), f"had moeten slagen: {result_1f}"
    used_leverage_1f = min(signal_1f.leverage, server.max_leverage)
    expected_qty_1f = executor._calc_margin_based_qty(server.mark_px, used_leverage_1f, 1000.0, server.sz_decimals)
    withdrawable_based_qty_1f = executor._calc_margin_based_qty(server.mark_px, used_leverage_1f, 500.0, server.sz_decimals)
    state = json.load(open(TEST_STATE_FILE))
    qty_1f = state["SOL:Buy"]["qty"]
    assert qty_1f == expected_qty_1f, \
        f"qty had gebaseerd moeten zijn op totale accountwaarde $1000, niet op withdrawable $500: {qty_1f} != {expected_qty_1f}"
    assert qty_1f != withdrawable_based_qty_1f, \
        "sanity check: qty moet AFWIJKEN van wat withdrawable-based sizing zou geven"
    print(f"OK stap 1f: qty={qty_1f} gebaseerd op totale accountwaarde ($1000), "
          f"NIET op de geslonken withdrawable ($500) -- blijft constant ongeacht andere open posities")

    # Opruimen, zelfde reden als na stap 1c-bis.
    async with executor._state_lock:
        state = executor._load_state()
        state.pop("SOL:Buy", None)
        executor._save_state(state)

    # --- Stap 1g: als de qty op basis van de TOTALE accountwaarde meer marge
    # vereist dan er WERKELIJK vrij is, wordt de trade netjes overgeslagen
    # (nieuwe check in place_entry_order) i.p.v. dat Hyperliquid de order zelf
    # afwijst. ---
    signal_1g = Signal(
        symbol="SOLUSDT", side="Buy", entry_low=191.8, entry_high=193.9,
        leverage=10, targets=[190.3], stop_loss=200.3, raw_text="insufficient-margin",
    )
    server.perp_withdrawable = 100.0    # veel minder vrij dan de 33%-van-$1000 (=$330) target
    server.spot_free_usdc = 0.0
    server.perp_account_value = 1000.0
    server.spot_total_usdc = 0.0
    calls_before = len(server.calls)
    result_1g = await executor.place_entry_order(signal_1g, dry_run=False, exchange=server, info=server)
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert result_1g is None, f"had overgeslagen moeten worden wegens onvoldoende vrije marge, kreeg: {result_1g}"
    assert "order" not in new_calls and "market_open" not in new_calls, f"had geen order-calls mogen doen: {new_calls}"
    assert len(notifications) == 1 and "onvoldoende vrije marge" in notifications[0], notifications
    notifications.clear()
    orders = db.recent_orders(limit=1)
    assert orders[0]["status"] == "skipped_insufficient_margin"
    print("OK stap 1g: qty (op basis van totale accountwaarde) paste niet binnen de werkelijk vrije marge -> "
          "netjes overgeslagen (skipped_insufficient_margin), geen Hyperliquid-afwijzing")

    server.perp_account_value = None  # override weer uit -- volgt weer perp_withdrawable
    server.spot_total_usdc = None

    # Saldo terugzetten voor de rest van het scenario.
    server.perp_withdrawable = 1000.0
    server.spot_free_usdc = 0.0

    # SOL:Sell (stap 1) terugzetten voor de rest van het scenario.
    async with executor._state_lock:
        executor._save_state(sol_sell_backup)

    # --- Stap 2 t/m 6: volledige 5-staps TP-ladder op de SOL:Sell-positie uit
    # stap 1 (taak 3b) -- na elke stap: juiste close_qty, juiste remaining_qty,
    # en aan het eind: som van alle deel-closes == originele qty exact. ---
    original_qty = qty
    remaining = original_qty
    total_closed = 0.0
    cum_pcts = {
        1: config.TP_EVENT_TARGET1_CLOSE_PCT,
        2: config.TP_EVENT_TARGET1_CLOSE_PCT + config.TP_EVENT_TARGET2_CLOSE_PCT,
        3: config.TP_EVENT_TARGET1_CLOSE_PCT + config.TP_EVENT_TARGET2_CLOSE_PCT + config.TP_EVENT_TARGET3_CLOSE_PCT,
        4: (config.TP_EVENT_TARGET1_CLOSE_PCT + config.TP_EVENT_TARGET2_CLOSE_PCT
            + config.TP_EVENT_TARGET3_CLOSE_PCT + config.TP_EVENT_TARGET4_CLOSE_PCT),
    }

    # --- Stap 2: target 1 -> 15% sluiten (jouw live .env-ladder), GEEN
    # SL-aanpassing (BE-shift zit nu op config.BREAKEVEN_MOVE_AFTER_TARGET,
    # default target 2 -- zie incident 2026-08-25: een BE-shift al bij
    # target 1 gaf bij een kleine TP1 te weinig ademruimte en liet TP2-5
    # stelselmatig missen) ---
    sl_oid_before_t1 = pos["sl_oid"]
    close_qty_1 = expected_cum_close_qty(original_qty, remaining, cum_pcts[1], server.sz_decimals)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=1, raw_text="test"), exchange=server, info=server,
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["meta_and_asset_ctxs", "order"], f"onverwachte volgorde bij TP1-event: {new_calls}"

    close_call = server.calls[calls_before + 1][1]

    assert close_call["order_type"] == {"limit": {"tif": "Ioc"}} and close_call["reduce_only"] is True, \
        "target1-close moet een reduce-only IOC-marketorder zijn, geen trigger-order"
    assert close_call["sz"] == close_qty_1, f"moet {config.TP_EVENT_TARGET1_CLOSE_PCT}% van de ORIGINELE qty sluiten"
    assert close_call["is_buy"] is True, "sluiten van een SHORT is een BUY"

    remaining = round(remaining - close_qty_1, server.sz_decimals)
    total_closed = round(total_closed + close_qty_1, server.sz_decimals)

    assert len(notifications) == 1 and "break-even" not in notifications[0] and "TP1" in notifications[0], \
        notifications
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    pos = state["SOL:Sell"]
    assert pos["tp1_done"] is True
    assert pos["remaining_qty"] == remaining
    assert pos["sl_oid"] == sl_oid_before_t1, "target 1 mag de SL niet meer aanraken (BE-shift zit nu op target 2)"
    print(f"OK stap 2: TP1-event verwerkt -- {close_qty_1} gesloten ({config.TP_EVENT_TARGET1_CLOSE_PCT}% van "
          f"origineel), geen SL-wijziging, resterende {remaining}")

    # --- Stap 2b: zelfde TP1-event nogmaals -> genegeerd (al verwerkt) ---
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=1, raw_text="test"), exchange=server, info=server,
    )
    assert len(server.calls) == calls_before, "een dubbel TP1-event mag GEEN nieuwe calls doen"
    assert len(notifications) == 0
    print("OK stap 2b: duplicaat TP1-event genegeerd, geen dubbele close")

    # --- Stap 3: target 2 -> cumulatief 30% sluiten + SL naar dynamische
    # break-even (config.BREAKEVEN_MOVE_AFTER_TARGET=2, gebaseerd op de ECHTE
    # gebankte winst uit target 1+2, config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT) ---
    close_qty_2 = expected_cum_close_qty(original_qty, remaining, cum_pcts[2], server.sz_decimals)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=2, raw_text="test"), exchange=server, info=server,
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["meta_and_asset_ctxs", "order", "cancel", "order"], \
        f"onverwachte volgorde bij TP2-event: {new_calls}"
    close_call = server.calls[calls_before + 1][1]
    cancel_call = server.calls[calls_before + 2][1]
    be_sl_call = server.calls[calls_before + 3][1]

    assert close_call["sz"] == close_qty_2, \
        f"target2 moet {config.TP_EVENT_TARGET2_CLOSE_PCT}% extra sluiten (cumulatief {cum_pcts[2]}%)"
    assert close_call["order_type"] == {"limit": {"tif": "Ioc"}} and close_call["reduce_only"] is True

    assert cancel_call["oid"] == sl_oid_before_t1, "moet de OORSPRONKELIJKE (nog-niet-verplaatste) SL annuleren"

    remaining = round(remaining - close_qty_2, server.sz_decimals)
    total_closed = round(total_closed + close_qty_2, server.sz_decimals)

    # mark_px staat hier gelijk aan de entry-prijs (geen koersbeweging in dit
    # scenario) -> beide closes leveren $0 banked_pnl op -> de dynamische
    # break-even-trigger komt daarom EXACT op de entry-prijs uit (offset 0),
    # niet op een vast percentage ernaast. Dat is het correcte nieuwe gedrag:
    # zonder gebankte winst is er ook geen ruimte om van entry af te wijken
    # (zie de aparte adaptiviteits-test verderop voor een scenario MET
    # koersbeweging, waar de trigger wel degelijk van entry afwijkt).
    expected_be_trigger = executor._round_px(pos["entry_price"], server.sz_decimals)
    assert be_sl_call["order_type"]["trigger"]["tpsl"] == "sl"
    assert be_sl_call["order_type"]["trigger"]["triggerPx"] == expected_be_trigger, \
        f"zonder gebankte winst (mark_px == entry) moet de dynamische SL exact op entry staan: " \
        f"verwacht {expected_be_trigger}, kreeg {be_sl_call['order_type']['trigger']['triggerPx']}"
    assert be_sl_call["sz"] == remaining, "nieuwe SL moet voor de resterende qty zijn"

    assert len(notifications) == 1 and "TP2" in notifications[0] and "break-even" in notifications[0], notifications
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    pos = state["SOL:Sell"]
    assert pos["tp2_done"] is True
    assert pos["remaining_qty"] == remaining
    assert pos["sl_oid"] != sl_oid_before_t1, "target 2 moet de SL nu wel verplaatsen (nieuwe sl_oid)"
    assert pos["sl_price"] == expected_be_trigger
    assert pos["banked_pnl"] == 0.0, \
        "geen koersbeweging in dit scenario -> TP1+TP2 sloten allebei op de entry-prijs, dus $0 gebankt"
    print(f"OK stap 3: TP2-event verwerkt -- {close_qty_2} extra gesloten (cumulatief "
          f"{cum_pcts[2]}%), SL naar dynamische break-even ({expected_be_trigger}, banked_pnl="
          f"{pos['banked_pnl']}), resterende {remaining}")

    # --- Stap 3b: zelfde TP2-event nogmaals -> genegeerd ---
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=2, raw_text="test"), exchange=server, info=server,
    )
    assert len(server.calls) == calls_before, "een dubbel TP2-event mag GEEN nieuwe calls doen"
    assert len(notifications) == 0
    print("OK stap 3b: duplicaat TP2-event genegeerd")

    # --- Stap 4: target 3 -> cumulatief 50% sluiten (nieuw gedrag: was
    # voorheen ALTIJD de volledige rest) ---
    close_qty_3 = expected_cum_close_qty(original_qty, remaining, cum_pcts[3], server.sz_decimals)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=3, raw_text="test"), exchange=server, info=server,
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["meta_and_asset_ctxs", "order"], f"onverwachte volgorde bij TP3-event: {new_calls}"
    close_call = server.calls[calls_before + 1][1]
    assert close_call["sz"] == close_qty_3, f"target3 moet cumulatief {cum_pcts[3]}% sluiten, niet de volledige rest"

    remaining = round(remaining - close_qty_3, server.sz_decimals)
    total_closed = round(total_closed + close_qty_3, server.sz_decimals)

    state = json.load(open(TEST_STATE_FILE))
    pos = state["SOL:Sell"]
    assert pos["tp3_done"] is True
    assert pos["remaining_qty"] == remaining
    assert "SOL:Sell" in state, "positie moet na target 3 NOG OPEN staan (nieuw gedrag, was voorheen 'klaar')"
    notifications.clear()
    print(f"OK stap 4: TP3-event verwerkt -- {close_qty_3} extra gesloten (cumulatief {cum_pcts[3]}%), "
          f"positie blijft open, resterende {remaining}")

    # --- Stap 5: target 4 -> cumulatief 75% sluiten ---
    close_qty_4 = expected_cum_close_qty(original_qty, remaining, cum_pcts[4], server.sz_decimals)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=4, raw_text="test"), exchange=server, info=server,
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["meta_and_asset_ctxs", "order"], f"onverwachte volgorde bij TP4-event: {new_calls}"
    close_call = server.calls[calls_before + 1][1]
    assert close_call["sz"] == close_qty_4, f"target4 moet cumulatief {cum_pcts[4]}% sluiten"

    remaining = round(remaining - close_qty_4, server.sz_decimals)
    total_closed = round(total_closed + close_qty_4, server.sz_decimals)

    state = json.load(open(TEST_STATE_FILE))
    pos = state["SOL:Sell"]
    assert pos["tp4_done"] is True
    assert pos["remaining_qty"] == remaining
    notifications.clear()
    print(f"OK stap 5: TP4-event verwerkt -- {close_qty_4} extra gesloten (cumulatief {cum_pcts[4]}%), "
          f"resterende {remaining}")

    # --- Stap 6: target 5 -> ALTIJD de volledige resterende qty sluiten,
    # positie klaar. Bevestigt: som van alle deel-closes == originele qty. ---
    close_qty_5 = remaining
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=5, raw_text="test"), exchange=server, info=server,
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["cancel", "meta_and_asset_ctxs", "order"], \
        f"onverwachte volgorde bij TP5-event: {new_calls}"

    cancel_call = server.calls[calls_before][1]
    close_call = server.calls[calls_before + 2][1]
    assert close_call["sz"] == close_qty_5, "target5 moet de volledige resterende qty sluiten"
    assert close_call["order_type"] == {"limit": {"tif": "Ioc"}} and close_call["reduce_only"] is True
    assert cancel_call["oid"] == pos["sl_oid"], "target5 moet de (break-even-)SL eerst annuleren"

    total_closed = round(total_closed + close_qty_5, server.sz_decimals)

    assert len(notifications) == 1 and "klaar" in notifications[0], notifications
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    assert "SOL:Sell" not in state, "positie had uit de state verwijderd moeten worden na target 5"
    assert abs(total_closed - original_qty) < 1e-9, \
        f"som van alle deel-closes ({total_closed}) moet EXACT de originele qty ({original_qty}) zijn -- " \
        f"dust of overshoot gedetecteerd"
    print(f"OK stap 6: TP5-event verwerkt -- resterende {close_qty_5} gesloten, positie uit state verwijderd. "
          f"Som van alle deel-closes: {round(close_qty_1+close_qty_2+close_qty_3+close_qty_4+close_qty_5, 2)} "
          f"== originele qty {original_qty} (geen dust, geen overshoot).")

    # --- Stap 7: TP-event voor coin zonder open v2-positie -> genegeerd ---
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="XRPUSDT", target_number=1, raw_text="test"), exchange=server, info=server,
    )
    assert len(server.calls) == calls_before, "geen open v2-positie -> geen enkele call"
    assert len(notifications) == 0
    tp_events = db.recent_tp_events(limit=1)
    assert tp_events[0]["event"] == "tp_event_ignored_no_position"
    print("OK stap 7: TP-event zonder open v2-positie genegeerd, wel gelogd")

    # --- Stap 8: DRY_RUN-veiligheidsnet in handle_tp_event() ---
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False, "entry_price": 192.85,
            "sl_price": 200.3, "sl_oid": 9999, "qty": 0.5, "remaining_qty": 0.5,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False, "sz_decimals": 2,
        }})
    config.DRY_RUN = True
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=1, raw_text="test"), exchange=server, info=server,
    )
    assert len(server.calls) == calls_before, "DRY_RUN=True met v2-entry had GEEN enkele call mogen doen"
    config.DRY_RUN = False
    async with executor._state_lock:
        executor._save_state({})
    print("OK stap 8: DRY_RUN-veiligheidsnet in handle_tp_event() werkt")

    # --- Stap 9: MIN_NOTIONAL_USD-doorschuif-edge-case (taak 3c) -- klein
    # account: originele positie qty=12 @ mark_px=1.0 (notional $12, dus de
    # entry zelf zou nog WEL boven MIN_NOTIONAL_USD=$10 zitten), maar de
    # eerste paar deel-closes (40%, +20%, +15% cumulatief) zitten elk onder
    # de $10-grens. Losse "EDGE"-market/server (i.p.v. SOL) en state direct
    # geseed, zodat dit scenario onafhankelijk van de margin-sizing exact
    # controleerbare getallen heeft:
    #   T1 cum 40% -> should=round(12*0.40)=5   -> notional $5  -> SKIP (schuift door)
    #   T2 cum 60% -> should=round(12*0.60)=7   -> notional $7  -> SKIP (schuift door)
    #   T3 cum 75% -> should=round(12*0.75)=9   -> notional $9  -> SKIP (schuift door)
    #   T4 cum 90% -> should=round(12*0.90)=11  -> close 11 (0 al dicht) -> notional $11 -> SLUIT
    #   T5         -> resterende 1 sluit ALTIJD, ongeacht notional ($1 < $10)
    # Bevestigt: elke ECHTE close-order (behalve de finale T5-exit) zit boven
    # MIN_NOTIONAL_USD, en de som van alle closes is exact 12.
    #
    # Deze percentages (40/20/15/15) zijn BEWUST losgekoppeld van jouw live
    # .env-ladder (15/15/20/25, zie stap 2-6 hierboven) -- dit scenario test
    # de MIN_NOTIONAL-cascade-mechaniek zelf, met getallen die precies om de
    # $10-grens heen liggen, niet je specifieke gekozen ladder. Lokaal
    # overschreven en na afloop teruggezet. ---
    tp_pcts_before_edge = (
        config.TP_EVENT_TARGET1_CLOSE_PCT, config.TP_EVENT_TARGET2_CLOSE_PCT,
        config.TP_EVENT_TARGET3_CLOSE_PCT, config.TP_EVENT_TARGET4_CLOSE_PCT,
    )
    config.TP_EVENT_TARGET1_CLOSE_PCT = 40.0
    config.TP_EVENT_TARGET2_CLOSE_PCT = 20.0
    config.TP_EVENT_TARGET3_CLOSE_PCT = 15.0
    config.TP_EVENT_TARGET4_CLOSE_PCT = 15.0

    edge_server = FakeHyperliquidServer(
        mark_px=1.0, max_leverage=10, sz_decimals=0, universe_symbol="EDGE",
    )
    edge_qty = 12.0
    async with executor._state_lock:
        executor._save_state({"EDGE:Sell": {
            "version": "v2", "symbol": "EDGE", "is_buy": False, "entry_price": 1.0,
            "sl_price": 1.1, "sl_oid": 4242, "qty": edge_qty, "remaining_qty": edge_qty,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False, "sz_decimals": 0,
        }})

    real_close_orders = []  # (target_number, sz, notional) voor elke ECHTE close-order

    async def run_edge_target(n):
        calls_before_edge = len(edge_server.calls)
        await executor.handle_tp_event(
            TPEvent(symbol="EDGEUSDT", target_number=n, raw_text="test"), exchange=edge_server, info=edge_server,
        )
        new_calls_edge = [name for name, _ in edge_server.calls[calls_before_edge:]]
        state_now = json.load(open(TEST_STATE_FILE))
        return new_calls_edge, state_now.get("EDGE:Sell")

    # T1: notional $5 < $10 -> geskipt, GEEN order-call, remaining blijft 12.
    new_calls, pos_edge = await run_edge_target(1)
    assert new_calls == ["meta_and_asset_ctxs"], f"T1 had alleen de market-lookup mogen doen: {new_calls}"
    assert pos_edge["remaining_qty"] == edge_qty, "T1 skip mag remaining_qty niet veranderen"
    assert pos_edge["tp1_done"] is True, "T1 moet wel als 'verwerkt' gemarkeerd zijn (voorkomt herhaling)"
    assert len(notifications) == 1 and "doorgeschoven" in notifications[0], notifications
    notifications.clear()
    tp_events = db.recent_tp_events(limit=1)
    assert tp_events[0]["event"] == "tp_event_skipped_min_notional"
    print("OK stap 9 (T1): deel-close $5 < $10-minimum -> overgeslagen, doorgeschoven, remaining_qty ongewijzigd")

    # T2: cumulatief zou nu 7 moeten sluiten -> notional $7 < $10 -> close
    # zelf blijft geskipt, MAAR target 2 == config.BREAKEVEN_MOVE_AFTER_TARGET,
    # dus de SL verplaatst nu tóch naar gebufferde break-even (ongeacht of de
    # close zelf doorging) -- cancel + nieuwe trigger-order, geen close-order.
    edge_sl_oid_before = 4242
    new_calls, pos_edge = await run_edge_target(2)
    assert new_calls == ["meta_and_asset_ctxs", "cancel", "order"], \
        f"T2 had geen close-order maar wel de BE-shift (cancel+order) moeten doen: {new_calls}"
    assert pos_edge["remaining_qty"] == edge_qty, "T2 skip mag remaining_qty niet veranderen"
    assert pos_edge["tp2_done"] is True
    assert pos_edge["sl_oid"] != edge_sl_oid_before, "BE-shift moet ook bij een geskipte close een nieuwe SL zetten"
    # T1 en T2 sloegen allebei over (MIN_NOTIONAL) -> geen enkele echte close,
    # dus $0 banked_pnl -> de dynamische break-even-trigger komt exact op de
    # entry-prijs uit (geen gebankte winst om ruimte aan te ontlenen).
    edge_expected_be_trigger = executor._round_px(1.0, 0)  # EDGE:Sell entry_price == 1.0
    assert pos_edge["sl_price"] == edge_expected_be_trigger
    assert pos_edge["banked_pnl"] == 0.0, "T1+T2 allebei geskipt -> geen echte close, dus $0 gebankt"
    assert len(notifications) == 1 and "break-even" in notifications[0] and "doorgeschoven" in notifications[0], \
        notifications
    notifications.clear()
    print(f"OK stap 9 (T2): deel-close $7 < $10-minimum -> overgeslagen, maar SL toch naar dynamische "
          f"break-even ({edge_expected_be_trigger}, banked_pnl=0.0 want geen echte close)")

    # T3: cumulatief zou nu 9 moeten sluiten -> notional $9 < $10 -> ook skip.
    new_calls, pos_edge = await run_edge_target(3)
    assert new_calls == ["meta_and_asset_ctxs"], f"T3 had alleen de market-lookup mogen doen: {new_calls}"
    assert pos_edge["remaining_qty"] == edge_qty, "T3 skip mag remaining_qty niet veranderen"
    assert pos_edge["tp3_done"] is True
    notifications.clear()
    print("OK stap 9 (T3): deel-close $9 < $10-minimum -> ook overgeslagen (3x op rij)")

    # T4: cumulatief 90% van 12 = 11 (0 al dicht, want T1-3 sloegen allemaal
    # over) -> notional $11 >= $10 -> sluit nu de OPGESTAPELDE 11 in één keer.
    new_calls, pos_edge = await run_edge_target(4)
    assert new_calls == ["meta_and_asset_ctxs", "order"], f"T4 had een echte close-order moeten plaatsen: {new_calls}"
    close_call = edge_server.calls[-1][1]
    assert close_call["sz"] == 11, f"T4 moet de opgestapelde 40+20+15+15=90% (11 van 12) in één keer sluiten: {close_call}"
    notional_t4 = close_call["sz"] * edge_server.mark_px
    assert notional_t4 >= config.MIN_NOTIONAL_USD, "T4's daadwerkelijke close-order moet boven MIN_NOTIONAL_USD zitten"
    real_close_orders.append((4, close_call["sz"], notional_t4))
    assert pos_edge["remaining_qty"] == 1, f"na T4 moet er nog 1 over zijn: {pos_edge['remaining_qty']}"
    assert pos_edge["tp4_done"] is True
    notifications.clear()
    print(f"OK stap 9 (T4): opgestapelde deel-close ({close_call['sz']}, ~${notional_t4:.2f}) EINDELIJK boven "
          f"MIN_NOTIONAL_USD -> in één keer gesloten, resterende {pos_edge['remaining_qty']}")

    # T5: sluit de laatste 1 ALTIJD, ook al is $1 ver onder MIN_NOTIONAL_USD --
    # dit is de finale exit, geen verdere target om naar door te schuiven.
    new_calls, _ = await run_edge_target(5)
    assert new_calls == ["cancel", "meta_and_asset_ctxs", "order"], \
        f"T5 moet ALTIJD sluiten, ongeacht notional: {new_calls}"
    close_call = edge_server.calls[-1][1]
    assert close_call["sz"] == 1, f"T5 moet de laatste resterende 1 sluiten: {close_call}"
    real_close_orders.append((5, close_call["sz"], close_call["sz"] * edge_server.mark_px))
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    assert "EDGE:Sell" not in state, "EDGE-positie moet na target 5 uit state verwijderd zijn"

    total_edge_closed = sum(sz for _, sz, _ in real_close_orders)
    assert total_edge_closed == edge_qty, \
        f"som van de daadwerkelijke closes ({total_edge_closed}) moet exact de originele qty ({edge_qty}) zijn"
    under_min_besides_final = [
        (n, sz, notional) for n, sz, notional in real_close_orders
        if notional < config.MIN_NOTIONAL_USD and n != 5
    ]
    assert not under_min_besides_final, \
        f"geen enkele close-order behalve de finale T5-exit mag onder MIN_NOTIONAL_USD zitten: {under_min_besides_final}"
    print(f"OK stap 9 (T5): finale exit sluit de laatste 1 (~$1.00, ONDER MIN_NOTIONAL_USD, maar dit is de "
          f"laatste target dus geen andere keuze) -- som van echte closes ({total_edge_closed}) == originele "
          f"qty ({edge_qty}). Geen enkele niet-finale close-order zat onder Hyperliquid's minimum.")

    # Jouw live .env-ladder (15/15/20/25) terugzetten voor de rest van het script.
    (config.TP_EVENT_TARGET1_CLOSE_PCT, config.TP_EVENT_TARGET2_CLOSE_PCT,
     config.TP_EVENT_TARGET3_CLOSE_PCT, config.TP_EVENT_TARGET4_CLOSE_PCT) = tp_pcts_before_edge

    # --- Stap 10: cancel-event -- kanaal trekt het signal in, v2-positie MOET
    # volledig sluiten (SL geannuleerd + volle resterende qty market-close),
    # ongeacht welke targets al gehaald zijn. Regressietest voor het incident
    # van 2026-08-12 (zie moduledocstring hierboven). ---
    result9 = await executor.place_entry_order(signal, dry_run=False, exchange=server, info=server)
    assert result9 is not None and result9.startswith("qty="), f"entry voor stap 10 had moeten slagen: {result9}"
    state = json.load(open(TEST_STATE_FILE))
    pos9 = state["SOL:Sell"]
    sl_oid_9 = pos9["sl_oid"]

    calls_before = len(server.calls)
    await executor.handle_cancel_event(
        CancelEvent(symbol="SOLUSDT", raw_text="test"), exchange=server, info=server,
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["cancel", "meta_and_asset_ctxs", "order"], \
        f"onverwachte volgorde bij cancel-event: {new_calls}"

    cancel_call = server.calls[calls_before][1]
    close_call = server.calls[calls_before + 2][1]
    assert cancel_call["oid"] == sl_oid_9, "moet de OORSPRONKELIJKE SL annuleren"
    assert close_call["sz"] == pos9["qty"], "cancel-event moet de VOLLE (nog niet TP1'de) qty sluiten"
    assert close_call["order_type"] == {"limit": {"tif": "Ioc"}} and close_call["reduce_only"] is True, \
        "cancel-close moet een reduce-only IOC-marketorder zijn, geen trigger-order"

    assert len(notifications) == 1 and "Cancel" in notifications[0], notifications
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    assert "SOL:Sell" not in state, "positie had uit de state verwijderd moeten worden na cancel-event"
    print("OK stap 10: cancel-event verwerkt -- volledige qty gesloten, SL geannuleerd, positie uit state verwijderd")

    # --- Stap 10b: hetzelfde cancel-event nogmaals (het kanaal stuurt
    # "Close X/USDT" EN "#X/USDT Cancelled" als losse berichten) -> positie
    # is al weg, dus genegeerd, geen dubbele market-close. ---
    calls_before = len(server.calls)
    await executor.handle_cancel_event(
        CancelEvent(symbol="SOLUSDT", raw_text="test"), exchange=server, info=server,
    )
    assert len(server.calls) == calls_before, "een duplicaat cancel-event mag GEEN nieuwe calls doen"
    assert len(notifications) == 0
    tp_events = db.recent_tp_events(limit=1)
    assert tp_events[0]["event"] == "cancel_event_ignored_no_position"
    print("OK stap 10b: duplicaat cancel-event genegeerd (positie al gesloten), voorkomt dubbele market-close")

    # --- Stap 10c: BIJNA-gelijktijdig duplicaat (i.p.v. sequentieel zoals
    # 10b) -- regressietest voor het incident van 2026-08-15: het kanaal
    # stuurt "Close X/USDT" en "#X/USDT Cancelled" met maar ~1-2s ertussen, en
    # de _call()-netwerkcalls hierboven yielden via asyncio.to_thread(), dus
    # zonder de _cancel_in_progress-claim lezen beide taken de nog-niet-
    # gepopte state en proberen ze allebei dezelfde SL te annuleren/positie
    # te sluiten ("Order was never placed, already canceled, or filled" +
    # "Reduce only order would increase position"). Met de fix mag er maar
    # EEN van de twee taken echte calls doen. ---
    result9c = await executor.place_entry_order(signal, dry_run=False, exchange=server, info=server)
    assert result9c is not None and result9c.startswith("qty="), f"entry voor stap 10c had moeten slagen: {result9c}"

    calls_before = len(server.calls)
    await asyncio.gather(
        executor.handle_cancel_event(CancelEvent(symbol="SOLUSDT", raw_text="test"), exchange=server, info=server),
        executor.handle_cancel_event(CancelEvent(symbol="SOLUSDT", raw_text="test"), exchange=server, info=server),
    )
    new_calls = [name for name, _ in server.calls[calls_before:]]
    assert new_calls == ["cancel", "meta_and_asset_ctxs", "order"], \
        f"bij gelijktijdige duplicaten mag maar EEN taak de SL-cancel/close-calls doen, kreeg: {new_calls}"

    tp_events_9c = db.recent_tp_events(limit=2)
    events_9c = {e["event"] for e in tp_events_9c}
    assert events_9c == {"cancel_event_closed", "cancel_event_ignored_in_progress"}, \
        f"verwacht 1x closed + 1x ignored_in_progress, kreeg: {tp_events_9c}"

    assert len(notifications) == 1 and "Cancel" in notifications[0], notifications
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    assert "SOL:Sell" not in state, "positie had uit de state verwijderd moeten worden na de gelijktijdige cancels"
    print("OK stap 10c: bijna-gelijktijdig duplicaat cancel-event -- maar EEN taak sluit de positie echt")

    # --- Stap 10d: DRY_RUN-veiligheidsnet in handle_cancel_event() ---
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False, "entry_price": 192.85,
            "sl_price": 200.3, "sl_oid": 9999, "qty": 0.5, "remaining_qty": 0.5,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False, "sz_decimals": 2,
        }})
    config.DRY_RUN = True
    calls_before = len(server.calls)
    await executor.handle_cancel_event(
        CancelEvent(symbol="SOLUSDT", raw_text="test"), exchange=server, info=server,
    )
    assert len(server.calls) == calls_before, "DRY_RUN=True met v2-entry had GEEN enkele call mogen doen"
    config.DRY_RUN = False
    async with executor._state_lock:
        executor._save_state({})
    print("OK stap 10d: DRY_RUN-veiligheidsnet in handle_cancel_event() werkt")

    # --- Stap 11: dynamische break-even-SL past zich aan aan ECHT gebankte
    # winst i.p.v. een vast %-buffer (2026-08-26: gebruiker wil TP1+TP2 als
    # "verzekering" zonder onnodig TP3-5 mis te lopen). Long-positie met een
    # echte koersstijging tussen TP1 en TP2 in -- de trigger moet verder van
    # entry af komen te liggen dan een vaste 0,5%-buffer ooit zou doen, want
    # er is hier veel meer winst gebankt dan die 0,5% zou dekken. ---
    adapt_server = FakeHyperliquidServer(
        mark_px=110.0, max_leverage=10, sz_decimals=2, universe_symbol="ADAPT",
    )
    adapt_entry = 100.0
    adapt_qty = 10.0
    async with executor._state_lock:
        executor._save_state({"ADAPT:Buy": {
            "version": "v2", "symbol": "ADAPT", "is_buy": True,
            "entry_price": adapt_entry, "sl_price": 90.0, "sl_oid": 7777,
            "qty": adapt_qty, "remaining_qty": adapt_qty,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "sz_decimals": 2, "banked_pnl": 0.0,
        }})

    close_qty_1_adapt = expected_cum_close_qty(adapt_qty, adapt_qty, cum_pcts[1], 2)
    await executor.handle_tp_event(
        TPEvent(symbol="ADAPTUSDT", target_number=1, raw_text="test"), exchange=adapt_server, info=adapt_server,
    )
    pos_adapt = json.load(open(TEST_STATE_FILE))["ADAPT:Buy"]
    expected_banked_1 = close_qty_1_adapt * (adapt_server.mark_px - adapt_entry)
    assert abs(pos_adapt["banked_pnl"] - expected_banked_1) < 1e-9, \
        f"banked_pnl na TP1 moet de echte fill-winst zijn: verwacht {expected_banked_1}, kreeg {pos_adapt['banked_pnl']}"

    remaining_after_1 = round(adapt_qty - close_qty_1_adapt, 2)
    adapt_server.mark_px = 115.0  # koers stijgt verder vóór TP2
    close_qty_2_adapt = expected_cum_close_qty(adapt_qty, remaining_after_1, cum_pcts[2], 2)
    await executor.handle_tp_event(
        TPEvent(symbol="ADAPTUSDT", target_number=2, raw_text="test"), exchange=adapt_server, info=adapt_server,
    )
    pos_adapt = json.load(open(TEST_STATE_FILE))["ADAPT:Buy"]
    expected_banked_2 = expected_banked_1 + close_qty_2_adapt * (adapt_server.mark_px - adapt_entry)
    assert abs(pos_adapt["banked_pnl"] - expected_banked_2) < 1e-9, \
        f"banked_pnl na TP2 moet cumulatief zijn: verwacht {expected_banked_2}, kreeg {pos_adapt['banked_pnl']}"

    remaining_after_2 = round(remaining_after_1 - close_qty_2_adapt, 2)
    # Veiligheidsmarge zit over de ORIGINELE notional (2026-09-01: was de
    # resterende notional, maar dat dekte de entry-fee nooit -- zie
    # executor._handle_tp_partial_event).
    safety_adapt = adapt_qty * adapt_entry * (config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT / 100)
    available_adapt = max(expected_banked_2 - safety_adapt, 0.0)
    expected_be_trigger_adapt = executor._round_px(adapt_entry - available_adapt / remaining_after_2, 2)
    fixed_buffer_trigger = executor._round_px(adapt_entry * (1 - 0.5 / 100), 2)  # wat de OUDE vaste 0.5%-buffer zou geven

    assert pos_adapt["sl_price"] == expected_be_trigger_adapt, \
        f"dynamische break-even moet de gebankte winst weerspiegelen: verwacht {expected_be_trigger_adapt}, " \
        f"kreeg {pos_adapt['sl_price']}"
    assert expected_be_trigger_adapt < fixed_buffer_trigger, \
        "met flink wat gebankte winst moet de dynamische SL VERDER van entry af liggen dan een vaste 0.5%-buffer " \
        "-- meer ademruimte voor TP3-5, precies wat de gebruiker vroeg"
    print(f"OK stap 11: dynamische break-even-SL ({expected_be_trigger_adapt}) legt méér ademruimte aan dan een "
          f"vaste 0,5%-buffer ({fixed_buffer_trigger}) zou geven, omdat TP1+TP2 hier ${expected_banked_2:.2f} "
          f"echte winst bankten (adaptief, niet een gegokt vast percentage)")

    async with executor._state_lock:
        executor._save_state({})

    # --- Stap 12: same-side re-entry terwijl de vorige positie nog open
    # staat (buiten het 60s-duplicaat-venster) -- moet de bestaande v2-state
    # SAMENVOEGEN i.p.v. overschrijven (zie place_entry_order's is_reentry).
    # Root cause die dit dichtzet: vóór deze fix overschreef een late re-entry
    # de state met ALLEEN de nieuwe fill, liet de oude SL-order ontraceerd
    # resten, en liet de TP-ladder daarna tegen een veel te kleine qty
    # rekenen (zelfde patroon als het 2026-08-14-incident, nu getriggerd
    # door een legitieme late re-entry i.p.v. een near-duplicate bericht). ---
    reentry_server = FakeHyperliquidServer(mark_px=195.0, max_leverage=10, sz_decimals=2, universe_symbol="SOL")
    old_sl_oid = 5555
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False,
            "entry_price": 192.85, "sl_price": 200.3, "sl_oid": old_sl_oid,
            "qty": 10.0, "remaining_qty": 10.0,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "sz_decimals": 2, "banked_pnl": 0.0,
            "opened_at": time.time() - 3600,  # ruim buiten DUPLICATE_POSITION_WINDOW_SECONDS (60s)
        }})

    # Simuleert wat Hyperliquid's ECHTE clearinghouseState rapporteert NA de
    # nieuwe fill: de exchange heeft de nieuwe order zelf al samengevoegd met
    # de bestaande 10-lot short tot een netto 16-lot short @ 195.0 (autoritatief,
    # niet zelf lokaal herberekend).
    reentry_server.live_position_override = {"coin": "SOL", "szi": "-16.0", "entryPx": "195.0"}

    signal_reentry = Signal(
        symbol="SOLUSDT", side="Sell", entry_low=193.0, entry_high=196.0,
        leverage=10, targets=[190.0, 188.0, 186.0, 184.0, 180.0], stop_loss=201.0, raw_text="reentry",
    )
    result_reentry = await executor.place_entry_order(signal_reentry, dry_run=False, exchange=reentry_server, info=reentry_server)
    assert result_reentry is not None and "toegevoegd" in result_reentry, f"re-entry had moeten slagen: {result_reentry}"

    cancel_calls = [c for name, c in reentry_server.calls if name == "cancel"]
    assert cancel_calls == [{"name": "SOL", "oid": old_sl_oid}], \
        f"de OUDE SL had geannuleerd moeten worden vóór de nieuwe SL: {cancel_calls}"

    sl_order_calls = [c for name, c in reentry_server.calls if name == "order"]
    assert len(sl_order_calls) == 1 and sl_order_calls[0]["sz"] == 16.0, \
        f"de nieuwe SL had de VOLLEDIGE samengevoegde qty (16.0) moeten dekken, niet alleen de nieuwe fill: {sl_order_calls}"

    state_reentry = json.load(open(TEST_STATE_FILE))["SOL:Sell"]
    assert state_reentry["qty"] == 16.0 and state_reentry["remaining_qty"] == 16.0, \
        f"state moet de ECHTE samengevoegde qty gebruiken (16.0), niet zelf herberekenen: {state_reentry}"
    assert state_reentry["entry_price"] == 195.0, \
        f"state moet de ECHTE (geblende) entry-prijs van Hyperliquid gebruiken: {state_reentry}"
    assert not any(state_reentry[f"tp{n}_done"] for n in (1, 2, 3, 4)), "TP-ladder moet resetten voor de samengevoegde positie"
    assert state_reentry["banked_pnl"] == 0.0, "banked_pnl moet resetten -- nieuwe entry-prijs, geen oude referentie meer"

    assert any("Re-entry samengevoegd" in m for m in notifications), notifications
    notifications.clear()
    print(f"OK stap 12: same-side re-entry samengevoegd -- oude SL (oid={old_sl_oid}) geannuleerd, "
          f"nieuwe SL dekt de volledige samengevoegde qty (16.0 @ 195.0), TP-ladder en banked_pnl gereset")

    # --- Stap 12b: fallback als de samengevoegde positie niet te bevestigen
    # is bij Hyperliquid (bv. de oude positie bleek intussen extern gesloten,
    # zie reconcile_positions) -- MOET de nieuwe fill als op zichzelf staande
    # positie behandelen i.p.v. te gokken op een qty die niet klopt. ---
    reentry_server2 = FakeHyperliquidServer(mark_px=195.0, max_leverage=10, sz_decimals=2, universe_symbol="SOL")
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False,
            "entry_price": 192.85, "sl_price": 200.3, "sl_oid": 6666,
            "qty": 10.0, "remaining_qty": 10.0,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "sz_decimals": 2, "banked_pnl": 0.0,
            "opened_at": time.time() - 3600,
        }})
    reentry_server2.live_position_override = None  # geen bevestigde live positie
    result_reentry2 = await executor.place_entry_order(signal_reentry, dry_run=False, exchange=reentry_server2, info=reentry_server2)
    assert result_reentry2 is not None
    state_reentry2 = json.load(open(TEST_STATE_FILE))["SOL:Sell"]
    fallback_qty = state_reentry2["qty"]
    assert fallback_qty != 16.0, "fallback moet NIET de vorige testcase se qty hergebruiken"
    assert state_reentry2["remaining_qty"] == fallback_qty
    notifications.clear()
    print(f"OK stap 12b: kon samengevoegde positie niet bevestigen -> fallback op enkel de nieuwe fill "
          f"(qty={fallback_qty}), geen gegokte qty")

    async with executor._state_lock:
        executor._save_state({})

    # --- Stap 13: nieuw signaal in de TEGENOVERGESTELDE richting van een
    # bestaand v2-record (incident 2026-09-08: PENGU:Buy's SL triggerde
    # buiten de bot om, en de daaropvolgende PENGU:Sell-entry werd gewoon
    # geplaatst zonder ooit te checken of er nog een tegengesteld v2-record
    # openstond -- twee "open" records voor dezelfde coin lieten daarna elk
    # TP/cancel-event voor die coin als ambigu genegeerd worden, waardoor 4
    # TP-targets nooit werden uitgevoerd op de wél-nog-echte positie).
    # place_entry_order() checkt nu vóór elke nieuwe entry of een
    # tegengesteld v2-record nog ECHT open staat op Hyperliquid. ---
    opp_server = FakeHyperliquidServer(mark_px=0.0086, max_leverage=5, sz_decimals=0, universe_symbol="PENGU")
    async with executor._state_lock:
        executor._save_state({"PENGU:Buy": {
            "version": "v2", "symbol": "PENGU", "is_buy": True,
            "entry_price": 0.008807, "sl_price": 0.008436, "sl_oid": 1111,
            "qty": 300000.0, "remaining_qty": 300000.0,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "sz_decimals": 0, "banked_pnl": 0.0, "opened_at": time.time() - 3600,
        }})
    signal_opp = Signal(
        symbol="PENGUUSDT", side="Sell", entry_low=0.0085, entry_high=0.0086,
        leverage=5, targets=[0.0084, 0.0083, 0.0082, 0.0081, 0.008], stop_loss=0.0089, raw_text="opp-test",
    )

    # Stap 13a: PENGU:Buy staat nog ECHT open op Hyperliquid -> nieuwe
    # Sell-entry moet overgeslagen worden (zou netten/flippen op de
    # exchange), state blijft ongewijzigd, gebruiker wordt gewaarschuwd.
    opp_server.live_position_override = {"coin": "PENGU", "szi": "300000", "entryPx": "0.008807"}
    calls_before = len(opp_server.calls)
    result_opp_live = await executor.place_entry_order(signal_opp, dry_run=False, exchange=opp_server, info=opp_server)
    assert result_opp_live is None, f"had overgeslagen moeten worden, kreeg: {result_opp_live}"
    new_calls_opp = [name for name, _ in opp_server.calls[calls_before:]]
    assert "order" not in new_calls_opp and "market_open" not in new_calls_opp, \
        f"had geen order-calls mogen doen: {new_calls_opp}"
    state_opp = json.load(open(TEST_STATE_FILE))
    assert "PENGU:Buy" in state_opp and "PENGU:Sell" not in state_opp, \
        "PENGU:Buy moet blijven staan, geen nieuwe PENGU:Sell erbij (zou de exacte 2026-09-08-bug herhalen)"
    assert any("PENGU:Buy" in m for m in notifications), notifications
    notifications.clear()
    print("OK stap 13a: tegengestelde v2-positie nog ECHT open op Hyperliquid -> nieuwe entry overgeslagen, "
          "geen dubbel state-record (voorkomt de exacte 2026-09-08-bug)")

    # Stap 13b: PENGU:Buy staat NIET meer echt open (bv. resting SL buiten
    # de bot om getriggerd) -> stale record wordt opgeruimd en de nieuwe
    # entry gaat gewoon door.
    opp_server.live_position_override = None
    calls_before = len(opp_server.calls)
    result_opp_stale = await executor.place_entry_order(signal_opp, dry_run=False, exchange=opp_server, info=opp_server)
    assert result_opp_stale is not None and result_opp_stale.startswith("qty="), \
        f"had moeten slagen na opruimen van de stale tegengestelde positie: {result_opp_stale}"
    state_opp2 = json.load(open(TEST_STATE_FILE))
    assert "PENGU:Buy" not in state_opp2, "stale PENGU:Buy had opgeruimd moeten worden"
    assert "PENGU:Sell" in state_opp2, "nieuwe PENGU:Sell-entry had geplaatst moeten worden"
    assert any("PENGU:Buy" in m for m in notifications), notifications
    notifications.clear()
    print("OK stap 13b: tegengestelde v2-positie bleek niet meer echt open -> stale record opgeruimd, "
          "nieuwe entry gaat gewoon door")

    async with executor._state_lock:
        executor._save_state({})

    import shutil
    shutil.rmtree(_TMP_DIR, ignore_errors=True)
    print("\nAlle simulatiestappen geslaagd.")


if __name__ == "__main__":
    asyncio.run(main())
