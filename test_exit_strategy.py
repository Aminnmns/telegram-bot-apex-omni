"""
Simulatietest voor de v2-exit-strategie in executor.py: margin-based sizing
bij entry (config.MAX_MARGIN_PCT_OF_FUNDS% van het beschikbare saldo), alleen
een SL bij entry (geen TP-trigger-order), en sluiten op basis van de groep's
eigen "Take-Profit target N ✅"-berichten via handle_tp_event(). 5-staps
TP-ladder (targets 1 t/m 4 elk hun eigen cumulatieve percentage van de
ORIGINELE qty, target 5 altijd de volledige rest) met automatische
MIN_NOTIONAL_USD-doorschuif-fallback (zie executor._handle_tp_partial_event).
Mockt de ApeX Omni-client (apexomni.http_private_sign.HttpPrivateSign)
volledig -- geen echte private key, netwerk of geld nodig:

    python test_exit_strategy.py

Isolatie (belangrijk, zie incident 2026-08-10): dit script geeft de fake
client als EXPLICIET functie-argument mee aan place_entry_order()/
handle_tp_event()/handle_cancel_event() -- geen monkeypatching van
executor._get_client() nodig. Daarnaast wordt executor.STATE_FILE en
db.DB_PATH allebei omgeleid naar bestanden buiten de projectmap, zodat dit
script het gedeelde open_positions.json/bot_history.db van de live
signal-bot.service nooit kan raken, ongeacht wat er verder misgaat.

OVERSTAP VAN HYPERLIQUID NAAR APEX OMNI: dit bestand mockte voorheen een
`FakeHyperliquidServer` (Exchange/Info-objecten met `market_open()`/
`order()`/`cancel()`/`user_state()`). ApeX Omni's `apexomni`-SDK werkt anders
genoeg (één client-object, `create_order_v3()`/`get_order_v3()`/
`delete_order_v3()`, market-orders die NIET synchroon vullen -- zie
executor._await_order_fill -- en één gecombineerd balance-endpoint i.p.v.
Hyperliquid's aparte perps/spot-calls) dat dit een structurele herbouw was,
geen 1-op-1 hernoeming. De business-logica die hier getest wordt (margin-
sizing-wiskunde, TP-ladder-percentages, breakeven-PnL-berekening, dupe-/
re-entry-/opposite-position-guards) is in executor.py ONGEWIJZIGD overgenomen
van de Hyperliquid-versie -- alleen de exchange-laag eronder is vervangen, en
dat is precies wat FakeApexServer hieronder simuleert. Waar de oude tests een
EXACTE call-sequentie op de fake Exchange/Info-objecten controleerden (bv.
`["user_state", "meta_and_asset_ctxs", ...]`), controleren de nieuwe tests in
plaats daarvan de MUTERENDE calls (create_order_v3/delete_order_v3/
set_initial_margin_rate_v3, zie `action_calls()`) -- de exacte read-call-
plumbing (hoeveel keer ticker_v3/get_account_v3/get_order_v3 wordt aangeroepen)
is nu ApeX-SDK-specifiek en geen onderdeel van wat deze tests willen
bewijzen; welke orders geplaatst/geannuleerd worden, met welke parameters, in
welke volgorde, is dat wel en blijft daarom net zo strak gecontroleerd als
voorheen.

Scenario's:
1.  place_entry_order() v2: margin-based qty (MAX_MARGIN_PCT_OF_FUNDS% van
    het beschikbare saldo * toegestane leverage / prijs), ALLEEN een SL
    geplaatst (geen TP-order), state-entry met "version": "v2".
1b. Max-posities-limiet -> overslaan, geen order-calls.
1c. Nagenoeg geen beschikbaar saldo (perps-withdrawable + spot-USDC SAMEN
    te laag) -> qty rondt af naar 0 -> overslaan, geen order-calls.
1c-bis. Perps-withdrawable alléén is te laag, maar vrije spot-USDC dekt het
    gat -> MOET slagen (ApeX Omni's get_account_balance_v3 geeft dit als één
    gecombineerd availableBalance-veld terug, zie _withdrawable_from_balance).
1d. qty > 0 na afronding, maar orderwaarde onder MIN_NOTIONAL_USD -> zelfde
    skip-pad als 1c.
1e. Losse unit-check: margin_to_use = available_funds * (MAX_MARGIN_PCT_OF_FUNDS
    / 100), dus nu 0.33 i.p.v. de oude 0.25 (taak 3a).
1f. qty mikt op MAX_MARGIN_PCT_OF_FUNDS% van de TOTALE accountwaarde, niet
    van de (door andere open posities geslonken) withdrawable -> blijft
    constant ongeacht hoeveel marge al vastzit elders.
1g. Diezelfde totale-accountwaarde-qty past niet binnen de werkelijk vrije
    marge -> nette skip (skipped_insufficient_margin), geen ApeX Omni-afwijzing.
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
11. Dynamische break-even-SL past zich aan aan ECHT gebankte winst.
12. Same-side re-entry -- samenvoegen i.p.v. overschrijven.
12b. Re-entry-fallback als de samengevoegde positie niet te bevestigen is.
13. Nieuw signaal in de TEGENOVERGESTELDE richting van een bestaand v2-record.
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
# Expliciet fout/nep gezet -- als er OOIT een pad zou zijn dat toch de echte
# executor._get_client() aanspreekt (i.p.v. de hieronder geïnjecteerde fake),
# moet dat hard falen bij het signen/authenticeren, niet stilletjes een order
# op een bestaand account plaatsen.
config.APEX_ETH_PRIVATE_KEY = "00" * 32
config.APEX_API_KEY = "test-fake-key"
config.APEX_API_SECRET = "test-fake-secret"
config.APEX_API_PASSPHRASE = "test-fake-passphrase"
config.APEX_ZK_SEEDS = "00" * 32
config.APEX_ZK_L2KEY = "0x" + "00" * 32

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

# De enige SDK-methodes die daadwerkelijk state op de (fake) exchange
# muteren -- orders plaatsen/annuleren, leverage/margin-rate zetten. Read-only
# calls (ticker_v3/get_account_v3/get_account_balance_v3/get_order_v3/
# get_worst_price_v3/historical_pnl_v3) zijn ApeX-SDK-plumbing en geen
# onderdeel van wat deze tests willen bewijzen (zie moduledocstring).
_ACTION_METHODS = {"create_order_v3", "delete_order_v3", "set_initial_margin_rate_v3"}


def action_calls(calls):
    return [(name, kwargs) for name, kwargs in calls if name in _ACTION_METHODS]


class FakeApexServer:
    """Houdt bij welke SDK-calls er zijn gedaan (`self.calls`, ALLE calls;
    gebruik `action_calls()` voor alleen de muterende) en simuleert ApeX
    Omni's v3-API-responses voor de v2-flow (SL-only entry via STOP_MARKET,
    reduce-only MARKET-close op TP-events). `universe_symbol` laat toe om
    per test-scenario een andere market te simuleren (bv. een losse,
    goedkope "EDGE"-coin voor de MIN_NOTIONAL_USD-edge-case, los van de
    SOL-market die de rest van het script gebruikt).

    MARKET-orders "vullen" synchroon op het moment van create_order_v3() --
    status/cumSuccessFillSize/averagePrice staan meteen goed, dus
    executor._await_order_fill() slaagt altijd op de EERSTE get_order_v3()-
    poll (deterministisch, geen trage tests). STOP_MARKET-orders blijven
    UNTRIGGERED (resting), net als een echte SL/breakeven-SL op ApeX Omni."""

    def __init__(self, mark_px: float, max_leverage: int, tick_size: str = "0.01", step_size: str = "0.01",
                 perp_withdrawable: float = 1000.0, spot_free_usdc: float = 0.0,
                 universe_symbol: str = "SOL",
                 perp_account_value: float = None, spot_total_usdc: float = None):
        self.calls = []
        self.universe_symbol = universe_symbol
        self.apex_symbol = f"{universe_symbol}-USDT"
        self.mark_px = mark_px
        self.max_leverage = max_leverage
        self.tick_size = tick_size
        self.step_size = step_size
        # Twee losse velden, want _withdrawable_from_balance() combineert ze
        # (via de fake get_account_balance_v3() hieronder) -- zelfde
        # empirisch geverifieerde reden als voorheen bij Hyperliquid (2026-08-11):
        # een exchange kan nieuwe posities accepteren die meer marge vereisen
        # dan het "withdrawable"-achtige veld alleen toestaat, gedekt door
        # vrije spot-USDC. ApeX Omni geeft dit als ÉÉN gecombineerd
        # availableBalance-veld terug (zie executor._withdrawable_from_balance's
        # docstring), maar deze fake houdt de twee bronnen bewust apart
        # instelbaar zodat bestaande scenario's (1c-bis, 1f, 1g) ze
        # onafhankelijk kunnen variëren.
        self.perp_withdrawable = perp_withdrawable
        self.spot_free_usdc = spot_free_usdc
        self._perp_account_value_override = perp_account_value
        self._spot_total_usdc_override = spot_total_usdc
        self._next_id = 1000
        self.open_positions_count = 0  # voor count_open_positions()
        # Optioneel: {"symbol", "side", "size", "entryPrice"} -- simuleert wat
        # ApeX Omni's ECHTE get_account_v3()["positions"] rapporteert voor
        # _get_live_position()/reconcile_positions(), los van
        # open_positions_count hierboven. Alleen gezet in scenario's die dit
        # expliciet testen (re-entry-merge, opposite-position-guard); overal
        # elders None, dus bestaand gedrag blijft ongewijzigd.
        self.live_position_override = None
        self.default_address = FAKE_OWNER
        self.configV3 = {"contractConfig": {"perpetualContract": [self._symbol_config()]}}
        self.orders = {}  # order-id(str) -> order-dict

    def _symbol_config(self):
        return {
            "symbol": self.apex_symbol,
            "tickSize": self.tick_size,
            "stepSize": self.step_size,
            "displayMaxLeverage": str(self.max_leverage),
        }

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

    def _next_order_id(self) -> str:
        self._next_id += 1
        return str(self._next_id)

    # --- v3 read endpoints ---
    def configs_v3(self):
        self.calls.append(("configs_v3", {}))
        return {"data": self.configV3}

    def get_account_v3(self):
        self.calls.append(("get_account_v3", {}))
        positions = [
            {"symbol": f"FILLER{i}-USDT", "side": "BUY", "size": "1", "entryPrice": "1"}
            for i in range(self.open_positions_count)
        ]
        if self.live_position_override is not None:
            positions.append(self.live_position_override)
        return {"data": {"positions": positions}}

    def get_account_balance_v3(self):
        self.calls.append(("get_account_balance_v3", {}))
        return {"data": {
            "totalEquityValue": str(self.perp_account_value + self.spot_total_usdc),
            "availableBalance": str(self.perp_withdrawable + self.spot_free_usdc),
        }}

    def ticker_v3(self, symbol):
        self.calls.append(("ticker_v3", {"symbol": symbol}))
        return {"data": [{"markPrice": str(self.mark_px)}]}

    def get_worst_price_v3(self, symbol, side, size):
        self.calls.append(("get_worst_price_v3", {"symbol": symbol, "side": side, "size": size}))
        return {"data": {"worstPrice": str(self.mark_px)}}

    def get_order_v3(self, id):
        self.calls.append(("get_order_v3", {"id": id}))
        return {"data": self.orders.get(id, {})}

    def historical_pnl_v3(self, symbol=None, limit=None):
        self.calls.append(("historical_pnl_v3", {"symbol": symbol, "limit": limit}))
        return {"data": {"historicalPnl": []}}

    # --- v3 write endpoints ---
    def set_initial_margin_rate_v3(self, symbol, initialMarginRate):
        self.calls.append(("set_initial_margin_rate_v3", {"symbol": symbol, "initialMarginRate": initialMarginRate}))
        return {"data": {}}

    def create_order_v3(self, symbol, side, type, size, price=None, reduceOnly=False,
                         triggerPrice=None, triggerPriceType=None, isPositionTpsl=False, **_kwargs):
        self.calls.append(("create_order_v3", {
            "symbol": symbol, "side": side, "type": type, "size": size, "price": price,
            "reduceOnly": reduceOnly, "triggerPrice": triggerPrice,
        }))
        oid = self._next_order_id()
        is_market = type == "MARKET"
        order = {
            "id": oid, "symbol": symbol, "side": side, "type": type, "size": size,
            "price": price, "reduceOnly": reduceOnly, "triggerPrice": triggerPrice,
            "status": "FILLED" if is_market else "UNTRIGGERED",
            "cumSuccessFillSize": size if is_market else "0",
            "averagePrice": str(self.mark_px) if is_market else "",
        }
        self.orders[oid] = order
        return {"data": order}

    def delete_order_v3(self, id):
        self.calls.append(("delete_order_v3", {"id": id}))
        if id in self.orders:
            self.orders[id]["status"] = "CANCELED"
        return {"data": id}


def expected_cum_close_qty(qty, remaining_before, cum_pct, step_size):
    """Onafhankelijke herimplementatie van executor._target_close_qty (als
    oracle voor de asserts hieronder, niet als vervanging van de eigenlijke
    implementatie): hoeveel er nu dicht moet voor cumulatief percentage
    `cum_pct` van de ORIGINELE qty. Gebruikt executor._round_sz (floor naar
    een veelvoud van step_size) als rond-primitief -- dezelfde die
    executor.py zelf gebruikt -- maar herberekent de cumulatieve-close-
    formule zelf, onafhankelijk van _target_close_qty."""
    total_should_be_closed = executor._round_sz(qty * (cum_pct / 100), step_size)
    already_closed = executor._round_sz(qty - remaining_before, step_size)
    close_qty = executor._round_sz(total_should_be_closed - already_closed, step_size)
    return max(0.0, min(close_qty, remaining_before))


async def main():
    db.init_db()

    # SOL op 192.85, 50x max leverage.
    server = FakeApexServer(mark_px=192.85, max_leverage=50, tick_size="0.01", step_size="0.01")

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

    # --- Stap 1: v2-entry plaatsen (fake client als argument) ---
    result = await executor.place_entry_order(signal, dry_run=False, client=server)
    assert result is not None and result.startswith("qty="), f"onverwacht return-resultaat: {result}"

    actions = action_calls(server.calls)
    action_names = [name for name, _ in actions]
    assert action_names == ["set_initial_margin_rate_v3", "create_order_v3", "create_order_v3"], \
        f"onverwachte volgorde van muterende calls: {action_names}"

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
    expected_qty = executor._calc_margin_based_qty(server.mark_px, used_leverage_1, total_equity_1, server.step_size)
    assert qty == expected_qty, f"margin-based qty klopt niet: {qty} != {expected_qty}"

    entry_call = actions[1][1]
    sl_call = actions[2][1]
    assert entry_call["type"] == "MARKET" and entry_call["size"] == str(qty)
    assert sl_call["type"] == "STOP_MARKET" and sl_call["size"] == str(qty), "SL moet op de VOLLE qty staan"
    assert sl_call["side"] == "BUY", "SL van een SHORT moet een reduce-only BUY-order zijn"
    assert sl_call["reduceOnly"] is True
    assert float(sl_call["price"]) > float(sl_call["triggerPrice"]), \
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
    result2 = await executor.place_entry_order(signal2, dry_run=False, client=server)
    new_actions = action_calls(server.calls[calls_before:])
    assert result2 is None, f"had overgeslagen moeten worden, kreeg: {result2}"
    assert new_actions == [], f"had geen muterende calls mogen doen: {new_actions}"
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
    result1c = await executor.place_entry_order(signal1c, dry_run=False, client=server)
    new_actions = action_calls(server.calls[calls_before:])
    assert result1c is None, f"had overgeslagen moeten worden wegens te kleine ordergrootte, kreeg: {result1c}"
    assert new_actions == [], f"had geen muterende calls mogen doen: {new_actions}"
    assert len(notifications) == 1 and "te kleine ordergrootte" in notifications[0], notifications
    notifications.clear()
    print("OK stap 1c: trade overgeslagen -- perps-withdrawable ($0.01) + spot ($0.00) samen te weinig voor een qty > 0")

    # --- Stap 1c-bis: perps-withdrawable ALLEEN is te laag, maar vrije
    # spot-USDC dekt het gat -- moet WEL slagen. ApeX Omni's
    # get_account_balance_v3() geeft dit als ÉÉN gecombineerd availableBalance-
    # veld terug (zie executor._withdrawable_from_balance) -- deze fake blijft
    # de twee bronnen apart bijhouden en optellen, zodat dit scenario nog
    # steeds bewijst dat een lage "perps-achtige" component gedekt kan worden
    # door een vrije "spot-achtige" component. ---
    server.perp_withdrawable = 0.0
    server.spot_free_usdc = 1000.0
    calls_before = len(server.calls)
    result1c_bis = await executor.place_entry_order(signal1c, dry_run=False, client=server)
    assert result1c_bis is not None and result1c_bis.startswith("qty="), \
        f"had moeten slagen dankzij vrije spot-USDC, kreeg: {result1c_bis}"
    print(f"OK stap 1c-bis: perps-withdrawable=$0 maar spot-USDC dekt het -> entry SLAAGT "
          f"({result1c_bis}), bevestigt dat de gecombineerde availableBalance de twee optelt")

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
    # notional ruim onder MIN_NOTIONAL_USD.
    server.perp_withdrawable = 2.0
    server.spot_free_usdc = 0.0
    calls_before = len(server.calls)
    result_tiny = await executor.place_entry_order(signal_tiny, dry_run=False, client=server)
    new_actions = action_calls(server.calls[calls_before:])
    assert result_tiny is None, f"had overgeslagen moeten worden wegens orderwaarde onder minimum, kreeg: {result_tiny}"
    assert new_actions == [], f"had geen muterende calls mogen doen: {new_actions}"
    assert len(notifications) == 1 and "te kleine ordergrootte" in notifications[0], notifications
    notifications.clear()
    orders = db.recent_orders(limit=1)
    assert orders[0]["status"] == "skipped_min_notional"
    print("OK stap 1d: qty > 0 na afronding maar orderwaarde onder MIN_NOTIONAL_USD -> overgeslagen, geen RuntimeError")

    # --- Stap 1e: losse unit-check dat margin_to_use = available_funds * 0.33
    # is (i.p.v. de oude 0.25) -- taak 3a. Ronde getallen (leverage=1,
    # entry_px=1.0) zodat er geen afrondingsonzekerheid in de assert zit. ---
    assert config.MAX_MARGIN_PCT_OF_FUNDS == 33.0, "deze check gaat uit van de nieuwe 33%-default"
    check_funds, check_lev, check_px, check_step = 100.0, 1, 1.0, "0.000001"
    check_qty = executor._calc_margin_based_qty(check_px, check_lev, check_funds, check_step)
    implied_margin_to_use = check_qty * check_px / check_lev
    expected_margin_to_use = check_funds * (config.MAX_MARGIN_PCT_OF_FUNDS / 100)
    assert abs(expected_margin_to_use - 33.0) < 1e-9, expected_margin_to_use
    assert abs(implied_margin_to_use - expected_margin_to_use) < 1e-6, \
        f"margin_to_use klopt niet: {implied_margin_to_use} != {expected_margin_to_use} (verwacht available_funds*0.33)"
    print(f"OK stap 1e: margin_to_use = available_funds * {config.MAX_MARGIN_PCT_OF_FUNDS/100} "
          f"(${check_funds:.2f} -> ${implied_margin_to_use:.2f} margin), niet meer *0.25")

    # --- Stap 1f: qty blijft CONSTANT als er al marge vastzit in andere
    # gelijktijdig open posities -- MAX_MARGIN_PCT_OF_FUNDS% wordt genomen van
    # de TOTALE accountwaarde, niet van wat er NU nog vrij is (zie
    # _total_equity_from_balance). Zonder deze fix zou de qty hier gebaseerd
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
    result_1f = await executor.place_entry_order(signal_1f, dry_run=False, client=server)
    assert result_1f is not None and result_1f.startswith("qty="), f"had moeten slagen: {result_1f}"
    used_leverage_1f = min(signal_1f.leverage, server.max_leverage)
    expected_qty_1f = executor._calc_margin_based_qty(server.mark_px, used_leverage_1f, 1000.0, server.step_size)
    withdrawable_based_qty_1f = executor._calc_margin_based_qty(server.mark_px, used_leverage_1f, 500.0, server.step_size)
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
    # (nieuwe check in place_entry_order) i.p.v. dat de exchange de order
    # zelf afwijst. ---
    signal_1g = Signal(
        symbol="SOLUSDT", side="Buy", entry_low=191.8, entry_high=193.9,
        leverage=10, targets=[190.3], stop_loss=200.3, raw_text="insufficient-margin",
    )
    server.perp_withdrawable = 100.0    # veel minder vrij dan de 33%-van-$1000 (=$330) target
    server.spot_free_usdc = 0.0
    server.perp_account_value = 1000.0
    server.spot_total_usdc = 0.0
    calls_before = len(server.calls)
    result_1g = await executor.place_entry_order(signal_1g, dry_run=False, client=server)
    new_actions = action_calls(server.calls[calls_before:])
    assert result_1g is None, f"had overgeslagen moeten worden wegens onvoldoende vrije marge, kreeg: {result_1g}"
    assert new_actions == [], f"had geen muterende calls mogen doen: {new_actions}"
    assert len(notifications) == 1 and "onvoldoende vrije marge" in notifications[0], notifications
    notifications.clear()
    orders = db.recent_orders(limit=1)
    assert orders[0]["status"] == "skipped_insufficient_margin"
    print("OK stap 1g: qty (op basis van totale accountwaarde) paste niet binnen de werkelijk vrije marge -> "
          "netjes overgeslagen (skipped_insufficient_margin), geen exchange-afwijzing")

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
    # default target 2) ---
    sl_oid_before_t1 = pos["sl_oid"]
    close_qty_1 = expected_cum_close_qty(original_qty, remaining, cum_pcts[1], server.step_size)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=1, raw_text="test"), client=server,
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["create_order_v3"], f"onverwachte volgorde bij TP1-event: {new_actions}"

    close_call = new_actions[0][1]
    assert close_call["type"] == "MARKET" and close_call["reduceOnly"] is True, \
        "target1-close moet een reduce-only MARKET-order zijn, geen trigger-order"
    assert close_call["size"] == str(close_qty_1), f"moet {config.TP_EVENT_TARGET1_CLOSE_PCT}% van de ORIGINELE qty sluiten"
    assert close_call["side"] == "BUY", "sluiten van een SHORT is een BUY"

    remaining = executor._round_sz(remaining - close_qty_1, server.step_size)
    total_closed = executor._round_sz(total_closed + close_qty_1, server.step_size)

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
        TPEvent(symbol="SOLUSDT", target_number=1, raw_text="test"), client=server,
    )
    assert action_calls(server.calls[calls_before:]) == [], "een dubbel TP1-event mag GEEN nieuwe muterende calls doen"
    assert len(notifications) == 0
    print("OK stap 2b: duplicaat TP1-event genegeerd, geen dubbele close")

    # --- Stap 3: target 2 -> cumulatief 30% sluiten + SL naar dynamische
    # break-even (config.BREAKEVEN_MOVE_AFTER_TARGET=2, gebaseerd op de ECHTE
    # gebankte winst uit target 1+2, config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT) ---
    close_qty_2 = expected_cum_close_qty(original_qty, remaining, cum_pcts[2], server.step_size)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=2, raw_text="test"), client=server,
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["create_order_v3", "delete_order_v3", "create_order_v3"], \
        f"onverwachte volgorde bij TP2-event: {new_actions}"
    close_call = new_actions[0][1]
    cancel_call = new_actions[1][1]
    be_sl_call = new_actions[2][1]

    assert close_call["size"] == str(close_qty_2), \
        f"target2 moet {config.TP_EVENT_TARGET2_CLOSE_PCT}% extra sluiten (cumulatief {cum_pcts[2]}%)"
    assert close_call["type"] == "MARKET" and close_call["reduceOnly"] is True

    assert cancel_call["id"] == sl_oid_before_t1, "moet de OORSPRONKELIJKE (nog-niet-verplaatste) SL annuleren"

    remaining = executor._round_sz(remaining - close_qty_2, server.step_size)
    total_closed = executor._round_sz(total_closed + close_qty_2, server.step_size)

    # mark_px staat hier gelijk aan de entry-prijs (geen koersbeweging in dit
    # scenario) -> beide closes leveren $0 banked_pnl op -> de dynamische
    # break-even-trigger komt daarom EXACT op de entry-prijs uit (offset 0),
    # niet op een vast percentage ernaast. Dat is het correcte nieuwe gedrag:
    # zonder gebankte winst is er ook geen ruimte om van entry af te wijken
    # (zie de aparte adaptiviteits-test verderop voor een scenario MET
    # koersbeweging, waar de trigger wel degelijk van entry afwijkt).
    expected_be_trigger = executor._round_px(pos["entry_price"], server.tick_size)
    assert be_sl_call["type"] == "STOP_MARKET" and be_sl_call["reduceOnly"] is True
    assert float(be_sl_call["triggerPrice"]) == expected_be_trigger, \
        f"zonder gebankte winst (mark_px == entry) moet de dynamische SL exact op entry staan: " \
        f"verwacht {expected_be_trigger}, kreeg {be_sl_call['triggerPrice']}"
    assert be_sl_call["size"] == str(remaining), "nieuwe SL moet voor de resterende qty zijn"

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
        TPEvent(symbol="SOLUSDT", target_number=2, raw_text="test"), client=server,
    )
    assert action_calls(server.calls[calls_before:]) == [], "een dubbel TP2-event mag GEEN nieuwe muterende calls doen"
    assert len(notifications) == 0
    print("OK stap 3b: duplicaat TP2-event genegeerd")

    # --- Stap 4: target 3 -> cumulatief 50% sluiten (nieuw gedrag: was
    # voorheen ALTIJD de volledige rest) ---
    close_qty_3 = expected_cum_close_qty(original_qty, remaining, cum_pcts[3], server.step_size)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=3, raw_text="test"), client=server,
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["create_order_v3"], f"onverwachte volgorde bij TP3-event: {new_actions}"
    close_call = new_actions[0][1]
    assert close_call["size"] == str(close_qty_3), f"target3 moet cumulatief {cum_pcts[3]}% sluiten, niet de volledige rest"

    remaining = executor._round_sz(remaining - close_qty_3, server.step_size)
    total_closed = executor._round_sz(total_closed + close_qty_3, server.step_size)

    state = json.load(open(TEST_STATE_FILE))
    pos = state["SOL:Sell"]
    assert pos["tp3_done"] is True
    assert pos["remaining_qty"] == remaining
    assert "SOL:Sell" in state, "positie moet na target 3 NOG OPEN staan (nieuw gedrag, was voorheen 'klaar')"
    notifications.clear()
    print(f"OK stap 4: TP3-event verwerkt -- {close_qty_3} extra gesloten (cumulatief {cum_pcts[3]}%), "
          f"positie blijft open, resterende {remaining}")

    # --- Stap 5: target 4 -> cumulatief 75% sluiten ---
    close_qty_4 = expected_cum_close_qty(original_qty, remaining, cum_pcts[4], server.step_size)
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=4, raw_text="test"), client=server,
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["create_order_v3"], f"onverwachte volgorde bij TP4-event: {new_actions}"
    close_call = new_actions[0][1]
    assert close_call["size"] == str(close_qty_4), f"target4 moet cumulatief {cum_pcts[4]}% sluiten"

    remaining = executor._round_sz(remaining - close_qty_4, server.step_size)
    total_closed = executor._round_sz(total_closed + close_qty_4, server.step_size)

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
        TPEvent(symbol="SOLUSDT", target_number=5, raw_text="test"), client=server,
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["delete_order_v3", "create_order_v3"], \
        f"onverwachte volgorde bij TP5-event: {new_actions}"

    cancel_call = new_actions[0][1]
    close_call = new_actions[1][1]
    assert close_call["size"] == str(close_qty_5), "target5 moet de volledige resterende qty sluiten"
    assert close_call["type"] == "MARKET" and close_call["reduceOnly"] is True
    assert cancel_call["id"] == pos["sl_oid"], "target5 moet de (break-even-)SL eerst annuleren"

    total_closed = executor._round_sz(total_closed + close_qty_5, server.step_size)

    assert len(notifications) == 1 and "klaar" in notifications[0], notifications
    notifications.clear()

    state = json.load(open(TEST_STATE_FILE))
    assert "SOL:Sell" not in state, "positie had uit de state verwijderd moeten worden na target 5"
    # Tolerantie van een paar step_size (i.p.v. exact 1e-9, zoals onder de
    # OUDE round-to-nearest-afronding kon): executor._round_sz rondt nu altijd
    # naar BENEDEN af (floor naar een veelvoud van step_size, zie
    # executor._round_down_to_step). Een geflorede subtractie op een float
    # die al binaire representatie-ruis draagt (bv. 36.36 - 6.42 ==
    # 29.939999999999998 in IEEE754, geen "echte" waarde onder 29.94) kan
    # daardoor een extra cent naar beneden afronden die bij round-to-nearest
    # (ongevoelig voor welke kant die ruis op valt) niet gebeurde. Dit
    # gebeurt zowel op de SUBTRACTIEVE weg (remaining_qty, 4x achter elkaar
    # geflored) als op de ADDITIEVE weg (dit total_closed-track, apart 5x
    # geflored), en kan zich over meerdere targets opstapelen (LET OP: dit
    # bevestigt een reëel, zij het klein en zelf-herstellend, precisie-
    # kenmerk van executor._round_down_to_step -- zie het testrapport voor
    # een aanbeveling om dit daar met een kleine epsilon-marge robuuster te
    # maken). Functioneel blijft de bot correct: target 5 sluit ALTIJD exact
    # "wat er nog over is" uit de ECHTE, autoritatieve remaining_qty (zie
    # hierboven, close_qty_5 == remaining), dus de positie zelf sluit op de
    # exchange altijd volledig af -- alleen deze onafhankelijk-bijgehouden
    # test-som kan met een paar step_size van de originele qty afwijken.
    tolerance = 5 * float(server.step_size)
    assert abs(total_closed - original_qty) <= tolerance + 1e-9, \
        f"som van alle deel-closes ({total_closed}) moet binnen {tolerance} van de originele qty " \
        f"({original_qty}) zijn -- grotere afwijking dan de verwachte floor-rounding-marge, dust of overshoot gedetecteerd"
    print(f"OK stap 6: TP5-event verwerkt -- resterende {close_qty_5} gesloten, positie uit state verwijderd. "
          f"Som van alle deel-closes: {round(close_qty_1+close_qty_2+close_qty_3+close_qty_4+close_qty_5, 2)} "
          f"vs. originele qty {original_qty} (binnen {tolerance} marge -- target 5 sluit altijd exact het "
          f"werkelijke restant, dus geen dust op de exchange zelf).")

    # --- Stap 7: TP-event voor coin zonder open v2-positie -> genegeerd ---
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="XRPUSDT", target_number=1, raw_text="test"), client=server,
    )
    assert action_calls(server.calls[calls_before:]) == [], "geen open v2-positie -> geen enkele muterende call"
    assert len(notifications) == 0
    tp_events = db.recent_tp_events(limit=1)
    assert tp_events[0]["event"] == "tp_event_ignored_no_position"
    print("OK stap 7: TP-event zonder open v2-positie genegeerd, wel gelogd")

    # --- Stap 8: DRY_RUN-veiligheidsnet in handle_tp_event() ---
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False, "entry_price": 192.85,
            "sl_price": 200.3, "sl_oid": "9999", "qty": 0.5, "remaining_qty": 0.5,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.01", "step_size": "0.01",
        }})
    config.DRY_RUN = True
    calls_before = len(server.calls)
    await executor.handle_tp_event(
        TPEvent(symbol="SOLUSDT", target_number=1, raw_text="test"), client=server,
    )
    assert action_calls(server.calls[calls_before:]) == [], "DRY_RUN=True met v2-entry had GEEN muterende call mogen doen"
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
    #   T1 cum 40% -> should=floor(12*0.40)=4  -> notional $4  -> SKIP (schuift door)
    #   T2 cum 60% -> should=floor(12*0.60)=7  -> notional $7  -> SKIP (schuift door)
    #   T3 cum 75% -> should=floor(12*0.75)=9  -> notional $9  -> SKIP (schuift door)
    #   T4 cum 90% -> should=floor(12*0.90)=10 -> close 10 (0 al dicht) -> notional $10 -> SLUIT
    #   T5         -> resterende 2 sluit ALTIJD, ongeacht notional ($2 < $10)
    # (should-waarden hier zijn floor(qty*pct), zie executor._round_sz/
    # _round_down_to_step -- ApeX Omni's afronding rondt altijd naar BENEDEN
    # af op een veelvoud van step_size, dus 12*0.90=10.8 wordt 10, niet 11.)
    # Bevestigt: elke ECHTE close-order (behalve de finale T5-exit) zit boven
    # MIN_NOTIONAL_USD, en de som van alle closes is exact 12. step_size="1"
    # (i.p.v. sz_decimals=0) houdt de qty's hele getallen, zelfde als voorheen.
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

    edge_server = FakeApexServer(
        mark_px=1.0, max_leverage=10, tick_size="0.01", step_size="1", universe_symbol="EDGE",
    )
    edge_qty = 12.0
    async with executor._state_lock:
        executor._save_state({"EDGE:Sell": {
            "version": "v2", "symbol": "EDGE", "is_buy": False, "entry_price": 1.0,
            "sl_price": 1.1, "sl_oid": "4242", "qty": edge_qty, "remaining_qty": edge_qty,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.01", "step_size": "1",
        }})

    real_close_orders = []  # (target_number, sz, notional) voor elke ECHTE close-order

    async def run_edge_target(n):
        calls_before_edge = len(edge_server.calls)
        await executor.handle_tp_event(
            TPEvent(symbol="EDGEUSDT", target_number=n, raw_text="test"), client=edge_server,
        )
        new_actions_edge = action_calls(edge_server.calls[calls_before_edge:])
        state_now = json.load(open(TEST_STATE_FILE))
        return new_actions_edge, state_now.get("EDGE:Sell")

    # T1: notional $5 < $10 -> geskipt, GEEN order-call, remaining blijft 12.
    new_actions, pos_edge = await run_edge_target(1)
    assert new_actions == [], f"T1 had geen muterende calls mogen doen: {new_actions}"
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
    edge_sl_oid_before = "4242"
    new_actions, pos_edge = await run_edge_target(2)
    assert [n for n, _ in new_actions] == ["delete_order_v3", "create_order_v3"], \
        f"T2 had geen close-order maar wel de BE-shift (cancel+order) moeten doen: {new_actions}"
    assert pos_edge["remaining_qty"] == edge_qty, "T2 skip mag remaining_qty niet veranderen"
    assert pos_edge["tp2_done"] is True
    assert pos_edge["sl_oid"] != edge_sl_oid_before, "BE-shift moet ook bij een geskipte close een nieuwe SL zetten"
    # T1 en T2 sloegen allebei over (MIN_NOTIONAL) -> geen enkele echte close,
    # dus $0 banked_pnl -> de dynamische break-even-trigger komt exact op de
    # entry-prijs uit (geen gebankte winst om ruimte aan te ontlenen).
    edge_expected_be_trigger = executor._round_px(1.0, "0.01")  # EDGE:Sell entry_price == 1.0
    assert pos_edge["sl_price"] == edge_expected_be_trigger
    assert pos_edge["banked_pnl"] == 0.0, "T1+T2 allebei geskipt -> geen echte close, dus $0 gebankt"
    assert len(notifications) == 1 and "break-even" in notifications[0] and "doorgeschoven" in notifications[0], \
        notifications
    notifications.clear()
    print(f"OK stap 9 (T2): deel-close $7 < $10-minimum -> overgeslagen, maar SL toch naar dynamische "
          f"break-even ({edge_expected_be_trigger}, banked_pnl=0.0 want geen echte close)")

    # T3: cumulatief zou nu 9 moeten sluiten -> notional $9 < $10 -> ook skip.
    new_actions, pos_edge = await run_edge_target(3)
    assert new_actions == [], f"T3 had geen muterende calls mogen doen: {new_actions}"
    assert pos_edge["remaining_qty"] == edge_qty, "T3 skip mag remaining_qty niet veranderen"
    assert pos_edge["tp3_done"] is True
    notifications.clear()
    print("OK stap 9 (T3): deel-close $9 < $10-minimum -> ook overgeslagen (3x op rij)")

    # T4: cumulatief 90% van 12 = floor(10.8)=10 (0 al dicht, want T1-3
    # sloegen allemaal over) -> notional $10 >= $10 -> sluit nu de
    # OPGESTAPELDE 10 in één keer (ApeX Omni se floor-afronding, zie boven --
    # de oude Hyperliquid-versie's round-to-nearest zou hier 11 gegeven hebben).
    new_actions, pos_edge = await run_edge_target(4)
    assert [n for n, _ in new_actions] == ["create_order_v3"], f"T4 had een echte close-order moeten plaatsen: {new_actions}"
    close_call = new_actions[-1][1]
    assert close_call["size"] == "10.0", f"T4 moet de opgestapelde 40+20+15+15=90% (floor(10.8)=10 van 12) in één keer sluiten: {close_call}"
    notional_t4 = float(close_call["size"]) * edge_server.mark_px
    assert notional_t4 >= config.MIN_NOTIONAL_USD, "T4's daadwerkelijke close-order moet boven MIN_NOTIONAL_USD zitten"
    real_close_orders.append((4, float(close_call["size"]), notional_t4))
    assert pos_edge["remaining_qty"] == 2, f"na T4 moet er nog 2 over zijn: {pos_edge['remaining_qty']}"
    assert pos_edge["tp4_done"] is True
    notifications.clear()
    print(f"OK stap 9 (T4): opgestapelde deel-close ({close_call['size']}, ~${notional_t4:.2f}) EINDELIJK boven "
          f"MIN_NOTIONAL_USD -> in één keer gesloten, resterende {pos_edge['remaining_qty']}")

    # T5: sluit de laatste 2 ALTIJD, ook al is $2 ver onder MIN_NOTIONAL_USD --
    # dit is de finale exit, geen verdere target om naar door te schuiven.
    new_actions, _ = await run_edge_target(5)
    assert [n for n, _ in new_actions] == ["delete_order_v3", "create_order_v3"], \
        f"T5 moet ALTIJD sluiten, ongeacht notional: {new_actions}"
    close_call = new_actions[-1][1]
    assert close_call["size"] == "2.0", f"T5 moet de laatste resterende 2 sluiten: {close_call}"
    real_close_orders.append((5, float(close_call["size"]), float(close_call["size"]) * edge_server.mark_px))
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
    print(f"OK stap 9 (T5): finale exit sluit de laatste 2 (~$2.00, ONDER MIN_NOTIONAL_USD, maar dit is de "
          f"laatste target dus geen andere keuze) -- som van echte closes ({total_edge_closed}) == originele "
          f"qty ({edge_qty}). Geen enkele niet-finale close-order zat onder de minimumgrens.")

    # Jouw live .env-ladder (15/15/20/25) terugzetten voor de rest van het script.
    (config.TP_EVENT_TARGET1_CLOSE_PCT, config.TP_EVENT_TARGET2_CLOSE_PCT,
     config.TP_EVENT_TARGET3_CLOSE_PCT, config.TP_EVENT_TARGET4_CLOSE_PCT) = tp_pcts_before_edge

    # --- Stap 10: cancel-event -- kanaal trekt het signal in, v2-positie MOET
    # volledig sluiten (SL geannuleerd + volle resterende qty market-close),
    # ongeacht welke targets al gehaald zijn. Regressietest voor het incident
    # van 2026-08-12 (zie moduledocstring hierboven). ---
    result9 = await executor.place_entry_order(signal, dry_run=False, client=server)
    assert result9 is not None and result9.startswith("qty="), f"entry voor stap 10 had moeten slagen: {result9}"
    state = json.load(open(TEST_STATE_FILE))
    pos9 = state["SOL:Sell"]
    sl_oid_9 = pos9["sl_oid"]

    calls_before = len(server.calls)
    await executor.handle_cancel_event(
        CancelEvent(symbol="SOLUSDT", raw_text="test"), client=server,
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["delete_order_v3", "create_order_v3"], \
        f"onverwachte volgorde bij cancel-event: {new_actions}"

    cancel_call = new_actions[0][1]
    close_call = new_actions[1][1]
    assert cancel_call["id"] == sl_oid_9, "moet de OORSPRONKELIJKE SL annuleren"
    assert close_call["size"] == str(pos9["qty"]), "cancel-event moet de VOLLE (nog niet TP1'de) qty sluiten"
    assert close_call["type"] == "MARKET" and close_call["reduceOnly"] is True, \
        "cancel-close moet een reduce-only MARKET-order zijn, geen trigger-order"

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
        CancelEvent(symbol="SOLUSDT", raw_text="test"), client=server,
    )
    assert action_calls(server.calls[calls_before:]) == [], "een duplicaat cancel-event mag GEEN nieuwe muterende calls doen"
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
    # te sluiten. Met de fix mag er maar EEN van de twee taken echte calls
    # doen. ---
    result9c = await executor.place_entry_order(signal, dry_run=False, client=server)
    assert result9c is not None and result9c.startswith("qty="), f"entry voor stap 10c had moeten slagen: {result9c}"

    calls_before = len(server.calls)
    await asyncio.gather(
        executor.handle_cancel_event(CancelEvent(symbol="SOLUSDT", raw_text="test"), client=server),
        executor.handle_cancel_event(CancelEvent(symbol="SOLUSDT", raw_text="test"), client=server),
    )
    new_actions = action_calls(server.calls[calls_before:])
    assert [n for n, _ in new_actions] == ["delete_order_v3", "create_order_v3"], \
        f"bij gelijktijdige duplicaten mag maar EEN taak de SL-cancel/close-calls doen, kreeg: {new_actions}"

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
            "sl_price": 200.3, "sl_oid": "9999", "qty": 0.5, "remaining_qty": 0.5,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.01", "step_size": "0.01",
        }})
    config.DRY_RUN = True
    calls_before = len(server.calls)
    await executor.handle_cancel_event(
        CancelEvent(symbol="SOLUSDT", raw_text="test"), client=server,
    )
    assert action_calls(server.calls[calls_before:]) == [], "DRY_RUN=True met v2-entry had GEEN muterende call mogen doen"
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
    adapt_server = FakeApexServer(
        mark_px=110.0, max_leverage=10, tick_size="0.01", step_size="0.01", universe_symbol="ADAPT",
    )
    adapt_entry = 100.0
    adapt_qty = 10.0
    async with executor._state_lock:
        executor._save_state({"ADAPT:Buy": {
            "version": "v2", "symbol": "ADAPT", "is_buy": True,
            "entry_price": adapt_entry, "sl_price": 90.0, "sl_oid": "7777",
            "qty": adapt_qty, "remaining_qty": adapt_qty,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.01", "step_size": "0.01", "banked_pnl": 0.0,
        }})

    close_qty_1_adapt = expected_cum_close_qty(adapt_qty, adapt_qty, cum_pcts[1], "0.01")
    await executor.handle_tp_event(
        TPEvent(symbol="ADAPTUSDT", target_number=1, raw_text="test"), client=adapt_server,
    )
    pos_adapt = json.load(open(TEST_STATE_FILE))["ADAPT:Buy"]
    expected_banked_1 = close_qty_1_adapt * (adapt_server.mark_px - adapt_entry)
    assert abs(pos_adapt["banked_pnl"] - expected_banked_1) < 1e-9, \
        f"banked_pnl na TP1 moet de echte fill-winst zijn: verwacht {expected_banked_1}, kreeg {pos_adapt['banked_pnl']}"

    remaining_after_1 = executor._round_sz(adapt_qty - close_qty_1_adapt, "0.01")
    adapt_server.mark_px = 115.0  # koers stijgt verder vóór TP2
    close_qty_2_adapt = expected_cum_close_qty(adapt_qty, remaining_after_1, cum_pcts[2], "0.01")
    await executor.handle_tp_event(
        TPEvent(symbol="ADAPTUSDT", target_number=2, raw_text="test"), client=adapt_server,
    )
    pos_adapt = json.load(open(TEST_STATE_FILE))["ADAPT:Buy"]
    expected_banked_2 = expected_banked_1 + close_qty_2_adapt * (adapt_server.mark_px - adapt_entry)
    assert abs(pos_adapt["banked_pnl"] - expected_banked_2) < 1e-9, \
        f"banked_pnl na TP2 moet cumulatief zijn: verwacht {expected_banked_2}, kreeg {pos_adapt['banked_pnl']}"

    remaining_after_2 = executor._round_sz(remaining_after_1 - close_qty_2_adapt, "0.01")
    # Veiligheidsmarge zit over de ORIGINELE notional (2026-09-01: was de
    # resterende notional, maar dat dekte de entry-fee nooit -- zie
    # executor._handle_tp_partial_event).
    safety_adapt = adapt_qty * adapt_entry * (config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT / 100)
    available_adapt = max(expected_banked_2 - safety_adapt, 0.0)
    expected_be_trigger_adapt = executor._round_px(adapt_entry - available_adapt / remaining_after_2, "0.01")
    fixed_buffer_trigger = executor._round_px(adapt_entry * (1 - 0.5 / 100), "0.01")  # wat een vaste 0.5%-buffer zou geven

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
    reentry_server = FakeApexServer(mark_px=195.0, max_leverage=10, tick_size="0.01", step_size="0.01", universe_symbol="SOL")
    old_sl_oid = "5555"
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False,
            "entry_price": 192.85, "sl_price": 200.3, "sl_oid": old_sl_oid,
            "qty": 10.0, "remaining_qty": 10.0,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.01", "step_size": "0.01", "banked_pnl": 0.0,
            "opened_at": time.time() - 3600,  # ruim buiten DUPLICATE_POSITION_WINDOW_SECONDS (60s)
        }})

    # Simuleert wat ApeX Omni's ECHTE get_account_v3()["positions"] rapporteert
    # NA de nieuwe fill: de exchange heeft de nieuwe order zelf al
    # samengevoegd met de bestaande 10-lot short tot een netto 16-lot short
    # @ 195.0 (autoritatief, niet zelf lokaal herberekend).
    reentry_server.live_position_override = {"symbol": "SOL-USDT", "side": "SELL", "size": "16.0", "entryPrice": "195.0"}

    signal_reentry = Signal(
        symbol="SOLUSDT", side="Sell", entry_low=193.0, entry_high=196.0,
        leverage=10, targets=[190.0, 188.0, 186.0, 184.0, 180.0], stop_loss=201.0, raw_text="reentry",
    )
    result_reentry = await executor.place_entry_order(signal_reentry, dry_run=False, client=reentry_server)
    assert result_reentry is not None and "toegevoegd" in result_reentry, f"re-entry had moeten slagen: {result_reentry}"

    cancel_calls = [c for name, c in action_calls(reentry_server.calls) if name == "delete_order_v3"]
    assert cancel_calls == [{"id": old_sl_oid}], \
        f"de OUDE SL had geannuleerd moeten worden vóór de nieuwe SL: {cancel_calls}"

    sl_order_calls = [c for name, c in action_calls(reentry_server.calls) if name == "create_order_v3" and c["type"] == "STOP_MARKET"]
    assert len(sl_order_calls) == 1 and sl_order_calls[0]["size"] == "16.0", \
        f"de nieuwe SL had de VOLLEDIGE samengevoegde qty (16.0) moeten dekken, niet alleen de nieuwe fill: {sl_order_calls}"

    state_reentry = json.load(open(TEST_STATE_FILE))["SOL:Sell"]
    assert state_reentry["qty"] == 16.0 and state_reentry["remaining_qty"] == 16.0, \
        f"state moet de ECHTE samengevoegde qty gebruiken (16.0), niet zelf herberekenen: {state_reentry}"
    assert state_reentry["entry_price"] == 195.0, \
        f"state moet de ECHTE (geblende) entry-prijs van ApeX Omni gebruiken: {state_reentry}"
    assert not any(state_reentry[f"tp{n}_done"] for n in (1, 2, 3, 4)), "TP-ladder moet resetten voor de samengevoegde positie"
    assert state_reentry["banked_pnl"] == 0.0, "banked_pnl moet resetten -- nieuwe entry-prijs, geen oude referentie meer"

    assert any("Re-entry samengevoegd" in m for m in notifications), notifications
    notifications.clear()
    print(f"OK stap 12: same-side re-entry samengevoegd -- oude SL (id={old_sl_oid}) geannuleerd, "
          f"nieuwe SL dekt de volledige samengevoegde qty (16.0 @ 195.0), TP-ladder en banked_pnl gereset")

    # --- Stap 12b: fallback als de samengevoegde positie niet te bevestigen
    # is bij ApeX Omni (bv. de oude positie bleek intussen extern gesloten,
    # zie reconcile_positions) -- MOET de nieuwe fill als op zichzelf staande
    # positie behandelen i.p.v. te gokken op een qty die niet klopt. ---
    reentry_server2 = FakeApexServer(mark_px=195.0, max_leverage=10, tick_size="0.01", step_size="0.01", universe_symbol="SOL")
    async with executor._state_lock:
        executor._save_state({"SOL:Sell": {
            "version": "v2", "symbol": "SOL", "is_buy": False,
            "entry_price": 192.85, "sl_price": 200.3, "sl_oid": "6666",
            "qty": 10.0, "remaining_qty": 10.0,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.01", "step_size": "0.01", "banked_pnl": 0.0,
            "opened_at": time.time() - 3600,
        }})
    reentry_server2.live_position_override = None  # geen bevestigde live positie
    result_reentry2 = await executor.place_entry_order(signal_reentry, dry_run=False, client=reentry_server2)
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
    # tegengesteld v2-record nog ECHT open staat op ApeX Omni. ---
    opp_server = FakeApexServer(mark_px=0.0086, max_leverage=5, tick_size="0.0001", step_size="1", universe_symbol="PENGU")
    async with executor._state_lock:
        executor._save_state({"PENGU:Buy": {
            "version": "v2", "symbol": "PENGU", "is_buy": True,
            "entry_price": 0.008807, "sl_price": 0.008436, "sl_oid": "1111",
            "qty": 300000.0, "remaining_qty": 300000.0,
            "tp1_done": False, "tp2_done": False, "tp3_done": False, "tp4_done": False,
            "tick_size": "0.0001", "step_size": "1", "banked_pnl": 0.0, "opened_at": time.time() - 3600,
        }})
    signal_opp = Signal(
        symbol="PENGUUSDT", side="Sell", entry_low=0.0085, entry_high=0.0086,
        leverage=5, targets=[0.0084, 0.0083, 0.0082, 0.0081, 0.008], stop_loss=0.0089, raw_text="opp-test",
    )

    # Stap 13a: PENGU:Buy staat nog ECHT open op ApeX Omni -> nieuwe
    # Sell-entry moet overgeslagen worden (zou netten/flippen op de
    # exchange), state blijft ongewijzigd, gebruiker wordt gewaarschuwd.
    opp_server.live_position_override = {"symbol": "PENGU-USDT", "side": "BUY", "size": "300000", "entryPrice": "0.008807"}
    calls_before = len(opp_server.calls)
    result_opp_live = await executor.place_entry_order(signal_opp, dry_run=False, client=opp_server)
    assert result_opp_live is None, f"had overgeslagen moeten worden, kreeg: {result_opp_live}"
    new_actions_opp = action_calls(opp_server.calls[calls_before:])
    assert new_actions_opp == [], f"had geen muterende calls mogen doen: {new_actions_opp}"
    state_opp = json.load(open(TEST_STATE_FILE))
    assert "PENGU:Buy" in state_opp and "PENGU:Sell" not in state_opp, \
        "PENGU:Buy moet blijven staan, geen nieuwe PENGU:Sell erbij (zou de exacte 2026-09-08-bug herhalen)"
    assert any("PENGU:Buy" in m for m in notifications), notifications
    notifications.clear()
    print("OK stap 13a: tegengestelde v2-positie nog ECHT open op ApeX Omni -> nieuwe entry overgeslagen, "
          "geen dubbel state-record (voorkomt de exacte 2026-09-08-bug)")

    # Stap 13b: PENGU:Buy staat NIET meer echt open (bv. resting SL buiten
    # de bot om getriggerd) -> stale record wordt opgeruimd en de nieuwe
    # entry gaat gewoon door.
    opp_server.live_position_override = None
    calls_before = len(opp_server.calls)
    result_opp_stale = await executor.place_entry_order(signal_opp, dry_run=False, client=opp_server)
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
