"""
Voert signals uit op ApeX Omni (apex.exchange) -- een perp-DEX waar het geld
rechtstreeks staat (geen Phantom/Hyperliquid-tussenstap meer, zie config.py's
module-docstring voor het twee-sleutel-signing-model).

OVERSTAP VAN HYPERLIQUID NAAR APEX OMNI (2026): dezelfde onderliggende
architectuurkeuzes als de Hyperliquid-versie (v2-exitstrategie,
margin-based sizing, TP-ladder via Telegram-groepsberichten i.p.v. resting
TP-trigger-orders) zijn hier bewust ONGEWIJZIGD overgenomen -- die logica is
exchange-onafhankelijk en al meerdere keren in productie bijgeschaafd (zie de
incident-verwijzingen in config.py). Alleen de laag die daadwerkelijk met de
exchange praat (get_market_info, order-plaatsing, positie-/saldo-queries) is
vervangen door de officiële `apexomni`-SDK (pip install apexomni,
github.com/ApeX-Protocol/apexpro-openapi).

BELANGRIJK -- wat zelf geverifieerd is tegen ApeX Omni's testnet (met een
wegwerp-testaccount, geen echt geld) vóór deze code werd geschreven:
account-registratie (zie setup_apex_account.py), order plaatsen (MARKET en
STOP_MARKET), order-status opvragen, annuleren, leverage/margin-rate zetten,
market-config/ticker/worst-price/account-balance-velden, EN de exacte
foutmelding voor "reduce-only op een positie die niet (meer) bestaat"
(`ORDER_IS_REDUCE_ONLY_CANNOT_OPEN_POSITION` -- zie _is_position_already_closed_error).
NIET rechtstreeks bevestigd tegen een écht gevulde/live positie (het
testaccount had geen saldo): het exacte veldenschema van get_account_v3()
["positions"] (zie _parse_live_positions) en de terminale status-string van
een volledig gevulde MARKET-order (zie _await_order_fill, die daarom
voornamelijk op het NUMERIEKE gevulde-qty-veld vertrouwt i.p.v. op één
verwachte status-tekst). Verifieer deze twee expliciet tijdens je eigen
testnet-tests (APEX_ENV=test) voor je live gaat.

BELANGRIJK: dit draait rechtstreeks tegen MAIN (config.APEX_ENV), niet tegen
test -- bewuste keuze om het bestaande ApeX Omni-saldo direct te hergebruiken.
DRY_RUN blijft daarom de eerste veiligheidsklep, en APEX_ENV=test is er
BOVENOP een tweede, onafhankelijke laag (die Hyperliquid nooit had) om de
hele pipeline eerst op nepgeld te verifiëren.

Architectuur:
- Markets worden geïdentificeerd met hun ApeX Omni-symbol ("BTC-USDT"),
  live opgevraagd via configs_v3()/ticker_v3() i.p.v. een lokale lijst die
  kan verouderen (zelfde aanpak als voorheen).
- De apexomni-SDK is SYNCHROON (gebruikt de `requests`-library, geen async).
  Alle calls lopen daarom via asyncio.to_thread(...) zodat ze de
  Telegram-event-loop niet blokkeren (zie _call()).
- Entry = MARKET-order met `price` = get_worst_price_v3()'s worstPrice
  (ApeX Omni's eigen max-slippage-mechanisme voor market-orders).

EXIT-STRATEGIE (v2, ONGEWIJZIGD overgenomen van de Hyperliquid-versie, geldt
voor alle posities): qty wordt direct afgeleid van
config.MAX_MARGIN_PCT_OF_FUNDS% van het beschikbare saldo (zie
_calc_margin_based_qty). Bij entry wordt ALLEEN een SL geplaatst (volle qty,
als STOP_MARKET reduce-only trigger-order, onafhankelijk actief op ApeX Omni
tussen entry en het eerste TP-bericht) -- geen TP-trigger-order. Sluiten
gebeurt expliciet op basis van de groep's eigen "Take-Profit target N
✅"-berichten (signal_parser.parse_tp_event): een 5-staps ladder over de
ORIGINELE qty, elke stap een DIRECTE reduce-only MARKET-close zodra het
bericht binnenkomt (geen resting TP-trigger-orders -- zie de
Hyperliquid-versie's moduledocstring voor de oorspronkelijke reden: een
TP-trigger-order bleek onbetrouwbaar te detecteren of een fill echt was).
Target 1 verplaatst ook de SL naar een dynamisch berekende break-even (zie
config.BREAKEVEN_MOVE_AFTER_TARGET/BREAKEVEN_PNL_SAFETY_MARGIN_PCT).
"""
import asyncio
import decimal
import json
import logging
import os
import time
from typing import Optional

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_MAIN,
    NETWORKID_TEST,
)
from apexomni.http_private_sign import HttpPrivateSign

import config
import db
from signal_parser import Signal

log = logging.getLogger("executor")

# Wordt door main.py gezet zodra de Telegram-client bestaat, zodat executor.py
# notificaties kan sturen zonder main.py te hoeven importeren (voorkomt een
# circulaire import).
notify_callback = None

# Overrideable via env var zodat test-scripts NOOIT het gedeelde, echte
# state-bestand van de live service kunnen raken -- dat gebeurde eerder
# (2026-08-10, oorspronkelijk op Hyperliquid) toen een test zonder deze
# isolatie een positie-entry in het gedeelde bestand achterliet, die de live
# service vervolgens oppikte en er (met de ECHTE exchange) actie op ondernam.
STATE_FILE = os.getenv("STATE_FILE_PATH") or os.path.join(os.path.dirname(__file__), "open_positions.json")
_state_lock = asyncio.Lock()
# Zie de duplicaat-guard in place_entry_order(): een echt duplicaat-signal
# komt (bijna) gelijktijdig binnen, dus 60s is ruim voldoende marge zonder
# latere, echt nieuwe signalen voor hetzelfde symbol+side te blokkeren.
DUPLICATE_POSITION_WINDOW_SECONDS = 60
# State-keys waarvoor handle_cancel_event() op dit moment een SL-cancel/close
# aan het uitvoeren is -- voorkomt dat een (bijna) gelijktijdig binnenkomend
# duplicaat-cancelbericht dezelfde positie nogmaals probeert te sluiten
# voordat de eerste klaar is en de state-pop heeft gedaan.
_cancel_in_progress: set[str] = set()

# Marge tussen triggerPrice en de limit-`price` van een reduce-only
# STOP_MARKET-order, zodat de order ook echt kan vullen als de markt net over
# de trigger heen schiet (zelfde reden als voorheen bij Hyperliquid).
TRIGGER_LIMIT_BUFFER_PCT = 0.03

# Hoeveel keer/hoe lang gepolld wordt op get_order_v3() na het plaatsen van
# een MARKET-order, om de fill te bevestigen (zie _await_order_fill). ApeX
# Omni's create_order_v3-response bevat -- anders dan Hyperliquid's
# synchrone market_open()-response -- geen directe fill-bevestiging.
ORDER_FILL_POLL_ATTEMPTS = 10
ORDER_FILL_POLL_INTERVAL_SECONDS = 0.5

_ORDER_TERMINAL_FAILURE_STATUSES = {"CANCELED", "EXPIRED", "REJECTED"}

_client = None
_client_lock = asyncio.Lock()


def _apex_endpoint():
    if config.APEX_ENV == "main":
        return APEX_OMNI_HTTP_MAIN, NETWORKID_MAIN
    return APEX_OMNI_HTTP_TEST, NETWORKID_TEST


async def _call(fn, *args, **kwargs):
    """De apexomni-SDK is synchroon (requests-library) -- via to_thread()
    zodat een trage/hangende HTTP-call de Telegram-listener niet blokkeert."""
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _get_client() -> HttpPrivateSign:
    """Eén gecachete, ingelogde ApeX Omni-client voor de hele module (zelfde
    singleton-patroon als voorheen _get_exchange()/_get_info()). De
    dual-key-signing-context (zk_seeds/zk_l2Key/api_key_credentials) komt uit
    config.py -- ALLEMAAL uit de eenmalige setup_apex_account.py-run, nooit
    hier opnieuw afgeleid."""
    global _client
    if _client is not None:
        return _client
    async with _client_lock:
        if _client is not None:
            return _client
        endpoint, network_id = _apex_endpoint()
        client = HttpPrivateSign(
            endpoint,
            network_id=network_id,
            eth_private_key=config.APEX_ETH_PRIVATE_KEY,
            zk_seeds=config.APEX_ZK_SEEDS,
            zk_l2Key=config.APEX_ZK_L2KEY,
            api_key_credentials={
                "key": config.APEX_API_KEY,
                "secret": config.APEX_API_SECRET,
                "passphrase": config.APEX_API_PASSPHRASE,
            },
            request_timeout=config.APEX_API_TIMEOUT_SECONDS,
        )
        await _call(client.configs_v3)
        await _call(client.get_account_v3)
        _client = client
        return _client


async def get_owner_address(client=None) -> str:
    client = client or await _get_client()
    return client.default_address


def _apex_symbol(base_symbol: str) -> str:
    return f"{base_symbol.upper()}-USDT"


def _round_down_to_step(value: float, step) -> float:
    """Naar beneden afgerond op een veelvoud van `step` (tickSize voor
    prijzen, stepSize voor qty). ApeX Omni's eigen create_order_v3 weigert
    een prijs die geen exact veelvoud van tickSize is (zelf geverifieerd:
    'the price must Multiple of tickSize') -- en net als bij Hyperliquid's
    sz_decimals-afronding willen we bij qty altijd naar BENEDEN afronden
    zodat de werkelijke margin na afronding nooit boven de
    MAX_MARGIN_PCT_OF_FUNDS-cap uitkomt."""
    step_dec = decimal.Decimal(str(step))
    if step_dec <= 0:
        return value
    # Eerst op 8 decimalen afronden (ruim onder elke realistische tick/
    # stepSize) voor we floor'en: `value` is vaak zelf al het resultaat van
    # eerdere float-op-/aftrekkingen (bv. qty - remaining_qty in
    # _target_close_qty), die IEEE754-restruis kunnen achterlaten (29.94 ->
    # 29.939999999999998). Zonder deze stap floort zo'n "eigenlijk exacte"
    # waarde een hele step te laag, wat over een 4-staps TP-ladder een paar
    # cent bookkeeping-drift veroorzaakte (ontdekt tijdens het overzetten van
    # test_exit_strategy.py naar ApeX Omni, 2026-09).
    val_dec = decimal.Decimal(str(value)).quantize(decimal.Decimal("1e-8"), rounding=decimal.ROUND_HALF_UP)
    steps = (val_dec / step_dec).to_integral_value(rounding=decimal.ROUND_DOWN)
    return float(steps * step_dec)


def _round_px(px: float, tick_size) -> float:
    return _round_down_to_step(px, tick_size)


def _round_sz(sz: float, step_size) -> float:
    return _round_down_to_step(sz, step_size)


async def get_market_info(symbol: str, client=None) -> dict:
    """
    Haalt max leverage, tick/step-size en de actuele markprijs live op via
    configs_v3()/ticker_v3() i.p.v. een handmatige lijst te vertrouwen.
    Raiset ValueError als de coin niet (meer) bestaat op ApeX Omni, zodat de
    trade wordt overgeslagen i.p.v. blind te traden.
    """
    client = client or await _get_client()
    apex_symbol = _apex_symbol(symbol)

    perp_contracts = ((client.configV3 or {}).get("contractConfig") or {}).get("perpetualContract") or []
    symbol_data = next((c for c in perp_contracts if c.get("symbol") == apex_symbol), None)
    if symbol_data is None:
        raise ValueError(
            f"Geen ApeX Omni market gevonden voor {apex_symbol}. Check handmatig op "
            f"https://omni.apex.exchange of deze coin daar (nog) verhandelbaar is."
        )

    ticker_resp = await _call(client.ticker_v3, symbol=apex_symbol)
    ticker_list = _check_order_status(ticker_resp, "ticker opvragen")
    if not ticker_list:
        raise ValueError(f"Geen ticker-data voor {apex_symbol} op ApeX Omni.")

    return {
        "apex_symbol": apex_symbol,
        "max_leverage": int(float(symbol_data.get("displayMaxLeverage", 1))),
        "tick_size": symbol_data.get("tickSize"),
        "step_size": symbol_data.get("stepSize"),
        "mark_px": float(ticker_list[0]["markPrice"]),
    }


def _parse_live_positions(positions) -> dict:
    """coin -> {"qty": abs(size), "is_buy": ..., "entry_price": ...} voor
    elke coin met een open positie in deze get_account_v3()-snapshot.

    LET OP (zie moduledocstring): dit veldenschema (symbol/side/size/
    entryPrice) volgt ApeX Omni's consistente camelCase-conventie die overal
    elders in de v3-API zelf geverifieerd is (orders, ticker, balance), maar
    is NIET rechtstreeks bevestigd tegen een écht gevulde positie. Verifieer
    dit tegen een kleine testnet-positie voor je live gaat."""
    result = {}
    for p in positions or []:
        symbol = str(p.get("symbol", ""))
        base = symbol.split("-")[0] if "-" in symbol else symbol
        size = float(p.get("size", 0) or 0)
        if not base or size == 0:
            continue
        side = str(p.get("side", "")).upper()
        result[base] = {
            "qty": abs(size),
            "is_buy": (side == "BUY") if side else size > 0,
            "entry_price": float(p.get("entryPrice", 0) or 0),
        }
    return result


async def _fetch_live_positions(client=None) -> dict:
    client = client or await _get_client()
    resp = await _call(client.get_account_v3)
    data = _check_order_status(resp, "posities opvragen")
    return _parse_live_positions(data.get("positions"))


async def count_open_positions(client=None) -> int:
    """Telt open live posities via ApeX Omni's EIGEN account-endpoint --
    bewust niet via de lokale open_positions.json, want die kan uit sync
    raken met wat er werkelijk op de exchange staat."""
    return len(await _fetch_live_positions(client))


async def _get_live_position(base_symbol: str, client=None) -> Optional[dict]:
    """Haalt de ECHTE huidige netto positie voor base_symbol op bij ApeX
    Omni zelf, of None als er geen open positie is. Gebruikt bij een
    same-side re-entry om de nieuwe, samengevoegde qty/entry-prijs
    autoritatief te bepalen i.p.v. zelf op te tellen/te wegen."""
    return (await _fetch_live_positions(client)).get(base_symbol)


async def _fetch_account_balance(client=None) -> dict:
    client = client or await _get_client()
    resp = await _call(client.get_account_balance_v3)
    return _check_order_status(resp, "account-balance opvragen")


def _withdrawable_from_balance(balance: dict) -> float:
    """Beschikbare marge voor een NIEUWE positie. ApeX Omni geeft dit --
    anders dan Hyperliquid, waar perps- en spot-saldo apart opgevraagd en
    opgeteld moesten worden -- als één rechtstreeks veld terug (zelf
    geverifieerd)."""
    return float(balance.get("availableBalance", 0) or 0)


def _total_equity_from_balance(balance: dict) -> float:
    """Totale accountwaarde als basis voor MAX_MARGIN_PCT_OF_FUNDS% (zelfde
    reden als voorheen bij Hyperliquid: hiermee blijft het bedrag per trade
    constant binnen een cyclus van meerdere gelijktijdig openende posities,
    i.p.v. steeds kleiner te worden als _withdrawable_from_balance() zou
    slinken naarmate er meer marge vastgezet wordt)."""
    return float(balance.get("totalEquityValue", 0) or 0)


async def get_withdrawable(client=None) -> float:
    return _withdrawable_from_balance(await _fetch_account_balance(client))


async def get_total_equity(client=None) -> float:
    return _total_equity_from_balance(await _fetch_account_balance(client))


def _calc_margin_based_qty(entry_px: float, leverage: int, available_funds: float, step_size) -> float:
    """qty zo dat de benodigde margin exact MAX_MARGIN_PCT_OF_FUNDS% van
    available_funds is: margin = available_funds * pct/100, notional =
    margin * leverage, qty = notional / entry_px. Naar BENEDEN afgerond op
    stepSize zodat de werkelijke margin na afronding nooit boven de cap
    uitkomt."""
    margin_to_use = available_funds * (config.MAX_MARGIN_PCT_OF_FUNDS / 100)
    raw_qty = (margin_to_use * leverage) / entry_px
    return _round_down_to_step(raw_qty, step_size)


async def _notify(message: str):
    if notify_callback is None:
        return
    try:
        await notify_callback(message)
    except Exception:
        log.exception("Kon notificatie niet versturen vanuit executor")


def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def _save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _check_order_status(resp: dict, action: str):
    """ApeX Omni geeft een JSON-envelope terug: een geslaagde call heeft géén
    (of een lege/0-)'code'-veld, een afgekeurde call heeft 'code' (meestal
    niet-nul) + 'msg' + meestal 'key' (zelf geverifieerd tegen hun testnet,
    zowel voor business-fouten als malformed-request-fouten). Geeft
    resp['data'] terug bij succes (kan een dict OF een list zijn, afhankelijk
    van het endpoint -- bv. ticker_v3 geeft een list terug)."""
    code = resp.get("code")
    if code:
        key = resp.get("key", code)
        raise RuntimeError(f"ApeX Omni wees {action} af: {resp.get('msg')} ({key})")
    return resp.get("data", {})


# Deze exacte key is zelf geverifieerd tegen ApeX Omni's testnet: een
# reduce-only order op een symbol zonder (of met te kleine) open positie
# geeft precies deze 'key' terug -- de ApeX Omni-tegenhanger van Hyperliquid's
# "Reduce only order would increase position". Dit is het enige scenario dat
# hier bevestigd is; andere "positie bleek al gesloten"-varianten (bv. een
# dubbele SL-cancel) komen als een aparte apexomni.exceptions.FailedRequestError
# binnen (HTTP 409) i.p.v. een JSON-foutrespons, en worden daarom niet via
# deze functie afgehandeld -- de aanroepende cancel-calls slikken sowieso
# alle Exceptions al (zie bv. place_entry_order's re-entry-SL-cancel).
_POSITION_ALREADY_CLOSED_KEYS = (
    "ORDER_IS_REDUCE_ONLY_CANNOT_OPEN_POSITION",
)


def _is_position_already_closed_error(exc: Exception) -> bool:
    """True als deze ApeX Omni-foutmelding erop wijst dat de positie (of de
    bijbehorende reduce-only close) al buiten de bot om gesloten is -- bv.
    doordat een eerder geplaatste break-even-SL intussen is getriggerd (zelfde
    incident-klasse als PENGU/BTC/HYPE, 2026-08-25, oorspronkelijk op
    Hyperliquid)."""
    return any(marker in str(exc) for marker in _POSITION_ALREADY_CLOSED_KEYS)


async def _await_order_fill(client, order_id: str, expected_size: float) -> dict:
    """Pollt get_order_v3() tot de order een terminale staat heeft. ApeX
    Omni's create_order_v3-response bevat -- anders dan Hyperliquid's
    synchrone market_open()-response -- geen directe fill-bevestiging.
    Vertrouwt vooral op het NUMERIEKE gevulde-qty-veld (cumSuccessFillSize)
    t.o.v. de gevraagde qty i.p.v. op één verwachte status-tekst, omdat de
    exacte status-string van een volledig gevulde MARKET-order niet
    rechtstreeks bevestigd is (zie moduledocstring) -- wél bevestigd:
    CANCELED/EXPIRED/REJECTED als duidelijke terminale mislukkingen."""
    last = None
    for _ in range(ORDER_FILL_POLL_ATTEMPTS):
        resp = await _call(client.get_order_v3, id=order_id)
        data = _check_order_status(resp, "order-status opvragen")
        last = data
        status = str(data.get("status", "")).upper()
        filled_size = float(data.get("cumSuccessFillSize") or data.get("cumMatchFillSize") or 0)
        if status in _ORDER_TERMINAL_FAILURE_STATUSES:
            raise RuntimeError(f"Order {order_id} werd niet gevuld (status={status}): {data}")
        if filled_size > 0 and (status == "FILLED" or filled_size >= expected_size * 0.999):
            return data
        await asyncio.sleep(ORDER_FILL_POLL_INTERVAL_SECONDS)
    raise RuntimeError(
        f"Order {order_id} niet binnen "
        f"{ORDER_FILL_POLL_ATTEMPTS * ORDER_FILL_POLL_INTERVAL_SECONDS:.0f}s bevestigd als gevuld: {last}"
    )


async def _place_market_order(
    client, apex_symbol: str, is_buy: bool, qty: float, reduce_only: bool,
) -> tuple[float, float, str]:
    """Plaatst een MARKET-order (entry of reduce-only close) en wacht op
    bevestigde fill. `price` = get_worst_price_v3()'s worstPrice -- ApeX
    Omni's eigen max-slippage-mechanisme voor market-orders (zelf
    geverifieerd), analoog aan Hyperliquid's agressieve IOC-limit-aanpak.
    Retourneert (filled_qty, avg_price, order_id)."""
    side = "BUY" if is_buy else "SELL"
    price_resp = await _call(client.get_worst_price_v3, symbol=apex_symbol, side=side, size=str(qty))
    price_data = _check_order_status(price_resp, "worst-price opvragen")
    worst_price = price_data["worstPrice"]

    order_resp = await _call(
        client.create_order_v3, symbol=apex_symbol, side=side, type="MARKET",
        size=str(qty), price=str(worst_price), reduceOnly=reduce_only,
    )
    order_data = _check_order_status(order_resp, "market-order plaatsen")
    order_id = order_data.get("id")
    if order_id is None:
        raise RuntimeError(f"Geen order-id in ApeX Omni-response: {order_data}")

    filled = await _await_order_fill(client, order_id, expected_size=qty)
    filled_qty = float(filled.get("cumSuccessFillSize") or filled.get("cumMatchFillSize") or qty)
    avg_px = float(filled.get("averagePrice") or worst_price)
    return filled_qty, avg_px, order_id


async def _place_stop_order(
    client, apex_symbol: str, exit_is_buy: bool, qty: float, trigger_price: float, limit_price: float,
) -> Optional[str]:
    """Plaatst een STOP_MARKET reduce-only trigger-order (de SL) -- ApeX
    Omni's tegenhanger van Hyperliquid's `order(..., trigger={isMarket:True,
    tpsl:'sl'})`. Geeft de order-id terug (voor latere annulering), of None
    als die niet in de response zat."""
    side = "BUY" if exit_is_buy else "SELL"
    resp = await _call(
        client.create_order_v3, symbol=apex_symbol, side=side, type="STOP_MARKET",
        size=str(qty), price=str(limit_price), triggerPrice=str(trigger_price),
        triggerPriceType="INDEX", reduceOnly=True, isPositionTpsl=True,
    )
    data = _check_order_status(resp, "stop-loss plaatsen")
    return data.get("id")


async def _cancel_order(client, order_id: str, action: str):
    resp = await _call(client.delete_order_v3, id=order_id)
    _check_order_status(resp, action)


async def _market_close_reduce_only(
    base_symbol: str, exit_is_buy: bool, sz: float, client=None,
) -> tuple[float, float]:
    """Sluit (een deel van) een v2-positie met een reduce-only MARKET-order.
    Retourneert (filled_qty, avg_price)."""
    client = client or await _get_client()
    apex_symbol = _apex_symbol(base_symbol)
    filled_qty, avg_px, _order_id = await _place_market_order(
        client, apex_symbol, is_buy=exit_is_buy, qty=sz, reduce_only=True,
    )
    return filled_qty, avg_px


def _cumulative_target_pct(target_number: int) -> float:
    """Som van TP_EVENT_TARGETn_CLOSE_PCT t/m (inclusief) target_number, voor
    target_number 1 t/m 4 (target 5 heeft geen eigen %, zie
    _handle_tp5_event)."""
    pcts = [
        config.TP_EVENT_TARGET1_CLOSE_PCT,
        config.TP_EVENT_TARGET2_CLOSE_PCT,
        config.TP_EVENT_TARGET3_CLOSE_PCT,
        config.TP_EVENT_TARGET4_CLOSE_PCT,
    ]
    return sum(pcts[:target_number])


def _target_close_qty(pos: dict, cum_pct: float) -> float:
    """Hoeveel er NU dicht moet voor een target met cumulatief percentage
    `cum_pct` van de ORIGINELE qty: het verschil tussen wat er in totaal al
    dicht had moeten zijn t/m deze target, en wat er al écht dicht is (qty -
    remaining_qty). Zo loopt een deel-close die een eerdere target oversloeg
    wegens MIN_NOTIONAL_USD automatisch mee in de eerstvolgende target die
    wel boven de grens uitkomt -- geen aparte "carry"-state nodig."""
    step_size = pos["step_size"]
    total_should_be_closed = _round_sz(pos["qty"] * (cum_pct / 100), step_size)
    already_closed = _round_sz(pos["qty"] - pos["remaining_qty"], step_size)
    close_qty = _round_sz(total_should_be_closed - already_closed, step_size)
    # Nooit meer sluiten dan er nog over is (dekt afrondingsverschillen af).
    return max(0.0, min(close_qty, pos["remaining_qty"]))


async def _handle_position_already_closed(key: str, symbol: str, context: str):
    """State opschonen wanneer een reduce-only close/SL-cancel faalt omdat de
    positie al (buiten de bot om, bv. via een getriggerde break-even-SL)
    volledig gesloten bleek te zijn -- voorkomt dat elk volgend TP/cancel-event
    voor dezelfde positie op dezelfde stale state blijft crashen."""
    async with _state_lock:
        state = _load_state()
        state.pop(key, None)
        _save_state(state)
    log.info(
        "%s voor %s: positie bleek al gesloten (waarschijnlijk break-even-SL) -- state opgeschoond.",
        context, symbol,
    )
    db.log_tp_event(
        symbol=symbol, event="tp_event_position_already_closed",
        detail=f"{context}: positie al gesloten buiten de bot om, state opgeschoond",
    )
    await _notify(f"ℹ️ {symbol} ({context}) — positie bleek al gesloten (waarschijnlijk SL), state opgeschoond")


async def place_entry_order(signal: Signal, dry_run: bool = False, client=None):
    """
    v2-entry (geldt voor alle NIEUWE signals): qty wordt direct afgeleid van
    config.MAX_MARGIN_PCT_OF_FUNDS% van het op dat moment beschikbare saldo
    (zie _calc_margin_based_qty) -- dus de margin die een trade kost staat
    vooraf vast als percentage, ongeacht welke leverage ApeX Omni voor die
    specifieke coin toestaat. Alleen een SL bij entry -- geen TP-trigger-order
    (zie moduledocstring). `client` is optioneel injecteerbaar zodat tests een
    fake object kunnen doorgeven als expliciet functie-argument."""
    base_symbol = signal.symbol.replace("USDT", "")

    # Veiligheidsnet tegen duplicaat-signals van het kanaal (zie de
    # Hyperliquid-versie's incident van 2026-08-14 -- deze logica is
    # ongewijzigd overgenomen, exchange-onafhankelijk).
    async with _state_lock:
        state = _load_state()
    existing_key = f"{base_symbol}:{signal.side}"
    existing = state.get(existing_key, {})
    existing_age = time.time() - existing.get("opened_at", 0)

    is_reentry = existing.get("version") == "v2" and existing_age > DUPLICATE_POSITION_WINDOW_SECONDS
    if is_reentry:
        log.info(
            "Re-entry voor %s %s: bestaande v2-positie (%.0fs geleden geopend) wordt samengevoegd "
            "i.p.v. overschreven -- oude SL wordt geannuleerd, nieuwe SL dekt de volledige positie.",
            signal.side, signal.symbol, existing_age,
        )

    if existing.get("version") == "v2" and existing_age <= DUPLICATE_POSITION_WINDOW_SECONDS:
        log.warning(
            "Gemiste trade wegens al open v2-positie: %s %s overgeslagen (waarschijnlijk "
            "duplicaat-signal van het kanaal voor %s, %.0fs geleden geopend).",
            signal.side, signal.symbol, existing_key, existing_age,
        )
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=dry_run,
            status="skipped_duplicate_position", leverage=signal.leverage, stop_loss=signal.stop_loss,
            error=f"v2-positie al open voor {existing_key}",
        )
        await _notify(
            f"⏭️ Gemiste trade wegens al open positie: {signal.side} {signal.symbol} "
            f"(waarschijnlijk duplicaat-signal van het kanaal)"
        )
        return None

    client = client or await _get_client()

    # Zelfde tegengestelde-positie-guard als de Hyperliquid-versie (incident
    # 2026-09-08): ApeX Omni kent, net als Hyperliquid, geen hedge-mode (maar
    # één netto positie per coin), dus vóór een nieuwe entry altijd
    # verifiëren of een tegengesteld v2-record nog ECHT open staat.
    opposite_side = "Sell" if signal.side == "Buy" else "Buy"
    opposite_key = f"{base_symbol}:{opposite_side}"
    async with _state_lock:
        state = _load_state()
        opposite = state.get(opposite_key)
    live_positions = None
    if opposite is not None and opposite.get("version") == "v2":
        live_positions = await _fetch_live_positions(client)
        live = live_positions.get(base_symbol)
        if live is not None and live["is_buy"] == opposite.get("is_buy"):
            log.warning(
                "Tegengestelde v2-positie (%s) nog open op ApeX Omni bij nieuw %s-signaal voor %s "
                "-- overgeslagen, handmatig checken (zou netten/flippen op de exchange).",
                opposite_key, signal.side, signal.symbol,
            )
            db.log_order(
                symbol=signal.symbol, side=signal.side, dry_run=dry_run,
                status="skipped_opposite_position_open", leverage=signal.leverage, stop_loss=signal.stop_loss,
                error=f"tegengestelde v2-positie {opposite_key} nog open op ApeX Omni",
            )
            await _notify(
                f"⚠️ {signal.side} {signal.symbol} overgeslagen: tegengestelde positie ({opposite_key}) "
                f"staat nog open op ApeX Omni. Zou netten/flippen op de exchange -- handmatig checken."
            )
            return None
        log.warning(
            "Tegengestelde v2-positie %s bleek niet meer open op ApeX Omni bij nieuw %s-signaal "
            "voor %s -- stale state opgeruimd.",
            opposite_key, signal.side, signal.symbol,
        )
        realized_pnl = await _estimate_realized_pnl_since(base_symbol, opposite.get("opened_at", 0), client)
        pnl_txt = f"~${realized_pnl:.2f}" if realized_pnl is not None else "onbekend"
        async with _state_lock:
            state = _load_state()
            state.pop(opposite_key, None)
            _save_state(state)
        db.log_tp_event(
            symbol=base_symbol, event="position_closed_externally_detected",
            detail=f"key={opposite_key}, niet meer open op ApeX Omni bij nieuw tegengesteld signaal, "
                   f"state opgeruimd, PnL sinds entry: {pnl_txt}",
        )
        await _notify(
            f"⚠️ {opposite_key} bleek al gesloten op ApeX Omni (ontdekt bij nieuw tegengesteld "
            f"{signal.side}-signaal voor {signal.symbol}) -- state opgeruimd. Gerealiseerde PnL sinds "
            f"entry: {pnl_txt}."
        )

    open_count = len(live_positions) if live_positions is not None else await count_open_positions(client)
    if open_count >= config.MAX_CONCURRENT_POSITIONS:
        log.warning(
            "Gemiste trade wegens max posities: %s %s overgeslagen (%d/%d open live posities op ApeX Omni).",
            signal.side, signal.symbol, open_count, config.MAX_CONCURRENT_POSITIONS,
        )
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=dry_run,
            status="skipped_max_positions", leverage=signal.leverage, stop_loss=signal.stop_loss,
            error=f"max posities bereikt ({open_count}/{config.MAX_CONCURRENT_POSITIONS})",
        )
        await _notify(
            f"⏭️ Gemiste trade wegens max posities: {signal.side} {signal.symbol} "
            f"({open_count}/{config.MAX_CONCURRENT_POSITIONS} open)"
        )
        return None

    try:
        market = await get_market_info(base_symbol, client=client)
    except ValueError as e:
        log.warning("Order overgeslagen: %s", e)
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=dry_run,
            status="skipped", leverage=signal.leverage, stop_loss=signal.stop_loss, error=str(e),
        )
        return None

    used_leverage = min(signal.leverage, market["max_leverage"])
    if used_leverage < signal.leverage:
        log.info(
            "Signal vroeg %sx, ApeX Omni staat max %sx toe voor %s -> gebruik %sx",
            signal.leverage, market["max_leverage"], signal.symbol, used_leverage,
        )

    apex_symbol = market["apex_symbol"]
    step_size = market["step_size"]
    tick_size = market["tick_size"]
    is_buy = signal.side == "Buy"
    exit_is_buy = not is_buy

    balance = await _fetch_account_balance(client)
    withdrawable = _withdrawable_from_balance(balance)
    if withdrawable <= 0:
        log.warning(
            "Gemiste trade wegens geen beschikbaar saldo: %s %s overgeslagen (beschikbaar $%.2f).",
            signal.side, signal.symbol, withdrawable,
        )
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=dry_run,
            status="skipped_insufficient_margin", leverage=signal.leverage, stop_loss=signal.stop_loss,
            error=f"geen beschikbaar saldo (${withdrawable:.2f})",
        )
        await _notify(
            f"⏭️ Gemiste trade wegens geen beschikbaar saldo: {signal.side} {signal.symbol} "
            f"(beschikbaar ${withdrawable:.2f})"
        )
        return None

    total_equity = _total_equity_from_balance(balance)
    qty = _calc_margin_based_qty(market["mark_px"], used_leverage, total_equity, step_size)
    notional = qty * market["mark_px"]
    margin_needed = notional / used_leverage

    if qty <= 0 or notional < config.MIN_NOTIONAL_USD:
        log.warning(
            "Gemiste trade wegens te kleine ordergrootte: %s %s overgeslagen "
            "(qty=%s, notional=$%.2f, minimum $%.0f -- %s%% van $%.2f totale accountwaarde bij %sx).",
            signal.side, signal.symbol, qty, notional, config.MIN_NOTIONAL_USD,
            config.MAX_MARGIN_PCT_OF_FUNDS, total_equity, used_leverage,
        )
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=dry_run,
            status="skipped_min_notional", leverage=signal.leverage, stop_loss=signal.stop_loss,
            error=f"notional ${notional:.2f} onder ${config.MIN_NOTIONAL_USD:.0f}-minimum "
                  f"({config.MAX_MARGIN_PCT_OF_FUNDS}% van ${total_equity:.2f} totale accountwaarde bij {used_leverage}x)",
        )
        await _notify(
            f"⏭️ Gemiste trade wegens te kleine ordergrootte: {signal.side} {signal.symbol} "
            f"(notional ${notional:.2f} onder ${config.MIN_NOTIONAL_USD:.0f}-minimum)"
        )
        return None

    if margin_needed > withdrawable:
        log.warning(
            "Gemiste trade wegens onvoldoende vrije marge: %s %s overgeslagen "
            "(nodig $%.2f, vrij $%.2f -- waarschijnlijk al marge vast in andere open posities).",
            signal.side, signal.symbol, margin_needed, withdrawable,
        )
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=dry_run,
            status="skipped_insufficient_margin", leverage=signal.leverage, stop_loss=signal.stop_loss,
            error=f"marge nodig ${margin_needed:.2f}, vrij ${withdrawable:.2f}",
        )
        await _notify(
            f"⏭️ Gemiste trade wegens onvoldoende vrije marge: {signal.side} {signal.symbol} "
            f"(nodig ${margin_needed:.2f}, vrij ${withdrawable:.2f})"
        )
        return None

    sl_trigger = _round_px(signal.stop_loss, tick_size)
    sl_limit = _round_px(
        sl_trigger * (1 - TRIGGER_LIMIT_BUFFER_PCT if is_buy else 1 + TRIGGER_LIMIT_BUFFER_PCT), tick_size
    )

    plan = {
        "coin": base_symbol,
        "apex_symbol": apex_symbol,
        "leverage": used_leverage,
        "leverage_requested": signal.leverage,
        "leverage_max_allowed": market["max_leverage"],
        "leverage_capped": used_leverage < signal.leverage,
        "mark_px_at_calc": market["mark_px"],
        "margin_pct_of_funds": config.MAX_MARGIN_PCT_OF_FUNDS,
        "margin_needed": margin_needed,
        "withdrawable": withdrawable,
        "entry_order": {"symbol": apex_symbol, "side": "BUY" if is_buy else "SELL", "type": "MARKET", "size": qty},
        "sl_order": {
            "symbol": apex_symbol, "side": "BUY" if exit_is_buy else "SELL", "type": "STOP_MARKET",
            "size": qty, "triggerPrice": sl_trigger, "price": sl_limit, "reduceOnly": True,
        },
    }

    if dry_run:
        log.info("[DRY RUN] Zou plaatsen op ApeX Omni:\n%s", json.dumps(plan, indent=2))
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=True, status="dry_run",
            entry_price=market["mark_px"], leverage=used_leverage, qty=qty, stop_loss=sl_trigger,
        )
        return plan

    # Isolated margin via initialMarginRate = 1/leverage (i.p.v. cross) zodat
    # elke trade z'n risico beperkt houdt tot deze ene positie.
    imr = str(round(1 / used_leverage, 6))
    lev_resp = await _call(client.set_initial_margin_rate_v3, symbol=apex_symbol, initialMarginRate=imr)
    _check_order_status(lev_resp, "leverage/margin-rate zetten")

    filled_qty, entry_price, _order_id = await _place_market_order(
        client, apex_symbol, is_buy=is_buy, qty=qty, reduce_only=False,
    )
    log.info(
        "Order geplaatst (v2, %s%% van saldo): %s %s qty=%.6f @ %s (%sx, margin~$%.2f)",
        config.MAX_MARGIN_PCT_OF_FUNDS, signal.side, signal.symbol, filled_qty, entry_price,
        used_leverage, margin_needed,
    )

    sl_qty = filled_qty
    state_qty = filled_qty
    state_entry_price = entry_price
    if is_reentry:
        old_sl_oid = existing.get("sl_oid")
        if old_sl_oid is not None:
            try:
                await _cancel_order(client, old_sl_oid, "oude SL annuleren (re-entry)")
            except Exception:
                log.exception("Kon oude SL niet annuleren voor %s bij re-entry", base_symbol)
        else:
            log.warning("Geen sl_oid bekend voor bestaande %s-positie bij re-entry.", existing_key)

        live_pos = await _get_live_position(base_symbol, client=client)
        if live_pos is not None and live_pos["is_buy"] == is_buy:
            sl_qty = live_pos["qty"]
            state_qty = live_pos["qty"]
            state_entry_price = live_pos["entry_price"]
        else:
            log.warning(
                "Kon samengevoegde positie voor %s niet bevestigen bij ApeX Omni -- "
                "state behandelt alleen de nieuwe fill (%.6f) als positie.",
                base_symbol, filled_qty,
            )

    sl_oid = await _place_stop_order(client, apex_symbol, exit_is_buy, sl_qty, sl_trigger, sl_limit)
    if sl_oid is None:
        log.warning("Kon geen order-id voor de stop-loss achterhalen.")

    async with _state_lock:
        state = _load_state()
        state[f"{base_symbol}:{signal.side}"] = {
            "version": "v2",
            "symbol": base_symbol,
            "is_buy": is_buy,
            "entry_price": state_entry_price,
            "sl_price": sl_trigger,
            "sl_oid": sl_oid,
            "qty": state_qty,
            "remaining_qty": state_qty,
            "tp1_done": False,
            "tp2_done": False,
            "tp3_done": False,
            "tp4_done": False,
            "tick_size": tick_size,
            "step_size": step_size,
            "banked_pnl": 0.0,
            "opened_at": time.time(),
        }
        _save_state(state)

    db.log_order(
        symbol=signal.symbol, side=signal.side, dry_run=False, status="placed",
        entry_price=state_entry_price, leverage=used_leverage, qty=state_qty, stop_loss=sl_trigger,
    )

    if is_reentry:
        await _notify(
            f"➕ Re-entry samengevoegd: {signal.side} {signal.symbol} — nieuwe fill qty={filled_qty} "
            f"@ {entry_price}, totale positie nu qty={state_qty} @ ~{state_entry_price} "
            f"({used_leverage}x), nieuwe SL {sl_trigger} dekt de volledige positie, TP-ladder reset."
        )
        return (
            f"qty={filled_qty} @ {entry_price} toegevoegd, totale positie qty={state_qty} "
            f"@ ~{state_entry_price} ({used_leverage}x, {config.MAX_MARGIN_PCT_OF_FUNDS}% van saldo)"
        )

    return f"qty={filled_qty} @ {entry_price} ({used_leverage}x, {config.MAX_MARGIN_PCT_OF_FUNDS}% van saldo)"


async def _handle_tp_partial_event(key: str, pos: dict, target_number: int, client=None):
    """Generieke handler voor targets 1 t/m 4 (ONGEWIJZIGDE logica t.o.v. de
    Hyperliquid-versie, zie config.py voor de incident-geschiedenis achter
    elk detail hieronder): sluit het cumulatieve percentage van de ORIGINELE
    qty dat nog niet dicht is (zie _target_close_qty). Bij target
    config.BREAKEVEN_MOVE_AFTER_TARGET wordt de SL verplaatst naar een
    dynamische break-even op basis van de ECHT gebankte winst uit eerdere
    targets."""
    symbol = pos["symbol"]
    done_key = f"tp{target_number}_done"
    if pos.get(done_key):
        log.info("TP%d-event voor %s ontvangen maar al verwerkt -- genegeerd", target_number, symbol)
        db.log_tp_event(
            symbol=symbol, event="tp_event_ignored_duplicate", detail=f"target {target_number} al verwerkt",
        )
        return

    step_size = pos["step_size"]
    tick_size = pos["tick_size"]
    exit_is_buy = not pos["is_buy"]
    cum_pct = _cumulative_target_pct(target_number)
    close_qty = _target_close_qty(pos, cum_pct)

    client = client or await _get_client()
    market = await get_market_info(symbol, client=client)
    mark_px = market["mark_px"]
    notional = close_qty * mark_px

    closed_this_step = not (close_qty <= 0 or notional < config.MIN_NOTIONAL_USD)
    remaining_qty = pos["remaining_qty"]
    banked_pnl = pos.get("banked_pnl", 0.0)

    if not closed_this_step:
        log.info(
            "TP%d-event voor %s: deel-close (%s, ~$%.2f notional) onder MIN_NOTIONAL_USD ($%.0f) "
            "-- overgeslagen, schuift door naar volgende target.",
            target_number, symbol, close_qty, notional, config.MIN_NOTIONAL_USD,
        )
        db.log_tp_event(
            symbol=symbol, event="tp_event_skipped_min_notional",
            detail=f"target {target_number}: {close_qty} (~${notional:.2f}) onder "
                   f"${config.MIN_NOTIONAL_USD:.0f}-minimum, doorgeschoven naar volgende target",
        )
    else:
        try:
            avg_px, filled_close_qty = None, None
            filled_close_qty, avg_px = await _market_close_reduce_only(symbol, exit_is_buy, close_qty, client=client)
        except RuntimeError as e:
            if _is_position_already_closed_error(e):
                await _handle_position_already_closed(key, symbol, f"target {target_number}")
                return
            raise

        step_pnl = close_qty * (avg_px - pos["entry_price"]) * (1 if pos["is_buy"] else -1)
        banked_pnl += step_pnl
        remaining_qty = _round_sz(pos["remaining_qty"] - close_qty, step_size)

    new_sl_oid = pos.get("sl_oid")
    sl_price = pos.get("sl_price")
    moves_to_breakeven = target_number == config.BREAKEVEN_MOVE_AFTER_TARGET
    if moves_to_breakeven:
        sl_oid = pos.get("sl_oid")
        if sl_oid is not None:
            try:
                await _cancel_order(client, sl_oid, "oorspronkelijke SL annuleren (break-even-shift)")
            except Exception:
                log.exception("Kon originele SL niet annuleren voor %s bij break-even-shift", symbol)
        else:
            log.warning("Geen sl_oid bekend voor %s bij break-even-shift -- plaats break-even-SL toch.", symbol)

        if remaining_qty > 0:
            original_notional = pos["qty"] * pos["entry_price"]
            safety = original_notional * (config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT / 100)
            available = max(banked_pnl - safety, 0.0)
            offset = available / remaining_qty
        else:
            offset = 0.0
        be_trigger = _round_px(
            pos["entry_price"] - offset if pos["is_buy"] else pos["entry_price"] + offset, tick_size,
        )
        be_limit = _round_px(
            be_trigger * (1 - TRIGGER_LIMIT_BUFFER_PCT if pos["is_buy"] else 1 + TRIGGER_LIMIT_BUFFER_PCT),
            tick_size,
        )
        new_sl_oid = await _place_stop_order(client, _apex_symbol(symbol), exit_is_buy, remaining_qty, be_trigger, be_limit)
        sl_price = be_trigger

    async with _state_lock:
        state = _load_state()
        if key in state:
            state[key]["remaining_qty"] = remaining_qty
            state[key][done_key] = True
            state[key]["sl_oid"] = new_sl_oid
            state[key]["sl_price"] = sl_price
            state[key]["banked_pnl"] = banked_pnl
            _save_state(state)

    close_desc = f"{close_qty} gesloten" if closed_this_step else "deel-close te klein, doorgeschoven"
    if moves_to_breakeven:
        db.log_tp_event(
            symbol=symbol, event=f"tp{target_number}_event_be_moved",
            detail=f"target {target_number} groep-bericht: {close_desc}, SL -> break-even (~${banked_pnl:.2f} "
                   f"gedekt uit eerdere targets) ({sl_price}) voor resterende {remaining_qty}",
        )
        await _notify(
            f"🎯 TP{target_number} (groep-bericht): {symbol} — {close_desc}, SL naar break-even "
            f"(~${banked_pnl:.2f} gedekt uit eerdere targets, {sl_price}) voor resterende {remaining_qty}"
        )
    elif closed_this_step:
        db.log_tp_event(
            symbol=symbol, event=f"tp{target_number}_event_closed",
            detail=f"{close_qty} gesloten (target {target_number} groep-bericht), resterende {remaining_qty}",
        )
        await _notify(
            f"🎯 TP{target_number} (groep-bericht): {symbol} — {close_qty} gesloten, resterende {remaining_qty}"
        )
    else:
        await _notify(
            f"⏭️ TP{target_number} (groep-bericht): {symbol} — deel-close te klein "
            f"(~${notional:.2f}), doorgeschoven naar volgende target"
        )


async def _handle_tp1_event(key: str, pos: dict, client=None):
    await _handle_tp_partial_event(key, pos, target_number=1, client=client)


async def _handle_tp2_event(key: str, pos: dict, client=None):
    await _handle_tp_partial_event(key, pos, target_number=2, client=client)


async def _handle_tp3_event(key: str, pos: dict, client=None):
    await _handle_tp_partial_event(key, pos, target_number=3, client=client)


async def _handle_tp4_event(key: str, pos: dict, client=None):
    await _handle_tp_partial_event(key, pos, target_number=4, client=client)


async def _handle_tp5_event(key: str, pos: dict, client=None):
    """Finale exit: sluit ALTIJD de volledige resterende qty (100%), ongeacht
    MIN_NOTIONAL_USD of afrondingsverschillen."""
    symbol = pos["symbol"]
    exit_is_buy = not pos["is_buy"]
    close_qty = pos["remaining_qty"]

    client = client or await _get_client()

    if close_qty <= 0:
        log.warning("TP5-event voor %s ontvangen maar remaining_qty is 0 -- state opgeschoond.", symbol)
        db.log_tp_event(symbol=symbol, event="tp_event_ignored_zero_remaining", detail="target 5, remaining_qty=0")
    else:
        sl_oid = pos.get("sl_oid")
        if sl_oid is not None:
            try:
                await _cancel_order(client, sl_oid, "SL annuleren (TP5-event)")
            except Exception:
                log.exception("Kon SL niet annuleren voor %s bij TP5-event", symbol)

        try:
            await _market_close_reduce_only(symbol, exit_is_buy, close_qty, client=client)
        except RuntimeError as e:
            if _is_position_already_closed_error(e):
                await _handle_position_already_closed(key, symbol, "target 5")
                return
            raise

        db.log_tp_event(
            symbol=symbol, event="tp5_event_closed",
            detail=f"resterende {close_qty} gesloten (target 5 groep-bericht), positie klaar",
        )
        await _notify(f"🏁 TP5 (groep-bericht): {symbol} — resterende {close_qty} gesloten, positie klaar")

    async with _state_lock:
        state = _load_state()
        state.pop(key, None)
        _save_state(state)


async def handle_cancel_event(cancel_event, client=None):
    """
    Verwerkt een CancelEvent (signal_parser.parse_cancel_event) -- ONGEWIJZIGDE
    logica t.o.v. de Hyperliquid-versie (incident 2026-08-15: kandidaat-selectie
    EN het claimen ervan via _cancel_in_progress gebeuren in dezelfde
    _state_lock-sectie, zie die versie's docstring voor het volledige incident).
    """
    base_symbol = cancel_event.symbol.replace("USDT", "")

    async with _state_lock:
        state = _load_state()

        candidates = [
            (key, pos) for key, pos in state.items()
            if pos.get("symbol") == base_symbol and pos.get("version") == "v2"
        ]

        if not candidates:
            log.info("Cancel-event voor %s ontvangen maar geen open v2-positie -- genegeerd.", base_symbol)
            db.log_tp_event(
                symbol=base_symbol, event="cancel_event_ignored_no_position", detail="geen open v2-positie",
            )
            return

        if len(candidates) > 1:
            log.warning(
                "Meerdere open v2-posities gevonden voor %s bij cancel-event -- genegeerd, handmatig checken.",
                base_symbol,
            )
            db.log_tp_event(
                symbol=base_symbol, event="cancel_event_ignored_ambiguous",
                detail=f"kandidaten: {[k for k, _ in candidates]}",
            )
            await _notify(
                f"⚠️ Cancel-event voor {base_symbol} genegeerd: meerdere open posities "
                f"({[k for k, _ in candidates]}) -- kan niet automatisch bepalen welke. "
                f"Handmatig checken op ApeX Omni."
            )
            return

        key, pos = candidates[0]

        if key in _cancel_in_progress:
            log.info(
                "Cancel-event voor %s wordt al verwerkt (bijna-gelijktijdig duplicaat) -- genegeerd.",
                base_symbol,
            )
            db.log_tp_event(
                symbol=base_symbol, event="cancel_event_ignored_in_progress", detail=f"key={key}",
            )
            return

        if config.DRY_RUN:
            log.warning(
                "DRY_RUN staat aan maar er staat een v2-positie voor %s in state -- dit zou niet "
                "moeten kunnen. Geen echte cancel/order-calls, alleen loggen (cancel-event).",
                base_symbol,
            )
            return

        _cancel_in_progress.add(key)

    try:
        symbol = pos["symbol"]
        exit_is_buy = not pos["is_buy"]
        close_qty = pos["remaining_qty"]

        client = client or await _get_client()

        sl_oid = pos.get("sl_oid")
        if sl_oid is not None:
            try:
                await _cancel_order(client, sl_oid, "SL annuleren (cancel-event)")
            except Exception:
                log.exception("Kon SL niet annuleren voor %s bij cancel-event", symbol)

        if close_qty > 0:
            try:
                await _market_close_reduce_only(symbol, exit_is_buy, close_qty, client=client)
            except RuntimeError as e:
                if _is_position_already_closed_error(e):
                    await _handle_position_already_closed(key, symbol, "cancel-event")
                    return
                raise

        db.log_tp_event(
            symbol=symbol, event="cancel_event_closed",
            detail=f"resterende {close_qty} gesloten (cancel-bericht van het kanaal), positie klaar",
        )
        await _notify(f"🚫 Cancel (kanaal): {symbol} — resterende {close_qty} gesloten, positie klaar")

        async with _state_lock:
            state = _load_state()
            state.pop(key, None)
            _save_state(state)
    finally:
        async with _state_lock:
            _cancel_in_progress.discard(key)


async def handle_tp_event(tp_event, client=None):
    """
    Verwerkt een TPEvent (signal_parser.parse_tp_event) voor een v2-positie --
    ONGEWIJZIGDE logica t.o.v. de Hyperliquid-versie."""
    base_symbol = tp_event.symbol.replace("USDT", "")

    async with _state_lock:
        state = _load_state()

    candidates = [
        (key, pos) for key, pos in state.items()
        if pos.get("symbol") == base_symbol and pos.get("version") == "v2"
    ]

    if not candidates:
        log.info(
            "TP-event %s target %d ontvangen maar geen open v2-positie voor %s -- genegeerd.",
            base_symbol, tp_event.target_number, base_symbol,
        )
        db.log_tp_event(
            symbol=base_symbol, event="tp_event_ignored_no_position",
            detail=f"target {tp_event.target_number}, geen open v2-positie",
        )
        return

    if len(candidates) > 1:
        log.warning(
            "Meerdere open v2-posities gevonden voor %s bij TP-event target %d -- genegeerd, handmatig checken.",
            base_symbol, tp_event.target_number,
        )
        db.log_tp_event(
            symbol=base_symbol, event="tp_event_ignored_ambiguous",
            detail=f"target {tp_event.target_number}, kandidaten: {[k for k, _ in candidates]}",
        )
        await _notify(
            f"⚠️ TP{tp_event.target_number}-event voor {base_symbol} genegeerd: meerdere open "
            f"posities ({[k for k, _ in candidates]}) -- kan niet automatisch bepalen welke. "
            f"Handmatig checken op ApeX Omni."
        )
        return

    key, pos = candidates[0]

    if config.DRY_RUN:
        log.warning(
            "DRY_RUN staat aan maar er staat een v2-positie voor %s in state -- dit zou niet "
            "moeten kunnen. Geen echte cancel/order-calls, alleen loggen (target %d).",
            base_symbol, tp_event.target_number,
        )
        return

    if tp_event.target_number == 1:
        await _handle_tp1_event(key, pos, client=client)
    elif tp_event.target_number == 2:
        await _handle_tp2_event(key, pos, client=client)
    elif tp_event.target_number == 3:
        await _handle_tp3_event(key, pos, client=client)
    elif tp_event.target_number == 4:
        await _handle_tp4_event(key, pos, client=client)
    elif tp_event.target_number == 5:
        await _handle_tp5_event(key, pos, client=client)
    else:
        log.info(
            "TP-event %s target %d ontvangen, buiten de bekende ladder (1-5) -- geen actie.",
            base_symbol, tp_event.target_number,
        )
        db.log_tp_event(
            symbol=base_symbol, event="tp_event_no_action", detail=f"target {tp_event.target_number}",
        )


async def _estimate_realized_pnl_since(symbol: str, opened_at, client=None) -> Optional[float]:
    """Best-effort schatting van de gerealiseerde PnL sinds `opened_at` voor
    `symbol`, uit historical_pnl_v3. Puur informatief (geen boekhoudkundige
    garantie), dus faalt stil (None) i.p.v. de aanroeper te laten crashen.

    LET OP: het exacte veldenschema van historicalPnl-items is NIET
    rechtstreeks bevestigd (het testaccount had geen trade-historie om tegen
    te checken) -- vandaar de brede try/except hieronder, die dit bewust naar
    "onbekend" laat degraderen i.p.v. te crashen als een aanname mis blijkt."""
    client = client or await _get_client()
    try:
        apex_symbol = _apex_symbol(symbol)
        opened_at_ms = int(opened_at * 1000)
        if not opened_at_ms:
            return None
        resp = await _call(client.historical_pnl_v3, symbol=apex_symbol, limit=200)
        data = _check_order_status(resp, "historical PnL opvragen")
        entries = data.get("historicalPnl") or []
        relevant = [e for e in entries if int(e.get("createdAt", 0) or 0) >= opened_at_ms]
        return sum(float(e.get("realizedPnl", 0) or 0) for e in relevant)
    except Exception:
        log.exception("Kon gerealiseerde PnL niet ophalen sinds entry voor %s", symbol)
        return None


async def reconcile_positions(client=None):
    """
    Achtergrondtaak (zie main.py): vergelijkt periodiek de lokale v2-state
    met de ECHTE open posities op ApeX Omni -- ONGEWIJZIGDE logica t.o.v. de
    Hyperliquid-versie (zie config.POSITION_RECONCILE_INTERVAL_SECONDS voor
    de incident-geschiedenis)."""
    async with _state_lock:
        state = _load_state()
    v2_keys = [k for k, pos in state.items() if pos.get("version") == "v2"]
    if not v2_keys:
        return

    client = client or await _get_client()
    live_positions = await _fetch_live_positions(client)

    for key in v2_keys:
        async with _state_lock:
            state = _load_state()
            pos = state.get(key)
        if pos is None:
            continue

        symbol = pos["symbol"]
        is_buy = pos["is_buy"]
        live = live_positions.get(symbol)
        if live is not None and live["is_buy"] == is_buy:
            continue  # nog echt open op ApeX Omni, niks te doen

        realized_pnl = await _estimate_realized_pnl_since(symbol, pos.get("opened_at", 0), client)

        async with _state_lock:
            state = _load_state()
            state.pop(key, None)
            _save_state(state)

        pnl_txt = f"~${realized_pnl:.2f}" if realized_pnl is not None else "onbekend"
        log.warning(
            "Reconciliatie: %s stond niet meer open op ApeX Omni maar wel nog in lokale state "
            "-- waarschijnlijk buiten de bot om gesloten (bv. resting SL). State opgeschoond, "
            "gerealiseerde PnL sinds entry: %s.",
            key, pnl_txt,
        )
        db.log_tp_event(
            symbol=symbol, event="position_closed_externally_detected",
            detail=f"key={key}, niet meer open op ApeX Omni, state opgeschoond, PnL sinds entry: {pnl_txt}",
        )
        await _notify(
            f"⚠️ {symbol} ({'Buy' if is_buy else 'Sell'}) bleek al gesloten op ApeX Omni "
            f"(waarschijnlijk SL geraakt) zonder dat de bot dit doorhad -- pas nu bij reconciliatie "
            f"ontdekt. Gerealiseerde PnL sinds entry: {pnl_txt}. Lokale state opgeschoond."
        )


async def execute_signal(signal: Signal):
    return await place_entry_order(signal, dry_run=config.DRY_RUN)
