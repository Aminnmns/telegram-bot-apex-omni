"""
Voert signals uit op Hyperliquid (perps) met de EVM-private key van de
Phantom-wallet's ingebouwde "Perps"-account (zelfde seed phrase als de
Solana-wallet, apart 0x-adres -- zie config.HYPERLIQUID_PRIVATE_KEY).

OVERSTAP VAN FLASH TRADE NAAR HYPERLIQUID (2026): Flash Trade gaf herhaaldelijk
on-chain problemen tijdens setup (vijf losse issues) en heeft geen Python-SDK
-- elke transactie moest zelf als ruwe Solana-tx gebouwd en gesigned worden.
Hyperliquid heeft een officieel onderhouden Python-SDK
(hyperliquid-dex/hyperliquid-python-sdk, pip install hyperliquid-python-sdk)
met directe order()/update_leverage()-calls en ingebouwde TP/SL-trigger-orders
-- geen ruwe transacties meer nodig. check_flash_setup.py/setup_flash_account.py/
debug_flash_tx.py blijven ongewijzigd staan als losse Flash-tooling (bv. om
het oude $5-saldo daar ooit terug te trekken via /transaction-builder/withdraw).

BELANGRIJK: dit draait rechtstreeks tegen MAINNET (config.HYPERLIQUID_ENV),
niet tegen testnet -- bewuste keuze om het bestaande Phantom Perps-saldo
(~$5-6) direct te hergebruiken. DRY_RUN blijft daarom de enige veiligheidsklep
voor deze volledig herschreven executor: laat 'm op True tot je de logs hebt
gecontroleerd.

Architectuur:
- Markets/coins worden geïdentificeerd met hun symbol-string (bv. "TAO"),
  live opgevraagd via info.meta_and_asset_ctxs() i.p.v. een lokale lijst die
  kan verouderen (zelfde aanpak als voorheen bij Flash).
- De Hyperliquid Python-SDK (Exchange/Info) is SYNCHROON (gebruikt de
  `requests`-library, geen async). Alle calls lopen daarom via
  asyncio.to_thread(...) zodat ze de Telegram-event-loop niet blokkeren.
- Entry = market_open() (agressieve IOC-limit-order, SDK regelt zelf de
  prijsafronding).

EXIT-STRATEGIE (v2, sinds 2026-08-11, geldt voor alle posities): qty wordt
direct afgeleid van config.MAX_MARGIN_PCT_OF_FUNDS% van het beschikbare
saldo (i.p.v. de oude MAX_RISK_USD-aanpak, zie config.MAX_MARGIN_PCT_OF_FUNDS's
docstring voor waarom -- kort samengevat: een vast dollarrisico onafhankelijk
van leverage kon bij een lage toegestane leverage per coin een veel te groot
deel van een klein account aan margin opeisen). Bij entry wordt ALLEEN een SL
geplaatst (volle qty, onafhankelijk actief op Hyperliquid tussen entry en het
eerste TP-bericht) -- geen TP-trigger-order, want die bleek onbetrouwbaar te
detecteren of een fill echt was (zie het PENGU-incident van 2026-08-11: een
TP1-order verdween uit frontend_open_orders zonder een bijbehorende fill,
waardoor de oude break-even-SL een verkeerde qty kreeg en een deel van de
positie onbeschermd bleef). Sluiten gebeurt expliciet op basis van de groep's
eigen "Take-Profit target N ✅"-berichten (signal_parser.parse_tp_event): een
5-staps ladder over de ORIGINELE qty (target 1 t/m 4 elk hun eigen
TP_EVENT_TARGETN_CLOSE_PCT%, target 5 altijd de volledige rest), waarbij
target 1 ook de SL naar break-even verplaatst. Elke stap is cumulatief
berekend (hoeveel had er tot-en-met deze target dicht moeten zijn, minus wat
er al dicht is) i.p.v. een onafhankelijk percentage per stap -- zo schuift een
deel-close die onder MIN_NOTIONAL_USD zou vallen (klein account) automatisch
door naar de eerstvolgende target die wel boven de grens uitkomt, met target
5 als uiteindelijke vangnet-fallback, zonder aparte "carry"-state nodig te
hebben. Elke state-entry heeft een "version":
"v2"-veld -- overblijfsel van een eerdere, inmiddels volledig uitgefaseerde
legacy-strategie (trigger-order-polling); nu altijd "v2", maar de check bleef
staan als goedkope garde tegen een onverwacht ander state-schema.
"""
import asyncio
import json
import logging
import math
import os
import time
from typing import Optional

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

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
# (2026-08-10) toen een test zonder deze isolatie een positie-entry in het
# gedeelde bestand achterliet, die de live service vervolgens oppikte en er
# (met de ECHTE exchange) actie op ondernam.
STATE_FILE = os.getenv("STATE_FILE_PATH") or os.path.join(os.path.dirname(__file__), "open_positions.json")
_state_lock = asyncio.Lock()
# Zie de duplicaat-guard in place_entry_order(): een echt duplicaat-signal
# komt (bijna) gelijktijdig binnen, dus 60s is ruim voldoende marge zonder
# latere, echt nieuwe signalen voor hetzelfde symbol+side te blokkeren.
DUPLICATE_POSITION_WINDOW_SECONDS = 60
# State-keys waarvoor handle_cancel_event() op dit moment een SL-cancel/close
# aan het uitvoeren is -- voorkomt dat een (bijna) gelijktijdig binnenkomend
# duplicaat-cancelbericht dezelfde positie nogmaals probeert te sluiten
# voordat de eerste klaar is en de state-pop heeft gedaan (zie
# handle_cancel_event() voor het geobserveerde incident).
_cancel_in_progress: set[str] = set()

# Hyperliquid-regel (zelf geverifieerd, geen aanname): prijzen mogen max. 5
# significante cijfers hebben, en max. (6 - szDecimals) decimalen voor perps.
PERP_MAX_DECIMALS = 6

# Marge tussen triggerPx en de limit_px van een reduce-only trigger-order
# (isMarket=True), zodat de resulterende IOC-order ook echt kan vullen als de
# markt net over de trigger heen schiet.
TRIGGER_LIMIT_BUFFER_PCT = 0.03

_API_URL = constants.MAINNET_API_URL if config.HYPERLIQUID_ENV == "mainnet" else constants.TESTNET_API_URL

_wallet = None
_exchange = None
_info = None


def _get_wallet():
    global _wallet
    if _wallet is None:
        _wallet = Account.from_key(config.HYPERLIQUID_PRIVATE_KEY)
    return _wallet


def get_owner_address() -> str:
    return config.HYPERLIQUID_ACCOUNT_ADDRESS or _get_wallet().address


def _get_exchange() -> Exchange:
    global _exchange
    if _exchange is None:
        _exchange = Exchange(
            _get_wallet(), _API_URL, account_address=config.HYPERLIQUID_ACCOUNT_ADDRESS or None,
            timeout=config.HYPERLIQUID_API_TIMEOUT_SECONDS,
        )
    return _exchange


def _get_info() -> Info:
    global _info
    if _info is None:
        _info = Info(_API_URL, skip_ws=True, timeout=config.HYPERLIQUID_API_TIMEOUT_SECONDS)
    return _info


async def _call(fn, *args, **kwargs):
    """De Hyperliquid-SDK is synchroon (requests-library) -- via to_thread()
    zodat een trage/hangende HTTP-call de Telegram-listener niet blokkeert."""
    return await asyncio.to_thread(fn, *args, **kwargs)


def _round_px(px: float, sz_decimals: int) -> float:
    """5 significante cijfers, max (6 - szDecimals) decimalen -- Hyperliquid's
    eigen regel (zelf geverifieerd via hun docs + de SDK's _slippage_price)."""
    sig_figs = float(f"{px:.5g}")
    return round(sig_figs, max(0, PERP_MAX_DECIMALS - sz_decimals))


def _round_sz(sz: float, sz_decimals: int) -> float:
    return round(sz, sz_decimals)


async def get_market_info(symbol: str, info=None) -> dict:
    """
    Haalt max leverage, szDecimals en de actuele markprijs live op uit
    /info (metaAndAssetCtxs) i.p.v. een handmatige lijst te vertrouwen --
    zelfde redenering als voorheen bij Flash Trade. Raiset ValueError als de
    coin niet (meer) bestaat op Hyperliquid, zodat de trade wordt
    overgeslagen i.p.v. blind te traden.

    `info` is optioneel injecteerbaar (i.p.v. via de module-singleton
    _get_info()) zodat tests een fake object kunnen doorgeven zonder op
    monkeypatching van module-globals te hoeven vertrouwen.
    """
    info = info or _get_info()
    meta, ctxs = await _call(info.meta_and_asset_ctxs)
    for idx, asset in enumerate(meta["universe"]):
        if asset["name"].upper() == symbol.upper() and not asset.get("isDelisted"):
            return {
                "max_leverage": int(asset["maxLeverage"]),
                "sz_decimals": int(asset["szDecimals"]),
                "mark_px": float(ctxs[idx]["markPx"]),
            }

    raise ValueError(
        f"Geen Hyperliquid market gevonden voor {symbol}. Check handmatig op "
        f"https://app.hyperliquid.xyz of deze coin daar (nog) verhandelbaar is."
    )


async def count_open_positions(info=None, owner=None) -> int:
    """
    Telt open live posities via Hyperliquid's EIGEN clearinghouseState --
    bewust niet via de lokale open_positions.json, want die kan uit sync
    raken met wat er werkelijk op de exchange staat (zie het incident van
    2026-08-10: een lokaal state-bestand met foutieve entries leidde al
    eens tot verkeerd gedrag). `info`/`owner` injecteerbaar voor tests.
    """
    info = info or _get_info()
    owner = owner or get_owner_address()
    state = await _call(info.user_state, owner)
    return len(state.get("assetPositions", []))


def _parse_live_positions(perp_state: dict) -> dict:
    """coin -> {"qty": abs(szi), "is_buy": szi > 0, "entry_price": entryPx}
    voor elke coin met een open positie in deze clearinghouseState-snapshot.
    Gedeeld door _get_live_position() en reconcile_positions() zodat er maar
    één plek is die assetPositions/szi-teken interpreteert."""
    result = {}
    for ap in perp_state.get("assetPositions", []):
        p = ap.get("position", {})
        coin = p.get("coin")
        szi = float(p.get("szi", 0) or 0)
        if coin and szi != 0:
            result[coin] = {"qty": abs(szi), "is_buy": szi > 0, "entry_price": float(p.get("entryPx", 0) or 0)}
    return result


async def _get_live_position(base_symbol: str, owner: str, info=None) -> Optional[dict]:
    """Haalt de ECHTE huidige netto positie voor base_symbol op bij
    Hyperliquid zelf (zie _parse_live_positions), of None als er geen open
    positie is. Gebruikt bij een same-side re-entry (zie place_entry_order)
    om de nieuwe, samengevoegde qty/entry-prijs autoritatief te bepalen
    i.p.v. zelf op te tellen/te wegen -- Hyperliquid kent maar één netto
    positie per coin (geen hedge-mode): een nieuwe order in dezelfde
    richting wordt door de exchange zelf al samengevoegd met de bestaande
    positie (inclusief eigen avgPx-berekening, die met funding/afronding kan
    afwijken van een simpele lokale herberekening)."""
    info = info or _get_info()
    state = await _call(info.user_state, owner)
    return _parse_live_positions(state).get(base_symbol)


async def _fetch_funds_state(info=None, owner=None):
    """Eén gedeelde fetch van perps- en spot-state, zodat get_withdrawable()
    en get_total_equity() niet allebei apart user_state()/spot_user_state()
    hoeven aan te roepen -- voorkomt dubbele API-calls én een mogelijk
    inconsistente snapshot (het ene getal net iets ouder dan het andere) als
    beide na elkaar in dezelfde trade nodig zijn (zie place_entry_order)."""
    info = info or _get_info()
    owner = owner or get_owner_address()
    perp_state = await _call(info.user_state, owner)
    spot_state = await _call(info.spot_user_state, owner)
    return perp_state, spot_state


def _withdrawable_from_state(perp_state: dict, spot_state: dict) -> float:
    """Beschikbare marge voor een NIEUWE positie: perps-`withdrawable` +
    vrije spot-USDC.

    Zelf empirisch geverifieerd (2026-08-11, geen aanname): clearinghouseState
    ("perps") toonde withdrawable=$0.00, terwijl er $17.67 vrije spot-USDC
    stond (spot_user_state's tokenToAvailableAfterMaintenance). Een kleine,
    niet-vulbare test-order (ALO, ver van de markt, meteen geannuleerd) die
    meer marge vereiste dan de $0.00 perps-withdrawable werd door Hyperliquid
    gewoon GEACCEPTEERD -- de matching-engine trekt bij het OPENEN van een
    nieuwe positie kennelijk automatisch op vrije spot-USDC, ook al rapporteert
    clearinghouseState.withdrawable dat niet mee (dat veld lijkt specifiek
    "wat kan ik nu naar mijn externe wallet overmaken" te betekenen, niet
    "hoeveel marge kan ik gebruiken om een nieuwe positie te openen"). Alleen
    naar clearinghouseState.withdrawable kijken is dus te conservatief en
    blokkeert onterecht v2-trades die Hyperliquid wel zou accepteren."""
    perp_withdrawable = float(perp_state.get("withdrawable", 0))

    usdc_token_id = None
    for bal in spot_state.get("balances", []):
        if bal.get("coin") == "USDC":
            usdc_token_id = bal.get("token")
            break

    spot_free_usdc = 0.0
    if usdc_token_id is not None:
        for token_id, available in spot_state.get("tokenToAvailableAfterMaintenance", []):
            if token_id == usdc_token_id:
                spot_free_usdc = float(available)
                break

    return perp_withdrawable + spot_free_usdc


def _total_equity_from_state(perp_state: dict, spot_state: dict) -> float:
    """Totale accountwaarde als basis voor MAX_MARGIN_PCT_OF_FUNDS%: perps
    accountValue (incl. marge die al in andere posities vastzit) + totale
    spot-USDC (incl. wat er 'on hold' staat).

    Bewust ANDERS dan _withdrawable_from_state() (dat alleen NU vrij
    beschikbare marge teruggeeft): als de qty-berekening op withdrawable zou
    blijven steunen, wordt elke volgende trade binnen dezelfde cyclus van
    meerdere gelijktijdig openende posities kleiner, omdat withdrawable
    slinkt naarmate er meer marge vastgezet wordt. Door op de TOTALE
    accountwaarde te mikken blijft het bedrag per trade constant; of een
    trade daadwerkelijk past wordt apart gecheckt tegen
    _withdrawable_from_state() (zie place_entry_order)."""
    perp_account_value = float(perp_state.get("marginSummary", {}).get("accountValue", 0))

    spot_total_usdc = 0.0
    for bal in spot_state.get("balances", []):
        if bal.get("coin") == "USDC":
            spot_total_usdc = float(bal.get("total", 0))
            break

    return perp_account_value + spot_total_usdc


async def get_withdrawable(info=None, owner=None) -> float:
    """Publieke wrapper rond _withdrawable_from_state() voor aanroepers die
    alléén deze waarde nodig hebben (bv. tests, dashboard). `info`/`owner`
    injecteerbaar voor tests, zelfde reden als count_open_positions()."""
    perp_state, spot_state = await _fetch_funds_state(info, owner)
    return _withdrawable_from_state(perp_state, spot_state)


async def get_total_equity(info=None, owner=None) -> float:
    """Publieke wrapper rond _total_equity_from_state(). `info`/`owner`
    injecteerbaar voor tests, zelfde reden als count_open_positions()."""
    perp_state, spot_state = await _fetch_funds_state(info, owner)
    return _total_equity_from_state(perp_state, spot_state)


def _calc_margin_based_qty(entry_px: float, leverage: int, available_funds: float, sz_decimals: int) -> float:
    """qty zo dat de benodigde margin exact MAX_MARGIN_PCT_OF_FUNDS% van
    available_funds is: margin = available_funds * pct/100, notional =
    margin * leverage, qty = notional / entry_px. Naar BENEDEN afgerond
    (niet _round_sz's normale afronding) zodat de werkelijke margin na
    afronding nooit boven de cap uitkomt -- normaal afronden kan naar boven
    afronden en daarmee de cap net overschrijden."""
    margin_to_use = available_funds * (config.MAX_MARGIN_PCT_OF_FUNDS / 100)
    raw_qty = (margin_to_use * leverage) / entry_px
    factor = 10 ** sz_decimals
    return math.floor(raw_qty * factor) / factor


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


_POSITION_ALREADY_CLOSED_MARKERS = (
    "Reduce only order would increase position",
    "Order was never placed, already canceled, or filled",
)


def _is_position_already_closed_error(exc: Exception) -> bool:
    """True als deze Hyperliquid-foutmelding erop wijst dat de positie (of de
    bijbehorende SL-order) al buiten de bot om gesloten/gecanceld is -- bv.
    doordat een eerder geplaatste break-even-SL intussen is getriggerd.
    Geobserveerd 2026-08-25: PENGU/BTC/HYPE crashten hierop bij latere
    TP-events omdat de lokale state (remaining_qty > 0) niet meer klopte met
    de werkelijke, inmiddels lege positie op de exchange."""
    return any(marker in str(exc) for marker in _POSITION_ALREADY_CLOSED_MARKERS)


def _check_order_status(resp: dict, action: str):
    """Hyperliquid geeft HTTP 200 terug voor zowel geslaagde als afgekeurde
    orders -- de echte fout zit in de JSON zelf (status != 'ok', of een
    status-item van het type 'error')."""
    if resp.get("status") != "ok":
        raise RuntimeError(f"Hyperliquid wees {action} af: {resp}")
    statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
    for status in statuses:
        if isinstance(status, dict) and "error" in status:
            raise RuntimeError(f"Hyperliquid order-fout bij {action}: {status['error']}")
    return statuses


async def place_entry_order(signal: Signal, dry_run: bool = False, exchange=None, info=None):
    """
    v2-entry (geldt voor alle NIEUWE signals): qty wordt direct afgeleid van
    config.MAX_MARGIN_PCT_OF_FUNDS% van het op dat moment beschikbare saldo
    (zie _calc_margin_based_qty) -- dus de margin die een trade kost staat
    vooraf vast als percentage, ongeacht welke leverage Hyperliquid voor die
    specifieke coin toestaat. Alleen een SL bij entry -- geen TP-trigger-order
    meer (zie moduledocstring). `exchange`/`info` zijn optioneel
    injecteerbaar zodat tests fakes kunnen doorgeven als expliciete
    functie-argumenten, i.p.v. te vertrouwen op monkeypatching van
    _get_exchange()/_get_info() (die aanpak faalde eerder op een manier die
    niet met zekerheid herleid kon worden -- dependency injection maakt dat
    hele faalpad onmogelijk)."""
    base_symbol = signal.symbol.replace("USDT", "")

    # Veiligheidsnet tegen duplicaat-signals van het kanaal (incident
    # 2026-08-14: exact dezelfde HYPE-entry kwam 2x binnen binnen 5s, wat 2
    # losse orders plaatste; de tweede overschreef de v2-state van de eerste
    # -- diens sl_oid/remaining_qty raakten kwijt, dus toen de cancel later
    # binnenkwam sloot die alleen de (getrackte) tweede order en bleef de
    # eerste ongezien open staan op Hyperliquid tot 'ie handmatig gesloten
    # werd). Zo'n duplicaat komt vrijwel nooit voor, en als het gebeurt gaat
    # het om (bijna-)gelijktijdige berichten -- daarom alleen als duplicaat
    # behandelen als de vorige v2-positie voor dit symbol+side hooguit
    # DUPLICATE_POSITION_WINDOW_SECONDS geleden geopend is. Een later signaal
    # (uren/dagen na de vorige positie) is een echt nieuw signaal en moet
    # gewoon uitgevoerd worden, ook al staat de vorige positie nog open.
    async with _state_lock:
        state = _load_state()
    existing_key = f"{base_symbol}:{signal.side}"
    existing = state.get(existing_key, {})
    existing_age = time.time() - existing.get("opened_at", 0)

    # Same-side re-entry (positie al open, ouder dan het duplicaat-venster --
    # dus WEL uitvoeren, zie de reasoning hierboven). Hyperliquid kent maar
    # één netto positie per coin (geen hedge-mode): deze nieuwe order voegt
    # zich op de exchange zelf al samen met de bestaande positie. Vroeger
    # overschreef place_entry_order() de state daarna gewoon met ALLEEN de
    # nieuwe order-gegevens (qty = alleen de nieuwe fill, sl_oid van de oude
    # positie kwijt) -- exact het 2026-08-14-incident hierboven, maar dan
    # getriggerd door een late, legitieme re-entry i.p.v. een near-duplicate
    # bericht. is_reentry hieronder zorgt dat de oude SL netjes wordt
    # geannuleerd en de nieuwe state de ECHTE (samengevoegde) positiegrootte
    # van Hyperliquid zelf gebruikt i.p.v. zelf te herberekenen.
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

    # Incident 2026-09-08: PENGU:Buy bleef in state staan nadat zijn SL
    # buiten de bot om triggerde (resting SL, geen Telegram-event); de
    # daaropvolgende PENGU:Sell-entry werd gewoon geplaatst zonder ooit te
    # checken of er nog een v2-record voor de TEGENOVERGESTELDE kant openstond
    # -- de same-side check hierboven kijkt alleen naar exact dezelfde kant.
    # Gevolg: twee "open" v2-records voor dezelfde coin in state, waardoor
    # elk later TP/cancel-event voor die coin als ambigu werd genegeerd (zie
    # handle_tp_event()) en 4 TP-targets nooit werden uitgevoerd op de
    # echt-nog-open Sell-positie. Hyperliquid kent geen hedge-mode (maar één
    # netto positie per coin), dus vóór een nieuwe entry altijd verifiëren of
    # een tegengesteld v2-record nog ECHT open staat.
    opposite_side = "Sell" if signal.side == "Buy" else "Buy"
    opposite_key = f"{base_symbol}:{opposite_side}"
    async with _state_lock:
        state = _load_state()
        opposite = state.get(opposite_key)
    # live_open_count wordt hieronder meegegeven aan de max-posities-check
    # zodat die niet nog een keer apart user_state() hoeft op te vragen --
    # zelfde snapshot, één /info-call in plaats van twee.
    live_open_count = None
    if opposite is not None and opposite.get("version") == "v2":
        perp_state = await _call((info or _get_info()).user_state, get_owner_address())
        live = _parse_live_positions(perp_state).get(base_symbol)
        live_open_count = len(perp_state.get("assetPositions", []))
        if live is not None and live["is_buy"] == opposite.get("is_buy"):
            # Nog echt open op Hyperliquid -- een nieuwe order in de andere
            # richting zou hier zelf tegenin netten/flippen op de exchange,
            # wat twee losse volledige state-records niet meer correct kunnen
            # weergeven. Niet automatisch plaatsen, handmatig laten beslissen.
            log.warning(
                "Tegengestelde v2-positie (%s) nog open op Hyperliquid bij nieuw %s-signaal voor %s "
                "-- overgeslagen, handmatig checken (zou netten/flippen op de exchange).",
                opposite_key, signal.side, signal.symbol,
            )
            db.log_order(
                symbol=signal.symbol, side=signal.side, dry_run=dry_run,
                status="skipped_opposite_position_open", leverage=signal.leverage, stop_loss=signal.stop_loss,
                error=f"tegengestelde v2-positie {opposite_key} nog open op Hyperliquid",
            )
            await _notify(
                f"⚠️ {signal.side} {signal.symbol} overgeslagen: tegengestelde positie ({opposite_key}) "
                f"staat nog open op Hyperliquid. Zou netten/flippen op de exchange -- handmatig checken."
            )
            return None
        # Niet meer echt open -- buiten de bot om gesloten (bv. resting SL).
        # Stale record opruimen zodat de nieuwe entry hieronder een schone
        # lei heeft.
        log.warning(
            "Tegengestelde v2-positie %s bleek niet meer open op Hyperliquid bij nieuw %s-signaal "
            "voor %s -- stale state opgeruimd.",
            opposite_key, signal.side, signal.symbol,
        )
        # Zelfde PnL-schatting als reconcile_positions() gebruikt voor exact
        # dit soort "buiten de bot om gesloten"-detectie -- zodat de melding
        # niet verschilt afhankelijk van welke van de twee het als eerste
        # opmerkt (zie _estimate_realized_pnl_since).
        realized_pnl = await _estimate_realized_pnl_since(
            base_symbol, opposite.get("opened_at", 0), get_owner_address(), info or _get_info()
        )
        pnl_txt = f"~${realized_pnl:.2f}" if realized_pnl is not None else "onbekend"
        async with _state_lock:
            state = _load_state()
            state.pop(opposite_key, None)
            _save_state(state)
        db.log_tp_event(
            symbol=base_symbol, event="position_closed_externally_detected",
            detail=f"key={opposite_key}, niet meer open op Hyperliquid bij nieuw tegengesteld signaal, "
                   f"state opgeruimd, PnL sinds entry: {pnl_txt}",
        )
        await _notify(
            f"⚠️ {opposite_key} bleek al gesloten op Hyperliquid (ontdekt bij nieuw tegengesteld "
            f"{signal.side}-signaal voor {signal.symbol}) -- state opgeruimd. Gerealiseerde PnL sinds "
            f"entry: {pnl_txt}."
        )

    open_count = live_open_count if live_open_count is not None else await count_open_positions(info=info)
    if open_count >= config.MAX_CONCURRENT_POSITIONS:
        log.warning(
            "Gemiste trade wegens max posities: %s %s overgeslagen (%d/%d open live posities op Hyperliquid).",
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
        market = await get_market_info(base_symbol, info=info)
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
            "Signal vroeg %sx, Hyperliquid staat max %sx toe voor %s -> gebruik %sx",
            signal.leverage, market["max_leverage"], signal.symbol, used_leverage,
        )

    sz_decimals = market["sz_decimals"]
    is_buy = signal.side == "Buy"
    exit_is_buy = not is_buy

    # Eén gedeelde state-fetch voor zowel withdrawable als total_equity (zie
    # _fetch_funds_state) -- zelfde aantal API-calls als voorheen, en beide
    # getallen komen uit exact dezelfde snapshot.
    perp_state, spot_state = await _fetch_funds_state(info=info)
    withdrawable = _withdrawable_from_state(perp_state, spot_state)
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

    # qty mikt op MAX_MARGIN_PCT_OF_FUNDS% van de TOTALE accountwaarde (niet
    # van withdrawable) zodat het bedrag per trade niet steeds kleiner wordt
    # naarmate er al meer posities tegelijk open staan (zie
    # _total_equity_from_state).
    total_equity = _total_equity_from_state(perp_state, spot_state)
    qty = _calc_margin_based_qty(market["mark_px"], used_leverage, total_equity, sz_decimals)
    notional = qty * market["mark_px"]
    margin_needed = notional / used_leverage

    # Hyperliquid weigert orders onder MIN_NOTIONAL_USD (zelf geverifieerd via
    # hun docs). Bij een klein beschikbaar saldo (of qty die naar 0 afrondt op
    # szDecimals) kan de op MAX_MARGIN_PCT_OF_FUNDS gebaseerde qty daaronder
    # uitkomen -- net als "geen beschikbaar saldo" hierboven is dit een
    # normale, te verwachten toestand (te weinig ruimte nu), geen
    # configuratieprobleem, dus overslaan i.p.v. een fout opgooien.
    if qty <= 0 or notional < config.MIN_NOTIONAL_USD:
        log.warning(
            "Gemiste trade wegens te kleine ordergrootte: %s %s overgeslagen "
            "(qty=%s, notional=$%.2f, Hyperliquid-minimum $%.0f -- %s%% van $%.2f totale accountwaarde bij %sx).",
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
            f"(notional ${notional:.2f} onder Hyperliquid's ${config.MIN_NOTIONAL_USD:.0f}-minimum)"
        )
        return None

    # De qty is gebaseerd op de TOTALE accountwaarde, maar of hij ook echt
    # past hangt af van wat er NU nog vrij is (withdrawable) -- bv. omdat er
    # al andere posities open staan die marge vasthouden. Zonder deze check
    # zou Hyperliquid de order gewoon afwijzen (RuntimeError via
    # _check_order_status) i.p.v. een nette, verwachte skip.
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

    # SL op de volle qty, onafhankelijk actief op Hyperliquid -- beschermt
    # tussen entry en het eerste TP-bericht van de groep. Geen TP-trigger-
    # order meer (zie moduledocstring): sluiten gebeurt via handle_tp_event().
    sl_trigger = _round_px(signal.stop_loss, sz_decimals)
    sl_limit = _round_px(
        sl_trigger * (1 - TRIGGER_LIMIT_BUFFER_PCT if is_buy else 1 + TRIGGER_LIMIT_BUFFER_PCT), sz_decimals
    )

    plan = {
        "coin": base_symbol,
        "leverage": {"value": used_leverage, "is_cross": False},
        "leverage_requested": signal.leverage,
        "leverage_max_allowed": market["max_leverage"],
        "leverage_capped": used_leverage < signal.leverage,
        "mark_px_at_calc": market["mark_px"],
        "margin_pct_of_funds": config.MAX_MARGIN_PCT_OF_FUNDS,
        "margin_needed": margin_needed,
        "withdrawable": withdrawable,
        "entry_order": {"coin": base_symbol, "is_buy": is_buy, "sz": qty, "order_type": {"limit": {"tif": "Ioc"}}},
        "sl_order": {
            "coin": base_symbol, "is_buy": exit_is_buy, "sz": qty, "limit_px": sl_limit,
            "order_type": {"trigger": {"triggerPx": sl_trigger, "isMarket": True, "tpsl": "sl"}},
            "reduce_only": True,
        },
    }

    if dry_run:
        log.info("[DRY RUN] Zou plaatsen op Hyperliquid:\n%s", json.dumps(plan, indent=2))
        db.log_order(
            symbol=signal.symbol, side=signal.side, dry_run=True, status="dry_run",
            entry_price=market["mark_px"], leverage=used_leverage, qty=qty, stop_loss=sl_trigger,
        )
        return plan

    exchange = exchange or _get_exchange()

    # Isolated margin (i.p.v. cross) zodat elke trade z'n risico beperkt
    # houdt tot deze ene positie.
    lev_resp = await _call(exchange.update_leverage, used_leverage, base_symbol, False)
    _check_order_status(lev_resp, "update_leverage")

    open_resp = await _call(exchange.market_open, base_symbol, is_buy, qty)
    statuses = _check_order_status(open_resp, "market_open")
    if not statuses or "filled" not in statuses[0]:
        raise RuntimeError(f"Entry-order voor {signal.symbol} is niet (meteen) gevuld: {open_resp}")

    filled = statuses[0]["filled"]
    filled_qty = float(filled["totalSz"])
    entry_price = float(filled["avgPx"])
    log.info(
        "Order geplaatst (v2, %s%% van saldo): %s %s qty=%.6f @ %s (%sx, margin~$%.2f)",
        config.MAX_MARGIN_PCT_OF_FUNDS, signal.side, signal.symbol, filled_qty, entry_price,
        used_leverage, margin_needed,
    )

    # sl_qty/state_qty/state_entry_price zijn bij een gewone (niet-re-entry)
    # trade gewoon de fill van hierboven. Bij een re-entry vervangen we ze
    # door de ECHTE, samengevoegde positie zoals Hyperliquid die zelf
    # rapporteert (autoritatief -- zie is_reentry hierboven), en annuleren we
    # eerst de oude SL zodat er nooit twee SL-orders voor dezelfde positie
    # naast elkaar resten.
    sl_qty = filled_qty
    state_qty = filled_qty
    state_entry_price = entry_price
    if is_reentry:
        old_sl_oid = existing.get("sl_oid")
        if old_sl_oid is not None:
            try:
                cancel_resp = await _call(exchange.cancel, base_symbol, old_sl_oid)
                _check_order_status(cancel_resp, "oude SL annuleren (re-entry)")
            except Exception:
                log.exception("Kon oude SL niet annuleren voor %s bij re-entry", base_symbol)
        else:
            log.warning("Geen sl_oid bekend voor bestaande %s-positie bij re-entry.", existing_key)

        live_pos = await _get_live_position(base_symbol, get_owner_address(), info=info)
        if live_pos is not None and live_pos["is_buy"] == is_buy:
            sl_qty = live_pos["qty"]
            state_qty = live_pos["qty"]
            state_entry_price = live_pos["entry_price"]
        else:
            # Kon de echte samengevoegde positie niet bevestigen (bv. de oude
            # positie bleek intussen al extern gesloten) -- veiligste fallback
            # is de nieuwe fill als op zichzelf staande positie te behandelen
            # i.p.v. te gokken op een samengevoegde qty die niet klopt.
            log.warning(
                "Kon samengevoegde positie voor %s niet bevestigen bij Hyperliquid -- "
                "state behandelt alleen de nieuwe fill (%.6f) als positie.",
                base_symbol, filled_qty,
            )

    sl_resp = await _call(
        exchange.order, base_symbol, exit_is_buy, sl_qty, sl_limit,
        {"trigger": {"triggerPx": sl_trigger, "isMarket": True, "tpsl": "sl"}},
        reduce_only=True,
    )
    sl_statuses = _check_order_status(sl_resp, "stop-loss plaatsen")
    sl_oid = sl_statuses[0].get("resting", {}).get("oid") if sl_statuses else None
    if sl_oid is None:
        log.warning("Kon geen order-id voor de stop-loss achterhalen uit response: %s", sl_resp)

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
            "sz_decimals": sz_decimals,
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


async def _market_close_reduce_only(
    base_symbol: str, exit_is_buy: bool, sz: float, sz_decimals: int,
    exchange=None, info=None, mark_px: float = None,
):
    """Sluit (een deel van) een v2-positie met een agressieve reduce-only
    IOC-limit-order -- zelfde "market order" aanpak als market_open(), maar
    via exchange.order() i.p.v. de SDK's market_close() helper, want die
    laatste doet zelf een niet-injecteerbare user_state-call om de
    positierichting op te zoeken (zou dependency injection in tests
    omzeilen). Wij kennen exit_is_buy/sz al uit onze eigen state.

    `mark_px` is optioneel voor te geven als de aanroeper 'm al heeft
    opgehaald (bv. voor een MIN_NOTIONAL_USD-check vooraf) -- scheelt dan een
    dubbele get_market_info()-call."""
    if mark_px is None:
        market = await get_market_info(base_symbol, info=info)
        mark_px = market["mark_px"]
    limit_px = _round_px(
        mark_px * (1 + TRIGGER_LIMIT_BUFFER_PCT if exit_is_buy else 1 - TRIGGER_LIMIT_BUFFER_PCT), sz_decimals
    )
    exchange = exchange or _get_exchange()
    resp = await _call(
        exchange.order, base_symbol, exit_is_buy, sz, limit_px,
        {"limit": {"tif": "Ioc"}}, reduce_only=True,
    )
    return _check_order_status(resp, f"reduce-only close ({sz} {base_symbol})")


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
    dicht had moeten zijn t/m deze target, en wat er al écht dicht is
    (qty - remaining_qty). Zo loopt een deel-close die een eerdere target
    oversloeg wegens MIN_NOTIONAL_USD automatisch mee in de eerstvolgende
    target die wel boven de grens uitkomt -- geen aparte "carry"-state
    nodig."""
    sz_decimals = pos["sz_decimals"]
    total_should_be_closed = _round_sz(pos["qty"] * (cum_pct / 100), sz_decimals)
    already_closed = _round_sz(pos["qty"] - pos["remaining_qty"], sz_decimals)
    close_qty = _round_sz(total_should_be_closed - already_closed, sz_decimals)
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


async def _handle_tp_partial_event(key: str, pos: dict, target_number: int, exchange=None, info=None):
    """Generieke handler voor targets 1 t/m 4: sluit het cumulatieve
    percentage van de ORIGINELE qty dat nog niet dicht is (zie
    _target_close_qty). Valt de notional van die deel-close onder
    MIN_NOTIONAL_USD, dan wordt deze stap overgeslagen (remaining_qty blijft
    ongewijzigd, target als "verwerkt" gemarkeerd zodat een duplicaat-event
    'm niet opnieuw probeert) -- de eerstvolgende target (uiteindelijk altijd
    target 5) sluit dan automatisch het opgestapelde verschil mee. Bij target
    config.BREAKEVEN_MOVE_AFTER_TARGET wordt daarnaast de SL verplaatst naar
    een dynamische break-even op basis van de ECHT gebankte winst uit eerdere
    targets (zie config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT), ongeacht of de
    close zelf is uitgevoerd of doorgeschoven naar later -- pas later dan
    target 1 zodat een normale terugval na een vroege, kleine TP1 niet meteen
    de hele rest van de positie eruit gooit voordat latere targets ooit
    geraakt worden (incident 2026-08-25: PENGU/BTC/HYPE)."""
    symbol = pos["symbol"]
    done_key = f"tp{target_number}_done"
    if pos.get(done_key):
        log.info("TP%d-event voor %s ontvangen maar al verwerkt -- genegeerd", target_number, symbol)
        db.log_tp_event(
            symbol=symbol, event="tp_event_ignored_duplicate", detail=f"target {target_number} al verwerkt",
        )
        return

    sz_decimals = pos["sz_decimals"]
    exit_is_buy = not pos["is_buy"]
    cum_pct = _cumulative_target_pct(target_number)
    close_qty = _target_close_qty(pos, cum_pct)

    exchange = exchange or _get_exchange()
    market = await get_market_info(symbol, info=info)
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
            close_statuses = await _market_close_reduce_only(
                symbol, exit_is_buy, close_qty, sz_decimals, exchange=exchange, info=info, mark_px=mark_px,
            )
        except RuntimeError as e:
            if _is_position_already_closed_error(e):
                await _handle_position_already_closed(key, symbol, f"target {target_number}")
                return
            raise

        # Winst van DEZE stap = wat de close-order echt ophaalde (avgPx uit de
        # fill, net als bij de entry-order in place_entry_order) t.o.v. de
        # entry-prijs -- basis voor de dynamische break-even-berekening
        # hieronder (zie config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT).
        filled = close_statuses[0].get("filled") if close_statuses else None
        avg_px = float(filled["avgPx"]) if filled else mark_px
        step_pnl = close_qty * (avg_px - pos["entry_price"]) * (1 if pos["is_buy"] else -1)
        banked_pnl += step_pnl
        remaining_qty = _round_sz(pos["remaining_qty"] - close_qty, sz_decimals)

    new_sl_oid = pos.get("sl_oid")
    sl_price = pos.get("sl_price")
    moves_to_breakeven = target_number == config.BREAKEVEN_MOVE_AFTER_TARGET
    if moves_to_breakeven:
        # Oude SL vervangen door een gebufferde break-even-SL voor de rest.
        sl_oid = pos.get("sl_oid")
        if sl_oid is not None:
            try:
                cancel_resp = await _call(exchange.cancel, symbol, sl_oid)
                _check_order_status(cancel_resp, "oorspronkelijke SL annuleren (break-even-shift)")
            except Exception:
                log.exception("Kon originele SL niet annuleren voor %s bij break-even-shift", symbol)
        else:
            log.warning("Geen sl_oid bekend voor %s bij break-even-shift -- plaats break-even-SL toch.", symbol)

        # Break-even-trigger op basis van de ECHTE, al gebankte winst uit
        # eerdere targets (zie config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT): het
        # prijsniveau waarbij de HELE trade (banked_pnl + PnL op het restant)
        # op $0 uitkomt, min een veiligheidsmarge voor fees/slippage.
        # `available` is geclamped op 0, dus dit kan nooit slechter zijn dan
        # exacte entry-prijs.
        #
        # banked_pnl is BRUTO (zie step_pnl hierboven -- geen enkele fee wordt
        # ooit afgetrokken, ook de entry-fee niet). Incident 2026-09-01: PENGU
        # raakte TP1+TP2, banked_pnl was bruto +$3,65, maar na entry-fee +
        # fees op TP1/TP2/de break-even-close (~$0,81 totaal) sloot de trade
        # netto op -$0,23 -- de marge was te klein (0,1% van alleen de
        # RESTERENDE notional, dus kromp precies wanneer er meer eerdere
        # targets al gesloten waren) om zowel de al-betaalde entry-fee als de
        # nog te betalen close-fee te dekken. Marge nu over de ORIGINELE
        # notional (blijft dus constant, ongeacht hoeveel al dicht is) i.p.v.
        # de resterende, plus een hoger percentage (zie config) dat het
        # volledige entry+exit fee-rondje op de originele notional dekt.
        if remaining_qty > 0:
            original_notional = pos["qty"] * pos["entry_price"]
            safety = original_notional * (config.BREAKEVEN_PNL_SAFETY_MARGIN_PCT / 100)
            available = max(banked_pnl - safety, 0.0)
            offset = available / remaining_qty
        else:
            offset = 0.0
        be_trigger = _round_px(
            pos["entry_price"] - offset if pos["is_buy"] else pos["entry_price"] + offset, sz_decimals,
        )
        be_limit = _round_px(
            be_trigger * (1 - TRIGGER_LIMIT_BUFFER_PCT if pos["is_buy"] else 1 + TRIGGER_LIMIT_BUFFER_PCT),
            sz_decimals,
        )
        be_sl_resp = await _call(
            exchange.order, symbol, exit_is_buy, remaining_qty, be_limit,
            {"trigger": {"triggerPx": be_trigger, "isMarket": True, "tpsl": "sl"}},
            reduce_only=True,
        )
        sl_statuses = _check_order_status(be_sl_resp, "break-even SL plaatsen (break-even-shift)")
        new_sl_oid = sl_statuses[0].get("resting", {}).get("oid") if sl_statuses else None
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


async def _handle_tp1_event(key: str, pos: dict, exchange=None, info=None):
    await _handle_tp_partial_event(key, pos, target_number=1, exchange=exchange, info=info)


async def _handle_tp2_event(key: str, pos: dict, exchange=None, info=None):
    await _handle_tp_partial_event(key, pos, target_number=2, exchange=exchange, info=info)


async def _handle_tp3_event(key: str, pos: dict, exchange=None, info=None):
    await _handle_tp_partial_event(key, pos, target_number=3, exchange=exchange, info=info)


async def _handle_tp4_event(key: str, pos: dict, exchange=None, info=None):
    await _handle_tp_partial_event(key, pos, target_number=4, exchange=exchange, info=info)


async def _handle_tp5_event(key: str, pos: dict, exchange=None, info=None):
    """Finale exit: sluit ALTIJD de volledige resterende qty (100%), ongeacht
    MIN_NOTIONAL_USD of afrondingsverschillen -- dit is het eindpunt van de
    ladder en de uiteindelijke vangnet-fallback voor elke eerdere deel-close
    die werd doorgeschoven (zie _handle_tp_partial_event)."""
    symbol = pos["symbol"]
    sz_decimals = pos["sz_decimals"]
    exit_is_buy = not pos["is_buy"]
    close_qty = pos["remaining_qty"]

    exchange = exchange or _get_exchange()

    if close_qty <= 0:
        log.warning("TP5-event voor %s ontvangen maar remaining_qty is 0 -- state opgeschoond.", symbol)
        db.log_tp_event(symbol=symbol, event="tp_event_ignored_zero_remaining", detail="target 5, remaining_qty=0")
    else:
        # SL eerst annuleren, dan pas sluiten -- voorkomt een wees-order die
        # blijft resten nadat de positie hieronder volledig gesloten is.
        sl_oid = pos.get("sl_oid")
        if sl_oid is not None:
            try:
                cancel_resp = await _call(exchange.cancel, symbol, sl_oid)
                _check_order_status(cancel_resp, "SL annuleren (TP5-event)")
            except Exception:
                log.exception("Kon SL niet annuleren voor %s bij TP5-event", symbol)

        try:
            await _market_close_reduce_only(symbol, exit_is_buy, close_qty, sz_decimals, exchange=exchange, info=info)
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


async def handle_cancel_event(cancel_event, exchange=None, info=None):
    """
    Verwerkt een CancelEvent (signal_parser.parse_cancel_event): het kanaal
    trekt het signal in ("Close X/USDT" + "#X/USDT  Cancelled"), dus de
    bijbehorende v2-positie wordt volledig gesloten -- zelfde close-logica
    als _handle_tp3_event (SL eerst annuleren, dan resterende qty
    reduce-only market-close), ongeacht welke targets al gehaald zijn.
    Zelfde kandidaat-/ambiguiteit-/DRY_RUN-veiligheidslogica als
    handle_tp_event() (raakt legacy-posities nooit, want die hebben geen
    "version": "v2"). Kan twee keer binnenkomen voor dezelfde annulering
    (het kanaal stuurt "Close X/USDT" en "#X/USDT Cancelled" als losse
    berichten, vlak na elkaar) -- de tweede keer is er geen kandidaat meer
    en wordt 'ie stil genegeerd, zelfde als een duplicaat TP-event.

    Kandidaat-selectie EN het claimen ervan (_cancel_in_progress) gebeuren
    in dezelfde _state_lock-sectie: de twee kanaalberichten komen vaak met
    maar ~1-2s ertussen binnen, en de eigenlijke SL-cancel/close hierna is
    een netwerkcall die makkelijk langer duurt dan dat. Zonder deze claim
    lezen beide events de nog-niet-gepopte state en proberen ze allebei
    dezelfde SL te annuleren en dezelfde positie te sluiten (geobserveerd
    2026-08-15: "Order was never placed, already canceled, or filled" en
    daarna "Reduce only order would increase position").
    """
    base_symbol = cancel_event.symbol.replace("USDT", "")

    async with _state_lock:
        state = _load_state()

        candidates = [
            (key, pos) for key, pos in state.items()
            if pos.get("symbol") == base_symbol and pos.get("version") == "v2"
        ]

        if not candidates:
            log.info(
                "Cancel-event voor %s ontvangen maar geen open v2-positie -- genegeerd.",
                base_symbol,
            )
            db.log_tp_event(
                symbol=base_symbol, event="cancel_event_ignored_no_position",
                detail="geen open v2-positie",
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
                f"Handmatig checken op Hyperliquid."
            )
            return

        key, pos = candidates[0]

        if key in _cancel_in_progress:
            log.info(
                "Cancel-event voor %s wordt al verwerkt (bijna-gelijktijdig duplicaat) -- genegeerd.",
                base_symbol,
            )
            db.log_tp_event(
                symbol=base_symbol, event="cancel_event_ignored_in_progress",
                detail=f"key={key}",
            )
            return

        if config.DRY_RUN:
            # Zelfde veiligheidsnet-redenering als handle_tp_event(): in DRY_RUN
            # schrijft place_entry_order() nooit een v2-entry naar state, dus als
            # die er toch is, is er iets mis -- alleen loggen, geen echte
            # cancel/order-calls.
            log.warning(
                "DRY_RUN staat aan maar er staat een v2-positie voor %s in state -- dit zou niet "
                "moeten kunnen. Geen echte cancel/order-calls, alleen loggen (cancel-event).",
                base_symbol,
            )
            return

        _cancel_in_progress.add(key)

    try:
        symbol = pos["symbol"]
        sz_decimals = pos["sz_decimals"]
        exit_is_buy = not pos["is_buy"]
        close_qty = pos["remaining_qty"]

        exchange = exchange or _get_exchange()

        sl_oid = pos.get("sl_oid")
        if sl_oid is not None:
            try:
                cancel_resp = await _call(exchange.cancel, symbol, sl_oid)
                _check_order_status(cancel_resp, "SL annuleren (cancel-event)")
            except Exception:
                log.exception("Kon SL niet annuleren voor %s bij cancel-event", symbol)

        if close_qty > 0:
            try:
                await _market_close_reduce_only(symbol, exit_is_buy, close_qty, sz_decimals, exchange=exchange, info=info)
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


async def handle_tp_event(tp_event, exchange=None, info=None):
    """
    Verwerkt een TPEvent (signal_parser.parse_tp_event) voor een v2-positie:
    5-staps ladder, target 1 t/m 4 sluiten elk hun eigen (cumulatieve)
    percentage van de ORIGINELE qty (target 1 verplaatst ook de SL naar
    break-even), target 5 sluit altijd de volledige rest (finale exit). Geen
    open v2-positie voor deze coin -> loggen en negeren (raakt de
    legacy-posities (TAO/HYPE/PENGU) NOOIT, want die hebben geen
    "version": "v2" in hun state-entry).
    """
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
            f"Handmatig checken op Hyperliquid."
        )
        return

    key, pos = candidates[0]

    if config.DRY_RUN:
        # In DRY_RUN schrijft place_entry_order() nooit een v2-entry naar
        # state, dus als die er toch is, is er iets mis -- alleen loggen,
        # geen echte cancel/order-calls.
        log.warning(
            "DRY_RUN staat aan maar er staat een v2-positie voor %s in state -- dit zou niet "
            "moeten kunnen. Geen echte cancel/order-calls, alleen loggen (target %d).",
            base_symbol, tp_event.target_number,
        )
        return

    if tp_event.target_number == 1:
        await _handle_tp1_event(key, pos, exchange=exchange, info=info)
    elif tp_event.target_number == 2:
        await _handle_tp2_event(key, pos, exchange=exchange, info=info)
    elif tp_event.target_number == 3:
        await _handle_tp3_event(key, pos, exchange=exchange, info=info)
    elif tp_event.target_number == 4:
        await _handle_tp4_event(key, pos, exchange=exchange, info=info)
    elif tp_event.target_number == 5:
        await _handle_tp5_event(key, pos, exchange=exchange, info=info)
    else:
        log.info(
            "TP-event %s target %d ontvangen, buiten de bekende ladder (1-5) -- geen actie.",
            base_symbol, tp_event.target_number,
        )
        db.log_tp_event(
            symbol=base_symbol, event="tp_event_no_action", detail=f"target {tp_event.target_number}",
        )


async def _estimate_realized_pnl_since(symbol: str, opened_at, owner: str, info) -> Optional[float]:
    """Best-effort schatting van de gerealiseerde PnL (netto, na close-fees)
    sinds `opened_at` voor `symbol`, uit user_fills_by_time. Gedeeld door
    reconcile_positions() en place_entry_order()'s tegengestelde-positie-
    guard, zodat een "buiten de bot om gesloten"-detectie altijd dezelfde
    PnL-melding oplevert, ongeacht welke van de twee 'm als eerste opmerkt.
    Puur informatief (geen boekhoudkundige garantie), dus faalt stil (None)
    i.p.v. de aanroeper te laten crashen op een ontbrekende PnL-schatting.

    feeToken: Hyperliquid-fees zijn meestal in USDC, maar kunnen ook in een
    ander token betaald zijn (bv. builder-fee-korting) -- 'fee' is dan NIET
    in USD, dus die fills tellen alleen mee voor de bruto PnL, niet voor de
    fee-aftrek (voorkomt een fee in de verkeerde eenheid van een USD-bedrag
    aftrekken)."""
    try:
        opened_at_ms = int(opened_at * 1000)
        if not opened_at_ms:
            return None
        fills = await _call(info.user_fills_by_time, owner, opened_at_ms, int(time.time() * 1000))
        closing_fills = [f for f in fills if f.get("coin") == symbol and "Close" in f.get("dir", "")]
        # closedPnl is bruto (excl. fee); Phantom/Hyperliquid's trade-geschiedenis
        # trekt per close alleen de fee van díe close eraf (de entry-fee zit al
        # verwerkt in de cost basis en duikt daar niet los op) -- dus alleen de
        # close-fees aftrekken geeft het bedrag dat overeenkomt met wat je daar ziet.
        gross_pnl = sum(float(f["closedPnl"]) for f in closing_fills)
        fees = sum(
            float(f.get("fee", 0.0)) for f in closing_fills if f.get("feeToken", "USDC") == "USDC"
        )
        return gross_pnl - fees
    except Exception:
        log.exception("Kon gerealiseerde PnL niet ophalen sinds entry voor %s", symbol)
        return None


async def reconcile_positions(exchange=None, info=None):
    """
    Achtergrondtaak (zie main.py): vergelijkt periodiek de lokale v2-state
    met de ECHTE open posities op Hyperliquid. Nodig omdat de lokale state
    tot nu toe ALLEEN werd bijgewerkt via Telegram TP/cancel-events -- een
    resting SL-order die rechtstreeks op de exchange getriggerd wordt (dus
    buiten de bot om) laat geen Telegram-bericht achter, en een TP/cancel-
    event dat ambigu is (meerdere v2-posities voor dezelfde coin, zie
    handle_tp_event()/handle_cancel_event()) werd tot nu toe genegeerd zonder
    dat de state ooit alsnog werd opgeschoond.

    Incident 2026-08-26/28: drie PENGU-posities sloten ná elkaar via hun
    resting SL zonder dat de bot het ooit doorhad -- open_positions.json
    bleef tot de gebruiker het zelf in Phantom checkte "open" tonen terwijl
    er op Hyperliquid allang niks meer stond, en de gebruiker kreeg daar
    nooit een Telegram-melding van.

    Voor elke lokale v2-entry: bestaat er nog een ECHTE open positie op
    Hyperliquid voor deze coin+richting (assetPositions, szi-teken bepaalt
    long/short)? Zo niet, dan is de positie buiten de bot om gesloten (SL
    geraakt, handmatig, of anders) -- state opschonen en de gebruiker een
    Telegram-melding sturen met een schatting van de gerealiseerde PnL sinds
    opened_at (uit user_fills_by_time). De PnL-schatting is best-effort/
    informatief (geen boekhoudkundige garantie als er tussentijds nog een
    andere positie voor dezelfde coin+richting is geweest) -- het doel is dat
    een stille close nooit meer onopgemerkt blijft, niet een exacte P&L-audit.
    """
    async with _state_lock:
        state = _load_state()
    v2_keys = [k for k, pos in state.items() if pos.get("version") == "v2"]
    if not v2_keys:
        return

    info = info or _get_info()
    owner = get_owner_address()
    perp_state = await _call(info.user_state, owner)
    live_positions = _parse_live_positions(perp_state)

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
            continue  # nog echt open op Hyperliquid, niks te doen

        realized_pnl = await _estimate_realized_pnl_since(symbol, pos.get("opened_at", 0), owner, info)

        async with _state_lock:
            state = _load_state()
            state.pop(key, None)
            _save_state(state)

        pnl_txt = f"~${realized_pnl:.2f}" if realized_pnl is not None else "onbekend"
        log.warning(
            "Reconciliatie: %s stond niet meer open op Hyperliquid maar wel nog in lokale state "
            "-- waarschijnlijk buiten de bot om gesloten (bv. resting SL). State opgeschoond, "
            "gerealiseerde PnL sinds entry: %s.",
            key, pnl_txt,
        )
        db.log_tp_event(
            symbol=symbol, event="position_closed_externally_detected",
            detail=f"key={key}, niet meer open op Hyperliquid, state opgeschoond, PnL sinds entry: {pnl_txt}",
        )
        await _notify(
            f"⚠️ {symbol} ({'Buy' if is_buy else 'Sell'}) bleek al gesloten op Hyperliquid "
            f"(waarschijnlijk SL geraakt) zonder dat de bot dit doorhad -- pas nu bij reconciliatie "
            f"ontdekt. Gerealiseerde PnL sinds entry: {pnl_txt}. Lokale state opgeschoond."
        )


async def execute_signal(signal: Signal):
    return await place_entry_order(signal, dry_run=config.DRY_RUN)
