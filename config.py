"""
Centrale configuratie. Alles wordt geladen uit een .env bestand
(zie .env.example) zodat je nooit keys in de code zelf zet.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


# --- Telegram ---
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "signal_bot_session")
TELEGRAM_GROUP = os.getenv("TELEGRAM_GROUP", "")  # groep username, invite-link of chat-ID
TELEGRAM_NOTIFY_CHAT = os.getenv("TELEGRAM_NOTIFY_CHAT", "")  # waar de bot JOU updates stuurt (bv. "me")

# --- ApeX Omni (vervangt Hyperliquid als trading-venue, september 2026) ---
# ApeX Omni (apex.exchange) is zelf de trading-venue -- geen Phantom/Hyperliquid
# meer tussen: het geld staat rechtstreeks op ApeX Omni. Officiële Python-SDK:
# apexomni (pip install apexomni), github.com/ApeX-Protocol/apexpro-openapi.
#
# BELANGRIJK VERSCHIL MET HYPERLIQUID: ApeX Omni gebruikt een twee-sleutel-
# systeem, geen kaal EVM-private-key-signing. Je EVM-key (hieronder) ondertekent
# EENMALIG een onboarding-bericht waaruit een aparte "L2 zk-key" wordt afgeleid;
# daarna wordt élke order dubbel gesigned (een API-key-signature + een losse
# L2-signature). Die L2-sleutel + de vaste API-credentials worden NIET elke
# run opnieuw afgeleid -- ze komen precies één keer uit setup_apex_account.py
# en moeten daarna hieronder in .env staan. Zelf geverifieerd tegen ApeX
# Omni's testnet (registratie, order plaatsen, annuleren, leverage zetten) --
# zie setup_apex_account.py voor de eenmalige setup-stap.
APEX_ETH_PRIVATE_KEY = os.getenv("APEX_ETH_PRIVATE_KEY", "")

# Vaste API-credentials + L2 zk-sleutels, ALLEEN te verkrijgen door eenmalig
# setup_apex_account.py te draaien (kunnen niet opnieuw opgevraagd worden --
# de setup print ze precies één keer).
APEX_API_KEY = os.getenv("APEX_API_KEY", "")
APEX_API_SECRET = os.getenv("APEX_API_SECRET", "")
APEX_API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE", "")
APEX_ZK_SEEDS = os.getenv("APEX_ZK_SEEDS", "")
APEX_ZK_L2KEY = os.getenv("APEX_ZK_L2KEY", "")

# "main" of "test". Bewust op "main" als default (zelfde redenering als
# voorheen bij Hyperliquid: dit gebruikt het bestaande ApeX Omni-saldo), maar
# ApeX Omni heeft -- anders dan Hyperliquid -- wél een volwaardig testnet
# (testnet.omni.apex.exchange, gratis faucet-geld). Zet hierop "test" voor je
# eerste end-to-end-tests, los van (en bovenop) DRY_RUN.
APEX_ENV = os.getenv("APEX_ENV", "main")

# --- Safety / mode ---
# Staat standaard op "veilig". Zet pas uit als je alles hebt getest.
DRY_RUN = _bool("DRY_RUN", True)  # True = alles loggen, niets echt uitvoeren

# Max. aantal gelijktijdig open live posities, simpele harde grens. Gecheckt
# via ApeX Omni's eigen account-positions vlak voor elke nieuwe entry (alle
# live posities, ongeacht via welke flow ze geopend zijn) -- niet een lokale
# telling die uit sync kan raken met wat er écht op de exchange staat.
MAX_CONCURRENT_POSITIONS = _int("MAX_CONCURRENT_POSITIONS", 8)

# --- Positiegrootte ---
# Elke trade gebruikt exact dit percentage van je op dat moment BESCHIKBARE
# saldo (get_withdrawable(): perps-withdrawable + vrije spot-USDC) als margin
# -- qty = (saldo * MAX_MARGIN_PCT_OF_FUNDS/100 * toegestane leverage) / prijs,
# naar beneden afgerond zodat de marge nooit boven de cap uitkomt (zie
# executor._calc_margin_based_qty). Isolated margin, dus dit is meteen ook je
# worst-case-verlies per trade (niet meer dan je marge).
#
# VERVANGT de oude MAX_RISK_USD-aanpak (vast dollarbedrag, qty = MAX_RISK_USD
# / |entry - SL|): die was volledig onafhankelijk van leverage, waardoor een
# lagere dan verwachte max-leverage per coin (bv. HYPE: signal vroeg 25x,
# de exchange stond destijds maar 10x toe) evenveel qty maar 2.5x zoveel margin kostte
# -- bij een klein account (~$20) at dat in de praktijk 70-80% van het totale
# saldo op (incident 2026-08-11/12). Direct als %-van-saldo sizen voorkomt dat
# soort verrassingen structureel, ongeacht welke leverage een coin toestaat.
MAX_MARGIN_PCT_OF_FUNDS = _float("MAX_MARGIN_PCT_OF_FUNDS", 33.0)

# Ondergrens die WIJ hanteren voor een deel-close (zie _target_close_qty):
# ApeX Omni's eigen ondergrens is per-symbol minOrderSize (uit configV3), niet
# een vaste dollarwaarde -- deze $10-drempel is een aparte, eigen veiligheidsmarge
# zodat een deel-close niet zo klein wordt dat fees het grootste deel opeten.
MIN_NOTIONAL_USD = _float("MIN_NOTIONAL_USD", 10.0)

# --- v2 TP-executie (alleen voor NIEUWE signals) ---
# 5-staps ladder over de ORIGINELE positiegrootte, één stap per "target N
# ✅"-bericht van de groep: target 1 sluit TARGET1_CLOSE_PCT%, target 2 sluit
# TARGET2_CLOSE_PCT% erbovenop, enz. Target 5 heeft bewust GEEN eigen
# %-config -- die sluit altijd de volledige resterende qty (finale exit,
# vangt ook cumulatieve afrondingsverschillen en MIN_NOTIONAL_USD-fallbacks
# van eerdere targets op, zie executor._handle_tpN_event's docstrings).
# SL-naar-break-even gebeurt bij target BREAKEVEN_MOVE_AFTER_TARGET (zie
# executor._handle_tp_partial_event), los van de close-percentages hierboven.
TP_EVENT_TARGET1_CLOSE_PCT = _float("TP_EVENT_TARGET1_CLOSE_PCT", 40.0)
TP_EVENT_TARGET2_CLOSE_PCT = _float("TP_EVENT_TARGET2_CLOSE_PCT", 20.0)
TP_EVENT_TARGET3_CLOSE_PCT = _float("TP_EVENT_TARGET3_CLOSE_PCT", 15.0)
TP_EVENT_TARGET4_CLOSE_PCT = _float("TP_EVENT_TARGET4_CLOSE_PCT", 15.0)

# Bij welke target de SL naar break-even verplaatst wordt (1 t/m 4). Later dan
# target 1 geeft de trade meer ademruimte vóórdat de tight break-even-stop
# actief wordt -- voorkomt dat een normale terugval na een vroege, kleine
# TP1 de hele rest van de positie er meteen uitgooit voordat verdere targets
# (2 t/m 5) ooit geraakt worden (incident 2026-08-25: PENGU/BTC/HYPE).
BREAKEVEN_MOVE_AFTER_TARGET = _int("BREAKEVEN_MOVE_AFTER_TARGET", 2)

# De break-even-SL bij BREAKEVEN_MOVE_AFTER_TARGET wordt NIET meer op een
# vaste afstand van de entry-prijs gelegd, maar op het prijsniveau waarbij de
# HELE trade (al gerealiseerde winst uit eerdere targets + PnL op het
# restant) op $0 uitkomt -- zie executor._handle_tp_partial_event's
# banked_pnl-berekening. Meer winst uit eerdere targets geeft dus automatisch
# meer ademruimte voor latere targets (2 t/m 5), zonder dat de trade als
# geheel ooit netto verlies kan maken (verzoek gebruiker 2026-08-26: TP1+TP2
# als "verzekering" i.p.v. een gegokt vast percentage).
# Dit percentage is een veiligheidsmarge (% van de ORIGINELE notional, zie
# incident 2026-09-01 hierboven) voor fees/slippage, afgetrokken van de
# gebankte (bruto) winst vóórdat de break-even-prijs berekend wordt. 0,15%
# dekt ruim het volledige entry+exit fee-rondje (~0,086% gemeten op
# Hyperliquid, oorspronkelijk) plus wat marge voor slippage op de SL-fill
# zelf -- ApeX Omni's taker-fee (0,05% per kant op testnet, zie
# contractAccount.takerFeeRate) ligt in dezelfde orde grootte, maar dit
# percentage is nog niet opnieuw empirisch gevalideerd tegen ApeX Omni's
# eigen fee-rondje in de praktijk.
BREAKEVEN_PNL_SAFETY_MARGIN_PCT = _float("BREAKEVEN_PNL_SAFETY_MARGIN_PCT", 0.15)

# Incident 2026-08-26/28 (oorspronkelijk op Hyperliquid): lokale
# open_positions.json werd ALLEEN bijgewerkt via Telegram TP/cancel-events.
# Een resting SL die rechtstreeks op de exchange triggert (buiten de bot om)
# of een ambigu TP/cancel-event (meerdere v2-posities voor dezelfde coin)
# liet de state dus voor onbepaalde tijd stil verouderen -- de gebruiker
# ontdekte drie zulke stille closes pas zelf in de wallet-app, dagen later.
# executor.reconcile_positions() vergelijkt nu periodiek de lokale state met
# de ECHTE ApeX Omni-posities en ruimt/meldt elke mismatch op. Interval is een
# compromis tussen snel ontdekken en niet onnodig vaak ApeX Omni's
# account-endpoint bevragen.
POSITION_RECONCILE_INTERVAL_SECONDS = _int("POSITION_RECONCILE_INTERVAL_SECONDS", 120)

# Incident 2026-09-08 (oorspronkelijk op Hyperliquid): een SDK zonder timeout
# op zijn requests-sessie liet reconcile_positions_loop() dagenlang stilzwijgend
# vastlopen op een hangende TCP-verbinding (geen enkele log-regel meer, ook
# geen foutmelding), waardoor state-drift niet meer werd opgevangen. Elke
# ApeX Omni-call krijgt daarom een harde requests-timeout (zie
# executor._call) zodat een hangende call altijd een Exception oplevert die
# de bestaande try/except-loops kunnen afvangen en waarna de loop gewoon
# doorgaat.
APEX_API_TIMEOUT_SECONDS = _float("APEX_API_TIMEOUT_SECONDS", 15.0)
