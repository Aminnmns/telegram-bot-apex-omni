# Telegram → ApeX Omni Signal Bot

Volgt signals uit een Telegram-groep blind op en voert ze uit als leveraged
perp-trades op **ApeX Omni** (apex.exchange), rechtstreeks met je eigen
EVM-wallet. Draait 24/7 op een Ubuntu-VPS.

⚠️ Deze bot doet **geen** validatie van signals — alles wat de parser herkent
wordt uitgevoerd. De enige automatische aanpassing is de leverage: als het
signal meer vraagt dan die specifieke market toestaat, wordt de hoogst
beschikbare leverage gebruikt in plaats van dat de order faalt.

**Waarom ApeX Omni?** Oorspronkelijk draaide dit op Drift Protocol, tot dat
op 1 april 2026 gehackt werd (~$285M, toegeschreven aan de Lazarus Group) en
nu als "Velocity DEX" in besloten private beta draait. Daarna Flash Trade
(Solana, geen Python-SDK, herhaaldelijke on-chain problemen), daarna
Hyperliquid (via de EVM-key van Phantom's ingebouwde "Perps"-account).
September 2026: overgestapt naar ApeX Omni als volledig zelfstandige
trading-venue (geen Phantom-tussenstap meer, het geld staat rechtstreeks op
ApeX Omni). Officiële Python-SDK: `apexomni` (pip install apexomni,
github.com/ApeX-Protocol/apexpro-openapi) met directe `create_order_v3()`-
calls, native `STOP_MARKET`/`TAKE_PROFIT_MARKET`-trigger-orders, én — anders
dan Hyperliquid — een volwaardig testnet.

⚠️ **Twee-sleutel-signing, anders dan de vorige versies:** ApeX Omni signt
elke order met een aparte "L2 zk-key" die éénmalig wordt afgeleid uit je
EVM-key (zie stap 3 hieronder, `setup_apex_account.py`). Die L2-sleutel +
bijbehorende API-credentials moeten daarna vast in `.env` staan — ze worden
niet bij elke bot-start opnieuw afgeleid en kunnen niet opnieuw opgevraagd
worden bij ApeX Omni.

⚠️ **Bewuste keuze, geen aanname:** `APEX_ENV` staat standaard op `main`,
zodat de bot je bestaande ApeX Omni-saldo direct hergebruikt. `DRY_RUN=True`
is de eerste veiligheidsklep; `APEX_ENV=test` (met gratis testnet-faucet-geld)
is er een tweede, onafhankelijke laag bovenop voor je allereerste
end-to-end-tests — die had de Hyperliquid-versie niet.

## 1. Server voorbereiden

- Ubuntu **24.04 LTS**, 1-2 vCPU, 2GB RAM is ruim voldoende.
- SSH erop, dan:
```bash
sudo apt update && sudo apt install -y python3.12 python3.12-venv git
git clone <jouw-repo-of-upload-hierheen> telegram-signal-bot
cd telegram-signal-bot
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Telegram API-credentials

1. Ga naar https://my.telegram.org, log in, maak een "app" aan.
2. Noteer `api_id` en `api_hash`.
3. Zorg dat je eigen Telegram-account lid is van de signal-groep.

## 3. ApeX Omni-account

1. Zorg voor een EVM (secp256k1) wallet met USDC-collateral op ApeX Omni
   (storten gaat via de ApeX Omni-app/website zelf). **Gebruik hiervoor bij
   voorkeur een aparte trading-wallet**, niet je hoofd-wallet — deze key komt
   op de VPS te staan.
2. `cp .env.example .env`, vul `APEX_ETH_PRIVATE_KEY` in en zet `APEX_ENV=test`
   voor je eerste run.
3. Draai het eenmalige registratiescript:
   ```bash
   python setup_apex_account.py
   ```
   Dit leidt je L2 zk-key af, registreert het account bij ApeX Omni, en print
   `APEX_API_KEY`/`APEX_API_SECRET`/`APEX_API_PASSPHRASE`/`APEX_ZK_SEEDS`/
   `APEX_ZK_L2KEY`. **Deze waarden kunnen niet opnieuw opgevraagd worden** —
   kopieer ze meteen naar `.env` (en een password manager, nooit in git).
4. Herhaal dit (met `APEX_ENV=main`) zodra je klaar bent om met je echte
   saldo te werken — testnet en main zijn losse accounts/credentials.

## 4. Configuratie

```bash
cp .env.example .env
nano .env
```
Vul minimaal in: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_GROUP`,
`APEX_ETH_PRIVATE_KEY` (+ de `APEX_API_*`/`APEX_ZK_*`-waarden uit stap 3).
Belangrijkste overige instellingen:

| Variabele | Betekenis |
|---|---|
| `APEX_ENV` | `test` of `main`. Zet op `test` voor je eerste end-to-end-tests (gratis faucet-geld), `main` voor echt geld. |
| `MAX_MARGIN_PCT_OF_FUNDS` | % van je beschikbare saldo dat als isolated margin ingezet wordt per nieuwe trade — dus ook je worst-case-verlies per trade. |
| `MAX_CONCURRENT_POSITIONS` | Harde grens op gelijktijdig open live posities, live gecheckt via ApeX Omni's account-endpoint. |
| `MIN_NOTIONAL_USD` | Eigen ondergrens voor een deel-close (ApeX Omni's eigen minimum is per-symbol, zie `minOrderSize` in hun market-config). |
| `DRY_RUN` | Laat op `True` staan tot je klaar bent om te testen met echt geld. |
| `TP_EVENT_TARGET1_CLOSE_PCT` t/m `TARGET4_CLOSE_PCT` | 5-staps TP-ladder: % van de ORIGINELE qty dat elk target-bericht sluit (cumulatief). Target 5 heeft geen eigen %, sluit altijd de volledige rest. |
| `BREAKEVEN_MOVE_AFTER_TARGET` | Bij welke target (1-4) de SL naar break-even verplaatst. |
| `BREAKEVEN_PNL_SAFETY_MARGIN_PCT` | De break-even-SL wordt dynamisch berekend uit de ECHT gebankte winst van eerdere targets (zodat de trade als geheel nooit netto verlies maakt) — dit percentage is alleen een kleine veiligheidsmarge (% van de resterende notional) voor fees/slippage, afgetrokken vóór die berekening. |

## 5. Testen (verplicht voor je live gaat)

```bash
# 1. Test de parser met een paar echte berichten (pas SAMPLE_MESSAGE aan)
python test_parser.py

# 2. Test de TP1-5/cancel-exit-logica (volledig gemockt, geen netwerk)
python test_exit_strategy.py

# 3. Eén losse testtrade -- verifieert de hele pipeline (market-lookup,
#    leverage-cap, qty, order, TP/SL) zonder op een signal te hoeven wachten.
#    Draai dit EERST met APEX_ENV=test (nepgeld), pas daarna met main.
#    Vraagt om expliciete bevestiging.
python manual_test_trade.py SOL 1

# 4. Start de bot met DRY_RUN=True -- hij logt wat hij ZOU doen, voert niks uit
python main.py
```
Bij de eerste `python main.py` vraagt Telethon interactief om je
telefoonnummer + inlogcode (bouwt de Telegram-sessie op). Laat 'm daarna zo
een paar signals meemaken en check de logs (en je Telegram "Saved Messages",
waar de bot notificaties naartoe stuurt).

Zet daarna pas `DRY_RUN=False` — begin met een klein `MAX_MARGIN_PCT_OF_FUNDS`
en controleer elke order in de ApeX Omni-app voor je het bedrag opschaalt.

## 6. Live zetten

In `.env`: `DRY_RUN=False` en `APEX_ENV=main` (met de main-credentials uit
stap 3.4). Daarna als achtergrondservice:

```bash
sudo cp signal-bot.service /etc/systemd/system/
sudo nano /etc/systemd/system/signal-bot.service   # pas paden en user aan
sudo systemctl daemon-reload
sudo systemctl enable --now signal-bot
sudo systemctl status signal-bot
journalctl -u signal-bot -f   # live logs volgen
```

Voor read-only inzicht in status/trades/PnL zonder in de logs te hoeven
duiken, is er ook `dashboard.py` (Flask, alleen bereikbaar via
localhost/Tailscale — zie `dashboard.service`).

## Bestanden

| Bestand | Functie |
|---|---|
| `main.py` | Start de Telegram-listener |
| `signal_parser.py` | Zet berichttekst om naar een Signal-object |
| `executor.py` | Plaatst orders op ApeX Omni via de officiële Python-SDK, incl. leverage-cap en de 5-staps TP-ladder (met break-even-SL-shift) en cancel-exitlogica |
| `setup_apex_account.py` | Eenmalig registratiescript: leidt de L2 zk-key af en registreert het account bij ApeX Omni |
| `config.py` | Leest alle instellingen uit `.env` |
| `dashboard.py` | Read-only statusdashboard (Flask, localhost/Tailscale-only) |
| `test_parser.py` | Test de parser zonder Telegram/ApeX Omni erbij |
| `test_exit_strategy.py` | Test de volledige TP1-5-ladder (incl. break-even-SL-shift) en cancel-exitlogica met een gemockte ApeX Omni-client |
| `manual_test_trade.py` | Eén losse testtrade, buiten de Telegram-flow om |
| `signal-bot.service` | systemd-service voor 24/7 draaien |
| `dashboard.service` | systemd-service voor het dashboard |
