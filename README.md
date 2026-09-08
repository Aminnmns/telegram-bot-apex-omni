# Telegram → Hyperliquid Signal Bot

Volgt signals uit een Telegram-groep blind op en voert ze uit als leveraged
perp-trades op **Hyperliquid**, via de EVM-private key van Phantom's
ingebouwde "Perps"-account. Draait 24/7 op een Ubuntu-VPS.

⚠️ Deze bot doet **geen** validatie van signals — alles wat de parser herkent
wordt uitgevoerd. De enige automatische aanpassing is de leverage: als het
signal meer vraagt dan die specifieke market toestaat, wordt de hoogst
beschikbare leverage gebruikt in plaats van dat de order faalt.

**Waarom Hyperliquid?** Oorspronkelijk draaide dit op Drift Protocol, tot dat
op 1 april 2026 gehackt werd (~$285M, toegeschreven aan de Lazarus Group) en
nu als "Velocity DEX" in besloten private beta draait — niet publiek
bruikbaar. Daarna is overgestapt naar Flash Trade (Solana), maar die gaf
herhaaldelijk on-chain problemen tijdens setup en heeft geen Python-SDK —
elke transactie moest zelf als ruwe Solana-tx gebouwd en gesigned worden.
Hyperliquid heeft een officieel onderhouden Python-SDK
(`hyperliquid-dex/hyperliquid-python-sdk`) met directe `order()`/
`update_leverage()`-calls en ingebouwde TP/SL-trigger-orders — geen ruwe
transacties meer nodig. De losse Flash Trade-tooling (`check_flash_setup.py`/
`setup_flash_account.py`/`debug_flash_tx.py`, en de bijbehorende `SOLANA_*`/
`FLASH_*`-configvariabelen) is inmiddels verwijderd uit de repo (2026-08-25) --
geen onderdeel meer van de live flow.

⚠️ **Bewuste keuze, geen aanname:** de bot draait rechtstreeks tegen
Hyperliquid **mainnet**, niet tegen hun testnet — om het bestaande Phantom
Perps-saldo direct te kunnen hergebruiken. `DRY_RUN=True` is daarom de enige
veiligheidsklep voor je eerste tests: hij logt wat de bot ZOU doen, zonder
iets echt uit te voeren.

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

## 3. Phantom wallet + Hyperliquid

Hyperliquid draait **niet** op Solana — het gebruikt Ethereum-stijl
(secp256k1) wallets. Dit is dezelfde EVM-account als Phantom's ingebouwde
"Perps"-feature gebruikt (zelfde seed phrase als je Solana-wallet, maar een
apart 0x-adres) — bevestigd via Phantom's eigen docs: "You can export your
perps account by exporting your Phantom wallet's private key and importing
it into any EVM-compatible wallet."

1. Exporteer die key via Phantom: Instellingen → Security & Privacy → Export
   Private Key, en kies netwerk **"Ethereum"** (Ethereum/Base/Polygon/HyperEVM
   delen dezelfde private key per account). **Gebruik hiervoor bij voorkeur
   een aparte trading-wallet**, niet je hoofd-wallet — deze key komt op de
   VPS te staan.
2. Zorg dat er collateral (USDC) in het Phantom Perps-saldo staat — storten
   gaat via Phantom zelf, er is geen apart setup-script voor nodig zoals bij
   de oude Flash Trade-flow.

## 4. Configuratie

```bash
cp .env.example .env
nano .env
```
Vul minimaal in: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_GROUP`,
`HYPERLIQUID_PRIVATE_KEY`. Belangrijkste overige instellingen:

| Variabele | Betekenis |
|---|---|
| `HYPERLIQUID_ACCOUNT_ADDRESS` | Optioneel — leeg = adres dat bij `HYPERLIQUID_PRIVATE_KEY` hoort. |
| `HYPERLIQUID_ENV` | `mainnet` of `testnet`. Staat standaard op `mainnet` (zie waarschuwing hierboven). |
| `MAX_MARGIN_PCT_OF_FUNDS` | % van je beschikbare saldo dat als isolated margin ingezet wordt per nieuwe trade — dus ook je worst-case-verlies per trade. |
| `MAX_CONCURRENT_POSITIONS` | Harde grens op gelijktijdig open live posities, live gecheckt via Hyperliquid's `clearinghouseState`. |
| `MIN_NOTIONAL_USD` | Hyperliquid's eigen minimum-orderwaarde. |
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

# 3. Eén losse testtrade op echt geld, buiten de Telegram-flow om -- verifieert
#    de hele pipeline (market-lookup, leverage-cap, qty, order, TP/SL) zonder
#    op een signal te hoeven wachten. Vraagt om expliciete bevestiging.
python manual_test_trade.py SOL 1

# 4. Start de bot met DRY_RUN=True -- hij logt wat hij ZOU doen, voert niks uit
python main.py
```
Laat 'm zo een paar signals meemaken en check de logs (en je Telegram
"Saved Messages", waar de bot notificaties naartoe stuurt).

Zet daarna pas `DRY_RUN=False` — begin met een klein `MAX_MARGIN_PCT_OF_FUNDS`
en controleer elke order in de Hyperliquid-app voor je het bedrag opschaalt.
Er is geen testnet-vangnet in gebruik, dus dit is met echt geld.

## 6. Live zetten

In `.env`: `DRY_RUN=False`. Daarna als achtergrondservice:

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
| `executor.py` | Plaatst orders op Hyperliquid via de officiële Python-SDK, incl. leverage-cap en de 5-staps TP-ladder (met break-even-SL-shift) en cancel-exitlogica |
| `config.py` | Leest alle instellingen uit `.env` |
| `dashboard.py` | Read-only statusdashboard (Flask, localhost/Tailscale-only) |
| `test_parser.py` | Test de parser zonder Telegram/Hyperliquid erbij |
| `test_exit_strategy.py` | Test de volledige TP1-5-ladder (incl. break-even-SL-shift) en cancel-exitlogica met gemockte API-calls |
| `manual_test_trade.py` | Eén losse testtrade op echt geld, buiten de Telegram-flow om |
| `signal-bot.service` | systemd-service voor 24/7 draaien |
| `dashboard.service` | systemd-service voor het dashboard |
