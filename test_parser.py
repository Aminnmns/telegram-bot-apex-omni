"""
Test de parser LOKAAL, zonder Telegram of Bybit erbij te halen.
Plak hieronder een echt bericht (met coin erin!) uit de groep en run:

    python test_parser.py

Controleer dat symbol, side, entry, leverage, targets en stop_loss
er allemaal correct uitkomen voor je de bot live zet.
"""
from signal_parser import parse_signal, parse_cancel_event

SAMPLE_MESSAGE = """
#CryptoSharks Signal [TAO/USDT]

🦈 Open SHORT at price between $191.8 - $193.9 with X25 leverage.

☑️ TARGETS

1️⃣ Close the order at the price $190.3
2️⃣ Close the order at the price $189.5
3️⃣ Close the order at the price $187.8
4️⃣ Close the order at the price $185.9
5️⃣ Close the order at the price $183

✖️ Stop loss: $200.3
"""

if __name__ == "__main__":
    result = parse_signal(SAMPLE_MESSAGE)
    if result is None:
        print("❌ Geen geldig signal herkend. Check of coin-ticker in het bericht staat")
        print("   en of SYMBOL_PATTERN/BLACKLIST in signal_parser.py moet worden aangepast.")
    else:
        print("✅ Signal herkend:")
        print(f"  Symbol:     {result.symbol}")
        print(f"  Side:       {result.side}")
        print(f"  Entry:      {result.entry_low} - {result.entry_high}")
        print(f"  Leverage:   {result.leverage}x")
        print(f"  Targets:    {result.targets}")
        print(f"  Stop loss:  {result.stop_loss}")

    # Regressietest voor het incident van 2026-08-12: "#HYPE/USDT  Cancelled"
    # en "Close HYPE/USDT" werden niet herkend als cancel-event.
    for cancel_text in ("#HYPE/USDT  Cancelled", "Close HYPE/USDT"):
        cancel_result = parse_cancel_event(cancel_text)
        if cancel_result is None or cancel_result.symbol != "HYPEUSDT":
            print(f"❌ Cancel-event niet (correct) herkend voor: {cancel_text!r} -> {cancel_result}")
        else:
            print(f"✅ Cancel-event herkend: {cancel_text!r} -> {cancel_result.symbol}")

    # Regressietest voor het incident van 2026-08-26/28: "#PENGU/USDT Closed
    # due to opposite direction signal ⚠" werd niet herkend, waardoor de oude
    # positie in state bleef staan tot een later tegengesteld signal een
    # tweede v2-entry opende (de "meerdere open v2-posities"-ambiguïteit).
    for cancel_text in ("#PENGU/USDT Closed due to opposite direction signal ⚠",):
        cancel_result = parse_cancel_event(cancel_text)
        if cancel_result is None or cancel_result.symbol != "PENGUUSDT":
            print(f"❌ Cancel-event niet (correct) herkend voor: {cancel_text!r} -> {cancel_result}")
        else:
            print(f"✅ Cancel-event herkend: {cancel_text!r} -> {cancel_result.symbol}")
