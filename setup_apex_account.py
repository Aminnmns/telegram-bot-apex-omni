"""
EENMALIGE setup: registreert je ApeX Omni-account en print de vaste
credentials die daarna in .env moeten (APEX_API_KEY/SECRET/PASSPHRASE +
APEX_ZK_SEEDS/ZK_L2KEY). Dit hoeft maar ÉÉN keer per wallet -- niet elke
keer dat de bot opstart.

Waarom dit nodig is: ApeX Omni gebruikt een twee-sleutel-systeem (zie
config.py's module-docstring). Je EVM-key ondertekent hier eenmalig een
onboarding-bericht waaruit een aparte L2 zk-key wordt afgeleid; de server
geeft daarbij ook vaste API-credentials terug. Die waarden kunnen NIET
opnieuw opgevraagd worden -- bewaar ze meteen (en NOOIT in git, zie
.gitignore/.env).

Gebruik:
    1. Zet APEX_ETH_PRIVATE_KEY en APEX_ENV in .env (env=test voor je eerste
       run, main pas als je het hele stappenplan uit README.md hebt gevolgd).
    2. python setup_apex_account.py
    3. Kopieer de geprinte APEX_API_*/APEX_ZK_*-regels naar .env.

Gebaseerd op ApeX Omni's eigen officiële voorbeeldscript
(tests/01_register_v3.py in github.com/ApeX-Protocol/apexpro-openapi) --
zelf tegen hun testnet geverifieerd dat deze flow werkt (registratie,
change_pub_key_v3-activatie).
"""
import sys
import time

from apexomni.constants import (
    APEX_OMNI_HTTP_MAIN,
    APEX_OMNI_HTTP_TEST,
    NETWORKID_MAIN,
    NETWORKID_TEST,
    NETWORKID_OMNI_MAIN_ARB,
    NETWORKID_OMNI_TEST_BNB,
)
from apexomni.http_private_v3 import HttpPrivate_v3

import config


def _env():
    if config.APEX_ENV == "main":
        return APEX_OMNI_HTTP_MAIN, NETWORKID_MAIN, NETWORKID_OMNI_MAIN_ARB
    return APEX_OMNI_HTTP_TEST, NETWORKID_TEST, NETWORKID_OMNI_TEST_BNB


def main():
    if not config.APEX_ETH_PRIVATE_KEY:
        print("Fout: APEX_ETH_PRIVATE_KEY staat niet in .env. Zet 'm eerst (zie .env.example).")
        sys.exit(1)

    endpoint, network_id, chain_id = _env()
    print(f"Omgeving: {config.APEX_ENV} ({endpoint})")

    client = HttpPrivate_v3(endpoint, network_id=network_id, eth_private_key=config.APEX_ETH_PRIVATE_KEY)
    print(f"Wallet-adres: {client.default_address}")
    client.configs_v3()

    print("\n[1/3] ZK-sleutels afleiden uit je EVM-key...")
    zk_keys = client.derive_zk_key(client.default_address)
    print(f"  l2Key      : {zk_keys['l2Key']}")
    print(f"  pubKeyHash : {zk_keys['pubKeyHash']}")

    print("\n[2/3] Account registreren bij ApeX Omni...")
    nonce = client.generate_nonce_v3(
        refresh="false", l2Key=zk_keys["l2Key"], ethAddress=client.default_address, chainId=chain_id,
    )
    if nonce.get("code"):
        print(f"Fout bij nonce ophalen: {nonce}")
        sys.exit(1)

    reg = client.register_user_v3(
        nonce=nonce["data"]["nonce"], l2Key=zk_keys["l2Key"], seeds=zk_keys["seeds"],
        ethereum_address=client.default_address,
    )
    if reg.get("code"):
        print(f"Fout bij registratie: {reg}")
        sys.exit(1)
    api_key = reg["data"]["apiKey"]

    print("\n[3/3] Signing activeren (change_pub_key)...")
    time.sleep(10)  # backend heeft even nodig om de registratie te verwerken
    account = client.get_account_v3()
    spot = (account or {}).get("spotAccount") or {}
    change = client.change_pub_key_v3(
        chainId=chain_id, seeds=zk_keys["seeds"], ethPrivateKey=config.APEX_ETH_PRIVATE_KEY,
        zkAccountId=spot.get("zkAccountId"), subAccountId=spot.get("defaultSubAccountId"),
        newPkHash=zk_keys["pubKeyHash"], nonce=spot.get("nonce"), l2Key=zk_keys["l2Key"],
    )
    if change.get("code"):
        print(f"Waarschuwing bij change_pub_key: {change} (registratie zelf is al gelukt, kan handmatig opnieuw)")

    print("\n" + "=" * 60)
    print("KLAAR -- kopieer onderstaande regels naar .env")
    print("Deze waarden kunnen NIET opnieuw opgevraagd worden. Bewaar ze")
    print("nu (en in een password manager). Nooit in git, screenshots of chat.")
    print("=" * 60)
    print(f"APEX_API_KEY={api_key['key']}")
    print(f"APEX_API_SECRET={api_key['secret']}")
    print(f"APEX_API_PASSPHRASE={api_key['passphrase']}")
    print(f"APEX_ZK_SEEDS={zk_keys['seeds']}")
    print(f"APEX_ZK_L2KEY={zk_keys['l2Key']}")


if __name__ == "__main__":
    main()
