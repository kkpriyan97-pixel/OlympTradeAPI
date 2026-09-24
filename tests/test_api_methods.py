import asyncio
import os
from olymptrade_ws import OlympTradeClient, BalanceAPI, MarketAPI, TradeAPI

ACCESS_TOKEN = os.environ.get("OLYMPTRADE_ACCESS_TOKEN", "").strip()

async def test_all_methods():

    client = OlympTradeClient(
        access_token=ACCESS_TOKEN,
        log_raw_messages=False,
        account_id=128751040,  # Let the client auto-detect
        account_group='demo',
        uri=r"wss://ws.olymptrade.com/otp?cid_ver=1&cid_app=web%40OlympTrade%402025.3.26878%4026878&cid_device=%40%40desktop&cid_os=mac_os%4010.15.7"
    )
    await client.start()
    print("Connected!")

    # Initialize session (send all startup messages and get account_id)
    await client.initialize_session()
    print(f"Initialized session. account_id={client.account_id}, account_group={client.account_group}")

    # Wait for balance to be received (poll for up to 10 seconds)
    balance = None
    for _ in range(20):
        balance = client.balance.get_last_balance()
        if balance and 'd' in balance and balance['d']:
            break
        await asyncio.sleep(0.5)
    if balance and 'd' in balance and balance['d']:
        print(f"get_last_balance: {balance}")
    else:
        print("Balance not received after waiting.")

    # 2. MarketAPI (example: subscribe to ticks for EURUSD)
    try:
        await client.market.subscribe_ticks("EURUSD")
        print("Subscribed to EURUSD ticks.")
    except Exception as e:
        print(f"subscribe_ticks failed: {e}")

    # 3. TradeAPI (example: list open trades)
    account_id = client.account_id
    try:
        if account_id:
            open_trades = await client.trade.get_open_trades(account_id)
            print(f"get_open_trades: {open_trades}")
        else:
            print("No account_id found, cannot call get_open_trades.")
    except Exception as e:
        print(f"get_open_trades failed: {e}")

    await client.stop()
    print("Client stopped.")

if __name__ == "__main__":
    asyncio.run(test_all_methods())
