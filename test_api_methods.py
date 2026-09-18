import asyncio
import os

from olymptrade_ws import OlympTradeClient


async def test_read_only_methods():
    token = os.environ.get("OLYMPTRADE_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("Set OLYMPTRADE_ACCESS_TOKEN in the environment.")

    client = OlympTradeClient(
        access_token=token,
        log_raw_messages=False,
    )
    await client.start()
    try:
        await client.initialize_session()
        print(f"Initialized session. account_id={client.account_id}, account_group={client.account_group}")

        assets = await client.market.get_available_assets()
        print(f"Authenticated read-only assets: {len(assets)}")
        for asset in assets:
            print(asset)

        await client.market.subscribe_ticks("BNBUSD_OTC")
        print("Subscribed to BNBUSD_OTC ticks.")

        candles = await client.market.get_candles("BNBUSD_OTC", size=60, count=5)
        print(f"Read-only candles: {candles}")
    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(test_read_only_methods())
