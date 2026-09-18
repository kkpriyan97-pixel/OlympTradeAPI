import asyncio
import os

from olymptrade_ws import OlympTradeClient


async def test_read_only_methods():
    token = os.environ.get("OLYMPTRADE_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("Set OLYMPTRADE_ACCESS_TOKEN in the environment.")

    client = OlympTradeClient(access_token=token, log_raw_messages=False)
    await client.start()
    try:
        await client.initialize_session()

        first = await client.market.get_first_available_asset()
        if not first:
            raise RuntimeError("No authenticated OlympTrade asset was returned.")

        pair = first.get("pair") or first.get("p") or first.get("symbol") or first.get("instrument")
        print(f"FIRST_OLYMPTRADE_ASSET={pair}")
        print(f"FIRST_OLYMPTRADE_ASSET_DATA={first}")

        await client.market.subscribe_ticks(str(pair))
        print(f"SUBSCRIBED_FIRST_ASSET={pair}")

        candles = await client.market.get_candles(str(pair), size=60, count=5)
        print(f"FIRST_ASSET_CANDLES={candles}")
    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(test_read_only_methods())
