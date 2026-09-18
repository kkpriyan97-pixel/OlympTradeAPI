import asyncio
import os

from olymptrade_ws import OlympTradeClient


async def main():
    token = os.environ.get("OLYMPTRADE_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("Set OLYMPTRADE_ACCESS_TOKEN in the environment.")

    client = OlympTradeClient(access_token=token)
    await client.start()
    try:
        balance = await client.balance.get_last_balance()
        print(f"Balance: {balance}")
    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
