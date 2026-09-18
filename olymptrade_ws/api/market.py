# api/market.py
import asyncio
import logging
import time
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Union
from datetime import datetime, timezone

from olymptrade_ws.core.protocol import get_current_timestamp_ms
from olymptrade_ws.olympconfig import parameters as settings

if TYPE_CHECKING:
    from olymptrade_ws.core.client import OlympTradeClient

logger = logging.getLogger(__name__)

class MarketAPI:
    def __init__(self, client: 'OlympTradeClient'):
        self._client = client

    async def subscribe_ticks(self, pair: str) -> None:
        logger.info(f"Subscribing to ticks for {pair}...")
        await self._client.send_request(12, [{"pair": pair}], requires_response=True)
        await self._client.send_request(280, [{"pair": pair}], requires_response=True)
        logger.info(f"Successfully sent tick subscription requests for {pair}.")

    async def unsubscribe_ticks(self, pair: str) -> None:
        logger.info(f"Unsubscribing from ticks for {pair}...")
        await self._client.send_request(13, [{"pair": pair}], requires_response=True)
        await self._client.send_request(281, [{"pair": pair}], requires_response=True)
        logger.info(f"Successfully sent tick unsubscription requests for {pair}.")

    async def get_candles(self, pair: str, size: int, count: int, end_time: Optional[Union[datetime, int]] = None) -> Optional[List[Dict[str, Any]]]:
        if end_time is None:
            to_ts = int(time.time())
        elif isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            to_ts = int(end_time.timestamp())
        else:
            to_ts = int(end_time)

        try:
            response = await self._client.send_request(
                10,
                [{"pair": pair, "size": size, "to": to_ts, "solid": True}],
                requires_response=True,
            )
            if response and response.get("e") == 1003 and isinstance(response.get("d"), list):
                return response["d"]
            logger.error(f"Did not receive expected candle response (e:1003). Got: {response}")
        except Exception as e:
            logger.error(f"Failed to get candles for {pair}: {e}")
        return None

    async def get_profitability(self, account_id: int) -> Optional[List[Dict[str, Any]]]:
        try:
            response = await self._client.send_request(182, [{"account_id": account_id}], requires_response=True)
            if response and response.get("e") == 182 and isinstance(response.get("d"), list):
                return response["d"]
            logger.error(f"Did not receive expected profitability response (e:182). Got: {response}")
        except Exception as e:
            logger.error(f"Failed to get profitability: {e}")
        return None

    async def get_available_assets(self, account_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return asset records visible to the authenticated account, read-only."""
        account_id = account_id or self._client.account_id
        assets: List[Dict[str, Any]] = []

        if account_id:
            profit = await self.get_profitability(account_id)
            if isinstance(profit, list):
                assets.extend(x for x in profit if isinstance(x, dict))

        for message in self._client.get_cached_events():
            data = message.get("d") if isinstance(message, dict) else None
            if isinstance(data, list):
                assets.extend(
                    item for item in data
                    if isinstance(item, dict)
                    and any(k in item for k in ("pair", "p", "symbol", "instrument"))
                )
            elif isinstance(data, dict) and any(k in data for k in ("pair", "p", "symbol", "instrument")):
                assets.append(data)

        unique: Dict[str, Dict[str, Any]] = {}
        for item in assets:
            pair = item.get("pair") or item.get("p") or item.get("symbol") or item.get("instrument")
            if pair:
                unique[str(pair)] = item
        return list(unique.values())

    async def get_first_available_asset(self, account_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get the first authenticated asset available to the account, read-only."""
        assets = await self.get_available_assets(account_id)
        if not assets:
            logger.warning("No authenticated assets were returned.")
            return None
        first = assets[0]
        pair = first.get("pair") or first.get("p") or first.get("symbol") or first.get("instrument")
        logger.info(f"First authenticated asset: {pair}")
        return first

    async def select_asset(self, pair: str, category: str = "digital") -> Optional[Dict[str, Any]]:
        logger.info(f"Selecting asset {pair} (category: {category})...")
        try:
            response_select = await self._client.send_request(95, [{"cat": category, "pair": pair}], requires_response=True)
            if not (response_select and response_select.get("e") == 95):
                logger.error(f"Failed to get confirmation for asset selection (e:95): {response_select}")

            future = asyncio.get_running_loop().create_future()

            async def temp_strike_callback(message: Dict[str, Any]):
                strike_data_list = message.get("d", [])
                if isinstance(strike_data_list, list):
                    for item in strike_data_list:
                        if isinstance(item, dict) and (item.get("p") or item.get("pair")) == pair:
                            if not future.done():
                                future.set_result(item)
                            break

            self._client.register_callback(80, temp_strike_callback)
            try:
                for message in self._client.get_cached_events(80):
                    await temp_strike_callback(message)
                    if future.done():
                        break
                if not future.done():
                    return await asyncio.wait_for(future, timeout=settings.DEFAULT_RESPONSE_TIMEOUT)
                return future.result()
            finally:
                self._client.unregister_callback(80, temp_strike_callback)
        except Exception as e:
            logger.error(f"Failed during asset selection/strike retrieval for {pair}: {e}")
            return None
