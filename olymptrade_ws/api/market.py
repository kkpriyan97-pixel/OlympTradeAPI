# api/market.py
import asyncio
import logging
import time
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Union
from datetime import datetime, timezone

from olymptrade_ws.olympconfig import parameters as settings

if TYPE_CHECKING:
    from olymptrade_ws.core.client import OlympTradeClient

logger = logging.getLogger(__name__)

class MarketAPI:
    def __init__(self, client: 'OlympTradeClient'):
        self._client = client

    async def subscribe_ticks(self, pair: str) -> None:
        """Subscribe to the verified live quote stream (event 12).
        
        Event 280 is not a tick subscription on the current session and can
        return invalid_request even when event 12 succeeds, so it is not sent
        here.
        """
        logger.info(f"Subscribing to ticks for {pair}...")
        response = await self._client.send_request(
            12, [{"pair": pair}], requires_response=True, timeout=5
        )
        if isinstance(response, dict) and response.get("err"):
            raise RuntimeError(f"tick subscription rejected for {pair}: {response.get('err')}")
        logger.info(f"Tick subscription accepted for {pair}.")
    
    async def get_live_snapshot(self, pair: str) -> Optional[Dict[str, Any]]:
        """Read the freshest available short-interval candle as a quote snapshot.
        
        This is read-only market data. It is used when the unsolicited tick
        stream does not deliver a current quote for a qualified asset.
        """
        to_ts = int(time.time())
        try:
            response = await self._client.send_request(
                10,
                [{"pair": pair, "size": 5, "to": to_ts, "solid": False}],
                requires_response=True,
                timeout=3,
            )
            if not (isinstance(response, dict) and response.get("e") in (10, 1003)):
                return None
            data = response.get("d")
            candles = []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        candles.extend(item.get("candles", [])) if isinstance(item.get("candles"), list) else candles.append(item)
            if not candles:
                return None
            valid=[]
            for c in candles:
                if not isinstance(c, dict):
                    continue
                q=c.get("close", c.get("c"))
                ts=c.get("time", c.get("t"))
                try:
                    if q is None:
                        continue
                    valid.append((float(ts) if ts is not None else float(to_ts), float(q)))
                except Exception:
                    continue
            if not valid:
                return None
            ts,price=max(valid,key=lambda x:x[0])
            return {"pair":pair,"price":price,"timestamp":ts}
        except Exception as e:
            logger.debug(f"Live snapshot failed for {pair}: {e}")
            return None

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
            if response and isinstance(response.get("d"), list) and response.get("e") in (10, 1003):
                return response["d"]
            logger.error(f"Did not receive expected candle response (e:10/e:1003). Got: {response}")
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
        """Return only the assets reported by the authenticated account-scoped
        profitability/availability response (event 182).

        Do not merge the generic cached websocket event stream here. The cache can
        contain global/region/catalog instruments that are not part of the current
        account's visible trading universe.
        """
        account_id = account_id or self._client.account_id
        if not account_id:
            logger.warning("ACCOUNT_ASSET_API_MISSING_ACCOUNT_ID")
            return []

        profit = await self.get_profitability(account_id)
        if not isinstance(profit, list):
            logger.warning("ACCOUNT_ASSET_API_EMPTY account_id=%s", account_id)
            return []

        def pair_of(item: Dict[str, Any]) -> str:
            return str(
                item.get("pair")
                or item.get("p")
                or item.get("symbol")
                or item.get("instrument")
                or item.get("id")
                or ""
            )

        def explicit_unavailable(item: Dict[str, Any]) -> bool:
            if item.get("disabled") is True:
                return True
            if item.get("locked") is True or item.get("locked_trading") is True:
                return True
            for key in ("active", "available", "tradable", "is_active", "is_available", "is_tradable"):
                if key in item and item.get(key) is False:
                    return True
            status = str(item.get("status") or item.get("state") or "").strip().lower()
            return status in {"disabled", "locked", "inactive", "unavailable", "closed", "off"}

        def richness(item: Dict[str, Any]) -> int:
            fields = (
                "title", "display_name", "displayName", "name", "pair", "p", "symbol",
                "allowed_multiplicators", "multiplicator_suggestions",
                "default_multiplicator", "min_multiplicator", "max_multiplicator",
                "group", "group_view", "locked", "locked_trading",
                "locked_buy", "locked_sell", "disabled", "rank", "volatility",
                "sales_success_fee", "purchase_fee", "active", "available",
                "tradable", "is_active", "is_available", "is_tradable", "status",
            )
            return sum(1 for field in fields if field in item)

        unique: Dict[str, Dict[str, Any]] = {}
        rejected = 0
        for item in profit:
            if not isinstance(item, dict):
                continue
            pair = pair_of(item)
            if not pair:
                continue
            if explicit_unavailable(item):
                rejected += 1
                continue
            current = unique.get(pair)
            if current is None or richness(item) > richness(current):
                unique[pair] = item

        result = list(unique.values())
        logger.info(
            "ACCOUNT_ASSET_API_SOURCE account_id=%s event=182 raw=%d accepted=%d rejected=%d",
            account_id, len(profit), len(result), rejected
        )
        return result

    async def get_otc_assets(self, account_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return all currently exposed OTC assets from the authenticated read-only feed.

        OTC assets are kept separate from Flex instruments because Olymptrade documents
        OTC assets under Fixed Time (FT) mode. This method never places or modifies trades.
        """
        assets = await self.get_available_assets(account_id)
        otc: List[Dict[str, Any]] = []
        seen = set()
        for item in assets:
            pair = item.get("pair") or item.get("p") or item.get("symbol") or item.get("instrument") or item.get("id")
            if pair and "_OTC" in str(pair).upper():
                key = str(pair).upper()
                if key not in seen:
                    seen.add(key)
                    otc.append(item)
        return otc

    async def get_first_available_asset(self, account_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
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
