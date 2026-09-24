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
        """Subscribe one pair to the authenticated Event-1 tick stream.

        The current connection has a small simultaneous-subscription capacity.
        Use only the verified Event-12 request and retry one transient rejection
        before returning control to the caller's rotation worker.
        """
        logger.info("Subscribing to ticks for %s...", pair)
        last_err = None
        for attempt in range(2):
            try:
                response = await self._client.send_request(
                    12,
                    [{"pair": pair}],
                    requires_response=True,
                    timeout=2.5,
                )
                err = response.get("err") if isinstance(response, dict) else None
                accepted = (
                    isinstance(response, dict)
                    and not err
                    and response.get("e") == 12
                )
                if accepted:
                    logger.info(
                        "Tick subscription event=12 accepted pair=%s attempt=%d",
                        pair, attempt + 1
                    )
                    return
                last_err = err or response
                logger.warning(
                    "Tick subscription event=12 rejected pair=%s attempt=%d err=%s",
                    pair, attempt + 1, last_err
                )
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:120]}"
                logger.warning(
                    "Tick subscription event=12 failed pair=%s attempt=%d error=%s",
                    pair, attempt + 1, last_err
                )
            if attempt == 0:
                await asyncio.sleep(0.35)
        raise RuntimeError(
            f"tick subscription rejected for {pair}: event 12 was rejected after 2 attempts"
        )

    async def unsubscribe_ticks(self, pair: str) -> None:
        logger.info(f"Unsubscribing from ticks for {pair}...")
        await self._client.send_request(13, [{"pair": pair}], requires_response=True)
        logger.info(f"Successfully sent tick unsubscription request for {pair} (event 13).")

    async def get_candles(
        self,
        pair: str,
        size: int,
        count: int,
        end_time: Optional[Union[datetime, int]] = None,
        solid: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch a deterministic number of candles using cursor-based pagination.

        The websocket candle endpoint is paged by the `to` timestamp; the
        caller-side `count` is not part of event 10's wire payload. Older
        versions of this wrapper accepted `count` but silently ignored it,
        which could leave callers with too little M1 history for analysis.
        """
        target = max(1, int(count or 1))

        if end_time is None:
            cursor_ts = int(time.time())
        elif isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            cursor_ts = int(end_time.timestamp())
        else:
            cursor_ts = int(end_time)

        candles: List[Dict[str, Any]] = []
        seen = set()

        # Typical server pages are much larger than this; the cap prevents a
        # malformed/empty endpoint from causing an unbounded request loop.
        max_pages = max(1, min(8, (target + 14) // 15))

        try:
            for page_no in range(1, max_pages + 1):
                response = await self._client.send_request(
                    10,
                    [{
                        "pair": pair,
                        "size": size,
                        "to": cursor_ts,
                        "solid": bool(solid),
                    }],
                    requires_response=True,
                )

                if not (
                    response
                    and isinstance(response.get("d"), list)
                    and response.get("e") in (10, 1003)
                ):
                    logger.error(
                        "Did not receive expected candle response "
                        "(e:10/e:1003) pair=%s page=%d response=%s",
                        pair, page_no, response,
                    )
                    break

                page = [
                    x for x in response["d"]
                    if isinstance(x, dict)
                    and ("time" in x or "t" in x)
                ]
                if not page:
                    break

                before = len(candles)
                for item in page:
                    try:
                        ts = float(item.get("time", item.get("t")))
                        if ts > 20_000_000_000:
                            ts /= 1000.0
                        key = int(ts // max(1, int(size)))
                    except (TypeError, ValueError):
                        continue
                    if key not in seen:
                        seen.add(key)
                        candles.append(item)

                if len(candles) >= target:
                    break

                # Move the cursor strictly before the oldest returned candle.
                valid_ts = []
                for item in page:
                    try:
                        ts = float(item.get("time", item.get("t")))
                        if ts > 20_000_000_000:
                            ts /= 1000.0
                        valid_ts.append(ts)
                    except (TypeError, ValueError):
                        pass

                if not valid_ts:
                    break

                oldest = min(valid_ts)
                next_cursor = int(oldest) - 1
                if next_cursor >= cursor_ts or len(candles) == before:
                    break
                cursor_ts = next_cursor

            if not candles:
                logger.error("CANDLE_FETCH_EMPTY pair=%s count=%d", pair, target)
                return None

            candles.sort(
                key=lambda x: float(x.get("time", x.get("t", 0)) or 0)
            )
            if len(candles) < target:
                logger.warning(
                    "CANDLE_FETCH_SHORT pair=%s requested=%d received=%d",
                    pair, target, len(candles),
                )
            return candles[-target:]

        except Exception as e:
            logger.error(
                "Failed to get candles for %s count=%d: %s",
                pair, target, e,
            )
            return None

    async def get_live_quote(self, pair: str) -> Optional[Dict[str, Any]]:
        """Fetch the broker's current, still-forming candle for a live quote.

        This is read-only market data from the same authenticated session. It is
        intentionally separate from closed-candle history used by indicators.
        """
        try:
            response = await self._client.send_request(
                10,
                [{"pair": pair, "size": 60, "to": int(time.time()), "solid": False}],
                requires_response=True,
            )
            if not (response and response.get("e") in (10, 1003)):
                logger.warning(
                    "LIVE_QUOTE_RESPONSE_REJECTED pair=%s response=%s",
                    pair, response
                )
                return None
            payload = response.get("d")
            items = payload if isinstance(payload, list) else [payload]
            candles=[]
            for item in items:
                if isinstance(item, dict) and isinstance(item.get("candles"), list):
                    candles.extend(x for x in item["candles"] if isinstance(x, dict))
                elif isinstance(item, dict) and any(
                    k in item for k in ("open","o","high","h","low","l","close","c")
                ):
                    candles.append(item)
            if not candles:
                logger.warning("LIVE_QUOTE_EMPTY pair=%s", pair)
                return None
            candles.sort(key=lambda x: float(x.get("time", x.get("t", 0)) or 0))
            current = candles[-1]
            price = current.get("close", current.get("c"))
            if price is None:
                return None
            return {
                "pair": pair,
                "price": float(price),
                "candle": current,
                "received_at": time.time(),
            }
        except Exception as e:
            logger.warning(
                "LIVE_QUOTE_FAILED pair=%s type=%s message=%s",
                pair, type(e).__name__, str(e)[:140]
            )
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
