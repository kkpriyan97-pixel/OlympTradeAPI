# api/trade.py
import logging
import time
from typing import TYPE_CHECKING, Dict, Any, Optional, Literal, Union


from olymptrade_ws.core.protocol import get_current_timestamp_ms

if TYPE_CHECKING:
    from core.client import OlympTradeClient

logger = logging.getLogger(__name__)

class TradeAPI:
    def __init__(self, client: 'OlympTradeClient'):
        self._client = client

    async def place_trade(
        self,
        pair: str,
        amount: Union[int, float],
        direction: Literal["up", "down"],
        duration: int, # In seconds, needs confirmation based on API
        account_id: int,
        group: Literal["real", "demo"] = "demo",
        category: Literal["digital", "forex", "stocks"] = "digital", # Verify categories
        source: str = "platform", # Seems constant in logs
        position: int = 0, # Seems constant in logs
        is_flex: bool = False, # Needs confirmation
        risk_free_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Places a trade order.

        Args:
            pair: Asset pair (e.g., "EURUSD").
            amount: Trade amount.
            direction: "up" or "down".
            duration: Trade duration in seconds (needs confirmation).
            account_id: The ID of the account to trade on.
            group: "real" or "demo".
            category: Trade category (e.g., "digital").
            source: Source identifier (default "platform").
            position: Position identifier (default 0).
            is_flex: Flexibility flag (default False).
            risk_free_id: ID for risk-free trade, if applicable.

        Returns:
            The initial trade confirmation dictionary from the server (Event 23 response),
            or None on error. The final trade result comes via Event 26 callback.
        """
        event_code = 23
        timestamp_ms = get_current_timestamp_ms()
        
        data = [{
            "amount": amount,
            "dir": direction,
            "pair": pair,
            "cat": category,
            "pos": position,
            "source": source,
            "account_id": account_id,
            "group": group,
            "timestamp": timestamp_ms,
            "risk_free_id": risk_free_id,
            "is_flex": is_flex,
            "duration": duration 
        }]

        logger.info(f"Placing {group} {category} trade: {pair} {direction} ${amount} for {duration}s")
        
        try:
            response = await self._client.send_request(event_code, data, requires_response=True)
            if response and response.get("e") == event_code:
                 trade_details = response.get("d")
                 if isinstance(trade_details, list) and len(trade_details) > 0:
                     initial_status = trade_details[0]
                     trade_id = initial_status.get("id")
                     logger.info(f"Trade placed successfully (ID: {trade_id}). Initial status: {initial_status.get('status')}")
                     # Note: This is the *initial* response. Updates (e:21, e:22, e:26) come separately.
                     return initial_status # Return the first item in the data list
                 else:
                     logger.error(f"Unexpected data format in place trade response: {trade_details}")
                     return None
            else:
                 # The response might contain error information
                 error_msg = response.get("d") if response else "No response"
                 logger.error(f"Failed to place trade. Response: {error_msg}")
                 # You might want to parse the error message here if the structure is known
                 return None # Indicate failure
        except Exception as e:
            logger.error(f"Exception placing trade: {e}")
            return None

    async def place_order(
        self,
        pair: str,
        amount: Union[int, float],
        direction: Literal["up", "down"],
        duration: int,
        account_id: int,
        group: Literal["real", "demo"] = "demo",
        category: str = "digital",
        pos: int = 0,
        source: str = "platform",
        timestamp: Optional[int] = None,
        risk_free_id: Optional[int] = None,
        is_flex: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Places an order (trade) with the exact structure required by the API.
        Args:
            pair: Asset symbol (e.g., "LATAM_X").
            amount: Trade amount.
            direction: "up" or "down".
            duration: Trade duration in seconds.
            account_id: Your account id (demo or real).
            group: "demo" or "real".
            category: Trade category (default "digital").
            pos: Position (default 0).
            source: Source (default "platform").
            timestamp: Timestamp in ms (default: now).
            risk_free_id: Risk-free id (default None).
            is_flex: Flex trade (default False).
        Returns:
            The initial trade confirmation dictionary from the server (Event 23 response), or None on error.
        """
        event_code = 23
        if timestamp is None:
            from olymptrade_ws.core.protocol import get_current_timestamp_ms
            timestamp = get_current_timestamp_ms()
        data = [{
            "amount": amount,
            "dir": direction,
            "pair": pair,
            "cat": category,
            "pos": pos,
            "source": source,
            "account_id": account_id,
            "group": group,
            "timestamp": timestamp,
            "risk_free_id": risk_free_id,
            "is_flex": is_flex,
            "duration": duration
        }]
        logger.info(f"Placing {group} {category} order: {pair} {direction} ${amount} for {duration}s")
        try:
            response = await self._client.send_request(event_code, data, requires_response=True)
            if response and response.get("e") == event_code:
                trade_details = response.get("d")
                if isinstance(trade_details, list) and len(trade_details) > 0:
                    initial_status = trade_details[0]
                    trade_id = initial_status.get("id")
                    logger.info(f"Order placed successfully (ID: {trade_id}). Initial status: {initial_status.get('status')}")
                    return initial_status
                else:
                    logger.error(f"Unexpected data format in place order response: {trade_details}")
                    return None
            else:
                error_msg = response.get("d") if response else "No response"
                logger.error(f"Failed to place order. Response: {error_msg}")
                return None
        except Exception as e:
            logger.error(f"Exception placing order: {e}")
            return None

    async def get_open_trades(self, account_id: int, group: str = "demo") -> Optional[list[Dict[str, Any]]]:
         """
         Requests currently open trades.
         NOTE: Event 31 is used in logs, but the response 'd' is empty.
               This might require different parameters or the log missed the actual data push.
               Functionality needs verification.
         """
         logger.info("Requesting open trades (e:31) account_id=%s group=%s", account_id, group)
         event_code = 31
         data = [{"account_id": account_id, "group": group}]
         try:
             response = await self._client.send_request(event_code, data, requires_response=True)
             # The structure of a successful response with open trades is unknown from logs.
             # Assuming it returns a list in 'd' if successful.
             if response and response.get("e") == event_code:
                 open_trades = response.get("d")
                 if isinstance(open_trades, list):
                     logger.info(f"Received open trades response (contains {len(open_trades)} items - format TBD).")
                     return open_trades
                 else:
                      logger.error(f"Unexpected data format in open trades response: {open_trades}")
                      return None
             else:
                 logger.error(f"Did not receive expected open trades response (e:{event_code}). Got: {response}")
                 return None
         except Exception as e:
             logger.error(f"Failed to get open trades: {e}")
             return None

    async def place_flex_order(
        self,
        pair: str,
        amount: Union[int, float],
        direction: Literal["up", "down"],
        duration: int,
        account_id: int,
        group: Literal["real", "demo"] = "demo",
        source: str = "platform",
        pos: int = 0,
        timestamp: Optional[int] = None,
        risk_free_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Place a Flex trade using the broker Event-23 order envelope.

        Flex is selected with the is_flex flag. Result tracking uses Event-22
        and Event-26 callbacks; Event-31 is not a Flex visibility requirement.
        """
        event_code = 23
        if timestamp is None:
            timestamp = get_current_timestamp_ms()
        data = [{
            "amount": amount,
            "dir": direction,
            "pair": pair,
            "cat": "digital",
            "pos": pos,
            "source": source,
            "account_id": account_id,
            "group": group,
            "timestamp": timestamp,
            "risk_free_id": risk_free_id,
            "is_flex": True,
            "duration": duration,
        }]
        logger.info(
            "Placing %s FLEX order: %s %s $%s for %ss account_id=%s",
            group, pair, direction, amount, duration, account_id
        )
        try:
            response = await self._client.send_request(
                event_code, data, requires_response=True
            )
            if response and response.get("e") == event_code:
                details = response.get("d")
                if isinstance(details, list) and details:
                    initial = details[0]
                    logger.info(
                        "FLEX order accepted ID=%s status=%s",
                        initial.get("id"), initial.get("status")
                    )
                    return initial
                logger.error("FLEX order response had no trade details: %s", details)
                return None
            logger.error(
                "FLEX order rejected response=%s",
                response.get("d") if response else "No response"
            )
            return None
        except Exception as e:
            logger.error("Exception placing FLEX order: %s", e)
            return None

    # Add subscribe/unsubscribe for trade updates (e:21, e:22, e:26) if needed,
    # likely using the generic event 98 subscription mechanism.

    # For backward compatibility
    place_trade = place_order
