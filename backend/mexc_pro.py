from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode

import aiohttp
import websockets
from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory
from websockets.exceptions import ConnectionClosed, WebSocketException

from backend.config import Settings, reveal_secret

logger = logging.getLogger(__name__)


JsonDict = dict[str, Any]
MarketCallback = Callable[[JsonDict], Awaitable[None] | None]
PrivateCallback = Callable[[JsonDict], Awaitable[None] | None]


class MexcAPIError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload


class MexcAuthenticationError(MexcAPIError):
    pass


class MexcRateLimitError(MexcAPIError):
    def __init__(self, message: str, *, retry_after: float | None = None, status: int | None = None, payload: Any = None) -> None:
        super().__init__(message, status=status, payload=payload)
        self.retry_after = retry_after


class AsyncWeightBudget:
    def __init__(self, max_weight: int, window_seconds: float) -> None:
        if max_weight <= 0:
            raise ValueError("max_weight must be positive")
        self.max_weight = max_weight
        self.window_seconds = window_seconds
        self._events: deque[tuple[float, int]] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int = 1) -> None:
        if weight <= 0:
            weight = 1
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0][0] >= self.window_seconds:
                    self._events.popleft()
                used = sum(item_weight for _, item_weight in self._events)
                if used + weight <= self.max_weight:
                    self._events.append((now, weight))
                    return
                oldest = self._events[0][0] if self._events else now
                sleep_for = max(0.01, self.window_seconds - (now - oldest))
            await asyncio.sleep(sleep_for)


class MexcProtoDecoder:
    def __init__(self) -> None:
        self._pool = descriptor_pool.DescriptorPool()
        self._wrapper_cls = self._build_wrapper_class()

    def _build_wrapper_class(self) -> type[Any]:
        fd = descriptor_pb2.FileDescriptorProto()
        fd.name = "mexc_spot_ws_runtime.proto"
        fd.package = "mexc"
        fd.syntax = "proto3"

        def message(name: str) -> descriptor_pb2.DescriptorProto:
            msg = fd.message_type.add()
            msg.name = name
            return msg

        def add_field(
            msg: descriptor_pb2.DescriptorProto,
            name: str,
            number: int,
            field_type: int,
            *,
            label: int = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
            type_name: str | None = None,
            oneof_index: int | None = None,
        ) -> None:
            field_desc = msg.field.add()
            field_desc.name = name
            field_desc.number = number
            field_desc.label = label
            field_desc.type = field_type
            if type_name is not None:
                field_desc.type_name = type_name
            if oneof_index is not None:
                field_desc.oneof_index = oneof_index

        type_string = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        type_int64 = descriptor_pb2.FieldDescriptorProto.TYPE_INT64
        type_bool = descriptor_pb2.FieldDescriptorProto.TYPE_BOOL
        type_int32 = descriptor_pb2.FieldDescriptorProto.TYPE_INT32
        type_message = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        repeated = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED

        item = message("PublicLimitDepthV3ApiItem")
        add_field(item, "price", 1, type_string)
        add_field(item, "quantity", 2, type_string)

        depth = message("PublicLimitDepthsV3Api")
        add_field(depth, "asks", 1, type_message, label=repeated, type_name=".mexc.PublicLimitDepthV3ApiItem")
        add_field(depth, "bids", 2, type_message, label=repeated, type_name=".mexc.PublicLimitDepthV3ApiItem")
        add_field(depth, "eventType", 3, type_string)
        add_field(depth, "version", 4, type_string)
        add_field(depth, "lastOrderCreateTime", 5, type_int64)

        account = message("PrivateAccountV3Api")
        add_field(account, "vcoinName", 1, type_string)
        add_field(account, "coinId", 2, type_string)
        add_field(account, "balanceAmount", 3, type_string)
        add_field(account, "balanceAmountChange", 4, type_string)
        add_field(account, "frozenAmount", 5, type_string)
        add_field(account, "frozenAmountChange", 6, type_string)
        add_field(account, "type", 7, type_string)
        add_field(account, "time", 8, type_int64)

        order = message("PrivateOrdersV3Api")
        add_field(order, "id", 1, type_string)
        add_field(order, "clientId", 2, type_string)
        add_field(order, "price", 3, type_string)
        add_field(order, "quantity", 4, type_string)
        add_field(order, "amount", 5, type_string)
        add_field(order, "avgPrice", 6, type_string)
        add_field(order, "orderType", 7, type_int32)
        add_field(order, "tradeType", 8, type_int32)
        add_field(order, "isMaker", 9, type_bool)
        add_field(order, "remainAmount", 10, type_string)
        add_field(order, "remainQuantity", 11, type_string)
        add_field(order, "lastDealQuantity", 12, type_string)
        add_field(order, "cumulativeQuantity", 13, type_string)
        add_field(order, "cumulativeAmount", 14, type_string)
        add_field(order, "status", 15, type_int32)
        add_field(order, "createTime", 16, type_int64)
        add_field(order, "market", 17, type_string)
        add_field(order, "triggerType", 18, type_int32)
        add_field(order, "triggerPrice", 19, type_string)
        add_field(order, "state", 20, type_int32)
        add_field(order, "ocoId", 21, type_string)
        add_field(order, "routeFactor", 22, type_string)
        add_field(order, "symbolId", 23, type_string)
        add_field(order, "marketId", 24, type_string)
        add_field(order, "marketCurrencyId", 25, type_string)
        add_field(order, "currencyId", 26, type_string)

        deal = message("PrivateDealsV3Api")
        add_field(deal, "price", 1, type_string)
        add_field(deal, "quantity", 2, type_string)
        add_field(deal, "amount", 3, type_string)
        add_field(deal, "tradeType", 4, type_int32)
        add_field(deal, "isMaker", 5, type_bool)
        add_field(deal, "isSelfTrade", 6, type_bool)
        add_field(deal, "tradeId", 7, type_string)
        add_field(deal, "clientOrderId", 8, type_string)
        add_field(deal, "orderId", 9, type_string)
        add_field(deal, "feeAmount", 10, type_string)
        add_field(deal, "feeCurrency", 11, type_string)
        add_field(deal, "time", 12, type_int64)

        wrapper = message("PushDataV3ApiWrapper")
        wrapper.oneof_decl.add().name = "body"
        add_field(wrapper, "channel", 1, type_string)
        add_field(wrapper, "symbol", 3, type_string)
        add_field(wrapper, "symbolId", 4, type_string)
        add_field(wrapper, "createTime", 5, type_int64)
        add_field(wrapper, "sendTime", 6, type_int64)
        add_field(wrapper, "publicLimitDepths", 303, type_message, type_name=".mexc.PublicLimitDepthsV3Api", oneof_index=0)
        add_field(wrapper, "privateOrders", 304, type_message, type_name=".mexc.PrivateOrdersV3Api", oneof_index=0)
        add_field(wrapper, "privateDeals", 306, type_message, type_name=".mexc.PrivateDealsV3Api", oneof_index=0)
        add_field(wrapper, "privateAccount", 307, type_message, type_name=".mexc.PrivateAccountV3Api", oneof_index=0)

        file_descriptor = self._pool.AddSerializedFile(fd.SerializeToString())
        descriptor = self._pool.FindMessageTypeByName("mexc.PushDataV3ApiWrapper")
        if hasattr(message_factory, "GetMessageClass"):
            return message_factory.GetMessageClass(descriptor)
        return message_factory.MessageFactory(self._pool).GetPrototype(descriptor)

    def decode(self, payload: str | bytes) -> JsonDict:
        if isinstance(payload, str):
            return json.loads(payload)

        try:
            decoded_text = payload.decode("utf-8")
            return json.loads(decoded_text)
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass

        wrapper = self._wrapper_cls()
        wrapper.ParseFromString(payload)
        try:
            return json_format.MessageToDict(
                wrapper,
                preserving_proto_field_name=False,
                including_default_value_fields=False,
            )
        except TypeError:
            return json_format.MessageToDict(
                wrapper,
                preserving_proto_field_name=False,
                always_print_fields_with_no_presence=False,
            )


@dataclass(slots=True)
class WebSocketHealth:
    name: str
    connected: bool = False
    last_message_monotonic: float = 0.0
    last_connect_monotonic: float = 0.0
    reconnect_count: int = 0
    last_error: str | None = None
    channels: list[str] = field(default_factory=list)

    def age_seconds(self) -> float:
        if not self.last_connect_monotonic:
            return 0.0
        return max(0.0, time.monotonic() - self.last_connect_monotonic)

    def stale_seconds(self) -> float:
        if not self.last_message_monotonic:
            return 0.0
        return max(0.0, time.monotonic() - self.last_message_monotonic)

    def as_dict(self) -> JsonDict:
        return {
            "name": self.name,
            "connected": self.connected,
            "last_message_age_seconds": round(self.stale_seconds(), 3),
            "connection_age_seconds": round(self.age_seconds(), 3),
            "reconnect_count": self.reconnect_count,
            "last_error": self.last_error,
            "channels": self.channels,
        }


class MexcRESTClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._api_key = reveal_secret(settings.mexc_api_key)
        self._secret_key = reveal_secret(settings.mexc_secret_key)
        self._session: aiohttp.ClientSession | None = None
        self._budget = AsyncWeightBudget(settings.rest_rate_limit_weight_per_10s, 10.0)

    async def __aenter__(self) -> "MexcRESTClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.settings.rest_timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout, raise_for_status=False)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def payload_sha256(params: dict[str, Any]) -> str:
        encoded = urlencode([(key, value) for key, value in params.items() if value is not None], doseq=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _ensure_credentials(self) -> tuple[str, str]:
        if not self._api_key or not self._secret_key:
            raise MexcAuthenticationError("MEXC API credentials are required for this endpoint")
        return self._api_key, self._secret_key

    def _signed_params(self, params: dict[str, Any] | None) -> dict[str, Any]:
        _, secret_key = self._ensure_credentials()
        signed = dict(params or {})
        signed.setdefault("recvWindow", self.settings.recv_window_ms)
        signed["timestamp"] = int(time.time() * 1000)
        query_string = urlencode([(key, value) for key, value in signed.items() if value is not None], doseq=True)
        signature = hmac.new(secret_key.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()
        signed["signature"] = signature
        return signed

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        auth_required: bool = False,
        weight: int = 1,
    ) -> JsonDict | list[Any]:
        await self.start()
        assert self._session is not None
        await self._budget.acquire(weight)

        final_params = self._signed_params(params) if signed else dict(params or {})
        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "ProjectMile3-MEXC-Quant/1.0",
            "X-LOCAL-PAYLOAD-SHA256": self.payload_sha256(final_params),
        }
        if signed or auth_required:
            api_key, _ = self._ensure_credentials()
            headers["X-MEXC-APIKEY"] = api_key

        url = f"{self.settings.rest_base_url.rstrip('/')}{path}"
        method_upper = method.upper()
        try:
            if method_upper == "GET":
                response = await self._session.request(method_upper, url, params=final_params, headers=headers)
            else:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                response = await self._session.request(method_upper, url, data=urlencode(final_params), headers=headers)
        except aiohttp.ClientError as exc:
            raise MexcAPIError(f"MEXC network error: {exc}") from exc

        text = await response.text()
        try:
            payload: JsonDict | list[Any] = json.loads(text) if text else {}
        except json.JSONDecodeError:
            payload = {"raw": text}

        if response.status in (418, 429):
            retry_after_header = response.headers.get("Retry-After")
            retry_after = float(retry_after_header) if retry_after_header else None
            raise MexcRateLimitError(
                f"MEXC rate limit response {response.status}",
                retry_after=retry_after,
                status=response.status,
                payload=payload,
            )
        if response.status in (401, 403):
            raise MexcAuthenticationError(f"MEXC authentication failed with status {response.status}", status=response.status, payload=payload)
        if response.status >= 400:
            raise MexcAPIError(f"MEXC REST error {response.status}: {payload}", status=response.status, payload=payload)
        if isinstance(payload, dict) and str(payload.get("code", "0")) not in ("0", "200", "") and "msg" in payload:
            raise MexcAPIError(f"MEXC API rejected request: {payload}", status=response.status, payload=payload)
        return payload

    async def server_time(self) -> JsonDict:
        payload = await self.request("GET", "/api/v3/time", weight=1)
        return payload if isinstance(payload, dict) else {"serverTime": None}

    async def exchange_info(self, symbol: str | None = None) -> JsonDict:
        params = {"symbol": symbol} if symbol else None
        payload = await self.request("GET", "/api/v3/exchangeInfo", params=params, weight=10)
        return payload if isinstance(payload, dict) else {"symbols": payload}

    async def ticker_price(self, symbol: str) -> JsonDict:
        payload = await self.request("GET", "/api/v3/ticker/price", params={"symbol": symbol}, weight=1)
        return payload if isinstance(payload, dict) else {"symbol": symbol, "price": None}

    async def depth_snapshot(self, symbol: str, limit: int = 5) -> JsonDict:
        payload = await self.request("GET", "/api/v3/depth", params={"symbol": symbol, "limit": limit}, weight=1)
        return payload if isinstance(payload, dict) else {"bids": [], "asks": []}

    async def klines(
        self,
        symbol: str,
        interval: str,
        *,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[list[Any]]:
        bounded_limit = min(500, max(1, int(limit)))
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": bounded_limit,
        }
        payload = await self.request("GET", "/api/v3/klines", params=params, weight=1)
        return payload if isinstance(payload, list) else []

    async def account_information(self) -> JsonDict:
        payload = await self.request("GET", "/api/v3/account", signed=True, weight=10)
        return payload if isinstance(payload, dict) else {"balances": []}

    async def create_listen_key(self) -> str:
        payload = await self.request("POST", "/api/v3/userDataStream", auth_required=True, weight=1)
        if not isinstance(payload, dict) or not payload.get("listenKey"):
            raise MexcAPIError(f"MEXC listenKey response did not include listenKey: {payload}")
        return str(payload["listenKey"])

    async def keepalive_listen_key(self, listen_key: str) -> None:
        await self.request("PUT", "/api/v3/userDataStream", params={"listenKey": listen_key}, auth_required=True, weight=1)

    async def close_listen_key(self, listen_key: str) -> None:
        await self.request("DELETE", "/api/v3/userDataStream", params={"listenKey": listen_key}, auth_required=True, weight=1)

    async def place_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | None = None,
        quote_order_qty: Decimal | None = None,
        price: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> JsonDict:
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type.upper(),
            "quantity": str(quantity) if quantity is not None else None,
            "quoteOrderQty": str(quote_order_qty) if quote_order_qty is not None else None,
            "price": str(price) if price is not None else None,
            "newClientOrderId": client_order_id,
        }
        payload = await self.request("POST", "/api/v3/order", params=params, signed=True, weight=1)
        return payload if isinstance(payload, dict) else {"order": payload}

    async def cancel_order(self, *, symbol: str, order_id: str | None = None, client_order_id: str | None = None) -> JsonDict:
        params = {"symbol": symbol, "orderId": order_id, "origClientOrderId": client_order_id}
        payload = await self.request("DELETE", "/api/v3/order", params=params, signed=True, weight=1)
        return payload if isinstance(payload, dict) else {"order": payload}


class MexcWebSocketSupervisor:
    def __init__(
        self,
        settings: Settings,
        rest_client: MexcRESTClient,
        *,
        on_market_data: MarketCallback,
        on_private_data: PrivateCallback,
    ) -> None:
        self.settings = settings
        self.rest_client = rest_client
        self.on_market_data = on_market_data
        self.on_private_data = on_private_data
        self.decoder = MexcProtoDecoder()
        self.public_health = WebSocketHealth(name="public")
        self.private_health = WebSocketHealth(name="private")
        self._listen_key: str | None = None

    def health(self) -> JsonDict:
        return {
            "public": self.public_health.as_dict(),
            "private": self.private_health.as_dict(),
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        tasks = [asyncio.create_task(self._supervise_public(stop_event), name="mexc-public-ws")]
        if self.settings.has_mexc_credentials:
            tasks.append(asyncio.create_task(self._supervise_private(stop_event), name="mexc-private-ws"))
            tasks.append(asyncio.create_task(self._listen_key_keepalive(stop_event), name="mexc-listen-key-keepalive"))
        await stop_event.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._listen_key:
            try:
                await self.rest_client.close_listen_key(self._listen_key)
            except Exception:
                logger.debug("Failed to close MEXC listenKey during shutdown", exc_info=True)

    async def _supervise_public(self, stop_event: asyncio.Event) -> None:
        channels = [f"spot@public.limit.depth.v3.api.pb@{symbol}@5" for symbol in self.settings.trading_symbols]
        self.public_health.channels = channels
        await self._supervise_connection(
            stop_event=stop_event,
            health=self.public_health,
            url=self.settings.ws_base_url,
            channels=channels,
            callback=self.on_market_data,
        )

    async def _supervise_private(self, stop_event: asyncio.Event) -> None:
        channels = [
            "spot@private.account.v3.api.pb",
            "spot@private.orders.v3.api.pb",
            "spot@private.deals.v3.api.pb",
        ]
        self.private_health.channels = channels
        while not stop_event.is_set():
            if self._listen_key is None:
                self._listen_key = await self.rest_client.create_listen_key()
            private_url = f"{self.settings.ws_base_url}?listenKey={self._listen_key}"
            await self._supervise_connection(
                stop_event=stop_event,
                health=self.private_health,
                url=private_url,
                channels=channels,
                callback=self.on_private_data,
            )

    async def _supervise_connection(
        self,
        *,
        stop_event: asyncio.Event,
        health: WebSocketHealth,
        url: str,
        channels: list[str],
        callback: MarketCallback | PrivateCallback,
    ) -> None:
        backoff = self.settings.ws_initial_backoff_seconds
        while not stop_event.is_set():
            try:
                await self._connection_once(stop_event, health, url, channels, callback)
                backoff = self.settings.ws_initial_backoff_seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health.connected = False
                health.last_error = f"{type(exc).__name__}: {exc}"
                health.reconnect_count += 1
                jitter = random.uniform(0.0, min(backoff, 3.0))
                sleep_for = min(self.settings.ws_max_backoff_seconds, backoff + jitter)
                logger.warning("MEXC websocket %s disconnected: %s; reconnecting in %.2fs", health.name, exc, sleep_for)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass
                backoff = min(self.settings.ws_max_backoff_seconds, backoff * 2)

    async def _connection_once(
        self,
        stop_event: asyncio.Event,
        health: WebSocketHealth,
        url: str,
        channels: list[str],
        callback: MarketCallback | PrivateCallback,
    ) -> None:
        async with websockets.connect(url, ping_interval=None, close_timeout=5, max_queue=2048) as ws:
            now = time.monotonic()
            health.connected = True
            health.last_error = None
            health.last_connect_monotonic = now
            health.last_message_monotonic = now
            await self._subscribe(ws, channels)
            heartbeat_task = asyncio.create_task(self._heartbeat(ws, stop_event), name=f"{health.name}-heartbeat")
            try:
                while not stop_event.is_set():
                    if time.monotonic() - health.last_connect_monotonic >= self.settings.ws_max_connection_age_seconds:
                        raise TimeoutError("websocket reached maximum connection age")
                    if time.monotonic() - health.last_message_monotonic >= self.settings.ws_stale_timeout_seconds:
                        raise TimeoutError("websocket stale timeout")
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    except ConnectionClosed as exc:
                        raise WebSocketException(f"connection closed: {exc}") from exc
                    health.last_message_monotonic = time.monotonic()
                    decoded = self.decoder.decode(message)
                    if self._is_control_message(decoded):
                        continue
                    result = callback(decoded)
                    if asyncio.iscoroutine(result):
                        await result
            finally:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
                health.connected = False

    async def _subscribe(self, ws: Any, channels: list[str]) -> None:
        request = {"method": "SUBSCRIPTION", "params": channels}
        await ws.send(json.dumps(request, separators=(",", ":")))

    async def _heartbeat(self, ws: Any, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(self.settings.ws_ping_interval_seconds)
            try:
                await ws.send(json.dumps({"method": "PING"}, separators=(",", ":")))
            except Exception:
                return

    async def _listen_key_keepalive(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=25 * 60)
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                return
            if self._listen_key:
                try:
                    await self.rest_client.keepalive_listen_key(self._listen_key)
                except MexcAPIError:
                    logger.warning("MEXC listenKey keepalive failed; rotating listenKey", exc_info=True)
                    self._listen_key = None

    @staticmethod
    def _is_control_message(message: JsonDict) -> bool:
        msg = str(message.get("msg") or message.get("message") or "").upper()
        method = str(message.get("method") or "").upper()
        channel = str(message.get("channel") or "").upper()
        return method in {"PONG", "PING"} or msg in {"PONG", "SUCCESS"} or channel in {"PONG", "PING"}
