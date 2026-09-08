from __future__ import annotations

import asyncio
import json
from typing import Any

import websockets

from .config import DEFAULT_TIMEOUT, WS_URL


class DerivApiError(RuntimeError):
    """The API accepted the connection but rejected the request."""


class DerivTransportError(RuntimeError):
    """A request could not be completed after the configured retries."""


class DerivPublicClient:
    """Small, read-only client for one-shot Deriv public WebSocket requests."""

    def __init__(
        self,
        url: str = WS_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def _request_once(
        self, payload: dict[str, Any], expected_msg_type: str
    ) -> dict[str, Any]:
        async with websockets.connect(
            self.url,
            open_timeout=self.timeout,
            close_timeout=min(self.timeout, 10),
            ping_interval=20,
            ping_timeout=20,
            max_size=16 * 1024 * 1024,
        ) as websocket:
            await websocket.send(json.dumps(payload))
            while True:
                raw = await asyncio.wait_for(websocket.recv(), timeout=self.timeout)
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise DerivTransportError("Deriv returned a non-object JSON message.")
                if "error" in message:
                    raise DerivApiError(f"Deriv API error: {message['error']}")
                if message.get("msg_type") == expected_msg_type:
                    return message

    async def _request(
        self, payload: dict[str, Any], expected_msg_type: str
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                return await self._request_once(payload, expected_msg_type)
            except DerivApiError:
                raise
            except (
                asyncio.TimeoutError,
                json.JSONDecodeError,
                OSError,
                websockets.WebSocketException,
            ) as error:
                last_error = error
                if attempt == self.max_retries:
                    break
                await asyncio.sleep(self.retry_delay * (2**attempt))
        raise DerivTransportError(
            f"Request failed after {self.max_retries + 1} attempt(s): {last_error}"
        ) from last_error

    async def active_symbols(self) -> list[dict[str, Any]]:
        response = await self._request(
            {"active_symbols": "brief", "req_id": 1},
            expected_msg_type="active_symbols",
        )
        symbols = response.get("active_symbols", [])
        if not isinstance(symbols, list):
            raise DerivTransportError("Deriv returned an invalid active_symbols response.")
        return symbols

    async def ticks_history(
        self, symbol: str, count: int = 1000, end: str | int = "latest"
    ) -> dict[str, Any]:
        if not symbol or not symbol.replace("_", "").isalnum():
            raise ValueError("symbol must contain only letters, numbers, and underscores")
        if count <= 0:
            raise ValueError("count must be positive")
        if isinstance(end, int) and end < 0:
            raise ValueError("end epoch cannot be negative")
        return await self._request(
            {
                "ticks_history": symbol,
                "count": count,
                "end": end,
                "style": "ticks",
                "req_id": 2,
            },
            expected_msg_type="history",
        )
