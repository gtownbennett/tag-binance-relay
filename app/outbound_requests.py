"""Governed provider-aware HTTP helpers.

Every retry is charged, GETs are coalesced and cached for a bounded interval,
and a stale last-good response is returned only when a caller explicitly opts
in.  App-facing code receives a small failure classification rather than an
endpoint, credential-bearing query string, TLS detail, or exception dump.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

import httpx

from .terminal_usage import consume_request_budget, usage_governor


PROVIDER_HOST_MARKERS: tuple[tuple[str, str], ...] = (
    ("binance", "binance"),
    ("dexscreener", "dexscreener"),
    ("gateio", "gate"),
    ("gate.io", "gate"),
    ("mexc", "mexc"),
    ("bitget", "bitget"),
    ("bingx", "bingx"),
    ("coingecko", "coingecko"),
    ("geckoterminal", "geckoterminal"),
    ("publicnode", "bnb_rpc"),
    ("bnbchain", "bnb_rpc"),
    ("nodereal", "bnb_rpc"),
    ("frankfurter", "fx"),
    ("coinmarketcap", "coinmarketcap"),
    ("coinalyze", "coinalyze"),
    ("kucoin", "kucoin"),
    ("bybit", "bybit"),
    ("okx", "okx"),
)


def provider_for_url(url: str) -> str:
    host = (urlsplit(str(url)).hostname or "unknown").lower()
    for marker, provider in PROVIDER_HOST_MARKERS:
        if marker in host:
            return provider
    return "other"


def classify_failure(value: BaseException | str) -> str:
    text = str(value or "").lower()
    if "401" in text or "unauthorized" in text:
        return "unauthorized"
    if "403" in text or "451" in text or "forbidden" in text:
        return "unavailable"
    if "429" in text or "rate limit" in text or "daily_limit" in text:
        return "rate_limited"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    return "unavailable"


class OutboundUnavailable(RuntimeError):
    def __init__(self, provider: str, state: str) -> None:
        self.provider = provider
        self.state = state
        super().__init__(f"{provider} is temporarily {state.replace('_', ' ')}")


@dataclass(frozen=True)
class _CachedResponse:
    stored_at: float
    status_code: int
    content: bytes
    headers: dict[str, str]


_cache_lock = threading.Lock()
_response_cache: dict[str, _CachedResponse] = {}
_inflight_lock: asyncio.Lock | None = None
_inflight: dict[str, asyncio.Task[httpx.Response]] = {}


def _request_key(
    method: str, url: str, params: Mapping[str, Any] | None, body: Any,
    *, client_identity: int,
) -> str:
    basis = json.dumps(
        {
            "method": method.upper(),
            "url": str(url),
            "params": sorted((str(k), str(v)) for k, v in (params or {}).items()),
            "body": body,
            "client": client_identity,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _cached_response(
    key: str,
    *,
    method: str,
    url: str,
    max_age_seconds: float,
    last_good: bool,
) -> httpx.Response | None:
    if max_age_seconds <= 0:
        return None
    with _cache_lock:
        cached = _response_cache.get(key)
    if cached is None:
        return None
    age = max(0.0, time.monotonic() - cached.stored_at)
    if age > max_age_seconds:
        return None
    headers = dict(cached.headers)
    headers["X-TAG-Cache"] = "last-good" if last_good else "hit"
    headers["X-TAG-Cache-Age"] = f"{age:.3f}"
    usage_governor.cache(True)
    return httpx.Response(
        cached.status_code,
        content=cached.content,
        headers=headers,
        request=httpx.Request(method.upper(), url),
    )


def _store_response(key: str, response: httpx.Response) -> None:
    if isinstance(response, httpx.Response) and response.is_success:
        with _cache_lock:
            _response_cache[key] = _CachedResponse(
                stored_at=time.monotonic(),
                status_code=response.status_code,
                content=bytes(response.content),
                headers={str(k): str(v) for k, v in response.headers.items()},
            )


def _authorize(provider: str, job: str) -> None:
    consume_request_budget("external_request")
    allowed, reason = usage_governor.authorize_external(provider=provider, job=job)
    if not allowed:
        raise OutboundUnavailable(provider, reason or "rate_limited")


def _request_kwargs(
    *,
    params: Mapping[str, Any] | None,
    json_body: Any,
    headers: Mapping[str, str] | None,
    timeout: float | httpx.Timeout | None = None,
) -> dict[str, Any]:
    """Avoid empty keywords that small adapter and test clients may reject."""

    result: dict[str, Any] = {}
    if params is not None:
        result["params"] = params
    if json_body is not None:
        result["json"] = json_body
    if headers is not None:
        result["headers"] = headers
    if timeout is not None:
        result["timeout"] = timeout
    return result


async def governed_async_request(
    client: Any,
    method: str,
    url: str,
    *,
    provider: str | None = None,
    job: str = "unspecified",
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float | httpx.Timeout | None = None,
    cache_ttl_seconds: float = 0,
    last_good_max_age_seconds: float = 0,
    attempts: int = 1,
) -> httpx.Response:
    """Perform one bounded async request with coalescing and last-good fallback."""

    selected_provider = provider or provider_for_url(url)
    key = _request_key(method, url, params, json_body, client_identity=id(client))
    cached = _cached_response(
        key, method=method, url=url, max_age_seconds=cache_ttl_seconds, last_good=False
    )
    if cached is not None:
        return cached
    usage_governor.cache(False)

    async def perform() -> httpx.Response:
        last_error: BaseException | None = None
        request_kwargs = _request_kwargs(
            params=params, json_body=json_body, headers=headers, timeout=timeout
        )
        for attempt in range(min(2, max(1, int(attempts)))):
            try:
                _authorize(selected_provider, job)
                operation = getattr(client, method.lower(), None)
                if operation is not None:
                    response = await operation(url, **request_kwargs)
                else:
                    response = await client.request(method.upper(), url, **request_kwargs)
                if getattr(response, "status_code", None) != 304:
                    response.raise_for_status()
                _store_response(key, response)
                return response
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt + 1 < min(2, max(1, int(attempts))):
                    await asyncio.sleep(0.25 * (attempt + 1))
        fallback = _cached_response(
            key,
            method=method,
            url=url,
            max_age_seconds=last_good_max_age_seconds,
            last_good=True,
        )
        if fallback is not None:
            return fallback
        raise OutboundUnavailable(selected_provider, classify_failure(last_error or "unavailable"))

    global _inflight_lock
    if _inflight_lock is None:
        _inflight_lock = asyncio.Lock()
    async with _inflight_lock:
        task = _inflight.get(key)
        if task is None:
            task = asyncio.create_task(perform())
            _inflight[key] = task
    try:
        return await task
    finally:
        if task.done():
            async with _inflight_lock:
                if _inflight.get(key) is task:
                    _inflight.pop(key, None)


def governed_sync_request(
    client: Any,
    method: str,
    url: str,
    *,
    provider: str | None = None,
    job: str = "unspecified",
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float | httpx.Timeout | None = None,
    cache_ttl_seconds: float = 0,
    last_good_max_age_seconds: float = 0,
    attempts: int = 1,
) -> httpx.Response:
    """Synchronous counterpart used by bounded worker-thread collectors."""

    selected_provider = provider or provider_for_url(url)
    key = _request_key(method, url, params, json_body, client_identity=id(client))
    cached = _cached_response(
        key, method=method, url=url, max_age_seconds=cache_ttl_seconds, last_good=False
    )
    if cached is not None:
        return cached
    usage_governor.cache(False)
    last_error: BaseException | None = None
    request_kwargs = _request_kwargs(
        params=params, json_body=json_body, headers=headers, timeout=timeout
    )
    for _ in range(min(2, max(1, int(attempts)))):
        try:
            _authorize(selected_provider, job)
            operation = getattr(client, method.lower(), None)
            if operation is not None:
                response = operation(url, **request_kwargs)
            else:
                response = client.request(method.upper(), url, **request_kwargs)
            if getattr(response, "status_code", None) != 304:
                response.raise_for_status()
            _store_response(key, response)
            return response
        except Exception as exc:
            last_error = exc
    fallback = _cached_response(
        key,
        method=method,
        url=url,
        max_age_seconds=last_good_max_age_seconds,
        last_good=True,
    )
    if fallback is not None:
        return fallback
    raise OutboundUnavailable(selected_provider, classify_failure(last_error or "unavailable"))
