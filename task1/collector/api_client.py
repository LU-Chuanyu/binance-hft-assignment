"""共享的 Binance 现货公开 REST 客户端；进程外同 IP 流量无法完全感知。"""

from collections import deque
from dataclasses import dataclass
from http.client import HTTPException, HTTPSConnection
from io import BytesIO
import json
import math
from pathlib import Path
import ssl
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import getproxies, proxy_bypass, urlopen


API_HOST = 'api.binance.com'


@dataclass(frozen=True)
class ApiResult:
    data: object
    headers: dict
    used_weight_1m: int | None
    sent_wall_ns: int
    received_wall_ns: int
    sent_monotonic_ns: int
    received_monotonic_ns: int
    connection_reused: bool | None = None


class RateLimitError(RuntimeError):
    def __init__(self, status, retry_after):
        self.status = status
        self.retry_after = retry_after
        super().__init__(f'Binance HTTP {status}; Retry-After={retry_after}')


class RateLimitWait(RuntimeError):
    def __init__(self, remaining):
        self.remaining = remaining
        super().__init__('等待时间未知，需要人工处理' if remaining is None else
                         f'请求暂缓，需要等待 {remaining:.3f} 秒')


def _header(headers, name):
    if headers is None:
        return None
    name = name.lower()
    return next((str(value) for key, value in headers.items() if key.lower() == name), None)


class PublicAPIClient:
    """一个实例供快照、成交和校时共用；序列化请求并保守控制本地预算。

    429/418 会保留限制时间并把异常交给调用方，不自动重发失败请求。
    当需要的等待超过 max_wait，抛 RateLimitWait，调用方稍后再试。
    启用 reuse_connections 时，直接连接会尝试复用 HTTPS 套接字。
    """

    def __init__(self, *, local_budget=600, ip_soft_limit=5000, timeout=10,
                 max_wait=5, transport=urlopen, monotonic=time.monotonic,
                 sleep=time.sleep, wall_ns=time.time_ns, monotonic_ns=time.monotonic_ns,
                 reuse_connections=False):
        if (type(local_budget) is not int or local_budget < 1 or
                type(ip_soft_limit) is not int or ip_soft_limit < 1 or
                timeout <= 0 or max_wait < 0):
            raise ValueError('请求预算和超时参数不合法')
        self.local_budget = local_budget
        self.ip_soft_limit = ip_soft_limit
        self.timeout = timeout
        self.max_wait = max_wait
        self.transport = transport
        self.monotonic = monotonic
        self.sleep = sleep
        self.wall_ns = wall_ns
        self.monotonic_ns = monotonic_ns
        self._lock = threading.Lock()
        self._closed = False
        self._recent = deque()
        self._blocked_until = 0.0
        self._halted = False
        self.last_weight_1m = None
        ca_file = Path('/etc/ssl/cert.pem')
        self.context = ssl.create_default_context(
            cafile=str(ca_file) if sys.platform == 'darwin' and ca_file.is_file() else None)
        self._connection = None
        if reuse_connections and transport is urlopen:
            # 代理流量继续由 urllib 处理，包括系统代理和代理认证。
            if not getproxies().get('https') or proxy_bypass(API_HOST):
                self._connection = HTTPSConnection(API_HOST, timeout=timeout, context=self.context)

    def close(self):
        with self._lock:
            self._closed = True
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def _wait_available(self, weight):
        if weight > self.local_budget:
            raise ValueError('单次请求权重大于本地每分钟预算')
        wait_started = self.monotonic()
        while True:
            if self._halted:
                raise RateLimitWait(None)
            now = self.monotonic()
            while self._recent and self._recent[0][0] <= now - 60:
                self._recent.popleft()
            delay = max(0.0, self._blocked_until - now)
            if sum(item[1] for item in self._recent) + weight > self.local_budget:
                delay = max(delay, self._recent[0][0] + 60 - now)
            if delay <= 0:
                return
            if self.monotonic() - wait_started + delay > self.max_wait:
                raise RateLimitWait(delay)
            self.sleep(delay)

    def _update_header(self, headers):
        value = _header(headers, 'X-MBX-USED-WEIGHT-1M')
        try:
            used = int(value) if value is not None else None
        except ValueError:
            used = None
        if used is not None and used >= 0:
            self.last_weight_1m = used
            if used >= self.ip_soft_limit:
                # 这是保守自限：不假定交易所分钟窗口的具体边界。
                self._blocked_until = max(self._blocked_until, self.monotonic() + 60)
            return used
        return None

    def _reserve(self, weight):
        self._wait_available(weight)
        # 即使未收到响应，对方也可能已收到请求。
        self._recent.append((self.monotonic(), weight))

    def _open_persistent(self, target, weight):
        connection = self._connection
        reused = connection.sock is not None
        try:
            connection.request('GET', target)
            return connection.getresponse(), reused
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, ssl.SSLEOFError):
            if not reused:
                raise
        # 已被对端关闭的旧连接最多重连一次，且这次请求也需要预算。
        connection.close()
        self._reserve(weight)
        connection.request('GET', target)
        return connection.getresponse(), False

    def _request(self, path, params, weight):
        query = urlencode(params)
        target = path + ('?' + query if query else '')
        url = f'https://{API_HOST}{target}'
        with self._lock:
            if self._closed:
                raise RuntimeError('REST 客户端已关闭')
            self._reserve(weight)
            sent_wall, sent_mono = self.wall_ns(), self.monotonic_ns()
            reused = None
            try:
                if self._connection is None:
                    response = self.transport(url, timeout=self.timeout, context=self.context)
                else:
                    response, reused = self._open_persistent(target, weight)
                with response as response:
                    http_error = reused is not None and not 200 <= response.status < 300
                    try:
                        payload = response.read()
                    except (OSError, HTTPException):
                        if not http_error:
                            raise
                        # 错误正文中断时，已收到的状态和限流响应头仍然有效。
                        self._connection.close()
                        payload = b''
                    received_wall, received_mono = self.wall_ns(), self.monotonic_ns()
                    headers = dict(response.headers.items())
                if http_error:
                    raise HTTPError(url, response.status, response.reason, response.headers, BytesIO(payload))
            except HTTPError as exc:
                self._update_header(exc.headers)
                if exc.code not in (418, 429):
                    raise
                header = _header(exc.headers, 'Retry-After')
                try:
                    retry = float(header) if header is not None else None
                    if retry is not None and (not math.isfinite(retry) or retry < 0):
                        retry = None
                except ValueError:
                    retry = None
                if retry is None:
                    self._halted = True  # 未知等待时长，不能猜测何时恢复。
                else:
                    self._blocked_until = max(self._blocked_until, self.monotonic() + retry)
                raise RateLimitError(exc.code, retry) from exc
            except Exception:
                if self._connection is not None:
                    self._connection.close()
                raise
            used = self._update_header(headers)
            data = json.loads(payload)
            return ApiResult(data, headers, used, sent_wall, received_wall, sent_mono, received_mono, reused)

    def server_time(self):
        return self._request('/api/v3/time', {}, 1)

    def depth(self, *, symbol='BTCUSDT', limit=1000):
        if type(limit) is not int or not 1 <= limit <= 5000:
            raise ValueError('盘口快照 limit 须为 1 到 5000')
        weight = 5 if limit <= 100 else 25 if limit <= 500 else 50 if limit <= 1000 else 250
        return self._request('/api/v3/depth', {'symbol': symbol, 'limit': limit}, weight)

    def aggtrades(self, from_id, limit, *, symbol='BTCUSDT'):
        if type(from_id) is not int or from_id < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError('聚合成交编号或 limit 不合法')
        return self._request('/api/v3/aggTrades', {'symbol': symbol, 'fromId': from_id, 'limit': limit}, 4)
