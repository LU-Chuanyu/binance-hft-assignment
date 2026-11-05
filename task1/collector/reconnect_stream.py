"""连接管理：将连接、消息和断线暴露为有顺序的事件。

连接恢复不代表断线期间的数据完整；消费者必须重建盘口并检查成交缺口。
"""

from dataclasses import dataclass, replace
import json
import threading
import time

import websocket

from market_stream import connect


@dataclass(frozen=True)
class StreamOpened:
    generation: int
    reconnected: bool
    requires_resync: bool
    observed_ns: int


@dataclass(frozen=True)
class StreamMessage:
    generation: int
    message: object


@dataclass(frozen=True)
class StreamLost:
    generation: int
    observed_ns: int
    reason: str
    requires_resync: bool


class ReconnectExhausted(RuntimeError):
    """有限次连接尝试后仍无法建立连接。"""


class StreamClosed(RuntimeError):
    """主动关闭连接后的预期停止。"""


class ReconnectingStream:
    def __init__(self, connector=connect, *, idle_timeout=30, max_age=23 * 3600,
                 max_attempts=5, delays=(0.25, 0.5, 1, 2, 4),
                 monotonic=time.monotonic, wall_ns=time.time_ns, wait=None):
        if idle_timeout <= 0 or max_age <= 0 or max_attempts < 1 or not delays:
            raise ValueError('连接超时、时长和重试参数无效')
        self.connector = connector
        self.idle_timeout = idle_timeout
        self.max_age = max_age
        self.max_attempts = max_attempts
        self.delays = delays
        self.monotonic = monotonic
        self.wall_ns = wall_ns
        self._stop = threading.Event()
        self.wait = wait or self._stop.wait
        self._lock = threading.Lock()
        self._reader = None
        self._opened_at = None
        self._backoff_index = 0
        self.generation = 0
        self.sequence = 0

    def _connect(self):
        last_error = None
        for attempt in range(self.max_attempts):
            if self._stop.is_set():
                raise StreamClosed('stream is closed')
            if self.generation or attempt:
                delay = self.delays[min(self._backoff_index, len(self.delays) - 1)]
                self._backoff_index += 1
                if self.wait(delay) or self._stop.is_set():
                    raise StreamClosed('stream is closed')
            try:
                reader = self.connector()
                try:
                    reader.connection.settimeout(self.idle_timeout)
                except Exception:
                    reader.close()
                    raise
            except websocket.WebSocketBadStatusException:
                # 451/限流等握手响应不绕过，也不盲目快速重试。
                raise
            except (websocket.WebSocketException, OSError, TimeoutError) as exc:
                last_error = exc
                continue
            with self._lock:
                if self._stop.is_set():
                    reader.close()
                    raise StreamClosed('stream is closed')
                self._reader = reader
                self.generation += 1
                self._opened_at = self.monotonic()
            return StreamOpened(self.generation, self.generation > 1, True, self.wall_ns())
        raise ReconnectExhausted(f'连接尝试 {self.max_attempts} 次失败: {last_error}') from last_error

    def _lost(self, reason):
        observed_ns = self.wall_ns()
        with self._lock:
            reader, self._reader = self._reader, None
        if reader is not None:
            reader.close()
        return StreamLost(self.generation, observed_ns, reason, True)

    def next_event(self):
        if self._stop.is_set():
            raise StreamClosed('stream is closed')
        with self._lock:
            reader = self._reader
        if reader is None:
            return self._connect()
        if self.monotonic() - self._opened_at >= self.max_age:
            return self._lost('connection_age_limit')
        try:
            message = reader.receive()
        except (websocket.WebSocketException, OSError, TimeoutError) as exc:
            if self._stop.is_set():
                raise StreamClosed('stream is closed') from exc
            return self._lost(str(exc) or type(exc).__name__)
        except ValueError as exc:
            # websocket-client 在收到 close 帧后 recv() 返回空串；MarketStream
            # 将其判为 ValueError。仅在底层明确已关闭时转换为断线事件。
            if getattr(reader.connection, 'connected', None) is False:
                if str(exc) != '连接收到空消息':
                    raise
                if self._stop.is_set():
                    raise StreamClosed('stream is closed') from exc
                return self._lost(str(exc))
            raise

        # 停机通知是连接事件，行情接收编号应保持连续。只有候选通知需要解码。
        if '!serverShutdown' in message.raw_text:
            try:
                notice = json.loads(message.raw_text)
            except json.JSONDecodeError:
                notice = None  # 无效消息仍由行情解码器报告。
            if isinstance(notice, dict) and notice.get('stream') == '!serverShutdown':
                data = notice.get('data')
                if isinstance(data, dict) and data.get('e') == 'serverShutdown':
                    return self._lost('server_shutdown')

        # 如果 receive 已成功返回原始消息，即便关闭刚刚发生，也必须把
        # 它交给调用方排队；否则截止边界上收到的数据会被悄悄丢掉。
        self.sequence += 1
        self._backoff_index = 0
        return StreamMessage(self.generation, replace(message, sequence=self.sequence))

    def close(self):
        self._stop.set()
        with self._lock:
            reader, self._reader = self._reader, None
        if reader is not None:
            reader.close()
