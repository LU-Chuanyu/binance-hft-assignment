"""第一步：接收三种公开行情，保留原文，并在需要时解码。"""

from dataclasses import dataclass
import json
from pathlib import Path
import sys
import time

import websocket


URL = (
    'wss://stream.binance.com:443/stream?streams='
    'btcusdt@depth@100ms/btcusdt@aggTrade/btcusdt@bookTicker'
)
STREAM_KINDS = {
    'btcusdt@depth@100ms': 'depth',
    'btcusdt@aggTrade': 'aggTrade',
    'btcusdt@bookTicker': 'bookTicker',
}


@dataclass(frozen=True)
class RawMessage:
    sequence: int
    received_ns: int
    monotonic_ns: int
    raw_text: str


@dataclass(frozen=True)
class CombinedEvent:
    stream: str
    kind: str
    data: dict


class MarketStream:
    def __init__(self, connection):
        self.connection = connection
        self.sequence = 0

    def receive(self):
        # recv 返回后立刻取时间；此处尚未进行 JSON 解析。
        raw_text = self.connection.recv()
        received_ns = time.time_ns()
        monotonic_ns = time.monotonic_ns()
        if not isinstance(raw_text, str):
            raise ValueError('连接收到非文本消息')
        if not raw_text:
            raise ValueError('连接收到空消息')
        self.sequence += 1
        return RawMessage(self.sequence, received_ns, monotonic_ns, raw_text)

    def close(self):
        self.connection.close()


def decode_message(message):
    """拆开合并流外层的 stream 和 data；不假定所有事件都有 E。"""
    try:
        envelope = json.loads(message.raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError('行情消息不是有效 JSON') from exc
    if not isinstance(envelope, dict):
        raise ValueError('行情消息外层不是 JSON 对象')
    stream = envelope.get('stream')
    data = envelope.get('data')
    if not isinstance(stream, str) or stream not in STREAM_KINDS:
        raise ValueError(f'未知或缺失行情流名称: {stream!r}')
    if not isinstance(data, dict):
        raise ValueError('行情消息的 data 不是 JSON 对象')
    return CombinedEvent(stream, STREAM_KINDS[stream], data)


def connect():
    """建立一次连接，默认校验证书；失败交给调用者处理。"""
    options = {'timeout': 5}
    mac_certificate = Path('/etc/ssl/cert.pem')
    if sys.platform == 'darwin' and mac_certificate.is_file():
        options['sslopt'] = {'ca_certs': str(mac_certificate)}
    return MarketStream(websocket.create_connection(URL, **options))
