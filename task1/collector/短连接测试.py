"""短连接检查：连接成功后接收约 3 秒，不保存数据。"""

import time
import websocket

from market_stream import connect, decode_message


def main():
    counts = {'depth': 0, 'aggTrade': 0, 'bookTicker': 0}
    reader = connect()
    deadline = time.monotonic() + 3
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            reader.connection.settimeout(remaining)
            try:
                raw = reader.receive()
            except websocket.WebSocketTimeoutException:
                break
            event = decode_message(raw)
            counts[event.kind] += 1
        print('短连接测试：接收约 3 秒；未保存数据。')
        for kind, count in counts.items():
            print(f'{kind}: {count} 条')
    finally:
        reader.close()


if __name__ == '__main__':
    main()
