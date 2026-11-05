"""接收约 3 秒三路公开行情，保存原始消息；用于学习，不是正式采集器。"""

from pathlib import Path
import time

import websocket

from market_stream import connect, decode_message
from raw_recording import write_raw_message


def main():
    data_dir = Path.home() / 'Documents' / 'KID_Binance_练习数据'
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f'三路原始行情_{time.time_ns()}.jsonl'
    reader = connect()
    count = 0
    try:
        # x 表示只创建新文件；文件名冲突时报错，不覆盖已有数据。
        with path.open('x', encoding='utf-8') as file:
            print(f'保存到：{path}', flush=True)
            deadline = time.monotonic() + 3
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    reader.connection.settimeout(remaining)
                    try:
                        message = reader.receive()
                    except websocket.WebSocketTimeoutException:
                        break
                    # 先保存原文，再解析；即使解析失败也不会先丢掉这条消息。
                    write_raw_message(file, message)
                    count += 1
                    decode_message(message)
            finally:
                print(f'已写入 {count} 条；如有错误，请查看错误信息。', flush=True)
    finally:
        reader.close()


if __name__ == '__main__':
    main()
