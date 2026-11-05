"""把一条原始行情及接收信息写成 JSONL 的一行。"""

import json


def write_raw_message(file, message):
    record = {
        'sequence': message.sequence,
        'received_ns': message.received_ns,
        'monotonic_ns': message.monotonic_ns,
        'raw_text': message.raw_text,
    }
    file.write(json.dumps(record, ensure_ascii=False) + '\n')
