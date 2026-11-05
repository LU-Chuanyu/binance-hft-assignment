"""离线读回教学短录，区分三类消息，并检查本地编号。"""

import argparse
import json

from market_stream import RawMessage, decode_message


def summarize(file):
    counts = {'depth': 0, 'aggTrade': 0, 'bookTicker': 0}
    examples = {}
    sequence_gaps = []
    expected = 1

    for line_number, line in enumerate(file, 1):
        try:
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError('记录不是 JSON 对象')
            sequence = record['sequence']
            received_ns = record['received_ns']
            monotonic_ns = record['monotonic_ns']
            raw_text = record['raw_text']
            if any(type(n) is not int for n in (sequence, received_ns, monotonic_ns)):
                raise ValueError('编号或时间不是整数')
            if not isinstance(raw_text, str):
                raise ValueError('raw_text 不是文本')
            message = RawMessage(sequence, received_ns, monotonic_ns, raw_text)
            event = decode_message(message)
        except (ValueError, KeyError) as exc:
            raise ValueError(f'第 {line_number} 行无法读取：{exc}') from exc

        if sequence != expected:
            sequence_gaps.append((line_number, expected, sequence))
        expected = sequence + 1
        counts[event.kind] += 1
        if event.kind not in examples:
            examples[event.kind] = {'sequence': sequence, 'data': event.data}

    return {'counts': counts, 'examples': examples, 'sequence_gaps': sequence_gaps}


def main():
    parser = argparse.ArgumentParser(description='离线读取三路原始行情 JSONL')
    parser.add_argument('path', help='要读取的 JSONL 文件路径')
    args = parser.parse_args()
    with open(args.path, encoding='utf-8') as file:
        summary = summarize(file)

    print(f'文件：{args.path}')
    total = sum(summary['counts'].values())
    print(f'共 {total} 条')
    for kind, count in summary['counts'].items():
        print(f'{kind}: {count} 条')
    if total == 0:
        print('文件没有记录，无法检查本地编号。')
    elif summary['sequence_gaps']:
        for line_number, expected, actual in summary['sequence_gaps']:
            print(f'第 {line_number} 行本地编号不连续：期望 {expected}，实际 {actual}')
    else:
        print('本地 sequence 从 1 开始连续；这不能证明 Binance 数据没有遗漏。')

    fields = {
        'depth': ('U', 'u'),
        'aggTrade': ('a', 'p', 'q', 'T'),
        'bookTicker': ('u', 'b', 'B', 'a', 'A'),
    }
    for kind, example in summary['examples'].items():
        selected = {key: example['data'][key] for key in fields[kind] if key in example['data']}
        print(f'{kind} 首个例子（本地 sequence={example["sequence"]}）：{json.dumps(selected, ensure_ascii=False)}')


if __name__ == '__main__':
    main()
