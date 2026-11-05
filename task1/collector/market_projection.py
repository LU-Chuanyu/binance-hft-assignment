"""将原始公开行情转为分类、扁平记录；不负责存储或同步。"""

from market_stream import decode_message


def _need(data, key, expected):
    value = data.get(key)
    if type(value) is not expected:
        raise ValueError(f'行情字段 {key} 类型或内容无效')
    return value


def _trade_fields(data, with_event_time):
    result = {key: _need(data, key, expected) for key, expected in (
        ('a', int), ('p', str), ('q', str), ('m', bool), ('T', int))}
    result['E'] = _need(data, 'E', int) if with_event_time else None
    for key in ('f', 'l'):
        if key in data:
            result[key] = _need(data, key, int)
    return result


def _generation(generation):
    if type(generation) is not int or generation < 1:
        raise ValueError('generation 必须是从 1 开始的整数')


def project_live(message, *, generation):
    """返回一条实时消息对应的列表；盘口每个变化价位分别生成一行。

    除投影外，调用者仍须保存 raw 消息，以便恢复原文、消息边界及回放。
    generation 和 receive_seq 联合标识重连后的一次本地接收。
    """
    _generation(generation)
    event = decode_message(message)
    data = event.data
    symbol = _need(data, 's', str)
    common = {'kind': event.kind, 'symbol': symbol, 'source': 'live',
              'generation': generation, 'receive_seq': message.sequence,
              'received_ns': message.received_ns, 'monotonic_ns': message.monotonic_ns}

    if event.kind == 'depth':
        first, last, event_time = _need(data, 'U', int), _need(data, 'u', int), _need(data, 'E', int)
        if first > last:
            raise ValueError('盘口更新编号范围无效')
        rows = []
        for field, side in (('b', 'bid'), ('a', 'ask')):
            levels = _need(data, field, list)
            for level in levels:
                if (not isinstance(level, list) or len(level) != 2
                        or any(type(item) is not str for item in level)):
                    raise ValueError(f'盘口 {field} 档位必须包含价格和数量文本')
                rows.append({**common, 'side': side, 'price': level[0], 'qty': level[1],
                             'U': first, 'u': last, 'E': event_time})
        return rows

    if event.kind == 'aggTrade':
        return [{**common, **_trade_fields(data, with_event_time=True),
                 'estimated_received_ns': None}]

    if event.kind == 'bookTicker':
        fields = {key: _need(data, key, expected) for key, expected in (
            ('u', int), ('b', str), ('B', str), ('a', str), ('A', str))}
        return [{**common, **fields}]
    raise ValueError(f'不支持的行情类型：{event.kind}')


def project_backfill(record, *, symbol, generation):
    """将 REST 补回的成交投影为一行，保留估计时间与估计依据。"""
    _generation(generation)
    if (not isinstance(record, dict) or record.get('source') != 'rest_backfill'
            or record.get('received_ns') is not None or type(symbol) is not str or not symbol):
        raise ValueError('补回记录的来源、本机接收时间或交易对无效')
    data = _need(record, 'data', dict)
    estimate = record.get('estimated_received_ns')
    if estimate is not None and type(estimate) is not int:
        raise ValueError('估计接收时间必须是整数纳秒或 None')
    basis = {key: record[key] for key in (
        'estimate_typical_delay_ns', 'estimate_clock_offset_ns',
        'estimate_clock_sample_ns', 'estimate_clock_rtt_ns', 'estimate_sample_count',
        'estimate_method',
    ) if key in record}
    return [{'kind': 'aggTrade', 'symbol': symbol, 'source': 'rest_backfill',
             'generation': generation, 'receive_seq': None, 'received_ns': None,
             'monotonic_ns': None, 'estimated_received_ns': estimate,
             'estimate_typical_delay_ms': record.get('estimate_typical_delay_ms'),
             'estimate_clock_offset_ms': record.get('estimate_clock_offset_ms'),
             **basis,
             **_trade_fields(data, with_event_time=False)}]
