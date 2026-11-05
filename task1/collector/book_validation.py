"""盘口摘要校验与有限条件下的交易所快照对比。

交易所比对依赖调用方提供的每价位可信覆盖和完整修改历史；本模块本身
不采集这些证据，也不能证明整个订单簿正确。
"""

from decimal import Decimal
from itertools import zip_longest


def compare_top20(captured, replayed):
    """比较采集时与回放时记录的前 20 档；价格数量按十进制数比较。"""
    differences = []
    for side in ('bids', 'asks'):
        expected = [(Decimal(p), Decimal(q)) for p, q in captured[side][:20]]
        actual = [(Decimal(p), Decimal(q)) for p, q in replayed[side][:20]]
        for level, (left, right) in enumerate(zip_longest(expected, actual), 1):
            if left != right:
                differences.append({'side': side, 'level': level,
                                    'captured': left, 'replayed': right})
    return {'matches': not differences, 'differences': differences}


def compare_exchange_snapshot(snapshot, local_known, last_changes, *, initial_id,
                              local_id, contiguous):
    """对比能够证明在交易所快照时间点未改变的价位。

    local_known[side] 为在本地可信覆盖范围内已知的 price->quantity，已知
    零数量的价位也须显式列出。last_changes[side][price] 为影响该价位的
    最后一条消息范围 (U,u)；显式 None 表示该价位自初始快照未更新。
    调用方须保证从初始快照到 local_id 的完整连续历史、全部相关的价位
    修改均有记录，且 local_known 确实属于初始快照可信覆盖范围（或已
    经后续更新成为已知）。缺证据的价位不参与比对。逐条消息区间跨过
    S 时，不能知道该价位到底在 S 之前还是之后改变，必须跳过。
    """
    snapshot_id = snapshot['lastUpdateId']
    if (type(initial_id) is not int or type(local_id) is not int
            or type(snapshot_id) is not int or initial_id < 0
            or local_id < 0 or snapshot_id < 0):
        raise ValueError('更新编号必须是非负整数')
    if contiguous is not True or not initial_id <= snapshot_id <= local_id:
        return {'eligible': False, 'compared': 0, 'matches': None, 'differences': [],
                'skipped': [], 'reason': 'untrusted_or_unaligned_history'}

    compared = 0
    differences = []
    skipped = []
    for side in ('bids', 'asks'):
        server = {Decimal(p): Decimal(q) for p, q in snapshot[side]}
        known = {Decimal(p): Decimal(q) for p, q in local_known[side].items()}
        changes = {Decimal(p): value for p, value in last_changes[side].items()}
        # 快照之外可能还有未知价位；只使用快照返回价格最差档以内的范围。
        boundary = (min(server) if side == 'bids' else max(server)) if server else None
        for price in sorted(server.keys() | known.keys(), reverse=(side == 'bids')):
            base = {'side': side, 'price': str(price)}
            if boundary is None or (side == 'bids' and price < boundary) or (side == 'asks' and price > boundary):
                skipped.append({**base, 'reason': 'outside_snapshot_coverage'})
                continue
            if price not in known:
                skipped.append({**base, 'reason': 'local_level_unknown'})
                continue
            if price not in changes:
                skipped.append({**base, 'reason': 'missing_provenance'})
                continue
            change = changes[price]
            if change is not None:
                if (not isinstance(change, (tuple, list)) or len(change) != 2
                        or any(type(x) is not int for x in change)
                        or change[0] < 0 or change[0] > change[1] or change[1] > local_id):
                    raise ValueError('价位修改编号范围无效')
                first, last = change
                if first <= snapshot_id < last:
                    skipped.append({**base, 'reason': 'overlaps_snapshot'})
                    continue
                if first > snapshot_id:
                    skipped.append({**base, 'reason': 'changed_after_snapshot'})
                    continue
            compared += 1
            server_qty = server.get(price, Decimal(0))
            if known[price] != server_qty:
                differences.append({**base, 'local_qty': str(known[price]),
                                    'snapshot_qty': str(server_qty)})
    return {'eligible': True, 'compared': compared,
            'matches': not differences if compared else None,
            'differences': differences, 'skipped': skipped}
