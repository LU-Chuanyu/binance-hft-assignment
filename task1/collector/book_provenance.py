"""跟踪单个同步轮次内各价位的已知数量及最后一次修改范围。

它不连接网络、不决定消息能否应用；调用方应先用订单簿同步器验证并应用，
再按相同顺序将实际 applied 的消息交给 observe_applied。缺口/断线后必须
停止使用旧实例并用新快照创建新轮次。快照外未出现的价位仍然未知。
"""

from decimal import Decimal, InvalidOperation


def _levels(levels):
    if not isinstance(levels, list):
        raise ValueError('价位列表无效')
    result = []
    for level in levels:
        if (not isinstance(level, (list, tuple)) or len(level) != 2
                or any(type(value) is not str for value in level)):
            raise ValueError('价位必须包含价格和数量文本')
        try:
            price, quantity = map(Decimal, level)
        except InvalidOperation as exc:
            raise ValueError('价格或数量不是有效十进制数') from exc
        if not price.is_finite() or not quantity.is_finite() or price <= 0 or quantity < 0:
            raise ValueError('价格或数量超出有效范围')
        result.append((price, quantity))
    return result


class BookProvenance:
    """保存已知价格及最新影响它的 depth 消息编号范围。"""

    def __init__(self, snapshot, *, epoch=1):
        update_id = snapshot['lastUpdateId']
        if type(update_id) is not int or update_id < 0 or type(epoch) is not int or epoch < 1:
            raise ValueError('快照编号或轮次无效')
        sides = {side: _levels(snapshot[side]) for side in ('bids', 'asks')}
        self.initial_id = self.current_id = update_id
        self.epoch = epoch
        self.known = {side: dict(levels) for side, levels in sides.items()}
        self.last_changes = {side: {price: None for price, _ in levels}
                             for side, levels in sides.items()}

    def observe_applied(self, event):
        """按已验证的应用顺序记录一条消息；拒绝旧消息、缺口和坏数据。"""
        first, last = event['U'], event['u']
        if (type(first) is not int or type(last) is not int or first < 0 or first > last
                or not first <= self.current_id + 1 <= last):
            raise ValueError('更新未覆盖下一个所需编号')
        # 在修改状态前先解析两侧，坏数据不会只更新一部分价位。
        sides = {'bids': _levels(event['b']), 'asks': _levels(event['a'])}
        for side, levels in sides.items():
            for price, quantity in levels:
                self.known[side][price] = quantity  # 零数量也保留为已知。
                self.last_changes[side][price] = (first, last)
        self.current_id = last

    def reset(self, snapshot):
        """用新快照返回新轮次；不会沿用旧轮次的已知价位。"""
        return BookProvenance(snapshot, epoch=self.epoch + 1)
