"""应用深度增量。首条更新须覆盖快照编号 S 后的 S+1。"""

from order_book import OrderBook


class DepthGapError(Exception):
    """盘口更新的编号无法连续衔接。"""


class UnsynchronizedError(Exception):
    """盘口已失效，需要用新快照重建。"""


class DepthSynchronizer:
    def __init__(self, snapshot, *, strict_followup=False):
        if type(strict_followup) is not bool:
            raise ValueError('后续盘口编号规则必须用布尔值表示')
        self.book = OrderBook(snapshot)
        self.last_update_id = snapshot['lastUpdateId']
        self._strict_followup = strict_followup
        self._bridged = False
        # valid 仅表示尚未发现缺口，不代表已完成快照和实时流的首次对齐。
        self.valid = True

    def apply(self, event):
        if not self.valid:
            raise UnsynchronizedError('盘口已失效，需要新快照并重新同步')

        try:
            first, last = event['U'], event['u']
            if type(first) is not int or type(last) is not int or first > last:
                raise ValueError('盘口更新编号范围无效')
            if last <= self.last_update_id:
                return 'ignored'

            expected = self.last_update_id + 1
            if first > expected:
                raise DepthGapError(f'盘口编号缺口：需要 {expected}，消息从 {first} 开始')
            if self._strict_followup and self._bridged and first != expected:
                raise DepthGapError(f'盘口编号重叠：需要从 {expected} 开始，消息从 {first} 开始')

            self.book.apply_update(event)
            self.last_update_id = last
            self._bridged = True
            return 'applied'
        except Exception:
            # 遇到缺口或异常时不再处理后续消息；必须重新同步。
            self.valid = False
            raise
