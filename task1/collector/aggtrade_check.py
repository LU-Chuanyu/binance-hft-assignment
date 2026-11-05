"""检查单一交易对的聚合成交 ID；不执行网络补查。"""


class AggTradeTracker:
    """跟踪已经看到的编号和待确认的缺失区间（两端均包含）。"""

    def __init__(self):
        self.first_id = None
        self.highest_id = None
        self.missing_ranges = []

    def observe(self, event):
        """读入一条 aggTrade 的 data，返回 first/continuous/gap 等状态。"""
        trade_id = event.get('a')
        if type(trade_id) is not int or trade_id < 0:
            raise ValueError('聚合成交编号 a 必须是非负整数')

        if self.highest_id is None:
            self.first_id = self.highest_id = trade_id
            return 'first'
        if trade_id > self.highest_id:
            expected = self.highest_id + 1
            self.highest_id = trade_id
            if trade_id == expected:
                return 'continuous'
            self.missing_ranges.append((expected, trade_id - 1))
            return 'gap'

        if trade_id < self.first_id:
            # 第一次观察之前的范围未知，不能断言该编号是重复还是遗漏。
            return 'older_than_baseline'
        for index, (start, end) in enumerate(self.missing_ranges):
            if start <= trade_id <= end:
                remaining = []
                if start < trade_id:
                    remaining.append((start, trade_id - 1))
                if trade_id < end:
                    remaining.append((trade_id + 1, end))
                self.missing_ranges[index:index + 1] = remaining
                return 'late'
        return 'duplicate'
