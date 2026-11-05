"""本地订单簿的价位更新，不负责判断快照和增量是否已对齐。"""

from decimal import Decimal


class OrderBook:
    def __init__(self, snapshot):
        # 价格为键，数量为值。Decimal 避免二进制浮点数误差。
        self.bids = {Decimal(price): Decimal(quantity) for price, quantity in snapshot['bids']}
        self.asks = {Decimal(price): Decimal(quantity) for price, quantity in snapshot['asks']}

    @staticmethod
    def _update_side(side, levels):
        for price_text, quantity_text in levels:
            price = Decimal(price_text)
            quantity = Decimal(quantity_text)
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def apply_update(self, event):
        """仅应用绝对数量；调用者须先验证 U/u 同步与连续性。"""
        self._update_side(self.bids, event['b'])
        self._update_side(self.asks, event['a'])

    def top_levels(self, count):
        """买价从高到低、卖价从低到高，各取前 count 档。"""
        if count < 0:
            raise ValueError('档数不能为负数')
        bids = sorted(self.bids.items(), reverse=True)[:count]
        asks = sorted(self.asks.items())[:count]
        return {'bids': bids, 'asks': asks}
