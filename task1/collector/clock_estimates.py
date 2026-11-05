"""时钟偏差告警和补回成交的本机接收时间估计。"""

from collections import deque
from statistics import median


_NS_PER_MS = 1_000_000
DEFAULT_OFFSET_ALERT_NS = 500 * _NS_PER_MS


def clock_offset_alert(sample, *, threshold_ns=DEFAULT_OFFSET_ALERT_NS):
    """根据中点偏差给出告警，并标明扣除 RTT 裕量后是否仍超阈值。"""
    if type(threshold_ns) is not int or threshold_ns <= 0:
        raise ValueError('时钟偏差告警阈值须为正整数纳秒')
    if sample.get('status') != 'ok':
        return None
    offset = sample['offset_ns']
    if abs(offset) <= threshold_ns:
        return None
    rtt = sample['rtt_ns']
    # 半个往返时间加服务器毫秒时间戳的分辨率，用于区分不确定的告警。
    margin = (rtt + 1) // 2 + _NS_PER_MS
    return {'offset_ns': offset, 'rtt_ns': rtt, 'threshold_ns': threshold_ns,
            'estimated_error_ns': margin,
            'outside_threshold_with_rtt_margin': abs(offset) - margin > threshold_ns,
            'clock_sample_usable': sample['usable'], 'sample_ns': sample['sample_ns'],
            'observed_ns': sample['sample_ns']}


class TradeReceiptEstimator:
    """同一交易对、同一校时窗口下，以近期实收成交估计补回成交的接收时刻。"""

    def __init__(self, calibration, *, min_samples=5, max_samples=256,
                 max_age_ns=60_000_000_000, max_trade_distance_ms=30_000):
        options = (min_samples, max_samples, max_age_ns, max_trade_distance_ms)
        if any(type(value) is not int or value <= 0 for value in options) or min_samples > max_samples:
            raise ValueError('补回接收时间估计参数无效')
        self.calibration = calibration
        self.min_samples = min_samples
        self.max_age_ns = max_age_ns
        self.max_trade_distance_ms = max_trade_distance_ms
        self.samples = deque(maxlen=max_samples)
        self.clock_window = None

    def _offset_at(self, wall_ns, mono_ns):
        offset = self.calibration.offset_at(wall_ns, mono_ns)
        current = self.calibration.current
        if current != self.clock_window:
            self.clock_window = current
            self.samples.clear()
        return offset

    def observe_live(self, event, received_ns, monotonic_ns):
        """处理线程仅调用已纳入的实收成交，延迟以成交时间 T 为起点。"""
        offset = self._offset_at(received_ns, monotonic_ns)
        trade_ms = event.get('T')
        if offset is None or type(trade_ms) is not int or trade_ms < 0:
            return
        delay_ns = received_ns - offset - trade_ms * _NS_PER_MS
        if delay_ns >= 0:
            self.samples.append((trade_ms, monotonic_ns, delay_ns))

    def estimate(self, record, *, now_wall_ns, now_monotonic_ns):
        """补回记录另存本机接收估计值。可信样本不足时沿用空值。"""
        if record.get('source') != 'rest_backfill' or record.get('received_ns') is not None:
            raise ValueError('只能估计没有实收时间的 REST 补回成交')
        if record.get('estimated_received_ns') is not None:
            return record
        offset = self._offset_at(now_wall_ns, now_monotonic_ns)
        trade_ms = record['data'].get('T')
        if offset is None or type(trade_ms) is not int or trade_ms < 0:
            return record
        delays = [delay for live_ms, mono, delay in self.samples
                  if 0 <= now_monotonic_ns - mono <= self.max_age_ns
                  and abs(live_ms - trade_ms) <= self.max_trade_distance_ms]
        if len(delays) < self.min_samples:
            return record
        typical_delay = int(median(delays))
        estimated = trade_ms * _NS_PER_MS + typical_delay + offset
        if not self.clock_window['wall_ns'] <= estimated <= now_wall_ns:
            return record
        return {**record, 'estimated_received_ns': estimated,
                'estimate_typical_delay_ms': typical_delay / _NS_PER_MS,
                'estimate_clock_offset_ms': offset / _NS_PER_MS,
                'estimate_typical_delay_ns': typical_delay,
                'estimate_clock_offset_ns': offset,
                'estimate_clock_sample_ns': self.clock_window['wall_ns'],
                'estimate_clock_rtt_ns': self.clock_window['rtt_ns'],
                'estimate_sample_count': len(delays),
                'estimate_method': 'nearby_live_trade_median'}
