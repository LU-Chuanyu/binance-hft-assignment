"""将记录中的校时样本用于保守估计事件到本机接收时间差。"""

from clock_monitor import estimate_clock, estimate_delay_ms


class ClockSamples:
    """仅使用最新有效样本；出错、过期、时钟跳变时停用它。"""

    def __init__(self, *, max_rtt_ns=1_000_000_000, max_age_ns=60_000_000_000,
                 jump_tolerance_ns=5_000_000):
        self.max_rtt_ns = max_rtt_ns
        self.max_age_ns = max_age_ns
        self.jump_tolerance_ns = jump_tolerance_ns
        self.current = None
        self.jumps = 0
        self.valid_samples = 0

    def observe(self, sample):
        self.current = None
        if sample.get('status') != 'ok' or sample.get('usable') is not True:
            return
        try:
            estimate = estimate_clock(sample['sent_wall_ns'], sample['received_wall_ns'],
                                      sample['server_ms'], sample['sent_monotonic_ns'],
                                      sample['received_monotonic_ns'],
                                      jump_tolerance_ns=self.jump_tolerance_ns)
            received = sample['received_wall_ns']
            if (sample['offset_ns'] != estimate.offset_ns or sample['rtt_ns'] != estimate.rtt_ns
                    or estimate.rtt_ns > self.max_rtt_ns or sample['sample_ns'] != received
                    or sample['valid_from_ns'] != received or type(sample['valid_until_ns']) is not int
                    or sample['valid_until_ns'] < received):
                return
            self.current = {'wall_ns': received, 'mono_ns': sample['received_monotonic_ns'],
                            'until_ns': min(sample['valid_until_ns'], received + self.max_age_ns),
                            'offset_ns': estimate.offset_ns, 'rtt_ns': estimate.rtt_ns}
            self.valid_samples += 1
        except (KeyError, TypeError, ValueError):
            return

    def offset_at(self, received_ns, monotonic_ns):
        """取有效窗口内的本机减交易所时钟偏差，单位纳秒。"""
        current = self.current
        if current is None or type(received_ns) is not int or type(monotonic_ns) is not int:
            return None
        if monotonic_ns < current['mono_ns']:
            # 校时结果落盘后才处理的旧消息，不代表计时器倒退。
            return None
        wall_elapsed = received_ns - current['wall_ns']
        mono_elapsed = monotonic_ns - current['mono_ns']
        if abs(wall_elapsed - mono_elapsed) > self.jump_tolerance_ns:
            self.current = None
            self.jumps += 1
            return None
        if received_ns > current['until_ns'] or mono_elapsed > self.max_age_ns:
            self.current = None
            return None
        if received_ns < current['wall_ns']:
            return None
        return current['offset_ns']

    def delay(self, received_ns, event_ms, monotonic_ns):
        """未经有效校时、样本过期或本机时钟跳变时返回 None。"""
        offset = self.offset_at(received_ns, monotonic_ns)
        if type(event_ms) is not int:
            return None
        return estimate_delay_ms(received_ns, event_ms, offset)
