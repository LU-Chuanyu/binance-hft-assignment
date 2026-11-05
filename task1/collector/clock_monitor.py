"""纯计算：估算本机时钟偏差与事件时间差；不发网络请求、不自动校时。"""

from dataclasses import dataclass
from math import ceil, isfinite
from statistics import median


class ClockJumpError(ValueError):
    """本机时钟在一次测量中发生跳变；调用方可记录这两个时间差。"""

    def __init__(self, wall_elapsed_ns, monotonic_elapsed_ns):
        self.wall_elapsed_ns = wall_elapsed_ns
        self.monotonic_elapsed_ns = monotonic_elapsed_ns
        super().__init__(f'本机时钟可能跳变：墙上时钟经过 {wall_elapsed_ns}ns，'
                         f'单调时钟经过 {monotonic_elapsed_ns}ns')


@dataclass(frozen=True)
class ClockEstimate:
    offset_ns: int  # 本机时间减交易所时间；正值表示本机更快。
    rtt_ns: int     # 单调时钟测量的往返时间。


def estimate_clock(sent_wall_ns, received_wall_ns, server_ms,
                   sent_monotonic_ns, received_monotonic_ns, *, jump_tolerance_ns=5_000_000):
    """用本机发送/收到时刻中点减服务器时间估计偏差；单位纳秒。"""
    inputs = (sent_wall_ns, received_wall_ns, server_ms, sent_monotonic_ns,
              received_monotonic_ns, jump_tolerance_ns)
    if any(type(value) is not int for value in inputs) or jump_tolerance_ns < 0:
        raise ValueError('时间和容差必须是整数，容差不能为负')
    rtt_ns = received_monotonic_ns - sent_monotonic_ns
    if rtt_ns < 0:
        raise ValueError('单调时钟不能倒退')
    wall_elapsed_ns = received_wall_ns - sent_wall_ns
    if wall_elapsed_ns < 0 or abs(wall_elapsed_ns - rtt_ns) > jump_tolerance_ns:
        raise ClockJumpError(wall_elapsed_ns, rtt_ns)
    midpoint_ns = (sent_wall_ns + received_wall_ns) // 2
    return ClockEstimate(offset_ns=midpoint_ns - server_ms * 1_000_000, rtt_ns=rtt_ns)


def estimate_delay_ms(received_ns, event_ms, offset_ns):
    """估计本机接收与交易所事件时间差；缺时间或偏差时返回 None。

    估计包含发布、传输和本机接收等影响，不等于纯网络延迟。
    """
    if received_ns is None or event_ms is None or offset_ns is None:
        return None
    if any(type(value) is not int for value in (received_ns, event_ms, offset_ns)):
        raise ValueError('时间和偏差必须是整数')
    return (received_ns - offset_ns - event_ms * 1_000_000) / 1_000_000


def summarize_delays(values):
    """中位数、最近秩 P99 和最大值；空样本无统计值。"""
    samples = list(values)
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value)
           for value in samples):
        raise ValueError('延迟样本必须是有限数字')
    if not samples:
        return {'count': 0, 'median_ms': None, 'p99_ms': None, 'max_ms': None}
    samples.sort()
    return {'count': len(samples), 'median_ms': median(samples),
            'p99_ms': samples[ceil(0.99 * len(samples)) - 1], 'max_ms': samples[-1]}
