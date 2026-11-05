"""Track processing rates and sampled event-to-receipt delays in 10-second windows."""

import math

from clock_monitor import summarize_delays


STREAMS = ('depth', 'aggTrade', 'bookTicker')
SECOND_NS = 1_000_000_000
WINDOW_NS = 10 * SECOND_NS


def _finite_number(value):
    return type(value) in (int, float) and math.isfinite(value)


class RuntimeStats:
    """Use processing time for windows and retain delay samples only for the open window.

    The caller computes delays from raw receive timestamps and valid clock samples.
    Startup messages and messages processed after the recording cutoff are excluded.
    """

    def __init__(self, start_monotonic_ns):
        if type(start_monotonic_ns) is not int or start_monotonic_ns < 0:
            raise ValueError('运行统计起点须为非负单调时钟纳秒')
        self._start_ns = start_monotonic_ns
        self._last_ns = start_monotonic_ns
        self._finished = False
        self._reset_streams()

    def _reset_streams(self):
        self._counts = dict.fromkeys(STREAMS, 0)
        self._delays = {kind: [] for kind in STREAMS}

    def _close_window(self, end_ns):
        duration_ns = end_ns - self._start_ns
        duration_seconds = duration_ns / SECOND_NS
        streams = {}
        for kind, count in self._counts.items():
            samples = self._delays[kind]
            delay = summarize_delays(samples)
            delay['min_ms'] = min(samples, default=None)
            streams[kind] = {'count': count, 'rate_per_second': count / duration_seconds,
                             'delay': delay}
        row = {'window_start_monotonic_ns': self._start_ns, 'window_end_monotonic_ns': end_ns,
               'partial': duration_ns < WINDOW_NS, 'streams': streams}
        self._start_ns = end_ns
        self._reset_streams()
        return row

    def advance(self, processed_monotonic_ns):
        """Return all closed windows, including windows with no messages."""
        if self._finished or type(processed_monotonic_ns) is not int or processed_monotonic_ns < self._last_ns:
            raise ValueError('运行统计已结束或处理时间倒退')
        self._last_ns = processed_monotonic_ns
        windows = []
        while self._start_ns + WINDOW_NS <= processed_monotonic_ns:
            windows.append(self._close_window(self._start_ns + WINDOW_NS))
        return windows

    def observe(self, kind, processed_monotonic_ns, delay_ms=None):
        """Count an event in its half-open processing window and return closed windows."""
        if kind not in STREAMS:
            raise ValueError('未知行情类型')
        if delay_ms is not None and (kind == 'bookTicker' or not _finite_number(delay_ms)):
            raise ValueError('运行统计的延迟样本无效')
        windows = self.advance(processed_monotonic_ns)
        self._counts[kind] += 1
        if delay_ms is not None:
            self._delays[kind].append(delay_ms)
        return windows

    def finish(self, stop_monotonic_ns):
        """Close completed windows and the final partial window, when present."""
        windows = self.advance(stop_monotonic_ns)
        if stop_monotonic_ns > self._start_ns:
            windows.append(self._close_window(stop_monotonic_ns))
        self._finished = True
        return windows


def validate_runtime_stats(row):
    """Check the runtime record's window, stream coverage and statistic ranges."""
    start = row.get('window_start_monotonic_ns')
    end = row.get('window_end_monotonic_ns')
    if (type(start) is not int or type(end) is not int or start < 0
            or not 0 < end - start <= WINDOW_NS
            or row.get('partial') is not (end - start < WINDOW_NS)
            or type(row.get('observed_ns')) is not int):
        raise ValueError('运行统计时间窗口无效')
    streams = row.get('streams')
    if not isinstance(streams, dict) or set(streams) != set(STREAMS):
        raise ValueError('运行统计行情类型无效')
    duration_seconds = (end - start) / SECOND_NS
    for kind, stream in streams.items():
        count, rate = stream['count'], stream['rate_per_second']
        if type(count) is not int or count < 0 or not _finite_number(rate) or rate != count / duration_seconds:
            raise ValueError('运行统计消息频率无效')
        delay = stream['delay']
        sample_count = delay['count']
        values = [delay[key] for key in ('min_ms', 'median_ms', 'p99_ms', 'max_ms')]
        if type(sample_count) is not int or not 0 <= sample_count <= count or (kind == 'bookTicker' and sample_count):
            raise ValueError('运行统计延迟样本覆盖无效')
        if sample_count == 0:
            valid = all(value is None for value in values)
        else:
            valid = all(_finite_number(value) for value in values) and values == sorted(values)
        if not valid:
            raise ValueError('运行统计延迟分布无效')
