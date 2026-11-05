import json
from pathlib import Path
import tempfile
import unittest

from quality_report import ClockWindow, summarize_file
from test_clock_samples import sample as clock_sample


def recorded():
    def raw(seq, stream, data, ns):
        return {'type': 'raw', 'receive_seq': seq, 'received_ns': ns,
                'monotonic_ns': seq * 1000000,
                'raw_text': json.dumps({'stream': stream, 'data': data})}
    rows = [
        {'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}},
        raw(1, 'btcusdt@depth@100ms', {'U': 101, 'u': 105, 'b': [['100', '1']], 'a': [], 'E': 1000}, 1_100_000_000),
        {'type': 'depth_action', 'receive_seq': 1, 'action': 'applied', 'before_id': 100, 'after_id': 105},
        raw(2, 'btcusdt@bookTicker', {'b': '100', 'a': '101'}, 2_000_000_000),
        raw(3, 'btcusdt@aggTrade', {'a': 4, 'E': 61000}, 61_100_000_000),
        {'type': 'end', 'status': 'success', 'received_count': 3, 'processed_depth': 1,
         'pending_depth': 0, 'last_update_id': 105, 'top': {'bids': [['100', '1']], 'asks': []}},
    ]
    return ''.join(json.dumps({'journal_seq': index, **row}) + '\n'
                   for index, row in enumerate(rows, 1))


class QualityReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'journal.jsonl'
        self.path.write_text(recorded(), encoding='utf-8')
        self.addCleanup(self.tmp.cleanup)

    def test_counts_minutes_size_and_does_not_assume_clock_offset(self):
        result = summarize_file(self.path)
        self.assertEqual(result['counts'], {'depth': 1, 'aggTrade': 1, 'bookTicker': 1})
        self.assertEqual(result['minute_counts_utc'], {'1970-01-01T00:00Z': 2, '1970-01-01T00:01Z': 1})
        self.assertEqual(result['minute_counts_by_kind_utc']['1970-01-01T00:00Z'], {'depth': 1, 'bookTicker': 1})
        self.assertEqual(result['observed_rate_per_second']['depth'], 500)
        self.assertEqual(result['file_bytes'], self.path.stat().st_size)
        self.assertEqual((result['gap_count'], result['reset_count']), (0, 0))
        self.assertEqual(result['latency']['depth']['count'], 0)
        self.assertEqual(result['latency']['bookTicker']['count'], 0)
        self.assertIsNone(result['latency']['depth']['median_ms'])
        self.assertIn('未提供', result['time_difference_note'])

    def test_bounded_clock_window_enables_only_covered_events(self):
        clock = ClockWindow(start_ns=0, end_ns=2_000_000_000,
                            offset_ns=50_000_000, rtt_ns=2_000_000, max_rtt_ns=5_000_000)
        result = summarize_file(self.path, clock=clock)
        self.assertEqual(result['latency']['depth']['median_ms'], 50)
        self.assertEqual(result['latency']['aggTrade']['count'], 0)
        self.assertEqual(result['latency']['bookTicker']['count'], 0)

    def test_bad_or_unbounded_clock_estimate_is_rejected(self):
        with self.assertRaises(ValueError):
            ClockWindow(start_ns=10, end_ns=0, offset_ns=0, rtt_ns=0, max_rtt_ns=5)
        with self.assertRaises(ValueError):
            ClockWindow(start_ns=0, end_ns=1, offset_ns=0, rtt_ns=10, max_rtt_ns=5)

    def test_incomplete_journal_is_not_reported_as_success(self):
        self.path.write_text('\n'.join(recorded().splitlines()[:-1]) + '\n', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, '结束记录'):
            summarize_file(self.path)

    def test_embedded_clock_sample_yields_only_covered_event_delays(self):
        rows = [json.loads(line) for line in recorded().splitlines()]
        rows.insert(1, {'type': 'clock_sample', **clock_sample()})
        for row in rows:
            if row['type'] == 'raw':
                row['monotonic_ns'] = row['received_ns'] - 1_000_000_000
        self.path.write_text(''.join(json.dumps({**row, 'journal_seq': i}) + '\n'
                                     for i, row in enumerate(rows, 1)), encoding='utf-8')
        result = summarize_file(self.path)
        self.assertEqual(result['usable_clock_samples'], 1)
        self.assertEqual(result['latency']['depth']['median_ms'], 50)
        self.assertEqual(result['latency']['aggTrade']['count'], 0)  # outside 60s window
        self.assertEqual(result['latency']['bookTicker']['count'], 0)

    def test_clock_failure_stops_previous_sample(self):
        rows = [json.loads(line) for line in recorded().splitlines()]
        rows.insert(1, {'type': 'clock_sample', **clock_sample()})
        rows.insert(2, {'type': 'clock_sample', 'status': 'error', 'usable': False, 'offset_ns': None,
                        'observed_ns': 1_015_000_000, 'error': 'timeout'})
        for row in rows:
            if row['type'] == 'raw':
                row['monotonic_ns'] = row['received_ns'] - 1_000_000_000
        self.path.write_text(''.join(json.dumps({**row, 'journal_seq': i}) + '\n'
                                     for i, row in enumerate(rows, 1)), encoding='utf-8')
        result = summarize_file(self.path)
        self.assertEqual(result['latency']['depth']['count'], 0)


if __name__ == '__main__':
    unittest.main()
