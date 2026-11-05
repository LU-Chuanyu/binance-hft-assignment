import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from quality_report import ClockWindow
from report_cli import write_report
from test_quality_report import recorded
from partition_store import PartitionStore


def long_recorded():
    rows = [{'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}}]
    for i in range(30):
        event_ms = 1000 + i * 3000
        data = {'U': 101 + i, 'u': 101 + i, 'b': [['100', '1']], 'a': [], 'E': event_ms}
        rows.append({'type': 'raw', 'receive_seq': i + 1,
                     'received_ns': (event_ms + 60) * 1_000_000,
                     'monotonic_ns': i * 3_000_000_000,
                     'raw_text': json.dumps({'stream': 'btcusdt@depth@100ms', 'data': data})})
        rows.append({'type': 'depth_action', 'receive_seq': i + 1, 'action': 'applied',
                     'before_id': 100 + i, 'after_id': 101 + i})
    rows.append({'type': 'end', 'status': 'success', 'received_count': 30, 'processed_depth': 30,
                 'pending_depth': 0, 'last_update_id': 130, 'top': {'bids': [['100', '1']], 'asks': []}})
    return ''.join(json.dumps({'journal_seq': n, **row}) + '\n' for n, row in enumerate(rows, 1))


class ReportCLITests(unittest.TestCase):
    def test_checkpoint_gaps_reported_despite_saved_checks_passing(self):
        rows = [json.loads(line) for line in recorded().splitlines()]
        rows.insert(3, {'type': 'book_checkpoint', 'last_update_id': 105,
                        'top': {'bids': [['100', '1']], 'asks': []}})
        rows.insert(4, {'type': 'checkpoint_gap', 'checkpoint_ns': 2000000000,
                        'missed_count': 2, 'reason': 'processing_delayed'})
        rows.insert(5, {'type': 'checkpoint_gap', 'checkpoint_ns': 3000000000,
                        'missed_count': 1, 'reason': 'unsynchronized'})
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'input.jsonl'
            p.write_text(''.join(json.dumps({**row, 'journal_seq': i}) + '\n'
                                 for i, row in enumerate(rows, 1)), encoding='utf-8')
            report = write_report(p, Path(tmp) / 'out').read_text(encoding='utf-8')
            self.assertIn('1/1', report)
            self.assertIn('错过 3 个', report)
            self.assertIn('processing_delayed', report)
            self.assertIn('unsynchronized', report)
            self.assertIn('覆盖不完整', report)

    def test_partitioned_report_recognizes_checkpoints_and_replay(self):
        rows = [json.loads(line) for line in recorded().splitlines()]
        rows.insert(3, {'type': 'book_checkpoint', 'last_update_id': 105,
                        'top': {'bids': [['100', '1']], 'asks': []}})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'capture'
            store = PartitionStore(root / 'journal', batch_size=2)
            for i, row in enumerate(rows, 1):
                row['journal_seq'] = i
                store.submit(row['type'], 0, row)
            store.close()
            market = PartitionStore(root / 'market')
            market.submit('depth', 0, {'price': '100', 'quantity': '1'})
            market.close()
            journal_bytes = sum(p.stat().st_size for p in (root / 'journal').rglob('*.csv.gz'))
            market_bytes = sum(p.stat().st_size for p in (root / 'market').rglob('*.csv.gz'))
            report = write_report(root, Path(tmp) / 'out')
            text = report.read_text(encoding='utf-8')
            self.assertIn('1/1', text)
            self.assertIn('压缩分片', text)
            self.assertIn('未提供校时窗口', text)
            self.assertIn(f'本次数据文件合计：{journal_bytes + market_bytes} 字节', text)

    def test_partitioned_incomplete_data_not_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'journal'
            store = PartitionStore(root)
            store.submit('snapshot', 0, {'journal_seq': 1, 'type': 'snapshot',
                         'snapshot': {'lastUpdateId': 100, 'bids': [], 'asks': []}})
            store.close()
            with self.assertRaisesRegex(ValueError, '结束记录'):
                write_report(root, Path(tmp) / 'out')

    def test_no_checkpoint_explicitly_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'input.jsonl'
            p.write_text(recorded(), encoding='utf-8')
            report = write_report(p, Path(tmp) / 'out')
            text = report.read_text(encoding='utf-8')
            self.assertIn('逐档回放校验通过率：N/A', text)
    def test_short_uncalibrated_run_marks_figures_and_delay_na(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'input.jsonl'
            p.write_text(recorded(), encoding='utf-8')
            report = write_report(p, Path(tmp) / 'out')
            text = report.read_text(encoding='utf-8')
            self.assertIn('未提供校时窗口', text)
            self.assertIn('N/A', text)
            self.assertIn('实时原始消息', text)
            self.assertIn('1970-01-01T00:00Z', text)
            self.assertFalse((report.parent / 'latency_distribution.png').exists())
            self.assertFalse((report.parent / 'messages_per_minute.png').exists())

    @unittest.skipUnless(importlib.util.find_spec('matplotlib'), 'matplotlib 未安装')
    def test_long_calibrated_sample_creates_both_plots(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'input.jsonl'
            p.write_text(long_recorded(), encoding='utf-8')
            clock = ClockWindow(0, 100_000_000_000, 50_000_000, 2_000_000, 5_000_000)
            report = write_report(p, Path(tmp) / 'out', clock=clock)
            self.assertGreater((report.parent / 'latency_distribution.png').stat().st_size, 0)
            self.assertGreater((report.parent / 'messages_per_minute.png').stat().st_size, 0)
            text = report.read_text(encoding='utf-8')
            self.assertIn('10.000', text)
            self.assertIn('不等同纯网络延迟', text)
            self.assertIn('86400', text)

    def test_existing_output_directory_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'input.jsonl'
            p.write_text(recorded(), encoding='utf-8')
            out = Path(tmp) / 'out'
            out.mkdir()
            (out / 'keep').write_text('old')
            with self.assertRaises(FileExistsError):
                write_report(p, out)
            self.assertEqual((out / 'keep').read_text(), 'old')


if __name__ == '__main__':
    unittest.main()
