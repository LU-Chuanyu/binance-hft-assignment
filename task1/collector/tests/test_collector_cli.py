import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from integrated_collector import build_parser, collect, main
from quality_report import summarize_file
from reconnect_stream import StreamOpened
from test_integrated_clock import BlockingStream, snapshot
from test_integrated_collector import depth, message
from trade_flow import TradeFlow


class CollectorCLITests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec('pyarrow'), 'pyarrow 未安装')
    def test_default_capture_writes_parquet_and_reports_published_bytes(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1'))])
        api = SimpleNamespace(depth=lambda **kwargs: SimpleNamespace(data=snapshot()),
                              server_time=lambda: None, close=lambda: None)
        with tempfile.TemporaryDirectory() as directory, \
                patch('integrated_collector.ReconnectingStream', return_value=stream), \
                patch('integrated_collector.PublicAPIClient', return_value=api):
            main(['--duration-seconds', '0.05', '--output-root', directory])
            root, = Path(directory).iterdir()
            journal_files = list((root / 'journal').rglob('*.parquet'))
            market_files = list((root / 'market').rglob('*.parquet'))
            self.assertTrue(journal_files)
            self.assertTrue(market_files)
            self.assertFalse(list(root.rglob('*.csv.gz')))
            summary = summarize_file(root)
            self.assertEqual(summary['last_update_id'], 105)
            self.assertIn('Parquet', summary['source_type'])
            self.assertEqual(summary['file_bytes'], sum(path.stat().st_size for path in journal_files))
            self.assertEqual(summary['market_file_bytes'], sum(path.stat().st_size for path in market_files))

    def test_real_sigint_marks_failed_end(self):
        script = '''
import io, json, os, signal, sys, threading
sys.path.insert(0, 'tests')
from integrated_collector import collect
from reconnect_stream import StreamOpened
from test_integrated_clock import BlockingStream, snapshot
from test_integrated_collector import depth, message
from trade_flow import TradeFlow
stream = BlockingStream([StreamOpened(1, False, True, 10), message(1, 1, 'depth', depth(101, 105, '1'))])
output = io.StringIO()
timer = threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGINT))
timer.start()
try:
    collect(stream, snapshot, TradeFlow(lambda a,b: []), output, duration=5, wait_timeout=1)
except KeyboardInterrupt:
    print(output.getvalue().splitlines()[-1])
else:
    raise RuntimeError('SIGINT did not interrupt collection')
finally:
    timer.cancel()
'''
        completed = subprocess.run([sys.executable, '-c', script], capture_output=True,
                                   text=True, timeout=3, check=True)
        end = json.loads(completed.stdout.strip())
        self.assertEqual(end['status'], 'failed')
        self.assertIn('KeyboardInterrupt', end['reason'])

    def test_duration_defaults_to_three_and_accepts_long_values(self):
        parser = build_parser()
        self.assertEqual(parser.parse_args([]).duration_seconds, 3)
        self.assertEqual(parser.parse_args(['--duration-seconds', '300']).duration_seconds, 300)
        self.assertEqual(parser.parse_args(['--duration-seconds', '1800']).duration_seconds, 1800)
        for value in ('0', '-1', 'nan', 'inf'):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                parser.parse_args(['--duration-seconds', value])

    def test_run_records_requested_and_measured_times(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1'))])
        output = io.StringIO()
        collect(stream, snapshot, TradeFlow(lambda a,b: []), output,
                duration=0.05, wait_timeout=0.5)
        end = json.loads(output.getvalue().splitlines()[-1])
        self.assertEqual(end['status'], 'success')
        self.assertEqual(end['requested_duration_seconds'], 0.05)
        self.assertGreaterEqual(end['stopped_monotonic_ns'] - end['started_monotonic_ns'], 50_000_000)
        self.assertIsInstance(end['started_wall_ns'], int)
        self.assertGreaterEqual(end['finalized_wall_ns'], end['started_wall_ns'])

    def test_keyboard_interrupt_records_failed_end(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1'))])
        def interrupted():
            raise KeyboardInterrupt()
        output = io.StringIO()
        with self.assertRaises(KeyboardInterrupt):
            collect(stream, interrupted, TradeFlow(lambda a,b: []), output,
                    duration=0.05, wait_timeout=0.5)
        end = json.loads(output.getvalue().splitlines()[-1])
        self.assertEqual(end['status'], 'failed')
        self.assertIn('KeyboardInterrupt', end['reason'])


if __name__ == '__main__':
    unittest.main()
