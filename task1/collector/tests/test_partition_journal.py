import tempfile
import unittest
from pathlib import Path
import csv
import gzip
import json

from integrated_collector import collect
from journal_replay import replay
from partition_journal import PartitionJournal
from partition_replay import iter_journal
from partition_store import PartitionStore
from reconnect_stream import StreamClosed, StreamOpened, StreamLost
from test_integrated_collector import FakeStream, depth, message, trade
from trade_flow import TradeFlow


class PartitionJournalTests(unittest.TestCase):
    def test_slow_close_does_not_count_ticks_after_capture_deadline(self):
        class SlowClose(FakeStream):
            def close(self):
                super().close()
                import time
                time.sleep(1.15)
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '0.5'))]
        import io
        output = io.StringIO()
        collect(SlowClose(events), lambda: {'lastUpdateId': 100, 'bids': [], 'asks': []},
                TradeFlow(lambda a,b: []), output, duration=0.05, wait_timeout=0.5)
        result = replay(io.StringIO(output.getvalue()))
        self.assertEqual(result['checkpoint_gaps'], 0)

    def test_long_snapshot_blocks_one_second_checkpoint_and_records_gap(self):
        class BlockingStream(FakeStream):
            def next_event(self):
                event = self.events.get()
                if event is None:
                    raise StreamClosed('closed')
                return event
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '0.5')),
                  message(2, 1, 'depth', depth(108, 110, '9')),
                  message(3, 1, 'depth', depth(111, 115, '3'))]
        calls = []
        def slow():
            calls.append(1)
            if len(calls) == 1:
                return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
            import time
            time.sleep(1.3)
            return {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}
        import io
        output = io.StringIO()
        collect(BlockingStream(events), slow, TradeFlow(lambda a,b: []),
                output, duration=1.1, wait_timeout=2)
        result = replay(io.StringIO(output.getvalue()))
        self.assertGreaterEqual(result['checkpoint_gaps'], 1)

    def test_checkpoint_is_written_during_idle_after_bridge(self):
        class BlockingStream(FakeStream):
            def next_event(self):
                event = self.events.get()
                if event is None:
                    raise StreamClosed('closed')
                return event
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '0.5'))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        import io
        output = io.StringIO()
        collect(BlockingStream(events), lambda: snapshot, TradeFlow(lambda a,b: []),
                output, duration=1.15, wait_timeout=2)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        checkpoints = [r for r in rows if r['type'] == 'book_checkpoint']
        self.assertGreaterEqual(len(checkpoints), 1)
        self.assertEqual(checkpoints[0]['last_update_id'], 105)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['checkpoints_checked'],
                         len(checkpoints))

    def test_market_projection_uses_separate_store_and_keeps_backfill_source(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '0.5')),
                  message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(102))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            market = PartitionStore(root / 'market', batch_size=2)
            try:
                with PartitionJournal(root / 'journal', batch_size=2) as output:
                    collect(FakeStream(events), lambda: snapshot,
                            TradeFlow(lambda a, b: [trade(101)]), output,
                            duration=0.05, wait_timeout=0.5, market_store=market)
            finally:
                market.close()
            self.assertEqual(replay(iter_journal(root / 'journal'))['last_update_id'], 105)
            rows = []
            for path in (root / 'market').rglob('*.csv.gz'):
                with gzip.open(path, 'rt', encoding='utf-8', newline='') as f:
                    rows.extend(json.loads(r['body_json']) for r in csv.DictReader(f))
            self.assertEqual(sum(r['kind'] == 'depth' for r in rows), 1)
            backed = [r for r in rows if r.get('source') == 'rest_backfill']
            self.assertEqual([r['a'] for r in backed], [101])
            self.assertIsNone(backed[0]['received_ns'])
            self.assertIsNone(backed[0]['estimated_received_ns'])
            self.assertEqual(backed[0]['partition_basis'], 'exchange_trade_time_ms')
            backfill_files = list((root / 'market' / 'aggTrade' / '1970-01-01' / '00').glob('*.csv.gz'))
            self.assertTrue(backfill_files)
            partition_sources = []
            for path in backfill_files:
                with gzip.open(path, 'rt', encoding='utf-8', newline='') as f:
                    partition_sources.extend(json.loads(r['body_json'])['source'] for r in csv.DictReader(f))
            self.assertIn('rest_backfill', partition_sources)

    def test_market_store_failure_does_not_report_success(self):
        class BrokenMarket:
            def submit(self, *args):
                raise OSError('market queue full')
            def close(self):
                pass
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        import io
        output = io.StringIO()
        with self.assertRaisesRegex(OSError, 'market queue full'):
            collect(FakeStream(events), lambda: snapshot, TradeFlow(lambda a,b: []),
                    output, duration=0.05, wait_timeout=0.5, market_store=BrokenMarket())
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_market_close_failure_does_not_report_success(self):
        class BrokenClose:
            def submit(self, *args):
                return 1
            def close(self):
                raise OSError('market flush failed')
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        import io
        output = io.StringIO()
        with self.assertRaisesRegex(OSError, 'market flush failed'):
            collect(FakeStream(events), lambda: snapshot, TradeFlow(lambda a,b: []),
                    output, duration=0.05, wait_timeout=0.5, market_store=BrokenClose())
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_integrated_reconnect_backfill_roundtrip_through_background_partitions(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '0.5')),
                  message(2, 1, 'aggTrade', trade(100)),
                  StreamLost(1, 20, 'lost', True), StreamOpened(2, True, True, 30),
                  message(3, 2, 'depth', depth(111, 115, '3')),
                  message(4, 2, 'aggTrade', trade(102))]
        snapshots = iter([{'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []},
                          {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}])
        with tempfile.TemporaryDirectory() as directory:
            with PartitionJournal(directory, batch_size=2, flush_interval=0.1) as output:
                result = collect(FakeStream(events), lambda: next(snapshots),
                                 TradeFlow(lambda a, b: [trade(101)]), output,
                                 duration=0.05, wait_timeout=0.5)
            self.assertEqual(result['last_update_id'], 115)
            self.assertTrue(list(Path(directory).rglob('*.csv.gz')))
            self.assertEqual(replay(iter_journal(directory))['last_update_id'], 115)

    def test_write_failure_is_propagated(self):
        class BrokenStore:
            def submit(self, *args):
                raise OSError('disk failed')
            def close(self):
                pass
        output = PartitionJournal('unused', store=BrokenStore())
        with self.assertRaisesRegex(OSError, 'disk failed'):
            output.write('{"journal_seq":1,"type":"raw"}\n')

    def test_close_failure_is_propagated(self):
        class BrokenStore:
            def submit(self, *args):
                return 1
            def close(self):
                raise OSError('disk failed at flush')
        with self.assertRaisesRegex(OSError, 'disk failed at flush'):
            with PartitionJournal('unused', store=BrokenStore()):
                pass


if __name__ == '__main__':
    unittest.main()
