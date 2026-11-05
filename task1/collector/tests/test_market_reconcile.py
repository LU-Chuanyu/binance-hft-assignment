import json
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from market_projection import project_live, project_backfill
from market_reconcile import reconcile
from market_stream import RawMessage
from partition_store import PartitionStore
from partition_journal import PartitionJournal
from integrated_collector import collect
from reconnect_stream import StreamOpened, StreamLost
from trade_flow import TradeFlow
from test_integrated_collector import FakeStream, message, depth, trade
from test_partition_store import pq


def msg(seq, kind, data):
    stream = {'depth': 'btcusdt@depth@100ms', 'aggTrade': 'btcusdt@aggTrade',
              'bookTicker': 'btcusdt@bookTicker'}[kind]
    return RawMessage(seq, 1_000_000_000 + seq, seq, json.dumps({'stream': stream, 'data': data}))


def fixture(root, *, omit_market=False, corrupt=False, cross_hour=False, journal_format='csv', market_format='csv'):
    book = msg(1, 'depth', {'s': 'BTCUSDT', 'E': 1000, 'U': 101, 'u': 105,
                           'b': [['100', '1']], 'a': []})
    ticker = msg(2, 'bookTicker', {'s': 'BTCUSDT', 'u': 105, 'b': '100', 'B': '1', 'a': '101', 'A': '2'})
    trade = msg(3, 'aggTrade', {'s': 'BTCUSDT', 'a': 3, 'p': '100', 'q': '1', 'm': False, 'T': 1000, 'E': 1000})
    if cross_hour:
        book = replace(book, received_ns=3_599_000_000_000)
        ticker = replace(ticker, received_ns=3_600_000_000_000)
        trade = replace(trade, received_ns=3_600_001_000_000)
    rows = [{'type': 'connection_open', 'generation': 1},
            {'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}}]
    def raw(message):
        return {'type': 'raw', 'generation': 1, 'receive_seq': message.sequence,
                'received_ns': message.received_ns, 'monotonic_ns': message.monotonic_ns,
                'raw_text': message.raw_text}
    rows.extend([raw(book), {'type': 'depth_action', 'receive_seq': 1, 'action': 'applied',
                            'before_id': 100, 'after_id': 105}, raw(ticker), raw(trade),
                 {'type': 'trade_live', 'receive_seq': 3, 'include': True, 'status': 'first', 'missing_ranges': []},
                 {'type': 'end', 'status': 'success', 'received_count': 3, 'processed_depth': 1,
                  'pending_depth': 0, 'last_update_id': 105,
                  'top': {'bids': [['100', '1']], 'asks': []}}])
    store = PartitionStore(Path(root) / 'journal', batch_size=2, file_format=journal_format)
    for i, row in enumerate(rows, 1):
        row['journal_seq'] = i
        store.submit(row['type'], 1000, row)
    store.close()
    market = PartitionStore(Path(root) / 'market', batch_size=2, file_format=market_format)
    for message in [book, ticker, trade]:
        for row in project_live(message, generation=1):
            if row['kind'] == 'aggTrade' and omit_market:
                continue
            if row['kind'] == 'aggTrade' and corrupt:
                row['q'] = '999'
            market.submit(row['kind'], message.received_ns // 1_000_000, row)
    market.close()


class ReconcileTests(unittest.TestCase):
    @unittest.skipIf(pq is None, 'pyarrow unavailable')
    def test_market_reconcile_reads_both_shard_formats(self):
        for journal_format, market_format in (('csv', 'parquet'), ('parquet', 'csv'), ('parquet', 'parquet')):
            with self.subTest(journal=journal_format, market=market_format), tempfile.TemporaryDirectory() as root:
                fixture(root, journal_format=journal_format, market_format=market_format)
                result = reconcile(root)
                self.assertEqual(result['status'], 'matched')
                self.assertEqual((result['expected_count'], result['actual_count']), (3, 3))

    def test_all_live_market_rows_match_verified_journal(self):
        with tempfile.TemporaryDirectory() as root:
            fixture(root)
            result = reconcile(root)
            self.assertEqual(result['status'], 'matched')
            self.assertEqual(result['expected_count'], 3)
            self.assertEqual(result['missing_count'], 0)

    def test_missing_tail_and_wrong_field_are_not_called_match(self):
        for kwargs in ({'omit_market': True}, {'corrupt': True}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as root:
                fixture(root, **kwargs)
                result = reconcile(root)
                self.assertEqual(result['status'], 'mismatch')
                self.assertGreater(result['missing_count'], 0)

    def test_missing_interior_shard_and_truncated_gzip_are_errors(self):
        with tempfile.TemporaryDirectory() as root:
            fixture(root)
            next((Path(root) / 'market' / 'bookTicker').rglob('*.csv.gz')).unlink()
            with self.assertRaisesRegex(ValueError, '编号'):
                reconcile(root)
        with tempfile.TemporaryDirectory() as root:
            fixture(root)
            path = next((Path(root) / 'market' / 'depth').rglob('*.csv.gz'))
            path.write_bytes(path.read_bytes()[:20])
            with self.assertRaisesRegex(ValueError, '分片'):
                reconcile(root)

    def test_duplicate_live_trade_and_backfill_generation_reconcile(self):
        with tempfile.TemporaryDirectory() as root:
            events = [StreamOpened(1, False, True, 10),
                      message(1, 1, 'depth', depth(101, 105, '1')),
                      message(2, 1, 'aggTrade', trade(100)),
                      message(3, 1, 'aggTrade', trade(100)),
                      StreamLost(1, 20, 'lost', True), StreamOpened(2, True, True, 30),
                      message(4, 2, 'depth', depth(111, 115, '3')),
                      message(5, 2, 'aggTrade', trade(102))]
            snapshots = iter([{'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []},
                              {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}])
            with PartitionJournal(Path(root) / 'journal') as output:
                collect(FakeStream(events), lambda: next(snapshots),
                        TradeFlow(lambda first, limit: [trade(101)]), output,
                        duration=0.1, wait_timeout=0.5,
                        market_store=PartitionStore(Path(root) / 'market'))
            result = reconcile(root)
            self.assertEqual(result['status'], 'matched')
            self.assertEqual(result['unverified_backfill_generation_count'], 0)
            self.assertEqual((result['missing_count'], result['extra_count']), (0, 0))

    def test_cross_hour_partition_and_incorrect_directory(self):
        with tempfile.TemporaryDirectory() as root:
            fixture(root, cross_hour=True)
            result = reconcile(root)
            self.assertEqual(result['status'], 'matched')
            file = next((Path(root) / 'market' / 'bookTicker').rglob('*.csv.gz'))
            wrong = file.parent.parent / '02' / file.name
            wrong.parent.mkdir(parents=True)
            file.rename(wrong)
            with self.assertRaisesRegex(ValueError, '日期小时'):
                reconcile(root)


if __name__ == '__main__':
    unittest.main()
