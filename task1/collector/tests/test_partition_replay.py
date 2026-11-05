import gzip
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from journal_replay import replay
import partition_replay
from partition_replay import iter_journal, list_runs
from partition_store import PartitionStore
from test_partition_store import pq


def fixture_records():
    return [
        {'journal_seq': 1, 'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}},
        {'journal_seq': 2, 'type': 'raw', 'receive_seq': 1, 'received_ns': 1000, 'monotonic_ns': 0,
         'raw_text': json.dumps({'stream': 'btcusdt@depth@100ms', 'data': {'U': 101, 'u': 105, 'b': [['100', '1']], 'a': []}})},
        {'journal_seq': 3, 'type': 'depth_action', 'receive_seq': 1, 'action': 'applied', 'before_id': 100, 'after_id': 105},
        {'journal_seq': 4, 'type': 'end', 'status': 'success', 'received_count': 1, 'processed_depth': 1,
         'pending_depth': 0, 'last_update_id': 105, 'top': {'bids': [['100', '1']], 'asks': []}},
    ]


def write_fixture(path, *, batch_size=10, file_format='csv'):
    store = PartitionStore(path, batch_size=batch_size, flush_interval=60, file_format=file_format)
    for record, ms in zip(fixture_records(), [0, 86_400_000, 3_600_000, 86_400_000]):
        store.submit(record['type'], ms, record)
    run = store._run_id
    store.close()
    return run


class PartitionReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def test_restores_order_across_kind_day_hour_and_ignores_temp(self):
        run = write_fixture(self.path)
        (self.path / 'unpublished.tmp').write_bytes(b'not a shard')
        self.assertEqual(list_runs(self.path), [run])
        lines = list(iter_journal(self.path))
        self.assertEqual([json.loads(line)['journal_seq'] for line in lines], [1, 2, 3, 4])
        result = replay(iter(lines))
        self.assertEqual(result['last_update_id'], 105)
        self.assertEqual(result['top']['bids'], [['100', '1']])

    def test_missing_middle_or_final_shard_fails(self):
        write_fixture(self.path)
        file = next((self.path / 'raw').rglob('*.csv.gz'))
        file.unlink()
        with self.assertRaisesRegex(ValueError, '缺失|不连续'):
            list(iter_journal(self.path))
        with tempfile.TemporaryDirectory() as other:
            write_fixture(other)
            next((Path(other) / 'end').rglob('*.csv.gz')).unlink()
            with self.assertRaisesRegex(ValueError, '结束'):
                list(iter_journal(other))

    def test_duplicate_sequence_is_reported(self):
        write_fixture(self.path)
        file = next((self.path / 'raw').rglob('*.csv.gz'))
        run, first, _ = file.name.removesuffix('.csv.gz').split('-')
        target = file.with_name(f'{run}-{first}-' + 'f' * 32 + '.csv.gz')
        shutil.copyfile(file, target)
        with self.assertRaisesRegex(ValueError, '重复|不连续'):
            list(iter_journal(self.path))

    def test_different_runs_require_explicit_selection(self):
        first = write_fixture(self.path)
        second = write_fixture(self.path)
        with self.assertRaisesRegex(ValueError, '多个'):
            list(iter_journal(self.path))
        self.assertEqual(len(list(iter_journal(self.path, run_id=first))), 4)
        self.assertEqual(len(list(iter_journal(self.path, run_id=second))), 4)
        with self.assertRaisesRegex(ValueError, '找不到'):
            list(iter_journal(self.path, run_id='0' * 32))

    def test_wrong_journal_sequence_or_kind_is_rejected(self):
        for record in ({'journal_seq': 2, 'type': 'end'}, {'journal_seq': 1, 'type': 'wrong'}):
            with self.subTest(record=record), tempfile.TemporaryDirectory() as other:
                store = PartitionStore(other)
                store.submit('end', 0, record)
                store.close()
                with self.assertRaises(ValueError):
                    list(iter_journal(other))

    def test_truncated_published_shard_reports_failure(self):
        write_fixture(self.path)
        file = next((self.path / 'raw').rglob('*.csv.gz'))
        data = file.read_bytes()
        file.write_bytes(data[:-8])
        with self.assertRaisesRegex(ValueError, '读取分片'):
            list(iter_journal(self.path))

    @unittest.skipIf(pq is None, 'pyarrow unavailable')
    def test_parquet_and_csv_runs_share_discovery_and_replay(self):
        csv_run = write_fixture(self.path)
        parquet_run = write_fixture(self.path, file_format='parquet')
        (self.path / 'unpublished.parquet.tmp').write_bytes(b'not a shard')
        self.assertEqual(list_runs(self.path), sorted([csv_run, parquet_run]))
        for run, suffix in ((csv_run, '.csv.gz'), (parquet_run, '.parquet')):
            with self.subTest(suffix=suffix):
                paths = partition_replay.shard_paths(self.path, run)
                self.assertEqual(len(paths), 4)
                self.assertTrue(all(path.name.startswith(run) and path.name.endswith(suffix) for path in paths))
                self.assertEqual([json.loads(line) for line in iter_journal(self.path, run_id=run)], fixture_records())

    @unittest.skipIf(pq is None, 'pyarrow unavailable')
    def test_truncated_parquet_reports_the_shard_path(self):
        write_fixture(self.path, file_format='parquet')
        path = next((self.path / 'raw').rglob('*.parquet'))
        path.write_bytes(path.read_bytes()[:-8])
        with self.assertRaisesRegex(ValueError, f'读取分片 .*{path.name}'):
            list(iter_journal(self.path))


if __name__ == '__main__':
    unittest.main()
