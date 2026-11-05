import csv
import gzip
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from partition_store import PartitionStore, StorageError

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except (ImportError, OSError, ValueError):
    pa = pq = None


class PartitionStoreTests(unittest.TestCase):
    def test_batches_are_atomic_compressed_and_partitioned_by_type_and_utc_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = PartitionStore(tmp, batch_size=100, flush_interval=60)
            store.submit('depth', 0, {'price': '100', 'quantity': '0.5'})
            store.submit('depth', 3_600_000, {'price': '101', 'quantity': '1'})
            store.submit('trade', 0, {'id': 123})
            store.close()
            root = Path(tmp)
            files = list(root.rglob('*.csv.gz'))
            self.assertEqual(len(files), 3)
            self.assertEqual(list(root.rglob('*.tmp')), [])
            got = {}
            for path in files:
                with gzip.open(path, 'rt', encoding='utf-8', newline='') as f:
                    rows = list(csv.DictReader(f))
                self.assertEqual(len(rows), 1)
                got[(path.parts[-4], path.parts[-3], path.parts[-2])] = rows[0]
            self.assertEqual(json.loads(got[('depth', '1970-01-01', '00')]['body_json']),
                             {'price': '100', 'quantity': '0.5'})
            self.assertEqual(json.loads(got[('depth', '1970-01-01', '01')]['body_json'])['price'], '101')
            self.assertEqual(json.loads(got[('trade', '1970-01-01', '00')]['body_json'])['id'], 123)
            self.assertEqual({row['sequence'] for row in got.values()}, {'1', '2', '3'})

    def test_queue_full_and_writer_failure_are_visible_to_the_caller(self):
        with tempfile.TemporaryDirectory() as tmp:
            arrived = threading.Event()
            release = threading.Event()
            def stalled_write(store, batch):
                arrived.set()
                if not release.wait(2):
                    raise RuntimeError('test timeout')
                raise OSError('disk failure')
            with patch.object(PartitionStore, '_write_batch', stalled_write):
                store = PartitionStore(tmp, batch_size=1, queue_size=1)
                store.submit('depth', 0, {'x': 1})
                self.assertTrue(arrived.wait(1))
                store.submit('depth', 1, {'x': 2})
                try:
                    with self.assertRaisesRegex(StorageError, '队列'):
                        store.submit('depth', 2, {'x': 3})
                finally:
                    release.set()
                with self.assertRaisesRegex(StorageError, '写盘'):
                    store.close()

    def test_existing_files_are_not_overwritten_and_closed_store_rejects_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = PartitionStore(tmp, batch_size=1)
            first.submit('depth', 0, {'value': 'first'})
            first.close()
            before = {p: p.read_bytes() for p in Path(tmp).rglob('*.csv.gz')}
            second = PartitionStore(tmp, batch_size=1)
            second.submit('depth', 0, {'value': 'second'})
            second.close()
            self.assertTrue(all(path.read_bytes() == data for path, data in before.items()))
            self.assertEqual(len(list(Path(tmp).rglob('*.csv.gz'))), 2)
            with self.assertRaisesRegex(StorageError, '关闭'):
                second.submit('depth', 0, {})

    def test_auto_uses_csv_without_pyarrow(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(sys.modules, {'pyarrow': None}):
            store = PartitionStore(tmp, file_format='auto')
            store.submit('depth', 0, {'value': 'fallback'})
            store.close()
            self.assertEqual(store.file_format, 'csv')
            self.assertEqual(len(list(Path(tmp).rglob('*.csv.gz'))), 1)
            with self.assertRaisesRegex(StorageError, 'pyarrow'):
                PartitionStore(tmp, file_format='parquet')

    @unittest.skipIf(pq is None, 'pyarrow unavailable')
    def test_auto_publishes_typed_parquet_with_exact_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = PartitionStore(tmp, file_format='auto')
            timestamp = 1_735_689_599_999
            store.submit('depth', timestamp, {'ns': 1_735_689_599_999_987_654, 'note': '行情'})
            store.close()
            self.assertEqual(store.file_format, 'parquet')
            path, = Path(tmp).rglob('*.parquet')
            self.assertEqual(path.parent.relative_to(tmp).parts, ('depth', '2024-12-31', '23'))
            self.assertEqual(list(Path(tmp).rglob('*.tmp')), [])
            with pq.ParquetFile(path) as file:
                table = file.read()
            self.assertEqual(table.schema.types, [pa.int64(), pa.int64(), pa.string()])
            self.assertEqual(table.to_pylist(), [{'sequence': 1, 'timestamp_ms': timestamp,
                                                'body_json': '{"ns":1735689599999987654,"note":"行情"}'}])

    @unittest.skipIf(pq is None, 'pyarrow unavailable')
    def test_auto_reports_write_failure_without_publishing_or_switching_formats(self):
        def failed_write(table, path, **options):
            Path(path).write_bytes(b'incomplete parquet')
            raise OSError('disk failure')

        with tempfile.TemporaryDirectory() as tmp:
            store = PartitionStore(tmp, file_format='auto')
            with patch.object(pq, 'write_table', failed_write):
                store.submit('depth', 0, {'value': 1})
                with self.assertRaisesRegex(StorageError, 'disk failure'):
                    store.close()
            self.assertEqual(list(Path(tmp).rglob('*.parquet')), [])
            self.assertEqual(list(Path(tmp).rglob('*.csv.gz')), [])
            self.assertEqual(list(Path(tmp).rglob('*.tmp')), [])


if __name__ == '__main__':
    unittest.main()
