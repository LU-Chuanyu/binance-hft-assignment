import csv
import json
from pathlib import Path
import tempfile
import unittest

from market_reconcile import _iter_market
from partition_replay import iter_journal
from partition_store import PartitionStore


class LargeCSVRecordTests(unittest.TestCase):
    def test_journal_preserves_valid_json_larger_than_csv_default_limit(self):
        record = {'journal_seq': 1, 'type': 'book_validation',
                  'status': 'matched', 'details': 'x' * 150_000}
        end = {'journal_seq': 2, 'type': 'end', 'status': 'success'}
        with tempfile.TemporaryDirectory() as directory:
            store = PartitionStore(directory)
            store.submit('book_validation', 0, record)
            store.submit('end', 0, end)
            store.close()
            original = csv.field_size_limit()
            try:
                csv.field_size_limit(131_072)
                rows = [json.loads(line) for line in iter_journal(directory)]
            finally:
                csv.field_size_limit(original)
        self.assertEqual(rows, [record, end])

    def test_market_preserves_valid_json_larger_than_csv_default_limit(self):
        record = {'kind': 'depth', 'details': 'x' * 150_000}
        with tempfile.TemporaryDirectory() as directory:
            store = PartitionStore(directory)
            store.submit('depth', 0, record)
            store.close()
            original = csv.field_size_limit()
            try:
                csv.field_size_limit(131_072)
                rows = list(_iter_market(Path(directory)))
            finally:
                csv.field_size_limit(original)
        self.assertEqual(rows, [(0, record)])


if __name__ == '__main__':
    unittest.main()
