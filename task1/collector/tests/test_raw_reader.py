import io
import json
import unittest

from raw_reader import summarize


def line(sequence, stream, data):
    return json.dumps({
        'sequence': sequence,
        'received_ns': 1000 + sequence,
        'monotonic_ns': 2000 + sequence,
        'raw_text': json.dumps({'stream': stream, 'data': data}),
    }) + '\n'


class RawReaderTests(unittest.TestCase):
    def test_summarizes_types_and_keeps_first_example(self):
        content = (
            line(1, 'btcusdt@bookTicker', {'b': '100', 'a': '101'})
            + line(2, 'btcusdt@depth@100ms', {'U': 10, 'u': 11})
            + line(3, 'btcusdt@aggTrade', {'a': 12, 'p': '100'})
            + line(4, 'btcusdt@bookTicker', {'b': '99', 'a': '102'})
        )
        result = summarize(io.StringIO(content))
        self.assertEqual(result['counts'], {'depth': 1, 'aggTrade': 1, 'bookTicker': 2})
        self.assertEqual(result['examples']['bookTicker']['data']['b'], '100')
        self.assertEqual(result['examples']['depth']['sequence'], 2)
        self.assertEqual(result['sequence_gaps'], [])

    def test_detects_local_sequence_breaks(self):
        content = line(1, 'btcusdt@bookTicker', {}) + line(3, 'btcusdt@bookTicker', {})
        result = summarize(io.StringIO(content))
        self.assertEqual(result['sequence_gaps'], [(2, 2, 3)])

    def test_bad_record_has_line_number(self):
        with self.assertRaisesRegex(ValueError, '第 2 行'):
            summarize(io.StringIO(line(1, 'btcusdt@bookTicker', {}) + 'not json\n'))
        with self.assertRaisesRegex(ValueError, '第 1 行'):
            summarize(io.StringIO('{"sequence": 1}\n'))
        with self.assertRaisesRegex(ValueError, '第 1 行'):
            summarize(io.StringIO(line(1, 'unknown', {})))


if __name__ == '__main__':
    unittest.main()
