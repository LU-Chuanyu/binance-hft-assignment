import io
import json
import unittest

from market_stream import RawMessage
from raw_recording import write_raw_message


class RawRecordingTests(unittest.TestCase):
    def test_each_message_is_one_json_line_and_preserves_the_original_text(self):
        output = io.StringIO()
        first = RawMessage(1, 1700000000000000000, 123, ' {"x":"a\\nb"} ')
        second = RawMessage(2, 1700000000000000001, 124, '{"stream":"test"}')
        write_raw_message(output, first)
        write_raw_message(output, second)

        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {
            'sequence': 1, 'received_ns': 1700000000000000000,
            'monotonic_ns': 123, 'raw_text': first.raw_text,
        })
        self.assertEqual(json.loads(lines[1])['raw_text'], second.raw_text)
        self.assertEqual(json.loads(lines[1])['sequence'], 2)
        self.assertTrue(output.getvalue().endswith('\n'))

    def test_write_failures_are_reported(self):
        class BrokenFile:
            def write(self, text):
                raise OSError('disk error')

        with self.assertRaisesRegex(OSError, 'disk error'):
            write_raw_message(BrokenFile(), RawMessage(1, 1, 1, '{}'))


if __name__ == '__main__':
    unittest.main()
