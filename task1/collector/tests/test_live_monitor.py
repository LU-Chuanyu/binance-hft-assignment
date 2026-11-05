import io
import json
import tempfile
import unittest

from live_monitor import MonitorOutput, MonitorState


def raw_row(sequence, stream, data):
    return {'type': 'raw', 'journal_seq': sequence, 'receive_seq': sequence,
            'received_ns': 1700000000000000000 + sequence,
            'raw_text': json.dumps({'stream': stream, 'data': data})}


class LiveMonitorTests(unittest.TestCase):
    def test_real_raw_messages_are_counted_and_original_text_is_kept(self):
        state = MonitorState(60)
        rows = [raw_row(1, 'btcusdt@depth@100ms', {'U': 10, 'u': 12}),
                raw_row(2, 'btcusdt@aggTrade', {'a': 123}),
                raw_row(3, 'btcusdt@bookTicker', {'b': '100', 'B': '2', 'a': '101', 'A': '3'})]
        for row in rows:
            state.observe(row)
        result = state.snapshot()
        self.assertEqual(result['counts'], {'depth': 1, 'aggTrade': 1, 'bookTicker': 1})
        self.assertEqual(result['total_messages'], 3)
        self.assertEqual(result['messages'][0]['raw_text'], rows[-1]['raw_text'])
        self.assertEqual(result['messages'][0]['receive_seq'], 3)
        self.assertEqual(result['best']['bid_price'], '100')

    def test_checkpoint_connection_and_clock_states_come_from_records(self):
        state = MonitorState(60)
        state.observe({'type': 'connection_open', 'generation': 1})
        state.observe({'type': 'snapshot', 'snapshot': {'lastUpdateId': 100}})
        state.observe({'type': 'depth_action', 'action': 'applied', 'after_id': 105})
        state.observe({'type': 'book_checkpoint', 'last_update_id': 105,
                       'top': {'bids': [['100', '2']], 'asks': [['101', '3']]}})
        state.observe({'type': 'clock_sample', 'status': 'ok', 'usable': False,
                       'rtt_ns': 1250000000})
        state.observe({'type': 'book_validation', 'status': 'matched', 'compared': 32})
        result = state.snapshot()
        self.assertEqual(result['connection']['generation'], 1)
        self.assertEqual(result['last_update_id'], 105)
        self.assertEqual(result['checkpoints'], 1)
        self.assertFalse(result['clock']['usable'])
        self.assertEqual(result['validation']['compared'], 32)

    def test_end_and_error_are_visible_and_recent_messages_are_bounded(self):
        state = MonitorState(60)
        for i in range(20):
            state.observe(raw_row(i + 1, 'btcusdt@aggTrade', {'a': i}))
        state.observe({'type': 'end', 'status': 'success', 'received_count': 20,
                       'started_monotonic_ns': 1000000000,
                       'stopped_monotonic_ns': 61000000000})
        self.assertEqual(state.snapshot()['state'], 'finishing')
        state.finish()
        result = state.snapshot()
        self.assertEqual(result['state'], 'finished')
        self.assertEqual(result['collection_seconds'], 60)
        self.assertEqual(len(result['messages']), 12)
        state.fail(ValueError('真实读取失败'))
        self.assertEqual(state.snapshot()['state'], 'failed')
        self.assertIn('真实读取失败', state.snapshot()['error'])

    def test_output_only_updates_after_underlying_write_succeeds(self):
        state = MonitorState(60)
        text = json.dumps(raw_row(1, 'btcusdt@aggTrade', {'a': 1})) + '\n'
        target = io.StringIO()
        MonitorOutput(target, state).write(text)
        self.assertEqual(target.getvalue(), text)
        self.assertEqual(state.snapshot()['total_messages'], 1)

        class Broken:
            def write(self, _):
                raise OSError('disk full')
        with self.assertRaises(OSError):
            MonitorOutput(Broken(), state).write(text)
        self.assertEqual(state.snapshot()['total_messages'], 1)


if __name__ == '__main__':
    unittest.main()
