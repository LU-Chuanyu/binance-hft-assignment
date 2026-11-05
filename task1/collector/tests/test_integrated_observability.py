import io
import json
import threading
import time
import unittest

from api_client import ApiResult
from integrated_collector import collect
from journal_replay import replay
from market_stream import RawMessage
from reconnect_stream import StreamMessage, StreamOpened
from test_integrated_collector import FakeStream, depth, message, trade
from trade_flow import TradeFlow


class ObservabilityIntegrationTests(unittest.TestCase):
    def test_capture_journals_runtime_stats_clock_alert_and_backfill_estimate(self):
        synchronized = threading.Event()
        calibrated = threading.Event()
        trade_times = {}

        class CaptureOutput(io.StringIO):
            def write(self, text):
                written = super().write(text)
                row = json.loads(text)
                if row['type'] == 'depth_action' and row['action'] == 'applied':
                    synchronized.set()
                if row['type'] == 'clock_sample' and row['usable']:
                    calibrated.set()
                return written

        class TimestampedStream(FakeStream):
            def next_event(self):
                item = super().next_event()
                if not isinstance(item, StreamMessage) or item.message.sequence == 1:
                    return item
                if not synchronized.wait(1) or not calibrated.wait(1):
                    raise TimeoutError('test collector did not synchronize and calibrate')
                original = json.loads(item.message.raw_text)
                wall = time.time_ns()
                mono = time.monotonic_ns()
                event = original['data']
                event['E'] = wall // 1_000_000 - 745
                if 'T' in event:
                    event['T'] = event['E'] - 5
                    trade_times[event['a']] = event['T']
                return StreamMessage(item.generation, RawMessage(
                    item.message.sequence, wall, mono, json.dumps(original)))

        def get_clock():
            wall = time.time_ns()
            mono = time.monotonic_ns()
            server_ms = (wall - 10_000_000) // 1_000_000 - 700
            return ApiResult({'serverTime': server_ms}, {}, None,
                             wall - 20_000_000, wall, mono - 20_000_000, mono)

        trade_ids = [*range(100, 120), 121]
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'depth', depth(106, 107, '1')),
                  *[message(seq, 1, 'aggTrade', trade(trade_id))
                    for seq, trade_id in enumerate(trade_ids, 3)]]
        flow = TradeFlow(lambda first, limit: [{**trade(first), 'T': trade_times[121]}])
        output = CaptureOutput()
        collect(TimestampedStream(events),
                lambda: {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []},
                flow, output, duration=0.2, wait_timeout=1, clock_query=get_clock)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        alerts = [row for row in rows if row['type'] == 'clock_offset_alert']
        self.assertEqual(len(alerts), 1)
        self.assertGreater(alerts[0]['offset_ns'], 500_000_000)
        stats = [row for row in rows if row['type'] == 'runtime_stats']
        self.assertEqual(len(stats), 1)
        self.assertTrue(stats[0]['partial'])
        self.assertEqual(stats[0]['streams']['aggTrade']['count'], len(trade_ids))
        self.assertEqual(stats[0]['streams']['aggTrade']['delay']['count'], len(trade_ids))
        backfill = next(row for row in rows if row['type'] == 'trade_backfill')
        self.assertEqual(backfill['origin_generation'], 1)
        self.assertIsNone(backfill['record']['received_ns'])
        self.assertIsInstance(backfill['record']['estimated_received_ns'], int)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 107)


if __name__ == '__main__':
    unittest.main()
