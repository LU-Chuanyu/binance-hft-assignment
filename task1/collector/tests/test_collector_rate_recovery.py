import io
import json
import threading
import time
import unittest

from aggtrade_backfill import BackfillError
from api_client import RateLimitError, RateLimitWait
from integrated_collector import collect
from journal_replay import replay
from reconnect_stream import StreamOpened
from trade_flow import TradeFlow

from test_integrated_collector import FakeStream, depth, message, trade


SNAPSHOT = {'lastUpdateId': 100, 'bids': [], 'asks': []}
INITIAL = [StreamOpened(1, False, True, 10), message(1, 1, 'depth', depth(101, 105, '1'))]
RETRY_AFTER = 0.12


class WaitJournal(io.StringIO):
    def __init__(self):
        super().__init__()
        self.waiting = threading.Event()

    def write(self, text):
        length = super().write(text)
        row = json.loads(text)
        if row['type'] == 'rest_retry' and row['state'] == 'waiting':
            self.waiting.set()
        return length

    def rows(self):
        return [json.loads(line) for line in self.getvalue().splitlines()]


class StreamDuringWait(FakeStream):
    def __init__(self, events, journal, sequence):
        super().__init__(events)
        self.waiting = journal.waiting
        self.sequence = sequence

    def next_event(self):
        event = super().next_event()
        if getattr(getattr(event, 'message', None), 'sequence', None) == self.sequence:
            if not self.waiting.wait(1):
                raise TimeoutError('retry did not enter waiting')
        return event


class CollectorRateRecoveryTests(unittest.TestCase):
    def test_snapshot_recovers_after_cooldown_while_trades_are_processed(self):
        for error in (RateLimitError(429, RETRY_AFTER), RateLimitError(418, RETRY_AFTER),
                      RateLimitWait(RETRY_AFTER)):
            with self.subTest(error=error):
                calls = []

                def get_snapshot():
                    calls.append(time.monotonic())
                    if len(calls) == 1:
                        raise error
                    return SNAPSHOT

                output = WaitJournal()
                stream = StreamDuringWait([*INITIAL, message(2, 1, 'aggTrade', trade(100))],
                                          output, sequence=2)
                collect(stream, get_snapshot, TradeFlow(lambda first, limit: []), output,
                        duration=0.04, wait_timeout=0.5, rate_retry_timeout=1)
                rows = output.rows()
                waiting, recovered = [row for row in rows if row['type'] == 'rest_retry']
                trade_live = next(row for row in rows if row['type'] == 'trade_live')
                self.assertEqual((waiting['operation'], waiting['state'], recovered['state']),
                                 ('snapshot', 'waiting', 'recovered'))
                self.assertLess(rows.index(waiting), rows.index(trade_live))
                self.assertLess(rows.index(trade_live), rows.index(recovered))
                self.assertGreaterEqual(calls[1] - calls[0], RETRY_AFTER)
                self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)

    def test_trade_retry_keeps_partial_page_and_requests_only_still_missing_ids(self):
        calls = []

        def fetch_page(first, limit):
            calls.append((first, time.monotonic()))
            if first == 102:
                raise BackfillError('limited', status=429, retry_after=RETRY_AFTER)
            return [trade(first)]

        output = WaitJournal()
        stream = StreamDuringWait([*INITIAL, message(2, 1, 'aggTrade', trade(100)),
                                   message(3, 1, 'aggTrade', trade(104)),
                                   message(4, 1, 'aggTrade', trade(102))], output, sequence=4)
        result = collect(stream, lambda: SNAPSHOT, TradeFlow(fetch_page), output,
                         duration=0.26, wait_timeout=0.5, rate_retry_timeout=1)
        rows = output.rows()
        recovered = [row for row in rows if row['type'] == 'trade_backfill']
        self.assertEqual([row['record']['data']['a'] for row in recovered], [101, 103])
        self.assertEqual([row['origin_generation'] for row in recovered], [1, 1])
        self.assertEqual([first for first, _ in calls], [101, 102, 103])
        self.assertGreaterEqual(calls[2][1] - calls[1][1], RETRY_AFTER)
        self.assertEqual([row['state'] for row in rows if row['type'] == 'rest_retry'],
                         ['waiting', 'recovered'])
        self.assertEqual(result['missing_ranges'], [])
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)

    def test_trade_451_is_recorded_and_not_retried(self):
        calls = []

        def fetch_page(first, limit):
            calls.append(first)
            raise BackfillError('access restricted', status=451)

        output = WaitJournal()
        events = [*INITIAL, message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(102))]
        with self.assertRaisesRegex(RuntimeError, '补查'):
            collect(FakeStream(events), lambda: SNAPSHOT, TradeFlow(fetch_page), output,
                    duration=0.05, wait_timeout=0.5, rate_retry_timeout=1)
        rows = output.rows()
        failure, = [row for row in rows if row['type'] == 'trade_repair']
        self.assertEqual(failure['errors'][0]['status'], 451)
        self.assertFalse(any(row['type'] == 'rest_retry' for row in rows))
        self.assertEqual(calls, [101])
        self.assertEqual(rows[-1]['status'], 'failed')

    def test_late_trade_closes_gap_before_inflight_retry_returns_error(self):
        retry_started = threading.Event()
        late_written = threading.Event()
        calls = []

        class InflightJournal(WaitJournal):
            def write(self, text):
                length = super().write(text)
                row = json.loads(text)
                if row['type'] == 'trade_live' and row['status'] == 'late':
                    late_written.set()
                return length

        class InflightStream(FakeStream):
            def next_event(self):
                event = super().next_event()
                if getattr(getattr(event, 'message', None), 'sequence', None) == 4:
                    if not retry_started.wait(1):
                        raise TimeoutError('retry did not start')
                return event

        def fetch_page(first, limit):
            calls.append(first)
            if len(calls) == 1:
                raise BackfillError('limited', status=429, retry_after=RETRY_AFTER)
            retry_started.set()
            if not late_written.wait(1):
                raise TimeoutError('late trade was not written')
            raise BackfillError('gateway error', status=502)

        output = InflightJournal()
        events = [*INITIAL, message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(102)), message(4, 1, 'aggTrade', trade(101))]
        result = collect(InflightStream(events), lambda: SNAPSHOT, TradeFlow(fetch_page), output,
                         duration=0.26, wait_timeout=0.5, rate_retry_timeout=1)
        self.assertEqual(calls, [101, 101])
        self.assertEqual(result['missing_ranges'], [])
        waiting, resolved = [row for row in output.rows() if row['type'] == 'rest_retry']
        self.assertEqual((waiting['state'], resolved['state']), ('waiting', 'resolved_live'))
        self.assertEqual(resolved['error']['status'], 502)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)

    def test_trade_retry_outside_cutoff_records_partial_page_and_remaining_gap(self):
        calls = []

        def fetch_page(first, limit):
            calls.append(first)
            if first == 102:
                raise BackfillError('limited', status=429, retry_after=120)
            return [trade(first)]

        output = WaitJournal()
        events = [*INITIAL, message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(103))]
        with self.assertRaisesRegex(RuntimeError, '补查'):
            collect(FakeStream(events), lambda: SNAPSHOT, TradeFlow(fetch_page), output,
                    duration=0.05, wait_timeout=0.5, rate_retry_timeout=1)
        rows = output.rows()
        backfill, = [row for row in rows if row['type'] == 'trade_backfill']
        repair, = [row for row in rows if row['type'] == 'trade_repair']
        failure, = [row for row in rows if row['type'] == 'rest_retry']
        self.assertEqual(backfill['record']['data']['a'], 101)
        self.assertEqual(repair['missing_ranges'], [[102, 102]])
        self.assertEqual((failure['state'], failure['error']['status']), ('failed', 429))
        self.assertEqual(calls, [101, 102])
        self.assertEqual(rows[-1]['status'], 'failed')
