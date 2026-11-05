import unittest
import threading

import websocket

from market_stream import MarketStream, RawMessage
from reconnect_stream import ReconnectingStream, StreamClosed, StreamLost, StreamMessage, StreamOpened, ReconnectExhausted


class Connection:
    def __init__(self, events):
        self.events = iter(events)
        self.closed = False
        self.timeout = None
        self.connection = self

    def settimeout(self, value):
        self.timeout = value

    def receive(self):
        item = next(self.events)
        if isinstance(item, Exception):
            raise item
        return RawMessage(1, 100, 200, item)

    def close(self):
        self.closed = True


class ReconnectTests(unittest.TestCase):
    def test_loss_is_reported_before_reconnect_and_sequence_spans_connections(self):
        first = Connection(['one', ConnectionError('lost')])
        second = Connection(['two'])
        connections = iter([first, second])
        waits = []
        manager = ReconnectingStream(connector=lambda: next(connections), wait=lambda t: waits.append(t) or False, wall_ns=lambda: 1000)
        self.assertEqual(manager.next_event(), StreamOpened(1, False, True, 1000))
        one = manager.next_event()
        self.assertIsInstance(one, StreamMessage)
        self.assertEqual((one.generation, one.message.sequence, one.message.raw_text), (1, 1, 'one'))
        loss = manager.next_event()
        self.assertIsInstance(loss, StreamLost)
        self.assertEqual((loss.generation, loss.reason, loss.requires_resync), (1, 'lost', True))
        self.assertTrue(first.closed)
        self.assertEqual(manager.next_event(), StreamOpened(2, True, True, 1000))
        two = manager.next_event()
        self.assertEqual((two.generation, two.message.sequence, two.message.raw_text), (2, 2, 'two'))
        self.assertEqual(waits, [0.25])
        manager.close()
        self.assertTrue(second.closed)

    def test_server_shutdown_reconnects_without_consuming_market_sequence(self):
        shutdown = '{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789}}'
        first = Connection(['one', shutdown])
        second = Connection(['two'])
        connections = iter([first, second])
        manager = ReconnectingStream(connector=lambda: next(connections), wait=lambda _: False)
        self.addCleanup(manager.close)

        self.assertEqual(manager.next_event().generation, 1)
        self.assertEqual(manager.next_event().message.sequence, 1)
        loss = manager.next_event()
        self.assertIsInstance(loss, StreamLost)
        self.assertEqual(loss.generation, 1)
        self.assertTrue(loss.requires_resync)
        self.assertTrue(first.closed)
        reopened = manager.next_event()
        self.assertIsInstance(reopened, StreamOpened)
        self.assertEqual(reopened.generation, 2)
        next_message = manager.next_event()
        self.assertEqual((next_message.generation, next_message.message.sequence,
                          next_message.message.raw_text), (2, 2, 'two'))

    def test_timeout_reports_lost_and_sets_idle_timeout(self):
        reader = Connection([websocket.WebSocketTimeoutException('timeout')])
        manager = ReconnectingStream(connector=lambda: reader, idle_timeout=7)
        manager.next_event()
        self.assertEqual(reader.timeout, 7)
        self.assertIsInstance(manager.next_event(), StreamLost)

    def test_peer_close_frame_is_not_mistaken_for_permanent_parse_failure(self):
        class ClosedSocket:
            connected = True
            def settimeout(self, value):
                pass
            def recv(self):
                self.connected = False
                return ''
            def close(self):
                pass
        manager = ReconnectingStream(connector=lambda: MarketStream(ClosedSocket()))
        manager.next_event()
        self.assertIsInstance(manager.next_event(), StreamLost)

    def test_failures_backoff_and_stop_at_limit(self):
        waits, calls = [], []
        def connect():
            calls.append(1)
            raise OSError('network down')
        manager = ReconnectingStream(connector=connect, wait=lambda t: waits.append(t) or False, max_attempts=3)
        with self.assertRaises(ReconnectExhausted):
            manager.next_event()
        self.assertEqual(len(calls), 3)
        self.assertEqual(waits, [0.25, 0.5])

    def test_http_451_is_immediately_reported(self):
        calls = []
        def connect():
            calls.append(1)
            raise websocket.WebSocketBadStatusException('restricted', 451)
        manager = ReconnectingStream(connector=connect)
        with self.assertRaises(websocket.WebSocketBadStatusException):
            manager.next_event()
        self.assertEqual(len(calls), 1)

    def test_rotation_and_close_stop_further_use(self):
        clock = iter([0, 23 * 3600 + 1])
        reader = Connection([])
        manager = ReconnectingStream(connector=lambda: reader, monotonic=lambda: next(clock))
        self.assertEqual(manager.next_event().generation, 1)
        loss = manager.next_event()
        self.assertIsInstance(loss, StreamLost)
        self.assertEqual(loss.reason, 'connection_age_limit')
        self.assertTrue(reader.closed)
        manager.close()
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            manager.next_event()

    def test_message_successfully_received_before_close_is_delivered(self):
        received = threading.Event()
        release = threading.Event()
        outcomes = []
        class RacingConnection(Connection):
            def receive(self):
                result = RawMessage(1, 123, 456, 'already-received')
                received.set()
                if not release.wait(2):
                    raise TimeoutError('test timeout')
                return result
        reader = RacingConnection([])
        manager = ReconnectingStream(connector=lambda: reader)
        manager.next_event()
        def receive_one():
            try:
                outcomes.append(manager.next_event())
            except Exception as exc:
                outcomes.append(exc)
        thread = threading.Thread(target=receive_one)
        thread.start()
        try:
            self.assertTrue(received.wait(1))
            manager.close()
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], StreamMessage)
        self.assertEqual(outcomes[0].message.raw_text, 'already-received')
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            manager.next_event()

    def test_expected_shutdown_empty_frame_is_not_reported_as_remote_loss(self):
        waiting, release = threading.Event(), threading.Event()
        outcomes = []
        class Socket:
            connected = True
            def settimeout(self, value):
                pass
            def recv(self):
                waiting.set()
                if not release.wait(2):
                    raise TimeoutError('test timeout')
                return ''
            def close(self):
                self.connected = False
        manager = ReconnectingStream(connector=lambda: MarketStream(Socket()))
        manager.next_event()
        def receive_one():
            try:
                outcomes.append(manager.next_event())
            except Exception as exc:
                outcomes.append(exc)
        thread = threading.Thread(target=receive_one)
        thread.start()
        try:
            self.assertTrue(waiting.wait(1))
            manager.close()
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcomes[0], StreamClosed)
        self.assertIn('closed', str(outcomes[0]))

    def test_data_error_after_close_is_not_classified_as_normal_close(self):
        waiting, release = threading.Event(), threading.Event()
        outcomes = []
        class BadData(Connection):
            connected = True
            def receive(self):
                waiting.set()
                if not release.wait(2):
                    raise TimeoutError('test timeout')
                raise ValueError('corrupt data received')
        reader = BadData([])
        manager = ReconnectingStream(connector=lambda: reader)
        manager.next_event()
        def receive_one():
            try:
                outcomes.append(manager.next_event())
            except Exception as exc:
                outcomes.append(exc)
        thread = threading.Thread(target=receive_one)
        thread.start()
        try:
            self.assertTrue(waiting.wait(1))
            manager.close()
        finally:
            release.set()
            thread.join(2)
        self.assertIsInstance(outcomes[0], ValueError)
        self.assertNotIsInstance(outcomes[0], StreamClosed)

    def test_nontext_frame_racing_close_is_not_suppressed(self):
        waiting, release = threading.Event(), threading.Event()
        outcomes = []
        class Socket:
            connected = True
            def settimeout(self, value):
                pass
            def recv(self):
                waiting.set()
                if not release.wait(2):
                    raise TimeoutError('test timeout')
                return b'not-text'
            def close(self):
                self.connected = False
        manager = ReconnectingStream(connector=lambda: MarketStream(Socket()))
        manager.next_event()
        def receive_one():
            try:
                outcomes.append(manager.next_event())
            except Exception as exc:
                outcomes.append(exc)
        thread = threading.Thread(target=receive_one)
        thread.start()
        try:
            self.assertTrue(waiting.wait(1))
            manager.close()
        finally:
            release.set()
            thread.join(2)
        self.assertIsInstance(outcomes[0], ValueError)
        self.assertNotIsInstance(outcomes[0], StreamClosed)


if __name__ == '__main__':
    unittest.main()
