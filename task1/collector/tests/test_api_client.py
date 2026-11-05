import io
import json
from email.message import Message
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from urllib.error import HTTPError
import unittest
from unittest.mock import patch

from api_client import PublicAPIClient, RateLimitError, RateLimitWait


class Clock:
    def __init__(self):
        self.t = 0.0
        self.waits = []
    def monotonic(self):
        return self.t
    def sleep(self, amount):
        self.waits.append(amount)
        self.t += amount
    def wall_ns(self):
        return 1_000_000_000 + int(self.t * 1_000_000_000)
    def monotonic_ns(self):
        return int(self.t * 1_000_000_000)


class Response:
    def __init__(self, data, headers=None):
        self.payload = json.dumps(data).encode()
        self.headers = Message()
        for key, value in (headers or {}).items():
            self.headers[key] = str(value)
    def read(self):
        return self.payload
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass


def http_error(code, retry=None):
    headers = Message()
    if retry is not None:
        headers['Retry-After'] = str(retry)
    return HTTPError('https://api.binance.com', code, 'error', headers, io.BytesIO(b'{}'))


class ClientTests(unittest.TestCase):
    def client(self, responses, **options):
        clock = Clock()
        calls = []
        def transport(url, *, timeout, context):
            calls.append((url, timeout, context))
            result = responses.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        client = PublicAPIClient(transport=transport, monotonic=clock.monotonic,
            sleep=clock.sleep, wall_ns=clock.wall_ns, monotonic_ns=clock.monotonic_ns, **options)
        return client, clock, calls

    def test_results_include_payload_headers_time_and_shared_weight_budget(self):
        client, clock, calls = self.client([Response({'serverTime': 1}, {'X-MBX-USED-WEIGHT-1M': 2}),
                                          Response({'bids': [], 'asks': []})], local_budget=6)
        result = client.server_time()
        self.assertEqual(result.data, {'serverTime': 1})
        self.assertEqual(result.used_weight_1m, 2)
        self.assertEqual(result.sent_wall_ns, result.received_wall_ns)
        self.assertIn('/api/v3/time', calls[0][0])
        result = client.depth(limit=100)
        self.assertEqual(result.data['bids'], [])
        with self.assertRaises(RateLimitWait):
            client.server_time()
        self.assertEqual(len(calls), 2)

    def test_429_waits_retry_after_before_next_request(self):
        client, clock, calls = self.client([http_error(429, 2), Response({'serverTime': 2})])
        with self.assertRaises(RateLimitError) as raised:
            client.server_time()
        self.assertEqual((raised.exception.status, raised.exception.retry_after), (429, 2))
        self.assertEqual(client.server_time().data['serverTime'], 2)
        self.assertEqual(clock.waits, [2])
        self.assertEqual(len(calls), 2)

    def test_418_long_wait_is_explicit_not_blocking_or_retried(self):
        client, clock, calls = self.client([http_error(418, 120), Response({})], max_wait=5)
        with self.assertRaises(RateLimitError):
            client.server_time()
        with self.assertRaises(RateLimitWait) as raised:
            client.server_time()
        self.assertEqual(raised.exception.remaining, 120)
        self.assertEqual(clock.waits, [])
        self.assertEqual(len(calls), 1)

    def test_429_missing_retry_after_never_guesses_when_to_resume(self):
        client, clock, calls = self.client([http_error(429)])
        with self.assertRaises(RateLimitError):
            client.server_time()
        with self.assertRaises(RateLimitWait):
            client.server_time()
        self.assertEqual(len(calls), 1)

    def test_high_ip_weight_header_pauses_across_routes(self):
        client, clock, calls = self.client([Response({'serverTime': 1}, {'x-mbx-used-weight-1m': 100})],
                                           ip_soft_limit=100, max_wait=2)
        client.server_time()
        with self.assertRaises(RateLimitWait) as raised:
            client.aggtrades(0, 2)
        self.assertEqual(raised.exception.remaining, 60)
        self.assertEqual(len(calls), 1)

    def test_http_451_and_other_status_propagate_without_retry(self):
        for status in (451, 404):
            with self.subTest(status=status):
                client, clock, calls = self.client([http_error(status)])
                with self.assertRaises(HTTPError) as raised:
                    client.server_time()
                self.assertEqual(raised.exception.code, status)
                self.assertEqual(len(calls), 1)

    def test_request_parameters_and_individual_weight(self):
        client, clock, calls = self.client([Response([]), Response({})])
        client.aggtrades(17, 5)
        self.assertIn('fromId=17', calls[0][0])
        self.assertIn('limit=5', calls[0][0])
        client.depth(limit=5000)
        self.assertIn('limit=5000', calls[1][0])
        self.assertEqual([weight for _, weight in client._recent], [4, 250])

    def test_max_wait_caps_total_wait_even_when_many_budget_entries_expire(self):
        client, clock, calls = self.client([Response({}) for _ in range(6)], local_budget=6, max_wait=2)
        for i in range(6):
            clock.t = i
            client.server_time()
        clock.t = 59
        with self.assertRaises(RateLimitWait):
            client.depth(limit=100)
        self.assertLessEqual(clock.t - 59, 2)
        self.assertEqual(len(calls), 6)

    def test_calls_on_same_client_are_serialized(self):
        entered = threading.Event()
        release = threading.Event()
        active = []
        overlap = []
        def transport(url, *, timeout, context):
            if active:
                overlap.append(url)
            active.append(url)
            entered.set()
            self.assertTrue(release.wait(2))
            active.pop()
            return Response({'serverTime': 1})
        client = PublicAPIClient(transport=transport)
        errors = []
        def call():
            try:
                client.server_time()
            except Exception as exc:
                errors.append(exc)
        first = threading.Thread(target=call)
        second = threading.Thread(target=call)
        first.start()
        self.assertTrue(entered.wait(1))
        second.start()
        release.set()
        first.join(2)
        second.join(2)
        self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(overlap, [])


class PersistentClientTests(unittest.TestCase):
    def local_client(self, responses, **options):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                requests.append((self.path, self.client_address))
                status, headers, silent_close = responses.pop(0)
                payload = b'{"serverTime": 1}'
                self.send_response(status)
                if 'Content-Length' not in headers:
                    self.send_header('Content-Length', str(len(payload)))
                for name, value in headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)
                if silent_close:
                    self.close_connection = True

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01})
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)

        def connect(host, *, timeout, context):
            self.assertEqual(host, 'api.binance.com')
            self.assertTrue(context.check_hostname)
            return HTTPConnection(*server.server_address, timeout=timeout)

        with patch('api_client.HTTPSConnection', connect, create=True), \
                patch('api_client.getproxies', return_value={}, create=True):
            client = PublicAPIClient(reuse_connections=True, **options)
        self.addCleanup(client.close)
        return client, requests

    def test_two_routes_share_a_socket_until_closed(self):
        client, requests = self.local_client([(200, {}, False)] * 3)
        first = client.server_time()
        second = client.depth(limit=100)
        self.assertIs(first.connection_reused, False)
        self.assertIs(second.connection_reused, True)
        self.assertEqual(requests[0][1], requests[1][1])
        self.assertEqual(requests[1][0], '/api/v3/depth?symbol=BTCUSDT&limit=100')
        client.close()
        client.close()
        with self.assertRaises(RuntimeError):
            client.server_time()
        self.assertEqual(len(requests), 2)

    def test_stale_socket_reconnects_with_another_budget_charge(self):
        client, requests = self.local_client([(200, {}, True), (200, {}, False)], local_budget=3)
        client.server_time()
        result = client.server_time()
        self.assertIs(result.connection_reused, False)
        self.assertNotEqual(requests[0][1], requests[1][1])
        with self.assertRaises(RateLimitWait):
            client.server_time()

    def test_http_statuses_keep_existing_error_and_budget_behavior(self):
        client, requests = self.local_client([
            (451, {'X-MBX-USED-WEIGHT-1M': '4'}, False),
            (429, {'Retry-After': '0'}, False),
            (418, {'Retry-After': '120'}, False),
        ])
        with self.assertRaises(HTTPError) as raised:
            client.server_time()
        self.assertEqual(raised.exception.code, 451)
        self.assertEqual(raised.exception.read(), b'{"serverTime": 1}')
        self.assertEqual(client.last_weight_1m, 4)
        for status, delay in ((429, 0), (418, 120)):
            with self.assertRaises(RateLimitError) as raised:
                client.server_time()
            self.assertEqual((raised.exception.status, raised.exception.retry_after), (status, delay))
        with self.assertRaises(RateLimitWait):
            client.server_time()
        self.assertEqual(len(requests), 3)
        self.assertEqual(len({peer for _, peer in requests}), 1)

    def test_truncated_rate_limit_body_still_applies_retry_after(self):
        client, requests = self.local_client([
            (429, {'Retry-After': '120', 'Content-Length': '100'}, True),
        ])
        with self.assertRaises(RateLimitError) as raised:
            client.server_time()
        self.assertEqual((raised.exception.status, raised.exception.retry_after), (429, 120))
        with self.assertRaises(RateLimitWait):
            client.server_time()
        self.assertEqual(len(requests), 1)


if __name__ == '__main__':
    unittest.main()
