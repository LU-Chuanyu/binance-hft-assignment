from concurrent.futures import Future
import threading
import unittest

from api_client import RateLimitError, RateLimitWait
from nonblocking_retry import NonblockingRetry


class FakeExecutor:
    def __init__(self):
        self.futures = []
    def submit(self, task):
        future = Future()
        self.futures.append(future)
        return future


class Clock:
    def __init__(self):
        self.now = 0
    def __call__(self):
        return self.now


class NonblockingRetryTests(unittest.TestCase):
    def setUp(self):
        self.executor = FakeExecutor()
        self.clock = Clock()
        self.retry = NonblockingRetry(lambda: None, executor=self.executor,
                                       monotonic=self.clock, max_age=10, max_attempts=3)
        self.addCleanup(self.retry.close)

    def test_rate_limit_waits_then_retries_and_preserves_status(self):
        self.retry.start(token=1)
        self.executor.futures[0].set_exception(RateLimitError(429, 2))
        wait = self.retry.poll(token=1)
        self.assertEqual((wait['status'], wait['remaining'], wait['error']['status']), ('retry_wait', 2, 429))
        self.clock.now = 1
        self.assertEqual(self.retry.poll(token=1)['status'], 'retry_wait')
        self.assertEqual(len(self.executor.futures), 1)
        self.clock.now = 2
        self.assertEqual(self.retry.poll(token=1)['status'], 'pending')
        self.assertEqual(len(self.executor.futures), 2)
        self.executor.futures[1].set_result({'answer': 3})
        self.assertEqual(self.retry.poll(token=1)['result'], {'answer': 3})
        self.assertIsNone(self.retry.poll(token=1))

    def test_unknown_wait_fails_safely_and_long_wait_exhausts_deadline(self):
        self.retry.start(token=1)
        self.executor.futures[0].set_exception(RateLimitError(418, None))
        self.assertEqual(self.retry.poll(token=1)['status'], 'error')
        self.retry.start(token=1)
        self.executor.futures[1].set_exception(RateLimitWait(120))
        self.assertEqual(self.retry.poll(token=1)['status'], 'exhausted')
        self.assertEqual(len(self.executor.futures), 2)

    def test_attempt_limit_and_non_rate_exception_do_not_fake_success(self):
        self.retry.start(token='a')
        for i in range(3):
            self.executor.futures[i].set_exception(RateLimitError(429, 0))
            outcome = self.retry.poll(token='a')
            if i < 2:
                self.assertEqual(outcome['status'], 'retry_wait')
                self.assertEqual(self.retry.poll(token='a')['status'], 'pending')
            else:
                self.assertEqual(outcome['status'], 'exhausted')
        self.retry.start(token='b')
        self.executor.futures[-1].set_exception(OSError('network'))
        result = self.retry.poll(token='b')
        self.assertEqual(result['status'], 'error')
        self.assertIn('network', result['error']['message'])

    def test_lost_connection_and_changed_epoch_discard_future(self):
        self.retry.start(token=1)
        self.executor.futures[0].set_result({'old': 1})
        self.assertEqual(self.retry.poll(token=2)['status'], 'discarded')
        self.retry.start(token=2)
        self.executor.futures[1].set_result({'old': 2})
        self.assertEqual(self.retry.poll(token=2, active=False)['status'], 'discarded')

    def test_timed_out_running_task_blocks_new_tasks_until_finished(self):
        self.retry.start(token=1)
        old = self.executor.futures[0]
        old.set_running_or_notify_cancel()
        self.clock.now = 10
        self.assertEqual(self.retry.poll(token=1)['status'], 'exhausted')
        with self.assertRaisesRegex(RuntimeError, '进行中'):
            self.retry.start(token=2)
        old.set_result('late')
        self.retry.start(token=2)
        self.assertEqual(len(self.executor.futures), 2)

    def test_absolute_recording_deadline_caps_retry_window(self):
        self.retry.start(token=1, deadline=3)
        self.executor.futures[0].set_exception(RateLimitError(429, 4))
        outcome = self.retry.poll(token=1)
        self.assertEqual(outcome['status'], 'exhausted')
        self.assertEqual(outcome['error']['retry_after'], 4)

    def test_completed_limit_error_retains_partial_result_at_cutoff(self):
        self.retry.start(token=1, deadline=3)
        error = RateLimitError(429, 4)
        error.partial_result = {'records': ['completed-page']}
        self.executor.futures[0].set_exception(error)
        self.clock.now = 3
        outcome = self.retry.poll(token=1)
        self.assertEqual(outcome['status'], 'exhausted')
        self.assertEqual(outcome['cause'].partial_result, {'records': ['completed-page']})
        self.assertEqual(len(self.executor.futures), 1)

    def test_real_task_is_nonblocking(self):
        started, release = threading.Event(), threading.Event()
        def task():
            started.set()
            if not release.wait(2):
                raise TimeoutError('test timeout')
            return 1
        retry = NonblockingRetry(task)
        try:
            retry.start(token=1)
            self.assertTrue(started.wait(1))
            self.assertEqual(retry.poll(token=1)['status'], 'pending')
        finally:
            release.set()
            retry.close()


if __name__ == '__main__':
    unittest.main()
