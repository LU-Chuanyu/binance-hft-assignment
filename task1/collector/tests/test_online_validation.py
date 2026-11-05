from concurrent.futures import Future
import threading
import unittest

from book_provenance import BookProvenance
from online_validation import OnlineValidation


def snapshot(update_id=100, amount='2'):
    return {'lastUpdateId': update_id, 'bids': [['100', amount]], 'asks': []}


class Executor:
    def __init__(self):
        self.futures = []
    def submit(self, function):
        future = Future()
        self.futures.append(future)
        return future


class Clock:
    def __init__(self):
        self.now = 0
    def __call__(self):
        return self.now


class OnlineValidationTests(unittest.TestCase):
    def setUp(self):
        self.executor = Executor()
        self.clock = Clock()
        self.validator = OnlineValidation(lambda: None, executor=self.executor, monotonic=self.clock, max_age=10)
        self.provenance = BookProvenance(snapshot())
        self.addCleanup(self.validator.close)

    def test_snapshot_ahead_waits_until_local_catches_up_then_compares(self):
        self.validator.submit(epoch=1)
        self.assertEqual(self.validator.poll(self.provenance, contiguous=True)['status'], 'pending')
        self.executor.futures[0].set_result(snapshot(105))
        self.assertEqual(self.validator.poll(self.provenance, contiguous=True)['status'], 'pending')
        self.provenance.observe_applied({'U': 101, 'u': 105, 'b': [], 'a': []})
        result = self.validator.poll(self.provenance, contiguous=True)
        self.assertEqual((result['status'], result['compared']), ('matched', 1))

    def test_epoch_change_discards_completed_result(self):
        self.validator.submit(epoch=1)
        self.executor.futures[0].set_result(snapshot(105))
        replacement = self.provenance.reset(snapshot(110))
        self.assertEqual(self.validator.poll(replacement, contiguous=True)['status'], 'discarded')

    def test_mismatch_is_failure_and_zero_comparisons_are_unknown(self):
        self.validator.submit(epoch=1)
        self.executor.futures[0].set_result(snapshot(100, '9'))
        result = self.validator.poll(self.provenance, contiguous=True)
        self.assertEqual(result['status'], 'mismatch')
        self.assertEqual(result['compared'], 1)
        empty = BookProvenance({'lastUpdateId': 100, 'bids': [], 'asks': []})
        self.validator.submit(epoch=1)
        self.executor.futures[-1].set_result(snapshot(100))
        result = self.validator.poll(empty, contiguous=True)
        self.assertEqual((result['status'], result['compared']), ('unverifiable', 0))

    def test_expired_unaligned_snapshot_and_untrusted_history_not_success(self):
        self.validator.submit(epoch=1)
        self.executor.futures[0].set_result(snapshot(105))
        self.clock.now = 11
        self.assertEqual(self.validator.poll(self.provenance, contiguous=True)['status'], 'unverifiable')
        self.validator.submit(epoch=1)
        self.executor.futures[-1].set_result(snapshot(100))
        self.assertEqual(self.validator.poll(self.provenance, contiguous=False)['status'], 'unverifiable')

    def test_network_exception_reported_and_double_submit_rejected(self):
        self.validator.submit(epoch=1)
        with self.assertRaisesRegex(RuntimeError, '进行中'):
            self.validator.submit(epoch=1)
        self.executor.futures[0].set_exception(OSError('offline'))
        result = self.validator.poll(self.provenance, contiguous=True)
        self.assertEqual(result['status'], 'error')
        self.assertIn('offline', result['reason'])

    def test_timed_out_running_request_cannot_accumulate_new_workers(self):
        self.validator.submit(epoch=1)
        self.executor.futures[0].set_running_or_notify_cancel()
        self.clock.now = 11
        self.assertEqual(self.validator.poll(self.provenance, contiguous=True)['status'], 'unverifiable')
        self.assertIsNone(self.validator.poll(self.provenance, contiguous=True))
        with self.assertRaisesRegex(RuntimeError, '进行中'):
            self.validator.submit(epoch=1)
        self.executor.futures[0].set_result(snapshot())
        self.validator.submit(epoch=1)
        self.assertEqual(len(self.executor.futures), 2)

    def test_real_worker_does_not_block_processing_thread(self):
        started, release = threading.Event(), threading.Event()
        def fetch():
            started.set()
            if not release.wait(2):
                raise TimeoutError('test release timeout')
            return snapshot()
        validator = OnlineValidation(fetch)
        try:
            validator.submit(epoch=1)
            self.assertTrue(started.wait(1))
            self.assertEqual(validator.poll(self.provenance, contiguous=True)['status'], 'pending')
        finally:
            release.set()
            validator.close()


if __name__ == '__main__':
    unittest.main()
