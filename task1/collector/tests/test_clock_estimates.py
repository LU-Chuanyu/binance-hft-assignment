import unittest

from clock_estimates import TradeReceiptEstimator, clock_offset_alert
from clock_samples import ClockSamples
from market_projection import project_backfill


MS = 1_000_000


def clock_sample(*, start_ms=1000, offset_ms=50, rtt_ms=10):
    end_ms = start_ms + rtt_ms
    return {'status': 'ok', 'sent_wall_ns': start_ms * MS,
            'received_wall_ns': end_ms * MS,
            'sent_monotonic_ns': (start_ms - 1000) * MS,
            'received_monotonic_ns': (end_ms - 1000) * MS,
            'server_ms': (start_ms + end_ms) // 2 - offset_ms,
            'sample_ns': end_ms * MS, 'valid_from_ns': end_ms * MS,
            'valid_until_ns': (end_ms + 60_000) * MS,
            'offset_ns': offset_ms * MS, 'rtt_ns': rtt_ms * MS,
            'max_rtt_ns': 1000 * MS, 'usable': rtt_ms <= 1000}


def backfill(trade_ms=1050):
    return {'data': {'T': trade_ms, 'a': 20, 'p': '1', 'q': '2', 'm': True},
            'source': 'rest_backfill', 'received_ns': None,
            'estimated_received_ns': None}


class ClockOffsetAlertTests(unittest.TestCase):
    def test_large_offset_warns_but_marks_high_rtt_uncertainty(self):
        alert = clock_offset_alert(clock_sample(offset_ms=-700, rtt_ms=1200))
        self.assertEqual(alert['offset_ns'], -700 * MS)
        self.assertEqual(alert['estimated_error_ns'], 601 * MS)
        self.assertFalse(alert['outside_threshold_with_rtt_margin'])
        self.assertFalse(alert['clock_sample_usable'])

        alert = clock_offset_alert(clock_sample(offset_ms=-700))
        self.assertTrue(alert['outside_threshold_with_rtt_margin'])
        self.assertTrue(alert['clock_sample_usable'])

    def test_threshold_is_explicit_and_missing_samples_do_not_warn(self):
        self.assertIsNone(clock_offset_alert(clock_sample(offset_ms=500)))
        self.assertIsNone(clock_offset_alert({'status': 'timeout'}))
        self.assertIsNotNone(clock_offset_alert(clock_sample(), threshold_ns=40 * MS))
        with self.assertRaises(ValueError):
            clock_offset_alert(clock_sample(), threshold_ns=0)


class TradeReceiptEstimatorTests(unittest.TestCase):
    def setUp(self):
        self.clock = ClockSamples()
        self.clock.observe(clock_sample())
        self.estimator = TradeReceiptEstimator(self.clock)

    def seed_live(self, delays=(40, 60, 45, 52, 1000)):
        for index, delay_ms in enumerate(delays):
            trade_ms = 1000 + index
            local_ms = trade_ms + 50 + delay_ms
            self.estimator.observe_live({'T': trade_ms}, local_ms * MS,
                                        (local_ms - 1000) * MS)

    def estimate_at(self, local_ms=2500, *, record=None, monotonic_ms=None):
        return self.estimator.estimate(
            backfill() if record is None else record,
            now_wall_ns=local_ms * MS,
            now_monotonic_ns=(local_ms - 1000 if monotonic_ms is None else monotonic_ms) * MS)

    def test_backfill_receipt_is_trade_time_plus_median_plus_local_offset(self):
        self.seed_live()
        original = backfill()
        estimated = self.estimate_at(record=original)
        self.assertIsNone(original['estimated_received_ns'])
        self.assertIsNone(estimated['received_ns'])
        self.assertEqual(estimated['estimated_received_ns'], (1050 + 52 + 50) * MS)
        self.assertEqual(estimated['estimate_typical_delay_ns'], 52 * MS)
        self.assertEqual(estimated['estimate_clock_offset_ns'], 50 * MS)
        self.assertEqual(estimated['estimate_sample_count'], 5)
        self.assertEqual(estimated['estimate_clock_sample_ns'], 1010 * MS)

        projected, = project_backfill(estimated, symbol='BTCUSDT', generation=1)
        for key, value in estimated.items():
            if key.startswith('estimate'):
                self.assertEqual(projected[key], value)
        self.assertIsNone(projected['received_ns'])
        legacy, = project_backfill(original, symbol='BTCUSDT', generation=1)
        self.assertNotIn('estimate_method', legacy)

    def test_missing_nearby_samples_and_receipts_outside_the_window_remain_unknown(self):
        self.seed_live((40, 60, 45, 52))
        self.assertIsNone(self.estimate_at()['estimated_received_ns'])
        self.seed_live()
        self.assertIsNone(self.estimate_at(record=backfill(50_000))['estimated_received_ns'])
        self.assertIsNone(self.estimate_at(record=backfill(800))['estimated_received_ns'])
        self.assertIsNone(self.estimate_at(record=backfill(2450))['estimated_received_ns'])

    def test_expiration_wall_jump_and_new_calibration_clear_old_evidence(self):
        self.seed_live()
        self.assertIsNone(self.estimate_at(62_000)['estimated_received_ns'])
        self.clock.observe(clock_sample())
        self.seed_live()
        self.assertIsNone(self.estimate_at(2510, monotonic_ms=1500)['estimated_received_ns'])
        self.clock.observe(clock_sample(start_ms=2500))
        self.assertIsNone(self.estimate_at(2600)['estimated_received_ns'])

    def test_invalid_clock_cannot_produce_an_estimate(self):
        self.seed_live()
        self.clock.observe({'status': 'error', 'usable': False})
        self.assertIsNone(self.estimate_at()['estimated_received_ns'])
