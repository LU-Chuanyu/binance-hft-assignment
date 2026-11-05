import unittest

from clock_samples import ClockSamples


def sample():
    return {'status': 'ok', 'sent_wall_ns': 1_000_000_000,
            'received_wall_ns': 1_010_000_000, 'sent_monotonic_ns': 0,
            'received_monotonic_ns': 10_000_000, 'server_ms': 955,
            'sample_ns': 1_010_000_000, 'valid_from_ns': 1_010_000_000,
            'valid_until_ns': 61_010_000_000, 'offset_ns': 50_000_000,
            'rtt_ns': 10_000_000, 'max_rtt_ns': 1_000_000_000, 'usable': True}


class ClockSampleTests(unittest.TestCase):
    def test_delay_from_valid_sample_and_no_bookticker_event_time(self):
        c = ClockSamples()
        c.observe(sample())
        self.assertEqual(c.delay(1_100_000_000, 1000, 100_000_000), 50)
        self.assertIsNone(c.delay(1_100_000_000, None, 100_000_000))

    def test_failed_calibration_invalidates_previous_sample(self):
        c = ClockSamples()
        c.observe(sample())
        c.observe({'status': 'error', 'usable': False})
        self.assertIsNone(c.delay(1_100_000_000, 1000, 100_000_000))

    def test_rtt_threshold_and_inconsistent_metadata_are_rejected(self):
        for bad in ({**sample(), 'rtt_ns': 2_000_000_000},
                    {**sample(), 'offset_ns': 0},
                    {**sample(), 'usable': False}):
            c = ClockSamples()
            c.observe(bad)
            self.assertIsNone(c.delay(1_100_000_000, 1000, 100_000_000))

    def test_backward_buffered_events_are_ignored_without_losing_sample(self):
        c = ClockSamples()
        c.observe(sample())
        self.assertIsNone(c.delay(1_000_000_000, 950, 0))
        self.assertEqual(c.delay(1_100_000_000, 1000, 100_000_000), 50)

    def test_wall_clock_jump_invalidates_sample_until_next_good_one(self):
        c = ClockSamples()
        c.observe(sample())
        self.assertIsNone(c.delay(1_200_000_000, 1000, 100_000_000))
        self.assertIsNone(c.delay(1_100_000_000, 1000, 100_000_000))
        c.observe(sample())
        self.assertEqual(c.delay(1_100_000_000, 1000, 100_000_000), 50)

    def test_expired_window_does_not_extrapolate(self):
        c = ClockSamples()
        c.observe(sample())
        self.assertIsNone(c.delay(61_020_000_000, 61000, 60_020_000_000))


if __name__ == '__main__':
    unittest.main()
