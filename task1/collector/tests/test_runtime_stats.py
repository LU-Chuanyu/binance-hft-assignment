import unittest

from clock_samples import ClockSamples
from runtime_stats import RuntimeStats, validate_runtime_stats
from test_clock_samples import sample


SECOND = 1_000_000_000


class RuntimeStatsTests(unittest.TestCase):
    def test_boundary_message_goes_into_next_window(self):
        stats = RuntimeStats(SECOND)
        stats.observe('depth', 2 * SECOND, 5.0)
        stats.observe('depth', 4 * SECOND, 15.0)
        stats.observe('aggTrade', 8 * SECOND)

        first, = stats.observe('bookTicker', 11 * SECOND)
        self.assertEqual(first['window_start_monotonic_ns'], SECOND)
        self.assertEqual(first['window_end_monotonic_ns'], 11 * SECOND)
        self.assertFalse(first['partial'])
        self.assertEqual(first['streams']['depth'], {
            'count': 2,
            'rate_per_second': 0.2,
            'delay': {'count': 2, 'min_ms': 5.0, 'median_ms': 10.0,
                      'p99_ms': 15.0, 'max_ms': 15.0},
        })
        self.assertEqual(first['streams']['bookTicker']['count'], 0)
        second, = stats.advance(21 * SECOND)
        self.assertEqual(second['streams']['bookTicker']['count'], 1)

    def test_advance_records_empty_windows_and_none_delays(self):
        stats = RuntimeStats(SECOND)
        first, second = stats.advance(25 * SECOND)
        for window, start in ((first, 1), (second, 11)):
            self.assertEqual(window['window_start_monotonic_ns'], start * SECOND)
            self.assertFalse(window['partial'])
            self.assertTrue(all(stream['rate_per_second'] == 0.0
                                for stream in window['streams'].values()))
            self.assertEqual(window['streams']['depth']['delay'], {
                'count': 0, 'min_ms': None, 'median_ms': None, 'p99_ms': None, 'max_ms': None,
            })

    def test_finish_emits_partial_with_actual_duration_and_coverage(self):
        stats = RuntimeStats(SECOND)
        stats.observe('aggTrade', 2 * SECOND, 9.0)
        # ClockSamples returns None for a message outside a valid clock window.
        stats.observe('aggTrade', 3 * SECOND)
        partial, = stats.finish(5 * SECOND)
        self.assertTrue(partial['partial'])
        self.assertEqual(partial['window_end_monotonic_ns'], 5 * SECOND)
        self.assertEqual(partial['streams']['aggTrade']['count'], 2)
        self.assertEqual(partial['streams']['aggTrade']['rate_per_second'], 0.5)
        self.assertEqual(partial['streams']['aggTrade']['delay']['count'], 1)
        self.assertEqual(partial['streams']['aggTrade']['delay']['median_ms'], 9.0)

    def test_finish_at_boundary_does_not_emit_zero_duration(self):
        stats = RuntimeStats(SECOND)
        only, = stats.finish(11 * SECOND)
        self.assertFalse(only['partial'])

    def test_late_clock_failure_keeps_prior_coverage_and_stops_new_delays(self):
        clock = ClockSamples()
        clock.observe(sample())
        stats = RuntimeStats(0)
        stats.observe('depth', 100_000_000, clock.delay(1_100_000_000, 1000, 100_000_000))
        clock.observe({'status': 'error', 'usable': False})
        stats.observe('depth', 200_000_000, clock.delay(1_200_000_000, 1100, 200_000_000))

        only, = stats.finish(SECOND)
        self.assertEqual(only['streams']['depth']['count'], 2)
        self.assertEqual(only['streams']['depth']['delay']['count'], 1)
        self.assertEqual(only['streams']['depth']['delay']['median_ms'], 50.0)

    def test_replay_validation_checks_rate_and_delay_coverage(self):
        stats = RuntimeStats(SECOND)
        stats.observe('depth', 2 * SECOND, 7.0)
        row, = stats.advance(11 * SECOND)
        row['observed_ns'] = 123_000_000_000
        validate_runtime_stats(row)
        row['streams']['depth']['delay']['count'] = 2
        with self.assertRaises(ValueError):
            validate_runtime_stats(row)
        row['streams']['depth']['delay']['count'] = 1
        row['streams']['depth']['rate_per_second'] = 0.2
        with self.assertRaises(ValueError):
            validate_runtime_stats(row)

    def test_rejects_processing_clock_goes_back_or_book_ticker_delay(self):
        stats = RuntimeStats(SECOND)
        stats.observe('depth', 2 * SECOND)
        with self.assertRaises(ValueError):
            stats.observe('depth', SECOND)
        with self.assertRaises(ValueError):
            stats.observe('bookTicker', 2 * SECOND, 3.0)
