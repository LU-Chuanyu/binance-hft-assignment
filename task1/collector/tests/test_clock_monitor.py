import unittest

from clock_monitor import ClockJumpError, estimate_clock, estimate_delay_ms, summarize_delays


class ClockMonitorTests(unittest.TestCase):
    def test_clock_offset_sign_and_round_trip(self):
        # 本机中点 1000 ms，服务端 900 ms：本机快 100 ms。
        estimate = estimate_clock(990_000_000, 1_010_000_000, 900,
                                  1_000_000, 21_000_000)
        self.assertEqual(estimate.offset_ns, 100_000_000)
        self.assertEqual(estimate.rtt_ns, 20_000_000)

    def test_delay_corrects_offset_and_does_not_hide_negative_values(self):
        self.assertEqual(estimate_delay_ms(1_120_000_000, 1000, 100_000_000), 20)
        self.assertEqual(estimate_delay_ms(1_090_000_000, 1000, 100_000_000), -10)
        self.assertIsNone(estimate_delay_ms(1_000_000_000, None, 0))
        self.assertIsNone(estimate_delay_ms(None, 1000, 0))

    def test_time_jump_and_invalid_time_are_rejected(self):
        with self.assertRaises(ClockJumpError):
            estimate_clock(1_000_000_000, 1_100_000_000, 1000, 0, 10_000_000)
        with self.assertRaises(ClockJumpError):
            estimate_clock(1_000_000_000, 900_000_000, 1000, 0, 10_000_000)
        with self.assertRaises(ValueError):
            estimate_clock(1_000_000_000, 1_010_000_000, 1000, 20_000_000, 10_000_000)

    def test_summary_includes_nearest_rank_p99_and_empty_input(self):
        self.assertEqual(summarize_delays([]), {'count': 0, 'median_ms': None, 'p99_ms': None, 'max_ms': None})
        self.assertEqual(summarize_delays([1, 2, 3, 100]), {'count': 4, 'median_ms': 2.5, 'p99_ms': 100, 'max_ms': 100})


if __name__ == '__main__':
    unittest.main()
