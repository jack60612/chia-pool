import time
import unittest

from chia.util.ints import uint64

from pool.difficulty_adjustment import get_new_difficulty


class TestDifficulty(unittest.TestCase):
    def test_no_things_in_db(self):

        time_target = 24 * 3600
        current_time = uint64(time.time())
        if get_new_difficulty([], 300, time_target, 10, current_time, 1) != 10:
            raise AssertionError

    def test_recently_updated(self):
        num_partials = 300
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((current_time - i * 200, 20))

        if get_new_difficulty(partials, num_partials, time_target, 50, current_time, 1) != 50:
            raise AssertionError

        partials[0] = (current_time, 50)

        if get_new_difficulty(partials, num_partials, time_target, 50, current_time, 1) != 50:
            raise AssertionError

    def test_really_slow(self):
        num_partials = 300
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((uint64(current_time - (i + 100) * 200), 20))

        # Decreases by 5x
        if get_new_difficulty(partials, num_partials, time_target, 20, current_time, 1) != 4:
            raise AssertionError

        # Respects min difficulty
        if get_new_difficulty(partials, num_partials, time_target, 20, current_time, 10) != 10:
            raise AssertionError

    def test_kind_of_slow(self):
        num_partials = 300
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((uint64(current_time - (i + 20) * 200), 20))

        # Decreases by 1.5x
        if get_new_difficulty(partials, num_partials, time_target, 20, current_time, 1) != (20 // 1.5):
            raise AssertionError

    def test_not_enough_partials_yet(self):
        num_partials = 300
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((uint64(current_time - (i) * 200), 20))

        partials[-1] = (partials[-1][0], 15)

        # Doesn't change diff
        if get_new_difficulty(partials, num_partials, time_target, 20, current_time, 1) != 20:
            raise AssertionError

    def test_increases_diff(self):
        num_partials = 300
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((uint64(current_time - (i) * 200), 20))

        if get_new_difficulty(partials, num_partials, time_target, 20, current_time, 1) != 28:
            raise AssertionError

    def test_decreases_diff(self):
        num_partials = 300
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((uint64(current_time - (i) * 380), 20))

        if get_new_difficulty(partials, num_partials, time_target, 20, current_time, 1) != 15:
            raise AssertionError

    def test_partials_low_24h_decreases_diff(self):
        num_partials = 150
        time_target = 24 * 3600
        partials = []
        current_time = uint64(time.time())
        for i in range(num_partials):
            partials.append((uint64(current_time - (i) * 600), 20))

        if get_new_difficulty(partials, num_partials * 2, time_target, 20, current_time, 1) != 9:
            raise AssertionError

if __name__ == "__main__":
    unittest.main()
