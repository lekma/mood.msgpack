import unittest
import struct
import random

from mood import msgpack


class TestPack(unittest.TestCase):

    def _test_samples(self, samples, *args):
        for x in samples:
            self.assertEqual(msgpack.pack(x), struct.pack(*args, x))


class TestConstant(TestPack):

    def test_true(self):
        self.assertEqual(msgpack.pack(True), b"\xc3")

    def test_false(self):
        self.assertEqual(msgpack.pack(False), b"\xc2")

    def test_nil(self):
        self.assertEqual(msgpack.pack(None), b"\xc0")

    def test_fixint(self):
        self._test_samples(range(-32, 128), ">b")


class TestUint(TestPack):

    def _get_samples(self, high, low=0, k=4):
        low = low or (high >> 1)
        lo, mid, hi = (1 << low), (1 << (high - 1)), ((1 << high) - 1)
        samples = [lo, mid, hi]
        samples.extend(random.sample(range(mid, hi), k))
        if lo != mid:
            samples.extend(random.sample(range(lo, mid), k))
        return samples

    def test_uint64(self):
        self._test_samples(self._get_samples(64), ">BQ", 0xcf)

    def test_uint32(self):
        self._test_samples(self._get_samples(32), ">BI", 0xce)

    def test_uint16(self):
        self._test_samples(self._get_samples(16), ">BH", 0xcd)

    def test_uint8(self):
        self._test_samples(self._get_samples(8, low=7, k=8), ">BB", 0xcc)


class TestInt(TestPack):

    def _get_samples(self, low, high=0, k=8):
        high = high or (low >> 1)
        lo, hi = -(1 << low), -(1 << high)
        samples = [lo, (hi - 1)]
        samples.extend(random.sample(range(lo, hi), k))
        return samples

    def test_int64(self):
        self._test_samples(self._get_samples(63), ">Bq", 0xd3)

    def test_int32(self):
        self._test_samples(self._get_samples(31), ">Bi", 0xd2)

    def test_int16(self):
        self._test_samples(self._get_samples(15), ">Bh", 0xd1)

    def test_int8(self):
        self._test_samples(self._get_samples(7, high=5), ">Bb", 0xd0)


class TestFloat(TestPack):

    flt32_min = float.fromhex("0x1.000000p-126")
    flt32_max = float.fromhex("0x1.fffffep+127")
    flt64_min = float.fromhex("0x1.0000000000000p-1022")
    flt64_max = float.fromhex("0x1.fffffffffffffp+1023")

    @classmethod
    def setUpClass(cls):
        cls.limits = [0.0]
        for l in (cls.flt32_min, cls.flt32_max, cls.flt64_min, cls.flt64_max):
            cls.limits.insert(0, -l)
            cls.limits.append(l)

    def test_float(self):
        self._test_samples(self.limits[:], ">Bd", 0xcb)


if __name__ == "__main__":
    unittest.main()
