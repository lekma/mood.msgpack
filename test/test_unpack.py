import unittest
import struct
import random

from mood import msgpack


class TestUnpack(unittest.TestCase):

    def _test_samples(self, samples, *args):
        for x in samples:
            self.assertEqual(msgpack.unpack(struct.pack(*args, x)), x)


class TestConstant(TestUnpack):

    def test_true(self):
        self.assertIs(msgpack.unpack(b"\xc3"), True)

    def test_false(self):
        self.assertIs(msgpack.unpack(b"\xc2"), False)

    def test_invalid(self):
        self.assertRaises(TypeError, msgpack.unpack, b"\xc1")

    def test_nil(self):
        self.assertIs(msgpack.unpack(b"\xc0"), None)

    def test_fixint(self):
        self._test_samples(range(-32, 128), ">b")


class TestUint(TestUnpack):

    @classmethod
    def setUpClass(cls):
        cls.limits = [((1 << l) - 1) for l in (0, 8, 16, 32, 63, 64)]

    def _get_samples(self, limits, k=4):
        samples = limits[:]
        for x in range(len(limits) - 1):
            samples.extend(random.sample(range(limits[x] + 1, limits[x + 1]), k))
        return samples

    def test_uint64(self):
        self._test_samples(self._get_samples(self.limits), ">BQ", 0xcf)

    def test_uint32(self):
        self._test_samples(self._get_samples(self.limits[:4]), ">BI", 0xce)

    def test_uint16(self):
        self._test_samples(self._get_samples(self.limits[:3]), ">BH", 0xcd)

    def test_uint8(self):
        self._test_samples(self._get_samples(self.limits[:2]), ">BB", 0xcc)


class TestInt(TestUnpack):

    @classmethod
    def setUpClass(cls):
        cls.limits = [0]
        for l in (7, 15, 31, 63):
            x = (1 << l)
            cls.limits.insert(0, -x)
            cls.limits.append(x - 1)

    def _get_samples(self, limits, k=4):
        samples = limits[:]
        for x in range(len(limits) - 1):
            samples.extend(random.sample(range(limits[x], limits[x + 1]), k))
        return samples

    def test_int64(self):
        self._test_samples(self._get_samples(self.limits), ">Bq", 0xd3)

    def test_int32(self):
        self._test_samples(self._get_samples(self.limits[1:-1]), ">Bi", 0xd2)

    def test_int16(self):
        self._test_samples(self._get_samples(self.limits[2:-2]), ">Bh", 0xd1)

    def test_int8(self):
        self._test_samples(self._get_samples(self.limits[3:-3]), ">Bb", 0xd0)


if __name__ == "__main__":
    unittest.main()
