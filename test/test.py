import collections
import datetime
import math
import pathlib
import random
import time
import unittest

import reference

from mood import msgpack


# ------------------------------------------------------------------------------

class _TestCase_(unittest.TestCase):

    def _test_pack(self, value):
        _packed = reference.pack(value)
        self.assertEqual(msgpack.pack(value), _packed)
        return _packed

    def _test_samples(self, values):
        for value in values:
            self.assertEqual(msgpack.unpack(self._test_pack(value)), value)

    def _test_constants(self, values):
        for value in values:
            self.assertIs(msgpack.unpack(self._test_pack(value)), value)


# ------------------------------------------------------------------------------

class TestConstant(_TestCase_):

    def test_constants(self):
        self._test_constants(
            (
                None,
                True,
                False,
                ...,
                NotImplemented,
                collections.deque,
                datetime.datetime,
                pathlib.Path,
            )
        )

class TestInstance(_TestCase_):

    def test_instances(self):
        self._test_samples(
            (
                datetime.datetime.now(),
                datetime.datetime.today(),
                msgpack.Timestamp.fromtimestamp(time.time()),
                msgpack.Timestamp.fromtimestamp(-time.time()),
                pathlib.Path("."),
            )
        )


# ------------------------------------------------------------------------------

class _TestInt_(object):

    @staticmethod
    def _samples(lo, mid, hi, k=64):
        samples = [lo, mid, hi]
        samples.extend(random.sample(range((lo + 1), mid), k))
        samples.extend(random.sample(range((mid + 1), hi), k))
        return samples


class TestInt(_TestCase_, _TestInt_):

    def _samples(self, limit):
        return super()._samples(
            -(1 << limit), -(1 << (limit - 1)), -(1 << (limit >> 1)) - 1)

    def test_fixint(self):
        self._test_samples(range(-32, 0))

    def test_int8(self):
        self._test_samples(range(-128, -32))

    def test_int16(self):
        self._test_samples(self._samples(15))

    def test_int32(self):
        self._test_samples(self._samples(31))

    def test_int64(self):
        self._test_samples(self._samples(63))

    def test_overflow(self):
        self.assertRaises(OverflowError, msgpack.pack, -(1 << 63)-1)


class TestUint(_TestCase_, _TestInt_):

    def _samples(self, limit):
        return super()._samples(
            1 << (limit >> 1), 1 << (limit - 1), (1 << limit) - 1)

    def test_fixuint(self):
        self._test_samples(range(0, 128))

    def test_uint8(self):
        self._test_samples(range(128, 256))

    def test_uint16(self):
        self._test_samples(self._samples(16))

    def test_uint32(self):
        self._test_samples(self._samples(32))

    def test_uint64(self):
        self._test_samples(self._samples(64))

    def test_overflow(self):
        self.assertRaises(OverflowError, msgpack.pack, (1 << 64))


# ------------------------------------------------------------------------------

class TestFloat(_TestCase_):

    flt64_min = float.fromhex("0x1.0000000000000p-1022")
    flt32_min = float.fromhex("0x1.000000p-126")
    flt32_max = float.fromhex("0x1.fffffep+127")
    flt64_max = float.fromhex("0x1.fffffffffffffp+1023")

    @classmethod
    def setUpClass(cls):
        cls.limits = [0.0]
        for l in (cls.flt64_min, cls.flt32_min, cls.flt32_max, cls.flt64_max):
            cls.limits.insert(0, -l)
            cls.limits.append(l)

    @staticmethod
    def _samples(a, b, k=64):
        n = 0
        while n < k:
            yield random.uniform(a, b)
            n += 1

    def test_limits(self):
        self._test_samples(self.limits)

    def test_constants(self):
        self._test_samples((-math.inf, math.inf))
        self._test_pack(math.nan)

    def test_float(self):
        for i in range(0, len(self.limits) - 1):
            self._test_samples(self._samples(self.limits[i], self.limits[i + 1]))


# ------------------------------------------------------------------------------

class _TestSeq_(object):

    @classmethod
    def _samples(cls, stop, start=-1, k=4):
        if start:
            if start < 0:
                start = (stop >> 1)
            start = (1 << start)
        return (cls._i * s for s in random.sample(range(start, (1 << stop)), k))


class TestSeq(_TestSeq_):

    def test_seq16(self):
        self._test_samples(self._samples(16, k=2))

    def test_seq32(self):
        self._test_samples(self._samples(32, k=1))

    def test_overflow(self):
        self.assertRaises(OverflowError, msgpack.pack, self._i * (1 << 32))


class TestBytes(_TestCase_, TestSeq):

    _i = b'a'

    def test_bin8(self):
        self._test_samples(self._samples(8, 0))


class TestStr(_TestCase_, TestSeq):

    _i = 'a'

    def test_fixstr(self):
        self._test_samples((self._i * s for s in range(0, 32)))

    def test_str8(self):
        self._test_samples(self._samples(8, 5))


# ------------------------------------------------------------------------------

class _TestCont_(object):

    def test_cont16(self):
        self._test_samples(self._samples(16, 4, k=2))

    def test_cont32(self):
        #self._test_samples(self._samples(32, k=1))
        self._test_samples(self._samples(24, 16, k=1))


class TestTuple(_TestCase_, _TestSeq_, _TestCont_):

    _i = (None,)

    def test_fixarray(self):
        self._test_samples((self._i * s for s in range(0, 16)))

    #def test_overflow(self):
    #    self.assertRaises(OverflowError, msgpack.pack, self._i * (1 << 32))


class TestList(_TestCase_, _TestSeq_, _TestCont_):

    _i = [None,]

    def test_fixarray(self):
        self._test_samples((self._i * s for s in range(0, 16)))

    #def test_overflow(self):
    #    self.assertRaises(OverflowError, msgpack.pack, self._i * (1 << 32))


class TestDict(_TestCase_, _TestCont_):

    @staticmethod
    def _samples(stop, start=-1, k=4):
        if start:
            if start < 0:
                start = (stop >> 1)
            start = (1 << start)
        return (dict((i, None) for i in range(s))
                for s in random.sample(range(start, (1 << stop)), k))

    def test_fixmap(self):
        self._test_samples((dict((i, None) for i in range(s))
                            for s in range(0, 16)))

    #def test_overflow(self):
    #    self.assertRaises(OverflowError, msgpack.pack,
    #                      dict((i, None) for i in range((1 << 32))))


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    msgpack.register(
        collections.deque, datetime.datetime, pathlib.Path, pathlib.PosixPath
    )
    unittest.main()
