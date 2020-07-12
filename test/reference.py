from enum import IntEnum, IntFlag, unique
from struct import pack as __pack__


# ------------------------------------------------------------------------------

@unique
class Types(IntFlag):
    FIXUINT = 0x00         #   0
    FIXUINT_END = 0x7f     # 127

    FIXMAP = 0x80
    FIXMAP_END = 0x8f

    FIXARRAY = 0x90
    FIXARRAY_END = 0x9f

    FIXSTR = 0xa0
    FIXSTR_END = 0xbf

    NIL = 0xc0
    INVALID = 0xc1         # invalid type
    FALSE = 0xc2
    TRUE = 0xc3

    BIN8 = 0xc4
    BIN16 = 0xc5
    BIN32 = 0xc6

    EXT8 = 0xc7
    EXT16 = 0xc8
    EXT32 = 0xc9

    FLOAT32 = 0xca
    FLOAT64 = 0xcb

    UINT8 = 0xcc
    UINT16 = 0xcd
    UINT32 = 0xce
    UINT64 = 0xcf

    INT8 = 0xd0
    INT16 = 0xd1
    INT32 = 0xd2
    INT64 = 0xd3

    FIXEXT1 = 0xd4
    FIXEXT2 = 0xd5
    FIXEXT4 = 0xd6
    FIXEXT8 = 0xd7
    FIXEXT16 = 0xd8

    STR8 = 0xd9
    STR16 = 0xda
    STR32 = 0xdb

    ARRAY16 = 0xdc
    ARRAY32 = 0xdd

    MAP16 = 0xde
    MAP32 = 0xdf

    FIXINT = 0xe0          # -32
    FIXINT_END = 0xff      #  -1


# ------------------------------------------------------------------------------

@unique
class Extensions(IntFlag):
    INVALID = 0x00              # invalid extension type

    # Python

    PYTHON_COMPLEX = 0x01
    PYTHON_BYTEARRAY = 0x02

    PYTHON_LIST = 0x03

    PYTHON_SET = 0x04
    PYTHON_FROZENSET = 0x05

    PYTHON_CLASS = 0x06
    PYTHON_SINGLETON = 0x07

    PYTHON_OBJECT = 0x7f        # last

    # MessagePack

    MSGPACK_TIMESTAMP = 0xff    #  -1


# ------------------------------------------------------------------------------

@unique
class Limits(IntEnum):
    UINT64_MAX = (1 << 64)    # 18446744073709551616
    INT64_MIN = -(1 << 63)    # -9223372036854775808

    UINT32_MAX = (1 << 32)    # 4294967296
    INT32_MIN = -(1 << 31)    # -2147483648

    UINT16_MAX = (1 << 16)    # 65536
    INT16_MIN = -(1 << 15)    # -32768

    UINT8_MAX = (1 << 8)      # 256
    INT8_MIN = -(1 << 7)      # -128

    FIXUINT_MAX = (1 << 7)    # 128
    FIXINT_MIN = -(1 << 5)    # -32

    FIXSTR_MAX = (1 << 5)     # 32
    FIXOBJ_MAX = (1 << 4)     # 16


# ------------------------------------------------------------------------------

def _pack_nil():
    return __pack__(">B", Types.NIL)

def _pack_false():
    return __pack__(">B", Types.FALSE)

def _pack_true():
    return __pack__(">B", Types.TRUE)


def _pack_fixint(value):
    return __pack__(">b", value)

def _pack_uint8(value):
    return __pack__(">BB", Types.UINT8, value)

def _pack_uint16(value):
    return __pack__(">BH", Types.UINT16, value)

def _pack_uint32(value):
    return __pack__(">BI", Types.UINT32, value)

def _pack_uint64(value):
    return __pack__(">BQ", Types.UINT64, value)

def _pack_int8(value):
    return __pack__(">Bb", Types.INT8, value)

def _pack_int16(value):
    return __pack__(">Bh", Types.INT16, value)

def _pack_int32(value):
    return __pack__(">Bi", Types.INT32, value)

def _pack_int64(value):
    return __pack__(">Bq", Types.INT64, value)


def _pack_float32(value):
    return __pack__(">Bf", Types.FLOAT32, value)

def _pack_float64(value):
    return __pack__(">Bd", Types.FLOAT64, value)


def _pack_fixstr(size, data):
    return __pack__(f">B{size}s", (Types.FIXSTR | size), data)

def _pack_str8(size, data):
    return __pack__(f">BB{size}s", Types.STR8, size, data)

def _pack_str16(size, data):
    return __pack__(f">BH{size}s", Types.STR16, size, data)

def _pack_str32(size, data):
    return __pack__(f">BI{size}s", Types.STR32, size, data)


def _pack_bin8(size, data):
    return __pack__(f">BB{size}s", Types.BIN8, size, data)

def _pack_bin16(size, data):
    return __pack__(f">BH{size}s", Types.BIN16, size, data)

def _pack_bin32(size, data):
    return __pack__(f">BI{size}s", Types.BIN32, size, data)


def _pack_fixarray(size):
    return __pack__(">B", (Types.FIXARRAY | size))

def _pack_array16(size):
    return __pack__(">BH", Types.ARRAY16, size)

def _pack_array32(size):
    return __pack__(">BI", Types.ARRAY32, size)


def _pack_fixmap(size):
    return __pack__(">B", (Types.FIXMAP | size))

def _pack_map16(size):
    return __pack__(">BH", Types.MAP16, size)

def _pack_map32(size):
    return __pack__(">BI", Types.MAP32, size)


def _pack_fixext1(_type, data):
    return __pack__(">BB1s", Types.FIXEXT1, _type, data)

def _pack_fixext2(_type, data):
    return __pack__(">BB2s", Types.FIXEXT2, _type, data)

def _pack_fixext4(_type, data):
    return __pack__(">BB4s", Types.FIXEXT4, _type, data)

def _pack_fixext8(_type, data):
    return __pack__(">BB8s", Types.FIXEXT8, _type, data)

def _pack_fixext16(_type, data):
    return __pack__(">BB16s", Types.FIXEXT16, _type, data)


def _pack_ext8(size, _type, data):
    return __pack__(f">BBB{size}s", Types.EXT8, size, _type, data)

def _pack_ext16(size, _type, data):
    return __pack__(f">BHB{size}s", Types.EXT16, size, _type, data)

def _pack_ext32(size, _type, data):
    return __pack__(f">BIB{size}s", Types.EXT32, size, _type, data)


# ------------------------------------------------------------------------------

_obj_too_big_ = "{0.__name__} object too big to pack"
_ext_data_too_big_ = "{0.__name__} object extension data too big to pack"
_cannot_pack_ = "cannot pack {0.__name__} objects"


def error(_type, msg, o):
    return _type(msg.format(type(o)))


# ------------------------------------------------------------------------------

def pack_int(o):
    if o >= Limits.INT64_MIN:
        if o < Limits.INT32_MIN:
            return _pack_int64(o)
        if o < Limits.INT16_MIN:
            return _pack_int32(o)
        if o < Limits.INT8_MIN:
            return _pack_int16(o)
        if o < Limits.FIXINT_MIN:
            return _pack_int8(o)
        if o < Limits.FIXUINT_MAX:
            return _pack_fixint(o)
        if o < Limits.UINT8_MAX:
            return _pack_uint8(o)
        if o < Limits.UINT16_MAX:
            return _pack_uint16(o)
        if o < Limits.UINT32_MAX:
            return _pack_uint32(o)
        if o < Limits.UINT64_MAX:
            return _pack_uint64(o)
    raise error(OverflowError, _obj_too_big_, o)

def pack_bytes(o):
    size = len(o)
    if size < Limits.UINT8_MAX:
        return _pack_bin8(size, o)
    elif size < Limits.UINT16_MAX:
        return _pack_bin16(size, o)
    elif size < Limits.UINT32_MAX:
        return _pack_bin32(size, o)
    raise error(OverflowError, _obj_too_big_, o)

def pack_str(o):
    o = o.encode("utf-8")
    size = len(o)
    if size < Limits.FIXSTR_MAX:
        return _pack_fixstr(size, o)
    elif size < Limits.UINT8_MAX:
        return _pack_str8(size, o)
    elif size < Limits.UINT16_MAX:
        return _pack_str16(size, o)
    elif size < Limits.UINT32_MAX:
        return _pack_str32(size, o)
    raise error(OverflowError, _obj_too_big_, o)

def pack_sequence(o):
    size = len(o)
    if size < Limits.FIXOBJ_MAX:
        msg = _pack_fixarray(size)
    elif size < Limits.UINT16_MAX:
        msg = _pack_array16(size)
    elif size < Limits.UINT32_MAX:
        msg = _pack_array32(size)
    else:
        raise error(OverflowError, _obj_too_big_, o)
    return b"".join((msg, *(pack(v) for v in o)))

def pack_dict(o):
    size = len(o)
    if size < Limits.FIXOBJ_MAX:
        msg = _pack_fixmap(size)
    elif size < Limits.UINT16_MAX:
        msg = _pack_map16(size)
    elif size < Limits.UINT32_MAX:
        msg = _pack_map32(size)
    else:
        raise error(OverflowError, _obj_too_big_, o)
    return b"".join((msg, *(b"".join((pack(k), pack(v))) for k, v in o.items())))


# ------------------------------------------------------------------------------

def pack_complex(o):
    return b"".join((__pack__(">d", v) for v in (o.real, o.imag)))

def pack_class(o):
    return b"".join((pack_str(v) for v in (o.__module__, o.__qualname__)))

def pack_object(o):
    try:
        _reduce_ = o.__reduce__()
    except AttributeError:
        raise error(TypeError, _cannot_pack_, o) from None
    if isinstance(_reduce_, str):
        return (Extensions.PYTHON_SINGLETON, pack_str(_reduce_))
    elif isinstance(_reduce_, tuple):
        return (Extensions.PYTHON_OBJECT, pack_sequence(_reduce_))
    raise TypeError("__reduce__() must return a str or a tuple")


# ------------------------------------------------------------------------------

_extension_types_ = {
    list: lambda o: (Extensions.PYTHON_LIST, pack_sequence(o)),
    set: lambda o: (Extensions.PYTHON_SET, pack_sequence(o)),
    frozenset: lambda o: (Extensions.PYTHON_FROZENSET, pack_sequence(o)),
    bytearray: lambda o: (Extensions.PYTHON_BYTEARRAY, o),
    type: lambda o: (Extensions.PYTHON_CLASS, pack_class(o)),
    complex: lambda o: (Extensions.PYTHON_COMPLEX, pack_complex(o)),
    #Timestamp: lambda o: (Extensions.MSGPACK_TIMESTAMP, pack_timestamp(o))
}

def pack_extension(o):
    _type, data = _extension_types_.get(type(o), pack_object)(o)
    size = len(data)
    if size < Limits.UINT8_MAX:
        if size == 1:
            return _pack_fixext1(_type, data)
        elif size == 2:
            return _pack_fixext2(_type, data)
        elif size == 4:
            return _pack_fixext4(_type, data)
        elif size == 8:
            return _pack_fixext8(_type, data)
        elif size == 16:
            return _pack_fixext16(_type, data)
        else:
            return _pack_ext8(size, _type, data)
    elif size < Limits.UINT16_MAX:
        return _pack_ext16(size, _type, data)
    elif size < Limits.UINT32_MAX:
        return _pack_ext32(size, _type, data)
    raise error(OverflowError, _ext_data_too_big_, o)


# ------------------------------------------------------------------------------

_pack_types_ = {
    type(None): lambda o: _pack_nil(),
    bool: lambda o: _pack_true() if o else _pack_false(),
    int: pack_int,
    float: lambda o: _pack_float64(o),
    bytes: pack_bytes,
    str: pack_str,
    tuple: pack_sequence,
    dict: pack_dict
}

def pack(o):
    return _pack_types_.get(type(o), pack_extension)(o)
