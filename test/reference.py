from struct import pack


def pack_nil(msg):
    return pack(">B", 0xc0)

def pack_false(msg):
    return pack(">B", 0xc2)

def pack_true(msg):
    return pack(">B", 0xc3)


def pack_fixint(msg, obj):
    return pack(">b", obj)

def pack_uint8(msg, obj):
    return pack(">BB", 0xcc, obj)

def pack_uint16(msg, obj):
    return pack(">BH", 0xcd, obj)

def pack_uint32(msg, obj):
    return pack(">BI", 0xce, obj)

def pack_uint64(msg, obj):
    return pack(">BQ", 0xcf, obj)

def pack_int8(msg, obj):
    return pack(">Bb", 0xd0, obj)

def pack_int16(msg, obj):
    return pack(">Bh", 0xd1, obj)

def pack_int32(msg, obj):
    return pack(">Bi", 0xd2, obj)

def pack_int64(msg, obj):
    return pack(">Bq", 0xd3, obj)


def pack_float32(msg, obj):
    return pack(">Bf", 0xca, obj)

def pack_float64(msg, obj):
    return pack(">Bd", 0xcb, obj)


def pack_fixstr(msg, size, obj):
    return pack(">B{}s".format(size), (0xa0 | size), obj)

def pack_str8(msg, size, obj):
    return pack(">BB{}s".format(size), 0xd9, size, obj)

def pack_str16(msg, size, obj):
    return pack(">BH{}s".format(size), 0xda, size, obj)

def pack_str32(msg, size, obj):
    return pack(">BI{}s".format(size), 0xdb, size, obj)


def pack_bin8(msg, size, obj):
    return pack(">BB{}s".format(size), 0xc4, size, obj)

def pack_bin16(msg, size, obj):
    return pack(">BH{}s".format(size), 0xc5, size, obj)

def pack_bin32(msg, size, obj):
    return pack(">BI{}s".format(size), 0xc6, size, obj)


def pack_fixarray(msg, size):
    return pack(">B", (0x90 | size))

def pack_array16(msg, size, obj):
    return pack(">BH", 0xdc, size)

def pack_array32(msg, size, obj):
    return pack(">BI", 0xdd, size)


def pack_fixmap(msg, size):
    return pack(">B", (0x80 | size))

def pack_map16(msg, size, obj):
    return pack(">BH", 0xde, size)

def pack_map32(msg, size, obj):
    return pack(">BI", 0xdf, size)


def pack_fixext1(msg, ext_type, obj):
    return pack(">BB1s", 0xd4, ext_type, obj)

def pack_fixext2(msg, ext_type, obj):
    return pack(">BB2s", 0xd5, ext_type, obj)

def pack_fixext4(msg, ext_type, obj):
    return pack(">BB4s", 0xd6, ext_type, obj)

def pack_fixext8(msg, ext_type, obj):
    return pack(">BB8s", 0xd7, ext_type, obj)

def pack_fixext16(msg, ext_type, obj):
    return pack(">BB16s", 0xd8, ext_type, obj)


def pack_ext8(msg, size, ext_type, obj):
    return pack(">BBB{}s".format(size), 0xc7, size, ext_type, obj)

def pack_ext16(msg, size, ext_type, obj):
    return pack(">BHB{}s".format(size), 0xc8, size, ext_type, obj)

def pack_ext32(msg, size, ext_type, obj):
    return pack(">BIB{}s".format(size), 0xc9, size, ext_type, obj)

