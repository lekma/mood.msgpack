#include "msgpack.h"


#define _PyErr_ObjTooBig_(n, ex) \
    PyErr_Format( \
        PyExc_OverflowError, "%.200s%s too big to convert", n, \
        (ex) ? " extension data" : "" \
    )


#define _Packing_(n) _While_(packing, n)


#define _PyBytes_FromPyByteArray(o) \
    PyBytes_FromStringAndSize(PyByteArray_AS_STRING(o), PyByteArray_GET_SIZE(o))


_Py_IDENTIFIER(__reduce__);


/* --------------------------------------------------------------------------
   pack
   -------------------------------------------------------------------------- */

static inline PyByteArrayObject *
__msg_new__(Py_ssize_t alloc)
{
    PyByteArrayObject *self = NULL;

    if ((self = PyObject_New(PyByteArrayObject, &PyByteArray_Type))) {
        if ((self->ob_bytes = PyObject_Malloc(alloc))) {
            self->ob_start = self->ob_bytes;
            self->ob_alloc = alloc;
            self->ob_exports = 0;
            Py_SIZE(self) = 0;
            self->ob_bytes[0] = '\0';
        }
        else {
            Py_CLEAR(self);
            PyErr_NoMemory();
        }
    }
    return self;
}


static inline int
__msg_resize__(PyByteArrayObject *self, Py_ssize_t nalloc)
{
    Py_ssize_t alloc = 0;
    void *bytes = NULL;

    if (self->ob_alloc < nalloc) {
        alloc = Py_MAX(nalloc, (self->ob_alloc << 1));
        if (!(bytes = PyObject_Realloc(self->ob_bytes, alloc))) {
            return -1;
        }
        self->ob_start = self->ob_bytes = bytes;
        self->ob_alloc = alloc;
    }
    return 0;
}


#define _PACK_BEGIN_ \
    size_t start = Py_SIZE(self), nsize = start + size; \
    if ((nsize >= PY_SSIZE_T_MAX) || __msg_resize__(self, (nsize + 1))) { \
        PyErr_NoMemory(); \
        return -1; \
    }


#define _PACK_END_ \
    Py_SIZE(self) = nsize; \
    self->ob_bytes[nsize] = '\0'; \
    return 0;


static inline int
__pack_buffer__(PyByteArrayObject *self, const void *buffer, size_t size)
{
    _PACK_BEGIN_

    memcpy((self->ob_bytes + start), buffer, size);

    _PACK_END_
}


static inline int
__msgpack_type__(PyByteArrayObject *self, uint8_t type)
{
    static const size_t size = 1;

    _PACK_BEGIN_

    self->ob_bytes[start] = type;

    _PACK_END_
}


static inline int
__msgpack_buffer__(
    PyByteArrayObject *self, uint8_t type,
    const void *_buffer, size_t _size
)
{
    size_t size = 1 + _size;

    _PACK_BEGIN_

    self->ob_bytes[start++] = type;
    memcpy((self->ob_bytes + start), _buffer, _size);

    _PACK_END_
}


static inline int
__msgpack_buffers__(
    PyByteArrayObject *self, uint8_t type,
    const void *_buffer1, size_t _size1,
    const void *_buffer2, size_t _size2
)
{
    size_t size = 1 + _size1 + _size2;

    _PACK_BEGIN_

    self->ob_bytes[start++] = type;
    memcpy((self->ob_bytes + start), _buffer1, _size1);
    start += _size1;
    memcpy((self->ob_bytes + start), _buffer2, _size2);

    _PACK_END_
}


/* -------------------------------------------------------------------------- */

static int
__pack_buffer(PyObject *msg, const void *buffer, size_t size)
{
    return __pack_buffer__((PyByteArrayObject *)msg, buffer, size);
}


static int
__msgpack_type(PyObject *msg, uint8_t type)
{
    return __msgpack_type__((PyByteArrayObject *)msg, type);
}


static int
__msgpack_buffer(
    PyObject *msg, uint8_t type,
    const void *_buffer, size_t _size
)
{
    return __msgpack_buffer__(
        (PyByteArrayObject *)msg, type,
        _buffer, _size
    );
}


static int
__msgpack_buffers(
    PyObject *msg, uint8_t type,
    const void *_buffer1, size_t _size1,
    const void *_buffer2, size_t _size2
)
{
    return __msgpack_buffers__(
        (PyByteArrayObject *)msg, type,
        _buffer1, _size1,
        _buffer2, _size2
    );
}


/* -------------------------------------------------------------------------- */

static inline int
__pack_value4(PyObject *msg, uint32_t value)
{
    uint32_t bevalue = htobe32(value);

    return __pack_buffer(msg, &bevalue, 4);
}

static inline int
__pack_value8(PyObject *msg, uint64_t value)
{
    uint64_t bevalue = htobe64(value);

    return __pack_buffer(msg, &bevalue, 8);
}


static inline int
__pack_float8(PyObject *msg, double value)
{
    float64_t fvalue = { .f = value };

    return __pack_value8(msg, fvalue.i);
}


/* -------------------------------------------------------------------------- */

static inline int
__msgpack_value1(PyObject *msg, uint8_t type, uint8_t value)
{
    return __msgpack_buffer(msg, type, &value, 1);
}

static inline int
__msgpack_value2(PyObject *msg, uint8_t type, uint16_t value)
{
    uint16_t bevalue = htobe16(value);

    return __msgpack_buffer(msg, type, &bevalue, 2);
}

static inline int
__msgpack_value4(PyObject *msg, uint8_t type, uint32_t value)
{
    uint32_t bevalue = htobe32(value);

    return __msgpack_buffer(msg, type, &bevalue, 4);
}

static inline int
__msgpack_value8(PyObject *msg, uint8_t type, uint64_t value)
{
    uint64_t bevalue = htobe64(value);

    return __msgpack_buffer(msg, type, &bevalue, 8);
}


static inline int
__msgpack_float4(PyObject *msg, uint8_t type, float value)
{
    float32_t fvalue = { .f = value };

    return __msgpack_value4(msg, type, fvalue.i);
}

static inline int
__msgpack_float8(PyObject *msg, uint8_t type, double value)
{
    float64_t fvalue = { .f = value };

    return __msgpack_value8(msg, type, fvalue.i);
}


static inline int
__msgpack_bytes1(PyObject *msg, uint8_t type, const char *bytes, Py_ssize_t len)
{
    return __msgpack_buffers(msg, type, &len, 1, bytes, len);
}

static inline int
__msgpack_bytes2(PyObject *msg, uint8_t type, const char *bytes, Py_ssize_t len)
{
    uint16_t belen = htobe16(len);

    return __msgpack_buffers(msg, type, &belen, 2, bytes, len);
}

static inline int
__msgpack_bytes4(PyObject *msg, uint8_t type, const char *bytes, Py_ssize_t len)
{
    uint32_t belen = htobe32(len);

    return __msgpack_buffers(msg, type, &belen, 4, bytes, len);
}


/* long, ulong -------------------------------------------------------------- */

#define __msgpack_fixint(m, v) __msgpack_type(m, v)
#define __msgpack_uint(m, s, v) __msgpack_value##s(m, MSGPACK_UINT##s, v)
#define __msgpack_int(m, s, v) __msgpack_value##s(m, MSGPACK_INT##s, v)

static inline int
__pack_long__(PyObject *msg, int64_t value)
{
    int res = -1;

    if (value < MSGPACK_FIXINT_MIN) {
        if (value < MSGPACK_INT2_MIN) {
            if (value < MSGPACK_INT4_MIN) {
                res = __msgpack_int(msg, 8, value);
            }
            else {
                res = __msgpack_int(msg, 4, value);
            }
        }
        else {
            if (value < MSGPACK_INT1_MIN) {
                res = __msgpack_int(msg, 2, value);
            }
            else {
                res = __msgpack_int(msg, 1, value);
            }
        }
    }
    else if (value < MSGPACK_FIXUINT_MAX) { // fixint
        res = __msgpack_fixint(msg, value);
    }
    else {
        if (value < MSGPACK_UINT2_MAX) {
            if (value < MSGPACK_UINT1_MAX) {
                res = __msgpack_uint(msg, 1, value);
            }
            else {
                res = __msgpack_uint(msg, 2, value);
            }
        }
        else {
            if (value < MSGPACK_UINT4_MAX) {
                res = __msgpack_uint(msg, 4, value);
            }
            else {
                res = __msgpack_uint(msg, 8, value);
            }
        }
    }
    return res;
}

static inline int
__pack_long(PyObject *msg, int64_t value)
{
    if ((value == -1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_long__(msg, value);
}

static inline int
__pack_ulong(PyObject *msg, uint64_t value)
{
    if ((value == (uint64_t)-1) && PyErr_Occurred()) {
        return -1;
    }
    return __msgpack_uint(msg, 8, value);
}


/* float -------------------------------------------------------------------- */

#define __msgpack_float(m, s, v) __msgpack_float##s(m, MSGPACK_FLOAT##s, v)

static inline int
__pack_float(PyObject *msg, double value)
{
    return __msgpack_float(msg, 8, value);
}


/* bytes -------------------------------------------------------------------- */

#define __msgpack_bin(m, s, b, l) __msgpack_bytes##s(m, MSGPACK_BIN##s, b, l)

static inline int
__pack_bytes(PyObject *msg, const char *bytes, Py_ssize_t len)
{
    int res = -1;

    if (len < MSGPACK_UINT1_MAX) {
        res = __msgpack_bin(msg, 1, bytes, len);
    }
    else if (len < MSGPACK_UINT2_MAX) {
        res = __msgpack_bin(msg, 2, bytes, len);
    }
    else if (len < MSGPACK_UINT4_MAX) {
        res = __msgpack_bin(msg, 4, bytes, len);
    }
    else {
        _PyErr_ObjTooBig_("bytes", 0);
    }
    return res;
}


/* unicode ------------------------------------------------------------------ */

#define __msgpack_fixstr(m, b, l) __msgpack_buffer(m, (MSGPACK_FIXSTR | l), b, l)
#define __msgpack_str(m, s, b, l) __msgpack_bytes##s(m, MSGPACK_STR##s, b, l)

static inline int
__pack_unicode(PyObject *msg, const char *bytes, Py_ssize_t len)
{
    int res = -1;

    if (len < MSGPACK_FIXSTR_MAX) { // fixstr
        res = __msgpack_fixstr(msg, bytes, len);
    }
    else if (len < MSGPACK_UINT1_MAX) {
        res = __msgpack_str(msg, 1, bytes, len);
    }
    else if (len < MSGPACK_UINT2_MAX) {
        res = __msgpack_str(msg, 2, bytes, len);
    }
    else if (len < MSGPACK_UINT4_MAX) {
        res = __msgpack_str(msg, 4, bytes, len);
    }
    else {
        _PyErr_ObjTooBig_("str", 0);
    }
    return res;
}


/* array -------------------------------------------------------------------- */

#define __msgpack_fixarray(m, l) __msgpack_type(m, (MSGPACK_FIXARRAY | l))
#define __msgpack_array(m, s, l) __msgpack_value##s(m, MSGPACK_ARRAY##s, l)

static inline int
__pack_array(PyObject *msg, Py_ssize_t len, const char *name)
{
    int res = -1;

    if (len < MSGPACK_FIXOBJ_MAX) { // fixarray
        res = __msgpack_fixarray(msg, len);
    }
    else if (len < MSGPACK_UINT2_MAX) {
        res = __msgpack_array(msg, 2, len);
    }
    else if (len < MSGPACK_UINT4_MAX) {
        res = __msgpack_array(msg, 4, len);
    }
    else {
        _PyErr_ObjTooBig_(name, 0);
    }
    return res;
}


/* sequence ----------------------------------------------------------------- */

static inline int
__pack_sequence__(PyObject *msg, PyObject **items, Py_ssize_t len,
                  const char *name, const char *where)
{
    Py_ssize_t i;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_array(msg, len, name)) {
            for (res = 0, i = 0; i < len; ++i) {
                if ((res = PackObject(msg, items[i]))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_sequence(m, i, l, n) __pack_sequence__(m, i, l, n, _Packing_(n))


/* dict --------------------------------------------------------------------- */

#define __msgpack_fixmap(m, l) __msgpack_type(m, (MSGPACK_FIXMAP | l))
#define __msgpack_map(m, s, l) __msgpack_value##s(m, MSGPACK_MAP##s, l)

static inline int
__pack_map(PyObject *msg, Py_ssize_t len)
{
    int res = -1;

    if (len < MSGPACK_FIXOBJ_MAX) { // fixmap
        res = __msgpack_fixmap(msg, len);
    }
    else if (len < MSGPACK_UINT2_MAX) {
        res = __msgpack_map(msg, 2, len);
    }
    else if (len < MSGPACK_UINT4_MAX) {
        res = __msgpack_map(msg, 4, len);
    }
    else {
        _PyErr_ObjTooBig_("dict", 0);
    }
    return res;
}

static inline int
__pack_dict(PyObject *msg, PyObject *obj)
{
    Py_ssize_t pos = 0;
    PyObject *key = NULL, *val = NULL;
    int res = -1;

    if (!Py_EnterRecursiveCall(_Packing_("dict"))) {
        if (!__pack_map(msg, PyDict_GET_SIZE(obj))) {
            while ((res = PyDict_Next(obj, &pos, &key, &val))) {
                if ((res = PackObject(msg, key)) ||
                    (res = PackObject(msg, val))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


/* Py_None, Py_False, Py_True ----------------------------------------------- */

#define _Py_None_Pack(m) __msgpack_type(m, MSGPACK_NIL)
#define _Py_False_Pack(m) __msgpack_type(m, MSGPACK_FALSE)
#define _Py_True_Pack(m) __msgpack_type(m, MSGPACK_TRUE)


/* PyLong ------------------------------------------------------------------- */

static int
_PyLong_Pack(PyObject *msg, PyObject *obj)
{
    int overflow = 0;
    int64_t value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if (overflow) {
        if (overflow < 0) {
            _PyErr_ObjTooBig_("int", 0);
            return -1;
        }
        return __pack_ulong(msg, PyLong_AsUnsignedLongLong(obj));
    }
    return __pack_long(msg, value);
}


/* PyFloat ------------------------------------------------------------------ */

static int
_PyFloat_Pack(PyObject *msg, PyObject *obj)
{
    return __pack_float(msg, PyFloat_AS_DOUBLE(obj));
}


/* PyBytes ------------------------------------------------------------------ */

static int
_PyBytes_Pack(PyObject *msg, PyObject *obj)
{
    const char *bytes = PyBytes_AS_STRING(obj);
    Py_ssize_t len = PyBytes_GET_SIZE(obj);

    return __pack_bytes(msg, bytes, len);
}


/* PyUnicode ---------------------------------------------------------------- */

static int
_PyUnicode_Pack(PyObject *msg, PyObject *obj)
{
    const char *bytes = NULL;
    Py_ssize_t len;

    if (!(bytes = PyUnicode_AsUTF8AndSize(obj, &len))) {
        return -1;
    }
    return __pack_unicode(msg, bytes, len);
}


/* PyTuple ------------------------------------------------------------------ */

#define __pack_tuple(m, i, l) __pack_sequence(m, i, l, "tuple")

static int
_PyTuple_Pack(PyObject *msg, PyObject *obj)
{
    PyObject **items = _PyTuple_ITEMS(obj);
    Py_ssize_t len = PyTuple_GET_SIZE(obj);

    return __pack_tuple(msg, items, len);
}


/* PyDict ------------------------------------------------------------------- */

static int
_PyDict_Pack(PyObject *msg, PyObject *obj)
{
    return __pack_dict(msg, obj);
}


/* --------------------------------------------------------------------------
   extensions
   -------------------------------------------------------------------------- */

#define __msgpack_fixext(m, s) __msgpack_type(m, MSGPACK_FIXEXT##s)
#define __msgpack_ext(m, s, l) __msgpack_value##s(m, MSGPACK_EXT##s, l)

static inline int
__pack_ext(PyObject *msg, Py_ssize_t len, const char *name)
{
    int res = -1;

    if (len < MSGPACK_UINT1_MAX) {
        if (len == 1) {
            res = __msgpack_fixext(msg, 1);
        }
        else if (len == 2) {
            res = __msgpack_fixext(msg, 2);
        }
        else if (len == 4) {
            res = __msgpack_fixext(msg, 4);
        }
        else if (len == 8) {
            res = __msgpack_fixext(msg, 8);
        }
        else if (len == 16) {
            res = __msgpack_fixext(msg, 16);
        }
        else {
            res = __msgpack_ext(msg, 1, len);
        }
    }
    else if (len < MSGPACK_UINT2_MAX) {
        res = __msgpack_ext(msg, 2, len);
    }
    else if (len < MSGPACK_UINT4_MAX) {
        res = __msgpack_ext(msg, 4, len);
    }
    else {
        _PyErr_ObjTooBig_(name, 1);
    }
    return res;
}

static inline int
__pack_extension(PyObject *msg, PyObject *data, uint8_t type, const char *name)
{
    Py_ssize_t len = PyByteArray_GET_SIZE(data);

    if (__pack_ext(msg, len, name)) {
        return -1;
    }
    return __msgpack_buffer(msg, type, PyByteArray_AS_STRING(data), len);
}


/* anyset ------------------------------------------------------------------- */

static inline int
__pack_anyset__(PyObject *msg, PyObject *obj,
                const char *name, const char *where)
{
    Py_ssize_t pos = 0;
    PyObject *item = NULL;
    Py_hash_t hash;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_array(msg, PySet_GET_SIZE(obj), name)) {
            while ((res = _PySet_NextEntry(obj, &pos, &item, &hash))) {
                if ((res = PackObject(msg, item))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_anyset(m, o, n) __pack_anyset__(m, o, n, _Packing_(n))


/* class -------------------------------------------------------------------- */

static PyObject *
__pack_class(PyObject *obj)
{
    _Py_IDENTIFIER(__module__);
    _Py_IDENTIFIER(__qualname__);
    PyObject *data = NULL, *_modname_ = NULL, *_qualname_ = NULL;

    if (
        (_modname_ = _PyObject_GetAttrId(obj, &PyId___module__)) &&
        (_qualname_ = _PyObject_GetAttrId(obj, &PyId___qualname__))
    ) {
        if (
            !PyUnicode_CheckExact(_modname_) ||
            !PyUnicode_CheckExact(_qualname_)
        ) {
            PyErr_Format(
                PyExc_TypeError,
                "expected strings, got: __module__: %.200s, __qualname__: %.200s",
                Py_TYPE(_modname_)->tp_name,
                Py_TYPE(_qualname_)->tp_name
            );
        }
        else if (
            (data = NewMessage()) &&
            (
                _PyUnicode_Pack(data, _modname_) ||
                _PyUnicode_Pack(data, _qualname_)
            )
        ) {
            Py_CLEAR(data);
        }
    }
    Py_XDECREF(_qualname_);
    Py_XDECREF(_modname_);
    return data;
}


/* singleton ---------------------------------------------------------------- */

static PyObject *
__pack_singleton(PyObject *obj)
{
    PyObject *data = NULL, *reduce = NULL;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if (!PyUnicode_CheckExact(reduce)) {
            PyErr_SetString(PyExc_TypeError, "__reduce__() must return a str");
        }
        else if (
            (data = NewMessage()) &&
            _PyUnicode_Pack(data, reduce)
        ) {
            Py_CLEAR(data);
        }
        Py_DECREF(reduce);
    }
    return data;
}


/* complex ------------------------------------------------------------------ */

static PyObject *
__pack_complex(PyObject *obj)
{
    Py_complex complex = ((PyComplexObject *)obj)->cval;
    PyObject *data = NULL;

    if (
        (data = NewMessage()) &&
        (__pack_float8(data, complex.real) || __pack_float8(data, complex.imag))
    ) {
        Py_CLEAR(data);
    }
    return data;
}


/* timestamp ---------------------------------------------------------------- */

static inline int
__pack_timestamp__(PyObject *msg, uint64_t seconds, uint32_t nanoseconds)
{
    uint64_t value = 0;

    if ((seconds >> 34) == 0) {
        value = (((uint64_t)nanoseconds << 34) | seconds);
        if ((value & 0xffffffff00000000L) == 0) {
            return __pack_value4(msg, (uint32_t)value);
        }
        return __pack_value8(msg, value);
    }
    return (__pack_value4(msg, nanoseconds)) ? -1 : __pack_value8(msg, seconds);
}

static PyObject *
__pack_timestamp(PyObject *obj)
{
    Timestamp *timestamp = (Timestamp *)obj;
    PyObject *data = NULL;

    if (
        (data = NewMessage()) &&
        __pack_timestamp__(data, timestamp->seconds, timestamp->nanoseconds)
    ) {
        Py_CLEAR(data);
    }
    return data;
}


/* -------------------------------------------------------------------------- */

#define __pack_ext_sequence(m, d, i, l, t, n) \
    ((__pack_sequence(d, i, l, n)) ? -1 : __pack_extension(m, d, t, n))


#define __pack_ext_anyset(m, d, o, t, n) \
    ((__pack_anyset(d, o, n)) ? -1 : __pack_extension(m, d, t, n))


/* PyList ------------------------------------------------------------------- */

#define __pack_ext_list(m, d, i, l) \
    __pack_ext_sequence(m, d, i, l, MSGPACK_EXT_PYLIST, "list")

static int
_PyList_Pack(PyObject *msg, PyObject *obj)
{
    PyObject **items = _PyList_ITEMS(obj);
    Py_ssize_t len = PyList_GET_SIZE(obj);
    PyObject *data = NULL;
    int res = -1;

    if ((data = NewMessage())) {
        res = __pack_ext_list(msg, data, items, len);
        Py_DECREF(data);
    }
    return res;
}


/* PySet -------------------------------------------------------------------- */

#define __pack_ext_set(m, d, o) \
    __pack_ext_anyset(m, d, o, MSGPACK_EXT_PYSET, "set")

static int
_PySet_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = NewMessage())) {
        res = __pack_ext_set(msg, data, obj);
        Py_DECREF(data);
    }
    return res;
}


/* PyFrozenSet -------------------------------------------------------------- */

#define __pack_ext_frozenset(m, d, o) \
    __pack_ext_anyset(m, d, o, MSGPACK_EXT_PYFROZENSET, "frozenset")

static int
_PyFrozenSet_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = NewMessage())) {
        res = __pack_ext_frozenset(msg, data, obj);
        Py_DECREF(data);
    }
    return res;
}


/* PyByteArray -------------------------------------------------------------- */

#define __pack_ext_bytearray(m, d) \
    __pack_extension(m, d, MSGPACK_EXT_PYBYTEARRAY, "bytearray")


static int
_PyByteArray_Pack(PyObject *msg, PyObject *obj)
{
    return __pack_ext_bytearray(msg, obj);
}


/* PyClass ------------------------------------------------------------------ */

#define __pack_ext_class(m, d) \
    __pack_extension(m, d, MSGPACK_EXT_PYCLASS, "class")

static int
_PyClass_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __pack_class(obj))) {
        res = __pack_ext_class(msg, data);
        Py_DECREF(data);
    }
    return res;
}


/* PyComplex ---------------------------------------------------------------- */

#define __pack_ext_complex(m, d) \
    __pack_extension(m, d, MSGPACK_EXT_PYCOMPLEX, "complex")

static int
_PyComplex_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __pack_complex(obj))) {
        res = __pack_ext_complex(msg, data);
        Py_DECREF(data);
    }
    return res;
}


/* mood.msgpack.Timestamp --------------------------------------------------- */

#define __pack_ext_timestamp(m, d) \
    __pack_extension(m, d, MSGPACK_EXT_TIMESTAMP, "mood.msgpack.Timestamp")

static int
_Timestamp_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __pack_timestamp(obj))) {
        res = __pack_ext_timestamp(msg, data);
        Py_DECREF(data);
    }
    return res;
}


/* PyObject ----------------------------------------------------------------- */

static int
_PyObject_Pack(PyObject *msg, PyObject *obj, const char *name)
{
    PyObject *reduce = NULL, *data = NULL;
    uint8_t type = MSGPACK_EXT_INVALID; // 0
    int res = -1;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if ((data = NewMessage())) {
            if (PyUnicode_CheckExact(reduce)) {
                if (!_PyUnicode_Pack(data, reduce)) {
                    type = MSGPACK_EXT_PYSINGLETON;
                }
            }
            else if (PyTuple_CheckExact(reduce)) {
                if (!_PyTuple_Pack(data, reduce)) {
                    type = MSGPACK_EXT_PYOBJECT;
                }
            }
            else {
                PyErr_SetString(
                    PyExc_TypeError, "__reduce__() must return a str or a tuple"
                );
            }
            if (type) {
                res = __pack_extension(msg, data, type, name);
            }
            Py_DECREF(data);
        }
        Py_DECREF(reduce);
    }
    else if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        PyErr_Format(PyExc_TypeError, "cannot pack '%.200s' objects", name);
    }
    return res;
}


/* Extension ---------------------------------------------------------------- */

static int
_Extension_Pack(PyTypeObject *type, PyObject *msg, PyObject *obj)
{
    int res = -1;

    if (type == &PyList_Type) {
        res = _PyList_Pack(msg, obj);
    }
    else if (type == &PySet_Type) {
        res = _PySet_Pack(msg, obj);
    }
    else if (type == &PyFrozenSet_Type) {
        res = _PyFrozenSet_Pack(msg, obj);
    }
    else if (type == &PyByteArray_Type) {
        res = _PyByteArray_Pack(msg, obj);
    }
    else if (type == &PyType_Type) {
        res = _PyClass_Pack(msg, obj);
    }
    else if (type == &PyComplex_Type) {
        res = _PyComplex_Pack(msg, obj);
    }
    else if (type == &Timestamp_Type) {
        res = _Timestamp_Pack(msg, obj);
    }
    else {
        res = _PyObject_Pack(msg, obj, type->tp_name);
    }
    return res;
}


/* --------------------------------------------------------------------------
   interface
   -------------------------------------------------------------------------- */

PyObject *
NewMessage(void)
{
    return _PyObject_CAST(__msg_new__(32));
}


int
RegisterObject(PyObject *registry, PyObject *obj)
{
    PyObject *data = NULL, *key = NULL;
    int res = -1;

    if (
        (data = (PyType_Check(obj) ? __pack_class(obj) : __pack_singleton(obj)))
    ) {
        if ((key = _PyBytes_FromPyByteArray(data))) {
            res = PyDict_SetItem(registry, key, obj);
            Py_DECREF(key);
        }
        Py_DECREF(data);
    }
    return res;
}


int
PackObject(PyObject *msg, PyObject *obj)
{
    PyTypeObject *type = Py_TYPE(obj);
    int res = -1;

    if (obj == Py_None) {
        res = _Py_None_Pack(msg);
    }
    else if (obj == Py_False) {
        res = _Py_False_Pack(msg);
    }
    else if (obj == Py_True) {
        res = _Py_True_Pack(msg);
    }
    else if (type == &PyLong_Type) {
        res = _PyLong_Pack(msg, obj);
    }
    else if (type == &PyFloat_Type) {
        res = _PyFloat_Pack(msg, obj);
    }
    else if (type == &PyBytes_Type) {
        res = _PyBytes_Pack(msg, obj);
    }
    else if (type == &PyUnicode_Type) {
        res = _PyUnicode_Pack(msg, obj);
    }
    else if (type == &PyTuple_Type) {
        res = _PyTuple_Pack(msg, obj);
    }
    else if (type == &PyDict_Type) {
        res = _PyDict_Pack(msg, obj);
    }
    else {
        res = _Extension_Pack(type, msg, obj);
    }
    return res;
}
