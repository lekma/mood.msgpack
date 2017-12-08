/*
#
# Copyright © 2017 Malek Hadj-Ali
# All rights reserved.
#
# This file is part of mood.
#
# mood is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3
# as published by the Free Software Foundation.
#
# mood is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with mood.  If not, see <http://www.gnu.org/licenses/>.
#
*/


/*
TODO:
 - complex numbers
 - bytearrays
*/


#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#define _PY_INLINE_HELPERS
#define _Py_MIN_ALLOC 32
#include "helpers/helpers.h"


/* endian.h is the POSIX way(?) */
#if defined(HAVE_ENDIAN_H)
#include <endian.h>
#elif defined(HAVE_SYS_ENDIAN_H)
#include <sys/endian.h>
#else
#error msgpack needs <endian.h> or <sys/endian.h>
#endif


/* fwd */
static PyModuleDef msgpack_def;
static int _register_obj(PyObject *registry, PyObject *obj);
static int _pack_obj(PyObject *msg, PyObject *obj);
static PyObject *_unpack_msg(Py_buffer *msg, Py_ssize_t *off);


/* for float conversion */
typedef union {
    float f;
    uint32_t u32;
} float_value;

typedef union {
    double d;
    uint64_t u64;
} double_value;


/* ----------------------------------------------------------------------------
 module state
 ---------------------------------------------------------------------------- */

typedef struct {
    PyObject *registry;
} msgpack_state;

#define msgpack_getstate() (msgpack_state *)_PyModuleDef_GetState(&msgpack_def)


static int
_init_state(PyObject *module)
{
    msgpack_state *state = NULL;

    if (
        !(state = _PyModule_GetState(module)) ||
        !(state->registry = PyDict_New()) ||
        _register_obj(state->registry, Py_NotImplemented) ||
        _register_obj(state->registry, Py_Ellipsis)
       ) {
        return -1;
    }
    return 0;
}


/* ----------------------------------------------------------------------------
 MessagePack definitions (these should probably go in a header)
 see https://github.com/msgpack/msgpack/blob/master/spec.md
 ---------------------------------------------------------------------------- */

#define _MSGPACK_UINT32_MAX (1LL << 32)
#define _MSGPACK_INT32_MIN -(1LL << 31)

#define _MSGPACK_UINT16_MAX (1LL << 16)
#define _MSGPACK_INT16_MIN -(1LL << 15)

#define _MSGPACK_UINT8_MAX (1LL << 8)
#define _MSGPACK_INT8_MIN -(1LL << 7)

#define _MSGPACK_FIXINT_MAX (1LL << 7)
#define _MSGPACK_FIXINT_MIN -(1LL << 5)

#define _MSGPACK_FIXSTR_LEN_MAX (1LL << 5)
#define _MSGPACK_FIXOBJ_LEN_MAX (1LL << 4)


/* MessagePack types */
enum {
    _MSGPACK_FIXPINT     = 0x00, //   0
    _MSGPACK_FIXPINTEND  = 0x7f, // 127
    _MSGPACK_FIXMAP      = 0x80,
    _MSGPACK_FIXMAPEND   = 0x8f,
    _MSGPACK_FIXARRAY    = 0x90,
    _MSGPACK_FIXARRAYEND = 0x9f,
    _MSGPACK_FIXSTR      = 0xa0,
    _MSGPACK_FIXSTREND   = 0xbf,

    _MSGPACK_NIL     = 0xc0,
    _MSGPACK_INVALID = 0xc1,
    _MSGPACK_FALSE   = 0xc2,
    _MSGPACK_TRUE    = 0xc3,

    _MSGPACK_BIN8  = 0xc4,
    _MSGPACK_BIN16 = 0xc5,
    _MSGPACK_BIN32 = 0xc6,

    _MSGPACK_EXT8  = 0xc7,
    _MSGPACK_EXT16 = 0xc8,
    _MSGPACK_EXT32 = 0xc9,

    _MSGPACK_FLOAT32 = 0xca,
    _MSGPACK_FLOAT64 = 0xcb,

    _MSGPACK_UINT8  = 0xcc,
    _MSGPACK_UINT16 = 0xcd,
    _MSGPACK_UINT32 = 0xce,
    _MSGPACK_UINT64 = 0xcf,

    _MSGPACK_INT8  = 0xd0,
    _MSGPACK_INT16 = 0xd1,
    _MSGPACK_INT32 = 0xd2,
    _MSGPACK_INT64 = 0xd3,

    _MSGPACK_FIXEXT1  = 0xd4,
    _MSGPACK_FIXEXT2  = 0xd5,
    _MSGPACK_FIXEXT4  = 0xd6,
    _MSGPACK_FIXEXT8  = 0xd7,
    _MSGPACK_FIXEXT16 = 0xd8,

    _MSGPACK_STR8  = 0xd9,
    _MSGPACK_STR16 = 0xda,
    _MSGPACK_STR32 = 0xdb,

    _MSGPACK_ARRAY16 = 0xdc,
    _MSGPACK_ARRAY32 = 0xdd,

    _MSGPACK_MAP16 = 0xde,
    _MSGPACK_MAP32 = 0xdf,

    _MSGPACK_FIXNINT    = 0xe0,  // -32
    _MSGPACK_FIXNINTEND = 0xff   //  -1
};


/* MessagePack extensions */
enum {
    _MSGPACK_EXT_TIMESTAMP = 0xff // -1
};


/* Python MessagePack extensions */
enum {
    _MSGPACK_PYEXT_INVALID = 0, // invalid extension

    _MSGPACK_PYEXT_COMPLEX,
    _MSGPACK_PYEXT_BYTEARRAY,

    _MSGPACK_PYEXT_LIST,
    _MSGPACK_PYEXT_SET,
    _MSGPACK_PYEXT_FROZENSET,

    _MSGPACK_PYEXT_CLASS,
    _MSGPACK_PYEXT_SINGLETON,

    _MSGPACK_PYEXT_INSTANCE = 127 // last
};


static Py_ssize_t
_type_size(uint8_t type)
{
    switch (type) {
        case _MSGPACK_BIN8:
        case _MSGPACK_EXT8:
        case _MSGPACK_UINT8:
        case _MSGPACK_INT8:
        case _MSGPACK_STR8:
            return 1;
        case _MSGPACK_BIN16:
        case _MSGPACK_EXT16:
        case _MSGPACK_UINT16:
        case _MSGPACK_INT16:
        case _MSGPACK_STR16:
        case _MSGPACK_ARRAY16:
        case _MSGPACK_MAP16:
            return 2;
        case _MSGPACK_BIN32:
        case _MSGPACK_EXT32:
        case _MSGPACK_FLOAT32:
        case _MSGPACK_UINT32:
        case _MSGPACK_INT32:
        case _MSGPACK_STR32:
        case _MSGPACK_ARRAY32:
        case _MSGPACK_MAP32:
            return 4;
        case _MSGPACK_FLOAT64:
        case _MSGPACK_UINT64:
        case _MSGPACK_INT64:
            return 8;
        default:
            return 0;
    }
}


/* ----------------------------------------------------------------------------
 register
 ---------------------------------------------------------------------------- */

static PyObject *
__pack_class(PyObject *obj)
{
    _Py_IDENTIFIER(__module__);
    _Py_IDENTIFIER(__qualname__);
    PyObject *result = NULL, *module = NULL, *qualname = NULL;

    if ((module = _PyObject_GetAttrId(obj, &PyId___module__)) &&
        (qualname = _PyObject_GetAttrId(obj, &PyId___qualname__)) &&
        (result = PyByteArray_FromStringAndSize(NULL, 0)) &&
        (_pack_obj(result, module) || _pack_obj(result, qualname))) {
        Py_CLEAR(result);
    }
    Py_XDECREF(qualname);
    Py_XDECREF(module);
    return result;
}


static PyObject *
__pack_singleton(PyObject *obj)
{
    _Py_IDENTIFIER(__reduce__);
    PyObject *result = NULL, *reduce = NULL;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if (!PyUnicode_Check(reduce)) {
            PyErr_SetString(PyExc_TypeError,
                            "__reduce__() must return a string");
        }
        else if ((result = PyByteArray_FromStringAndSize(NULL, 0)) &&
                 _pack_obj(result, reduce)) {
            Py_CLEAR(result);
        }
        Py_DECREF(reduce);
    }
    return result;
}


static int
_register_obj(PyObject *registry, PyObject *obj)
{
    PyObject *data = NULL, *key = NULL;
    int res = -1;

    if ((data = PyType_Check(obj) ? __pack_class(obj) : __pack_singleton(obj))) {
        if ((key = PyBytes_FromStringAndSize(PyByteArray_AS_STRING(data),
                                             Py_SIZE(data)))) {
            res = PyDict_SetItem(registry, key, obj);
            Py_DECREF(key);
        }
        Py_DECREF(data);
    }
    return res;
}


/* ----------------------------------------------------------------------------
 pack
 ---------------------------------------------------------------------------- */

#define __pack__(m, s, b) \
    __PyByteArray_Grow(((PyByteArrayObject *)(m)), (s), (b), _Py_MIN_ALLOC)


#define __pack(m, s, b) \
    __pack__((m), (s), ((const char *)(b)))


static inline int
__pack_type(PyObject *msg, uint8_t type)
{
    return __pack(msg, 1, &type);
}


#define __pack_msg(m, t, s, b) \
    __pack_type((m), (t)) ? -1 : __pack__((m), (s), (b))


#define __pack_value(m, t, s, b) \
    __pack_msg((m), (t), (s), ((const char *)(b)))


static inline int
__pack_8(PyObject *msg, uint8_t type, uint8_t value)
{
    return __pack_value(msg, type, 1, &value);
}


static inline int
__pack_16(PyObject *msg, uint8_t type, uint16_t value)
{
    uint16_t bevalue = htobe16(value);

    return __pack_value(msg, type, 2, &bevalue);
}


static inline int
__pack_32(PyObject *msg, uint8_t type, uint32_t value)
{
    uint32_t bevalue = htobe32(value);

    return __pack_value(msg, type, 4, &bevalue);
}


static inline int
__pack_64(PyObject *msg, uint8_t type, uint64_t value)
{
    uint64_t bevalue = htobe64(value);

    return __pack_value(msg, type, 8, &bevalue);
}


static inline int
__pack_float(PyObject *msg, float value)
{
    float_value fval = { .f = value };

    return __pack_32(msg, _MSGPACK_FLOAT32, fval.u32);
}


static inline int
__pack_double(PyObject *msg, double value)
{
    double_value dval = { .d = value };

    return __pack_64(msg, _MSGPACK_FLOAT64, dval.u64);
}


static int
__pack_len(PyObject *msg, uint8_t type, Py_ssize_t len)
{
    Py_ssize_t size;

    switch ((size = _type_size(type))) {
        case 0:
            return __pack_type(msg, type);
        case 1:
            return __pack_8(msg, type, (uint8_t)len);
        case 2:
            return __pack_16(msg, type, (uint16_t)len);
        case 4:
            return __pack_32(msg, type, (uint32_t)len);
        default:
            PyErr_BadInternalCall();
            return -1;
    }
}


static inline int
__pack_sequence(PyObject *msg, uint8_t type, Py_ssize_t len, PyObject **items)
{
    Py_ssize_t i;
    int res = -1;

    if (!Py_EnterRecursiveCall(" while packing a sequence")) {
        if (!__pack_len(msg, type, len)) {
            for (res = 0, i = 0; i < len; ++i) {
                if ((res = _pack_obj(msg, items[i]))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


static inline int
__pack_dict(PyObject *msg, uint8_t type, Py_ssize_t len, PyObject *obj)
{
    Py_ssize_t pos = 0;
    PyObject *key = NULL, *value = NULL;
    int res = -1;

    if (!Py_EnterRecursiveCall(" while packing a dict")) {
        if (!__pack_len(msg, type, len)) {
            while ((res = PyDict_Next(obj, &pos, &key, &value))) {
                if ((res = _pack_obj(msg, key)) ||
                    (res = _pack_obj(msg, value))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


static inline int
__pack_anyset(PyObject *msg, uint8_t type, Py_ssize_t len, PyObject *obj)
{
    Py_ssize_t pos = 0;
    PyObject *item = NULL;
    Py_hash_t hash;
    int res = -1;

    if (!Py_EnterRecursiveCall(" while packing a set")) {
        if (!__pack_len(msg, type, len)) {
            while ((res = _PySet_NextEntry(obj, &pos, &item, &hash))) {
                if ((res = _pack_obj(msg, item))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


#define __pack_all(m, t, l, b) \
    (__pack_len((m), (t), (l)) ? -1 : __pack__((m), (l), (b)))


#define __pack_bytes(m, t, l, b) \
    ((l) ? __pack_all((m), (t), (l), (b)) : __pack_len((m), (t), (l)))


#define __pack_extension(m, t, l, _t, b) \
    (__pack_len((m), (t), (l)) ? -1 : __pack_msg((m), (_t), (l), (b)))


/* -------------------------------------------------------------------------- */

#define _pack_none(msg) __pack_type((msg), _MSGPACK_NIL)
#define _pack_false(msg) __pack_type((msg), _MSGPACK_FALSE)
#define _pack_true(msg) __pack_type((msg), _MSGPACK_TRUE)


static int
_pack_long(PyObject *msg, PyObject *obj)
{
    int overflow = 0;
    int64_t value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if (value == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (overflow) {
        if (overflow < 0) {
            PyErr_SetString(PyExc_OverflowError, "int too big to convert");
            return -1;
        }
        uint64_t uvalue = PyLong_AsUnsignedLongLong(obj);
        if (PyErr_Occurred()) {
            return -1;
        }
        return __pack_64(msg, _MSGPACK_UINT64, uvalue);
    }
    if (value < _MSGPACK_FIXINT_MIN) {
        if (value < _MSGPACK_INT16_MIN) {
            if (value < _MSGPACK_INT32_MIN) {
                return __pack_64(msg, _MSGPACK_INT64, (uint64_t)value);
            }
            else {
                return __pack_32(msg, _MSGPACK_INT32, (uint32_t)value);
            }
        }
        else {
            if (value < _MSGPACK_INT8_MIN) {
                return __pack_16(msg, _MSGPACK_INT16, (uint16_t)value);
            }
            else {
                return __pack_8(msg, _MSGPACK_INT8, (uint8_t)value);
            }
        }
    }
    else if (value < _MSGPACK_FIXINT_MAX) { // fixint
        return __pack_type(msg, (uint8_t)value);
    }
    else {
        if (value < _MSGPACK_UINT16_MAX) {
            if (value < _MSGPACK_UINT8_MAX) {
                return __pack_8(msg, _MSGPACK_UINT8, (uint8_t)value);
            }
            else {
                return __pack_16(msg, _MSGPACK_UINT16, (uint16_t)value);
            }
        }
        else {
            if (value < _MSGPACK_UINT32_MAX) {
                return __pack_32(msg, _MSGPACK_UINT32, (uint32_t)value);
            }
            else {
                return __pack_64(msg, _MSGPACK_UINT64, (uint64_t)value);
            }
        }
    }
}


static int
_pack_float(PyObject *msg, PyObject *obj)
{
    // NOTE: this is not the same as FLT_MAX
    static const double float_max = 0x1.ffffffp+127;
    double value = PyFloat_AS_DOUBLE(obj);

    if (fabs(value) < float_max) {
        return __pack_float(msg, (float)value);
    }
    return __pack_double(msg, value);
}


static int
_pack_check_len(PyObject *obj, Py_ssize_t len)
{
    if (len >= _MSGPACK_UINT32_MAX) {
        PyErr_Format(PyExc_OverflowError,
                     "%.200s too long to convert", Py_TYPE(obj)->tp_name);
        return -1;
    }
    return 0;
}


static int
_pack_bytes(PyObject *msg, PyObject *obj)
{
    uint8_t type = _MSGPACK_BIN32;
    Py_ssize_t len = Py_SIZE(obj);

    if (len < _MSGPACK_UINT8_MAX) {
        type = _MSGPACK_BIN8;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_BIN16;
    }
    else if (_pack_check_len(obj, len)) {
        return -1;
    }
    return __pack_bytes(msg, type, len, PyBytes_AS_STRING(obj));
}


static int
_pack_unicode(PyObject *msg, PyObject *obj)
{
    uint8_t type = _MSGPACK_STR32;
    Py_ssize_t len;
    char *buf = NULL;

    if (!(buf = PyUnicode_AsUTF8AndSize(obj, &len))) {
        return -1;
    }
    if (len < _MSGPACK_FIXSTR_LEN_MAX) { // fixstr
        type = _MSGPACK_FIXSTR | (uint8_t)len;
    }
    else if (len < _MSGPACK_UINT8_MAX) {
        type = _MSGPACK_STR8;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_STR16;
    }
    else if (_pack_check_len(obj, len)) {
        return -1;
    }
    return __pack_bytes(msg, type, len, buf);
}


static uint8_t
_pack_get_array_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_ARRAY32;

    if (len < _MSGPACK_FIXOBJ_LEN_MAX) { // fixarray
        type = _MSGPACK_FIXARRAY | (uint8_t)len;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_ARRAY16;
    }
    else if (_pack_check_len(obj, len)) {
        type = _MSGPACK_INVALID;
    }
    return type;
}


static int
_pack_sequence(PyObject *msg, PyObject *obj, PyObject **items)
{
    Py_ssize_t len = Py_SIZE(obj);
    uint8_t type = _MSGPACK_INVALID;

    if ((type = _pack_get_array_type(obj, len)) == _MSGPACK_INVALID) {
        return -1;
    }
    return __pack_sequence(msg, type, len, items);
}


#define _pack_tuple(msg, obj) \
    _pack_sequence((msg), (obj), _PyTuple_ITEMS((obj)))


static int
_pack_extension(PyObject *msg, uint8_t _type, PyObject *data)
{
    uint8_t type = _MSGPACK_EXT32;
    Py_ssize_t len;

    if (!PyByteArray_CheckExact(data)) {
        PyErr_BadInternalCall();
        return -1;
    }
    switch ((len = Py_SIZE(data))) {
        case 1:
            type = _MSGPACK_FIXEXT1;
            break;
        case 2:
            type = _MSGPACK_FIXEXT2;
            break;
        case 4:
            type = _MSGPACK_FIXEXT4;
            break;
        case 8:
            type = _MSGPACK_FIXEXT8;
            break;
        case 16:
            type = _MSGPACK_FIXEXT16;
            break;
        default:
            if (len < _MSGPACK_UINT8_MAX) {
                type = _MSGPACK_EXT8;
            }
            else if (len < _MSGPACK_UINT16_MAX) {
                type = _MSGPACK_EXT16;
            }
            else if (_pack_check_len(data, len)) {
                return -1;
            }
            break;
    }
    return __pack_extension(msg, type, len, _type, PyByteArray_AS_STRING(data));
}


static int
_pack_list(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = PyByteArray_FromStringAndSize(NULL, 0))) {
        if (!_pack_sequence(data, obj, _PyList_ITEMS(obj))) {
            res = _pack_extension(msg, _MSGPACK_PYEXT_LIST, data);
        }
        Py_DECREF(data);
    }
    return res;
}


static int
_pack_dict(PyObject *msg, PyObject *obj)
{
    uint8_t type = _MSGPACK_MAP32;
    Py_ssize_t len = _PyDict_GET_SIZE(obj);

    if (len < _MSGPACK_FIXOBJ_LEN_MAX) { // fixmap
        type = _MSGPACK_FIXMAP | (uint8_t)len;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_MAP16;
    }
    else if (_pack_check_len(obj, len)) {
        return -1;
    }
    return __pack_dict(msg, type, len, obj);
}


static int
_pack_anyset(PyObject *msg, PyObject *obj, uint8_t _type)
{
    Py_ssize_t len = PySet_GET_SIZE(obj);
    uint8_t type = _MSGPACK_INVALID;
    PyObject *data = NULL;
    int res = -1;

    if (((type = _pack_get_array_type(obj, len)) != _MSGPACK_INVALID) &&
        (data = PyByteArray_FromStringAndSize(NULL, 0))) {
        if (!__pack_anyset(data, type, len, obj)) {
            res = _pack_extension(msg, _type, data);
        }
        Py_DECREF(data);
    }
    return res;
}


#define _pack_set(msg, obj) \
    _pack_anyset((msg), (obj), _MSGPACK_PYEXT_SET)


#define _pack_frozenset(msg, obj) \
    _pack_anyset((msg), (obj), _MSGPACK_PYEXT_FROZENSET)


static int
_pack_class(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __pack_class(obj))) {
        res = _pack_extension(msg, _MSGPACK_PYEXT_CLASS, data);
        Py_DECREF(data);
    }
    return res;
}


static int
_pack_instance(PyObject *msg, PyObject *obj)
{
    _Py_IDENTIFIER(__reduce__);
    PyObject *reduce = NULL, *data = NULL;
    uint8_t _type = _MSGPACK_PYEXT_INVALID; // 0
    int res = -1;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if (PyTuple_Check(reduce)) {
            _type = _MSGPACK_PYEXT_INSTANCE;
        }
        else if (PyUnicode_Check(reduce)) {
            _type = _MSGPACK_PYEXT_SINGLETON;
        }
        else {
            PyErr_SetString(PyExc_TypeError,
                            "__reduce__() must return a string or a tuple");
        }
        if (_type && (data = PyByteArray_FromStringAndSize(NULL, 0))) {
            if (!_pack_obj(data, reduce)) {
                res = _pack_extension(msg, _type, data);
            }
            Py_DECREF(data);
        }
        Py_DECREF(reduce);
    }
    else if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        PyErr_Format(PyExc_TypeError,
                     "cannot pack '%.200s' objects", Py_TYPE(obj)->tp_name);
    }
    return res;
}


static int
_pack_obj(PyObject *msg, PyObject *obj)
{
    PyTypeObject *type = Py_TYPE(obj);

    if (obj == Py_None) {
        return _pack_none(msg);
    }
    if (obj == Py_False) {
        return _pack_false(msg);
    }
    if (obj == Py_True) {
        return _pack_true(msg);
    }
    if (type == &PyLong_Type) {
        return _pack_long(msg, obj);
    }
    if (type == &PyFloat_Type) {
        return _pack_float(msg, obj);
    }
    if (type == &PyBytes_Type) {
        return _pack_bytes(msg, obj);
    }
    if (type == &PyUnicode_Type) {
        return _pack_unicode(msg, obj);
    }
    if (type == &PyTuple_Type) {
        return _pack_tuple(msg, obj);
    }
    if (type == &PyList_Type) {
        return _pack_list(msg, obj);
    }
    if (type == &PyDict_Type) {
        return _pack_dict(msg, obj);
    }
    if (type == &PySet_Type) {
        return _pack_set(msg, obj);
    }
    if (type == &PyFrozenSet_Type) {
        return _pack_frozenset(msg, obj);
    }
    if (type == &PyType_Type) {
        return _pack_class(msg, obj);
    }
    return _pack_instance(msg, obj);
}


/* ----------------------------------------------------------------------------
 unpack
 ---------------------------------------------------------------------------- */

/* PyLong */
static PyObject *
_PyLong_FromSigned(const char *buf, Py_ssize_t size)
{
    switch (size) {
        case 1:
            return PyLong_FromLong((*(int8_t *)buf));
        case 2:
            return PyLong_FromLong((int16_t)be16toh((*(uint16_t *)buf)));
        case 4:
            return PyLong_FromLong((int32_t)be32toh((*(uint32_t *)buf)));
        case 8:
            return PyLong_FromLongLong((int64_t)be64toh((*(uint64_t *)buf)));
        default:
            PyErr_BadInternalCall();
            return NULL;
    }
}

static PyObject *
_PyLong_FromUnsigned(const char *buf, Py_ssize_t size)
{
    switch (size) {
        case 1:
            return PyLong_FromUnsignedLong((*(uint8_t *)buf));
        case 2:
            return PyLong_FromUnsignedLong(be16toh((*(uint16_t *)buf)));
        case 4:
            return PyLong_FromUnsignedLong(be32toh((*(uint32_t *)buf)));
        case 8:
            return PyLong_FromUnsignedLongLong(be64toh((*(uint64_t *)buf)));
        default:
            PyErr_BadInternalCall();
            return NULL;
    }
}

#define PyLong_FromStringAndSize(b, s, is) \
    ((is) ? _PyLong_FromSigned((b), (s)) : _PyLong_FromUnsigned((b), (s)))


/* PyFloat */
static inline PyObject *
__PyFloat_FromUnsignedLong(uint32_t value)
{
    float_value fval = { .u32 = value };

    return PyFloat_FromDouble(fval.f);
}

static inline PyObject *
__PyFloat_FromUnsignedLongLong(uint64_t value)
{
    double_value dval = { .u64 = value };

    return PyFloat_FromDouble(dval.d);
}

static PyObject *
PyFloat_FromStringAndSize(const char *buf, Py_ssize_t size)
{
    switch (size) {
        case 4:
            return __PyFloat_FromUnsignedLong(be32toh((*(uint32_t *)buf)));
        case 8:
            return __PyFloat_FromUnsignedLongLong(be64toh((*(uint64_t *)buf)));
        default:
            PyErr_BadInternalCall();
            return NULL;
    }
}


/* PySequence */
static PyObject *
_PySequence_FromBufferAndSize(Py_buffer *msg, Py_ssize_t len, Py_ssize_t *off, int list)
{
    PyObject *result = NULL, *item = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a sequence")) {
        if ((result = (list) ? PyList_New(len) : PyTuple_New(len))) {
            for (i = 0; i < len; ++i) {
                if (!(item = _unpack_msg(msg, off))) {
                    Py_CLEAR(result);
                    break;
                }
                if (list) {
                    PyList_SET_ITEM(result, i, item);
                }
                else  {
                    PyTuple_SET_ITEM(result, i, item);
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define PyTuple_FromBufferAndSize(m, s, o) \
    _PySequence_FromBufferAndSize((m), (s), (o), 0)

#define PyList_FromBufferAndSize(m, s, o) \
    _PySequence_FromBufferAndSize((m), (s), (o), 1)


/* PyDict */
static PyObject *
PyDict_FromBufferAndSize(Py_buffer *msg, Py_ssize_t len, Py_ssize_t *off)
{
    PyObject *result = NULL, *key = NULL, *val = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a dict")) {
        if ((result = PyDict_New())) {
            for (i = 0; i < len; ++i) {
                if (!(key = _unpack_msg(msg, off)) ||
                    !(val = _unpack_msg(msg, off)) ||
                    PyDict_SetItem(result, key, val)) {
                    Py_XDECREF(key);
                    Py_XDECREF(val);
                    Py_CLEAR(result);
                    break;
                }
                Py_DECREF(key);
                Py_DECREF(val);
            }
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}


/* PySet */
static PyObject *
_PyAnySet_FromBufferAndSize(Py_buffer *msg, Py_ssize_t len, Py_ssize_t *off, int frozen)
{
    PyObject *result = NULL, *item = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a set")) {
        if ((result = (frozen) ? PyFrozenSet_New(NULL) : PySet_New(NULL))) {
            for (i = 0; i < len; ++i) {
                if (!(item = _unpack_msg(msg, off)) ||
                    PySet_Add(result, item)) {
                    Py_XDECREF(item);
                    Py_CLEAR(result);
                    break;
                }
                Py_DECREF(item);
            }
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define PySet_FromBufferAndSize(m, s, o) \
    _PyAnySet_FromBufferAndSize((m), (s), (o), 0)

#define PyFrozenSet_FromBufferAndSize(m, s, o) \
    _PyAnySet_FromBufferAndSize((m), (s), (o), 1)


/* -------------------------------------------------------------------------- */

static int
_unpack_check_off(Py_buffer *msg, Py_ssize_t off)
{
    if (off > msg->len) {
        PyErr_SetString(PyExc_EOFError, "Ran out of input");
        return -1;
    }
    return 0;
}


static Py_ssize_t
__unpack_len(const char *buf, Py_ssize_t size)
{
    switch (size) {
        case 1:
            return (*(uint8_t *)buf);
        case 2:
            return be16toh((*(uint16_t *)buf));
        case 4:
            return be32toh((*(uint32_t *)buf));
        default:
            PyErr_BadInternalCall();
            return -1;
    }
}


static Py_ssize_t
_unpack_len(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t _off = *off, noff = _off + size;
    char *buf = ((char *)msg->buf) + _off;
    Py_ssize_t len = -1;

    if (!_unpack_check_off(msg, noff) &&
        (len = __unpack_len(buf, size)) >= 0) {
        *off = noff;
    }
    return len;
}


static Py_ssize_t
_unpack_array_len(Py_buffer *msg, Py_ssize_t *off)
{
    Py_ssize_t _off = *off, noff = _off + 1, len = -1;
    uint8_t type = _MSGPACK_INVALID;

    if (!_unpack_check_off(msg, noff)) {
        type = (((char *)msg->buf) + _off)[0];
        *off = noff;
        switch (type) {
            case _MSGPACK_FIXARRAY ... _MSGPACK_FIXARRAYEND:
                len = (type & 0x0f);
                break;
            case _MSGPACK_ARRAY16:
            case _MSGPACK_ARRAY32:
                len = _unpack_len(msg, _type_size(type), off);
                break;
            default:
                PyErr_Format(PyExc_TypeError,
                             "cannot unpack, invalid array type: '0x%02x'", type);
                break;
        }
    }
    return len;
}


static PyObject *
__unpack_registered(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    msgpack_state *state = NULL;
    PyObject *result = NULL, *key = NULL;
    Py_ssize_t _off = *off, noff = _off + size;
    char *buf = ((char *)msg->buf) + _off;

    if ((state = msgpack_getstate()) &&
        !_unpack_check_off(msg, noff) &&
        (key = PyBytes_FromStringAndSize(buf, size))) {
        if ((result = PyDict_GetItem(state->registry, key))) { // borrowed
            Py_INCREF(result);
            *off = noff;
        }
        Py_DECREF(key);
    }
    return result;
}


/* extension ---------------------------------------------------------------- */

#define __PySequence_FromBuffer(t, m, o) \
    do { \
        PyObject *r = NULL; \
        Py_ssize_t l = -1; \
        if ((l = _unpack_array_len((m), (o))) >= 0) { \
            r = t##_FromBufferAndSize((m), l, (o)); \
        } \
        return r; \
    } while (0)

#define _PyList_FromBuffer(m, o) \
    __PySequence_FromBuffer(PyList, (m), (o))

#define _PySet_FromBuffer(m, o) \
    __PySequence_FromBuffer(PySet, (m), (o))

#define _PyFrozenSet_FromBuffer(m, o) \
    __PySequence_FromBuffer(PyFrozenSet, (m), (o))


/* _MSGPACK_PYEXT_CLASS */
static inline void
__PyClass_ErrFromBuffer(Py_buffer *msg, Py_ssize_t *off)
{
    _Py_IDENTIFIER(builtins);
    PyObject *module = NULL, *qualname = NULL;

    if ((module = _unpack_msg(msg, off)) &&
        (qualname = _unpack_msg(msg, off))) {
        if (_PyUnicode_CompareWithId(module, &PyId_builtins)) {
            if (!PyErr_Occurred()) {
                PyErr_Format(PyExc_TypeError,
                             "cannot unpack <class '%U.%U'>", module, qualname);
            }
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "cannot unpack <class '%U'>", qualname);
        }
    }
    Py_XDECREF(qualname);
    Py_XDECREF(module);
}

static PyObject *
PyClass_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, size, off))) {
        __PyClass_ErrFromBuffer(msg, off);
    }
    return result;
}


/* _MSGPACK_PYEXT_SINGLETON */
static inline void
__PySingleton_ErrFromBuffer(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *name = NULL;

    if ((name = _unpack_msg(msg, off))) {
        PyErr_Format(PyExc_TypeError,
                     "cannot unpack '%U'", name);
        Py_DECREF(name);
    }
}

static PyObject *
PySingleton_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, size, off))) {
        __PySingleton_ErrFromBuffer(msg, off);
    }
    return result;
}


/* _MSGPACK_PYEXT_INSTANCE */

/* object.__setstate__() */
static int
_PyObject_SetState(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(__setstate__);
    _Py_IDENTIFIER(__dict__);
    Py_ssize_t pos = 0;
    PyObject *key = NULL, *value = NULL;
    PyObject *result = NULL, *dict = NULL;
    int res = -1;

    if ((result = _PyObject_CallMethodIdObjArgs(self, &PyId___setstate__,
                                                arg, NULL))) {
        res = 0;
        Py_DECREF(result);
    }
    else if (PyErr_ExceptionMatches(PyExc_AttributeError) &&
             PyDict_Check(arg)) {
        PyErr_Clear();
        if ((dict = _PyObject_GetAttrId(self, &PyId___dict__))) {
            while ((res = PyDict_Next(arg, &pos, &key, &value))) {
                /* normally the keys for instance attributes are interned.
                   we should do that here. */
                Py_INCREF(key);
                if (!PyUnicode_Check(key)) {
                    PyErr_Format(PyExc_TypeError,
                                 "expected state key to be unicode, not '%.200s'",
                                 Py_TYPE(key)->tp_name);
                    Py_DECREF(key);
                    break;
                }
                PyUnicode_InternInPlace(&key);
                /* __dict__ can be a dictionary or other mapping object
                   https://docs.python.org/3.5/library/stdtypes.html#object.__dict__ */
                if ((res = PyObject_SetItem(dict, key, value))) {
                    Py_DECREF(key);
                    break;
                }
                Py_DECREF(key);
            }
            Py_DECREF(dict);
        }
    }
    return res;
}


/* object.extend() */
static int
_PySequence_InPlaceConcatOrAdd(PyObject *self, PyObject *arg)
{
    PyTypeObject *type = Py_TYPE(self);
    PySequenceMethods *seq_methods = NULL;
    PyNumberMethods *num_methods = NULL;
    PyObject *result = NULL;
    int res = -1;

    if ((seq_methods = type->tp_as_sequence) &&
        seq_methods->sq_inplace_concat) {
        if ((result = seq_methods->sq_inplace_concat(self, arg))) {
            res = 0;
            Py_DECREF(result);
        }
    }
    else if ((num_methods = type->tp_as_number) &&
             num_methods->nb_inplace_add) {
        if ((result = num_methods->nb_inplace_add(self, arg))) {
            if (result != Py_NotImplemented) {
                res = 0;
            }
            Py_DECREF(result);
        }
    }
    if (res && !PyErr_Occurred()) {
        PyErr_Format(PyExc_TypeError,
                     "cannot extend '%.200s' objects", Py_TYPE(self)->tp_name);
    }
    return res;
}

static int
_PyObject_Extend(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(extend);
    PyObject *result = NULL;

    if ((result = _PyObject_CallMethodIdObjArgs(self, &PyId_extend,
                                                arg, NULL))) {
        Py_DECREF(result);
        return 0;
    }
    if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        return _PySequence_InPlaceConcatOrAdd(self, arg);
    }
    return -1;
}


/* object.update() */
static PyObject *
_PySequence_Fast(PyObject *obj, Py_ssize_t len, const char *message)
{
    PyObject *result = NULL;

    if ((result = PySequence_Fast(obj, message)) &&
        (PySequence_Fast_GET_SIZE(result) != len)) {
        PyErr_Format(PyExc_ValueError,
                     "expected a sequence of len %zd", len);
        Py_CLEAR(result);
    }
    return result;
}

static int
_PyMapping_MergeFromIter(PyObject *self, PyObject *iter)
{
    PyObject *item = NULL, *fast = NULL;

    while ((item = PyIter_Next(iter))) {
        if (!(fast = _PySequence_Fast(item, 2, "not a sequence")) ||
            PyObject_SetItem(self,
                             PySequence_Fast_GET_ITEM(fast, 0),
                             PySequence_Fast_GET_ITEM(fast, 1))) {
            Py_XDECREF(fast);
            Py_DECREF(item);
            break;
        }
        Py_DECREF(fast);
        Py_DECREF(item);
    }
    return PyErr_Occurred() ? -1 : 0;
}

static int
_PyObject_Merge(PyObject *self, PyObject *arg)
{
    PyObject *items = NULL, *iter = NULL;
    int res = -1;

    if (PyIter_Check(arg)) {
        Py_INCREF(arg);
        iter = arg;
    }
    else if ((items = PyMapping_Items(arg))) {
        iter = PyObject_GetIter(items);
        Py_DECREF(items);
    }
    else {
        PyErr_Clear();
        iter = PyObject_GetIter(arg);
    }
    if (iter) {
        res = _PyMapping_MergeFromIter(self, iter);
        Py_DECREF(iter);
    }
    return res;
}

static int
_PyObject_Update(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(update);
    PyObject *result = NULL;

    if ((result = _PyObject_CallMethodIdObjArgs(self, &PyId_update,
                                                arg, NULL))) {
        Py_DECREF(result);
        return 0;
    }
    if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        return _PyObject_Merge(self, arg);
    }
    return -1;
}


static int
_PyCallable_Check(PyObject *arg, void *addr)
{
    if (PyCallable_Check(arg)) {
        *(PyObject **)addr = arg;
        return 1;
    }
    PyErr_Format(PyExc_TypeError,
                 "argument 1 must be a callable, not %.200s",
                 Py_TYPE(arg)->tp_name);
    return 0;
}

/*static int
_PyIter_Or_Py_None_Check(PyObject *arg, void *addr)
{
    if (PyIter_Check(arg) || (arg == Py_None)) {
        *(PyObject **)addr = arg;
        return 1;
    }
    PyErr_Format(PyExc_TypeError,
                 "argument 4 and 5 must be iterators or None, not %.200s",
                 Py_TYPE(arg)->tp_name);
    return 0;
}*/

static PyObject *
_PyInstance_New(PyObject *reduce)
{
    PyObject *callable, *args;
    PyObject *setstatearg = Py_None, *extendarg = Py_None, *updatearg = Py_None;
    PyObject *result = NULL;

    if (
        PyArg_ParseTuple(reduce, "O&O!|OOO",
                         _PyCallable_Check, &callable, &PyTuple_Type, &args,
                         &setstatearg, &extendarg, &updatearg) &&
        (result = PyObject_CallObject(callable, args)) &&
        (
         (setstatearg != Py_None && _PyObject_SetState(result, setstatearg)) ||
         (extendarg != Py_None && _PyObject_Extend(result, extendarg)) ||
         (updatearg != Py_None && _PyObject_Update(result, updatearg))
        )
       ) {
        Py_CLEAR(result);
    }
    return result;
}


static PyObject *
PyInstance_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL, *reduce = NULL;
    Py_ssize_t _off = *off, noff = _off + size;

    if (!_unpack_check_off(msg, noff) && (reduce = _unpack_msg(msg, off))) {
        result = _PyInstance_New(reduce);
        Py_DECREF(reduce);
    }
    return result;
}


/* _MSGPACK_EXT_TIMESTAMP */
static PyObject *
Timestamp_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyErr_SetString(PyExc_NotImplementedError,
                    "MessagePack timestamps are not implemented");
    return NULL;
}


static PyObject *
_PyExtension_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t _off = *off, noff = _off + 1;
    if (_unpack_check_off(msg, noff)) {
        return NULL;
    }
    uint8_t type = (((char *)msg->buf) + _off)[0];
    *off = noff;

    switch (type) {
        case _MSGPACK_PYEXT_LIST:
            _PyList_FromBuffer(msg, off);
        case _MSGPACK_PYEXT_SET:
            _PySet_FromBuffer(msg, off);
        case _MSGPACK_PYEXT_FROZENSET:
            _PyFrozenSet_FromBuffer(msg, off);
        case _MSGPACK_PYEXT_CLASS:
            return PyClass_FromBufferAndSize(msg, size, off);
        case _MSGPACK_PYEXT_SINGLETON:
            return PySingleton_FromBufferAndSize(msg, size, off);
        case _MSGPACK_PYEXT_INSTANCE:
            return PyInstance_FromBufferAndSize(msg, size, off);
        case _MSGPACK_EXT_TIMESTAMP:
            return Timestamp_FromBufferAndSize(msg, size, off);
        default:
            return PyErr_Format(PyExc_TypeError,
                                "cannot unpack, unknown extension type: '0x%02x'",
                                type);
    }
}


static PyObject *
_PyExtension_FromBuffer(Py_buffer *msg, uint8_t type, Py_ssize_t *off)
{
    Py_ssize_t len = -1;

    switch (type) {
        case _MSGPACK_FIXEXT1:
            len = 1;
            break;
        case _MSGPACK_FIXEXT2:
            len = 2;
            break;
        case _MSGPACK_FIXEXT4:
            len = 4;
            break;
        case _MSGPACK_FIXEXT8:
            len = 8;
            break;
        case _MSGPACK_FIXEXT16:
            len = 16;
            break;
        default:
            len = _unpack_len(msg, _type_size(type), off);
            break;
    }
    return (len < 0) ? NULL : _PyExtension_FromBufferAndSize(msg, len, off);
}


/* -------------------------------------------------------------------------- */

#define __PyObject_FromBufferAndSize(t, m, s, o, ...) \
    do { \
        PyObject *r = NULL; \
        Py_ssize_t _o = *(o), no = _o + (s); \
        char *b = ((char *)(m)->buf) + _o; \
        if (!_unpack_check_off((m), no) && \
            (r = t##_FromStringAndSize(b, (s), ##__VA_ARGS__))) { \
            *(o) = no; \
        } \
        return r; \
    } while (0)

#define _PyLong_FromBufferAndSize(m, s, o, is) \
    __PyObject_FromBufferAndSize(PyLong, (m), (s), (o), (is))

#define _PyFloat_FromBufferAndSize(m, s, o) \
    __PyObject_FromBufferAndSize(PyFloat, (m), (s), (o))

#define _PyBytes_FromBufferAndSize(m, s, o) \
    __PyObject_FromBufferAndSize(PyBytes, (m), (s), (o))

#define _PyUnicode_FromBufferAndSize(m, s, o) \
    __PyObject_FromBufferAndSize(PyUnicode, (m), (s), (o))

#define _PyTuple_FromBufferAndSize(m, s, o) \
    return PyTuple_FromBufferAndSize((m), (s), (o))

#define _PyDict_FromBufferAndSize(m, s, o) \
    return PyDict_FromBufferAndSize((m), (s), (o))


#define __PyObject_FromBuffer(t, m, s, o) \
    do { \
        Py_ssize_t l = -1; \
        if ((l = _unpack_len((m), (s), (o))) < 0) { \
            return NULL; \
        } \
        _##t##_FromBufferAndSize((m), l, (o)); \
    } while (0)

#define _PyBytes_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyBytes, (m), (s), (o))

#define _PyUnicode_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyUnicode, (m), (s), (o))

#define _PyTuple_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyTuple, (m), (s), (o))

#define _PyDict_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyDict, (m), (s), (o))


static PyObject *
_unpack_msg(Py_buffer *msg, Py_ssize_t *off)
{
    Py_ssize_t _off = *off, noff = _off + 1;
    if (_unpack_check_off(msg, noff)) {
        return NULL;
    }
    uint8_t type = (((char *)msg->buf) + _off)[0];
    *off = noff;

    switch (type) {
        case _MSGPACK_FIXPINT ... _MSGPACK_FIXPINTEND:
            return PyLong_FromLong(type);
        case _MSGPACK_FIXMAP ... _MSGPACK_FIXMAPEND:
            _PyDict_FromBufferAndSize(msg, (type & 0x0f), off);
        case _MSGPACK_FIXARRAY ... _MSGPACK_FIXARRAYEND:
            _PyTuple_FromBufferAndSize(msg, (type & 0x0f), off);
        case _MSGPACK_FIXSTR ... _MSGPACK_FIXSTREND:
            _PyUnicode_FromBufferAndSize(msg, (type & 0x1f), off);
        case _MSGPACK_NIL:
            Py_RETURN_NONE;
        case _MSGPACK_FALSE:
            Py_RETURN_FALSE;
        case _MSGPACK_TRUE:
            Py_RETURN_TRUE;
        case _MSGPACK_BIN8:
        case _MSGPACK_BIN16:
        case _MSGPACK_BIN32:
            _PyBytes_FromBuffer(msg, _type_size(type), off);
        case _MSGPACK_EXT8:
        case _MSGPACK_EXT16:
        case _MSGPACK_EXT32:
            return _PyExtension_FromBuffer(msg, type, off);
        case _MSGPACK_FLOAT32:
        case _MSGPACK_FLOAT64:
            _PyFloat_FromBufferAndSize(msg, _type_size(type), off);
        case _MSGPACK_UINT8:
        case _MSGPACK_UINT16:
        case _MSGPACK_UINT32:
        case _MSGPACK_UINT64:
            _PyLong_FromBufferAndSize(msg, _type_size(type), off, 0);
        case _MSGPACK_INT8:
        case _MSGPACK_INT16:
        case _MSGPACK_INT32:
        case _MSGPACK_INT64:
            _PyLong_FromBufferAndSize(msg, _type_size(type), off, 1);
        case _MSGPACK_FIXEXT1:
        case _MSGPACK_FIXEXT2:
        case _MSGPACK_FIXEXT4:
        case _MSGPACK_FIXEXT8:
        case _MSGPACK_FIXEXT16:
            return _PyExtension_FromBuffer(msg, type, off);
        case _MSGPACK_STR8:
        case _MSGPACK_STR16:
        case _MSGPACK_STR32:
            _PyUnicode_FromBuffer(msg, _type_size(type), off);
        case _MSGPACK_ARRAY16:
        case _MSGPACK_ARRAY32:
            _PyTuple_FromBuffer(msg, _type_size(type), off);
        case _MSGPACK_MAP16:
        case _MSGPACK_MAP32:
            _PyDict_FromBuffer(msg, _type_size(type), off);
        case _MSGPACK_FIXNINT ... _MSGPACK_FIXNINTEND:
            return PyLong_FromLong((int8_t)type);
        default:
            return PyErr_Format(PyExc_TypeError,
                                "cannot unpack, unknown type: '0x%02x'", type);
    }
}


/* ----------------------------------------------------------------------------
 ipc packing helpers
 ---------------------------------------------------------------------------- */

static inline PyObject *
__ipc_pack_obj(PyObject *obj)
{
    PyObject *result = NULL;

    if ((result = PyByteArray_FromStringAndSize(NULL, 0)) &&
        _pack_obj(result, obj)) {
        Py_CLEAR(result);
    }
    return result;
}


static int
_ipc_check_len(uint8_t type, Py_ssize_t max, Py_ssize_t len)
{
    if (len >= max) {
        PyErr_Format(PyExc_OverflowError,
                     "msg too long for ipc type: '0x%02x'", type);
        return -1;
    }
    return 0;
}


static uint8_t
_ipc_get_type(Py_ssize_t len)
{
    if (len < _MSGPACK_UINT8_MAX) {
        return _MSGPACK_UINT8;
    }
    if (len < _MSGPACK_UINT16_MAX) {
        return _MSGPACK_UINT16;
    }
    if (len < _MSGPACK_UINT32_MAX) {
        return _MSGPACK_UINT32;
    }
    PyErr_SetString(PyExc_OverflowError, "msg too long to be packed");
    return _MSGPACK_INVALID;
}


static uint8_t
_ipc_check_type(uint8_t type, Py_ssize_t len)
{
    Py_ssize_t max = 0;

    switch (type) {
        case _MSGPACK_UINT8:
            max = _MSGPACK_UINT8_MAX;
            break;
        case _MSGPACK_UINT16:
            max = _MSGPACK_UINT16_MAX;
            break;
        case _MSGPACK_UINT32:
            max = _MSGPACK_UINT32_MAX;
            break;
        default:
            PyErr_Format(PyExc_TypeError,
                         "invalid ipc type: '0x%02x'", type);
            return _MSGPACK_INVALID;
    }
    return _ipc_check_len(type, max, len) ? _MSGPACK_INVALID : type;
}


#define __ipc_pack_any(s, o) \
    do { \
        PyObject *r = NULL, *b = NULL; \
        Py_ssize_t l = 0; \
        uint8_t t = _MSGPACK_UINT##s; \
        if ((b = __ipc_pack_obj((o)))) { \
            l = Py_SIZE(b); \
            if (!_ipc_check_len(t, _MSGPACK_UINT##s##_MAX, l) && \
                (r = PyByteArray_FromStringAndSize(NULL, 0)) && \
                (__pack_##s(r, t, l) || \
                 __pack__(r, l, PyByteArray_AS_STRING(b)))) { \
                Py_CLEAR(r); \
            } \
            Py_DECREF(b); \
        } \
        return r; \
    } while (0)


/* ----------------------------------------------------------------------------
 module
 ---------------------------------------------------------------------------- */

/* msgpack_def.m_doc */
PyDoc_STRVAR(msgpack_m_doc,
"Python MessagePack implementation");


/* msgpack.register() */
PyDoc_STRVAR(msgpack_register_doc,
"register(obj)");

static PyObject *
msgpack_register(PyObject *module, PyObject *obj)
{
    msgpack_state *state = NULL;

    if (!(state = _PyModule_GetState(module)) ||
        _register_obj(state->registry, obj)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


/* msgpack.pack() */
PyDoc_STRVAR(msgpack_pack_doc,
"pack(obj) -> msg");

static PyObject *
msgpack_pack(PyObject *module, PyObject *obj)
{
    PyObject *msg = NULL;

    if ((msg = PyByteArray_FromStringAndSize(NULL, 0)) &&
        _pack_obj(msg, obj)) {
        Py_CLEAR(msg);
    }
    return msg;
}


/* msgpack.unpack() */
PyDoc_STRVAR(msgpack_unpack_doc,
"unpack(msg) -> obj");

static PyObject *
msgpack_unpack(PyObject *module, PyObject *args)
{
    PyObject *obj = NULL;
    Py_buffer msg;
    Py_ssize_t off = 0;

    if (PyArg_ParseTuple(args, "y*:unpack", &msg)) {
        obj = _unpack_msg(&msg, &off);
        PyBuffer_Release(&msg);
    }
    return obj;
}


/* msgpack.size() */
PyDoc_STRVAR(msgpack_size_doc,
"size(type) -> int");

static PyObject *
msgpack_size(PyObject *module, PyObject *args)
{
    uint8_t type;

    if (!PyArg_ParseTuple(args, "b:size", &type)) {
        return NULL;
    }
    return PyLong_FromSsize_t(_type_size(type));
}


/* msgpack.instance() */
/*PyDoc_STRVAR(msgpack_instance_doc,
"instance(reduce) -> obj");

static PyObject *
msgpack_instance(PyObject *module, PyObject *reduce)
{
    return _PyInstance_New(reduce);
}*/


/* msgpack.ipc_size() */
PyDoc_STRVAR(msgpack_ipc_size_doc,
"ipc_size(type) -> int");

static PyObject *
msgpack_ipc_size(PyObject *module, PyObject *args)
{
    uint8_t type;

    if (!PyArg_ParseTuple(args, "b:ipc_size", &type)) {
        return NULL;
    }
    switch (type) {
        case _MSGPACK_UINT8:
        case _MSGPACK_UINT16:
        case _MSGPACK_UINT32:
            break;
        default:
            return PyErr_Format(PyExc_TypeError,
                                "invalid ipc type: '0x%02x'", type);
    }
    return PyLong_FromSsize_t(_type_size(type) + 1);
}


/* msgpack.ipc_pack() */
PyDoc_STRVAR(msgpack_ipc_pack_doc,
"ipc_pack(obj[, type]) -> msg");

static PyObject *
msgpack_ipc_pack(PyObject *module, PyObject *args)
{
    PyObject *result = NULL, *obj = NULL, *bytes = NULL;
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = 0;

    if (PyArg_ParseTuple(args, "O|b:ipc_pack", &obj, &type) &&
        (bytes = __ipc_pack_obj(obj))) {
        len = Py_SIZE(bytes);
        if (type == _MSGPACK_INVALID) {
            type = _ipc_get_type(len);
        }
        else {
            type = _ipc_check_type(type, len);
        }
        if ((type != _MSGPACK_INVALID) &&
            (result = PyByteArray_FromStringAndSize(NULL, 0)) &&
            (__pack_len(result, type, len) ||
             __pack__(result, len, PyByteArray_AS_STRING(bytes)))) {
            Py_CLEAR(result);
        }
        Py_DECREF(bytes);
    }
    return result;
}


/* msgpack.ipc_pack8() */
PyDoc_STRVAR(msgpack_ipc_pack8_doc,
"ipc_pack8(obj) -> msg");

static PyObject *
msgpack_ipc_pack8(PyObject *module, PyObject *obj)
{
    __ipc_pack_any(8, obj);
}


/* msgpack.ipc_pack16() */
PyDoc_STRVAR(msgpack_ipc_pack16_doc,
"ipc_pack16(obj) -> msg");

static PyObject *
msgpack_ipc_pack16(PyObject *module, PyObject *obj)
{
    __ipc_pack_any(16, obj);
}


/* msgpack.ipc_pack32() */
PyDoc_STRVAR(msgpack_ipc_pack32_doc,
"ipc_pack32(obj) -> msg");

static PyObject *
msgpack_ipc_pack32(PyObject *module, PyObject *obj)
{
    __ipc_pack_any(32, obj);
}


/* msgpack_def.m_methods */
static PyMethodDef msgpack_m_methods[] = {
    {"register", (PyCFunction)msgpack_register,
     METH_O, msgpack_register_doc},
    {"pack", (PyCFunction)msgpack_pack,
     METH_O, msgpack_pack_doc},
    {"unpack", (PyCFunction)msgpack_unpack,
     METH_VARARGS, msgpack_unpack_doc},
    {"size", (PyCFunction)msgpack_size,
     METH_VARARGS, msgpack_size_doc},
    /*{"instance", (PyCFunction)msgpack_instance,
     METH_O, msgpack_instance_doc},*/
    {"ipc_size", (PyCFunction)msgpack_ipc_size,
     METH_VARARGS, msgpack_ipc_size_doc},
    {"ipc_pack", (PyCFunction)msgpack_ipc_pack,
     METH_VARARGS, msgpack_ipc_pack_doc},
    {"ipc_pack8", (PyCFunction)msgpack_ipc_pack8,
     METH_O, msgpack_ipc_pack8_doc},
    {"ipc_pack16", (PyCFunction)msgpack_ipc_pack16,
     METH_O, msgpack_ipc_pack16_doc},
    {"ipc_pack32", (PyCFunction)msgpack_ipc_pack32,
     METH_O, msgpack_ipc_pack32_doc},
    {NULL} /* Sentinel */
};


/* msgpack_def.m_traverse */
static int
msgpack_m_traverse(PyObject *module, visitproc visit, void *arg)
{
    msgpack_state *state = NULL;

    if (!(state = _PyModule_GetState(module))) {
        return -1;
    }
    Py_VISIT(state->registry);
    return 0;
}


/* msgpack_def.m_clear */
static int
msgpack_m_clear(PyObject *module)
{
    msgpack_state *state = NULL;

    if (!(state = _PyModule_GetState(module))) {
        return -1;
    }
    Py_CLEAR(state->registry);
    return 0;
}


/* msgpack_def.m_free */
static void
msgpack_m_free(PyObject *module)
{
    msgpack_m_clear(module);
}


/* msgpack_def */
static PyModuleDef msgpack_def = {
    PyModuleDef_HEAD_INIT,
    "msgpack",                                /* m_name */
    msgpack_m_doc,                            /* m_doc */
    sizeof(msgpack_state),                    /* m_size */
    msgpack_m_methods,                        /* m_methods */
    NULL,                                     /* m_slots */
    (traverseproc)msgpack_m_traverse,         /* m_traverse */
    (inquiry)msgpack_m_clear,                 /* m_clear */
    (freefunc)msgpack_m_free                  /* m_free */
};


/* module initialization */
PyMODINIT_FUNC
PyInit_msgpack(void)
{
    PyObject *module = NULL;

    if ((module = PyState_FindModule(&msgpack_def))) {
        Py_INCREF(module);
    }
    else if (
             (module = PyModule_Create(&msgpack_def)) &&
             (
              _init_state(module) ||
              _PyModule_AddUnsignedIntConstant(module, "IPC8", _MSGPACK_UINT8) ||
              _PyModule_AddUnsignedIntConstant(module, "IPC16", _MSGPACK_UINT16) ||
              _PyModule_AddUnsignedIntConstant(module, "IPC32", _MSGPACK_UINT32) ||
              PyModule_AddStringConstant(module, "__version__", PKG_VERSION)
             )
            ) {
        Py_CLEAR(module);
    }
    return module;
}
