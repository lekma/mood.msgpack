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


#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#define _PY_INLINE_HELPERS
#define _Py_MIN_ALLOC 32
#include "helpers/helpers.c"


/* we need a 64bit type */
#if !defined(HAVE_LONG_LONG)
#error "msgpack needs a long long integer type"
#endif /* HAVE_LONG_LONG */


/* endian.h is the POSIX way(?) */
#if defined(HAVE_ENDIAN_H)
#include <endian.h>
#elif defined(HAVE_SYS_ENDIAN_H)
#include <sys/endian.h>
#else
#error "msgpack needs <endian.h> or <sys/endian.h>"
#endif


/* forward declarations */
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
 Timestamp
 ---------------------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD
    int64_t seconds;
    uint32_t nanoseconds;
} Timestamp;


#define _MSGPACK_NS_MAX 1e9

#define trunc_round(d) trunc(((d) < 0) ? ((d) - .5) : ((d) + .5))


/* Timestamp_Type helpers --------------------------------------------------- */

static PyObject *
_Timestamp_New(PyTypeObject *type, int64_t seconds, uint32_t nanoseconds)
{
    Timestamp *self = NULL;

    if (nanoseconds < _MSGPACK_NS_MAX) {
        if ((self = (Timestamp *)type->tp_alloc(type, 0))) {
            self->seconds = seconds;
            self->nanoseconds = nanoseconds;
        }
    }
    else {
        PyErr_SetString(PyExc_OverflowError,
                        "argument 'nanoseconds' greater than maximum");
    }
    return (PyObject *)self;
}


static PyObject *
_Timestamp_FromPyFloat(PyTypeObject *type, PyObject *timestamp)
{
    double value = PyFloat_AS_DOUBLE(timestamp), intpart, fracpart, diff;
    int64_t seconds;

    fracpart = fabs(modf(value, &intpart)) * _MSGPACK_NS_MAX;
    seconds = (int64_t)trunc_round(intpart);
    diff = intpart - (double)seconds;
    if (((intpart < LLONG_MIN) || (LLONG_MAX < intpart)) ||
        ((diff <= -1.0) || (1.0 <= diff))) {
        PyErr_SetString(PyExc_OverflowError,
                        "timestamp out of range for platform");
        return NULL;
    }
    return _Timestamp_New(type, seconds, (uint32_t)trunc_round(fracpart));
}


static PyObject *
_Timestamp_FromPyLong(PyTypeObject *type, PyObject *timestamp)
{
    int64_t seconds = 0;

    if (((seconds = PyLong_AsLongLong(timestamp)) == -1) && PyErr_Occurred()) {
        return NULL;
    }
    return _Timestamp_New(type, seconds, 0);
}


/* Timestamp_Type ----------------------------------------------------------- */

/* Timestamp_Type.tp_dealloc */
static void
Timestamp_tp_dealloc(Timestamp *self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}


/* Timestamp_Type.tp_repr */
static PyObject *
Timestamp_tp_repr(Timestamp *self)
{
    return PyUnicode_FromFormat("%s(%lld.%09u)", Py_TYPE(self)->tp_name,
                                self->seconds, self->nanoseconds);
}


/* Timestamp_Type.tp_doc */
PyDoc_STRVAR(Timestamp_tp_doc,
"Timestamp(seconds[, nanoseconds=0])");


/* Timestamp.fromtimestamp() */
PyDoc_STRVAR(Timestamp_fromtimestamp_doc,
"@classmethod\n\
fromtimestamp(timestamp) -> Timestamp");

static PyObject *
Timestamp_fromtimestamp(PyObject *cls, PyObject *timestamp)
{
    PyTypeObject *type = (PyTypeObject *)cls;
    PyObject *result = NULL;

    if (PyFloat_Check(timestamp)) {
        result = _Timestamp_FromPyFloat(type, timestamp);
    }
    else if (PyLong_Check(timestamp)) {
        result = _Timestamp_FromPyLong(type, timestamp);
    }
    else {
        PyErr_Format(PyExc_TypeError,
                     "expected a 'float' or an 'int', got: '%.200s'",
                     Py_TYPE(timestamp)->tp_name);
    }
    return result;
}


/* Timestamp.timestamp() */
PyDoc_STRVAR(Timestamp_timestamp_doc,
"timestamp() -> float");

static PyObject *
Timestamp_timestamp(Timestamp *self)
{
    double seconds = (double)self->seconds;

    return PyFloat_FromDouble(
        seconds + copysign((self->nanoseconds / _MSGPACK_NS_MAX), seconds));
}


/* TimestampType.tp_methods */
static PyMethodDef Timestamp_tp_methods[] = {
    {"fromtimestamp", (PyCFunction)Timestamp_fromtimestamp,
     METH_O | METH_CLASS, Timestamp_fromtimestamp_doc},
    {"timestamp", (PyCFunction)Timestamp_timestamp,
     METH_NOARGS, Timestamp_timestamp_doc},
    {NULL}  /* Sentinel */
};


/* Timestamp_Type.tp_members */
static PyMemberDef Timestamp_tp_members[] = {
    {"seconds", T_LONGLONG, offsetof(Timestamp, seconds), READONLY, NULL},
    {"nanoseconds", T_UINT, offsetof(Timestamp, nanoseconds), READONLY, NULL},
    {NULL}  /* Sentinel */
};


/* Timestamp_Type.tp_new */
static PyObject *
Timestamp_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"seconds", "nanoseconds", NULL};
    int64_t seconds;
    uint32_t nanoseconds = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "L|I:__new__", kwlist,
                                     &seconds, &nanoseconds)) {
        return NULL;
    }
    return _Timestamp_New(type, seconds, nanoseconds);
}


/* Timestamp_Type */
static PyTypeObject Timestamp_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "mood.msgpack.Timestamp",                 /*tp_name*/
    sizeof(Timestamp),                        /*tp_basicsize*/
    0,                                        /*tp_itemsize*/
    (destructor)Timestamp_tp_dealloc,         /*tp_dealloc*/
    0,                                        /*tp_print*/
    0,                                        /*tp_getattr*/
    0,                                        /*tp_setattr*/
    0,                                        /*tp_compare*/
    (reprfunc)Timestamp_tp_repr,              /*tp_repr*/
    0,                                        /*tp_as_number*/
    0,                                        /*tp_as_sequence*/
    0,                                        /*tp_as_mapping*/
    0,                                        /*tp_hash */
    0,                                        /*tp_call*/
    0,                                        /*tp_str*/
    0,                                        /*tp_getattro*/
    0,                                        /*tp_setattro*/
    0,                                        /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    Timestamp_tp_doc,                         /*tp_doc*/
    0,                                        /*tp_traverse*/
    0,                                        /*tp_clear*/
    0,                                        /*tp_richcompare*/
    0,                                        /*tp_weaklistoffset*/
    0,                                        /*tp_iter*/
    0,                                        /*tp_iternext*/
    Timestamp_tp_methods,                     /*tp_methods*/
    Timestamp_tp_members,                     /*tp_members*/
    0,                                        /*tp_getsets*/
    0,                                        /*tp_base*/
    0,                                        /*tp_dict*/
    0,                                        /*tp_descr_get*/
    0,                                        /*tp_descr_set*/
    0,                                        /*tp_dictoffset*/
    0,                                        /*tp_init*/
    0,                                        /*tp_alloc*/
    Timestamp_tp_new,                         /*tp_new*/
};


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
#define _MSGPACK_FIXSTR_BIT 0x1f

#define _MSGPACK_FIXOBJ_LEN_MAX (1LL << 4)
#define _MSGPACK_FIXOBJ_BIT 0x0f


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
    _MSGPACK_INVALID = 0xc1,     // invalid type
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
    Py_ssize_t size = 0;

    switch (type) {
        case _MSGPACK_BIN8:
        case _MSGPACK_EXT8:
        case _MSGPACK_UINT8:
        case _MSGPACK_INT8:
        case _MSGPACK_STR8:
            size = 1;
            break;
        case _MSGPACK_BIN16:
        case _MSGPACK_EXT16:
        case _MSGPACK_UINT16:
        case _MSGPACK_INT16:
        case _MSGPACK_STR16:
        case _MSGPACK_ARRAY16:
        case _MSGPACK_MAP16:
            size = 2;
            break;
        case _MSGPACK_BIN32:
        case _MSGPACK_EXT32:
        case _MSGPACK_FLOAT32:
        case _MSGPACK_UINT32:
        case _MSGPACK_INT32:
        case _MSGPACK_STR32:
        case _MSGPACK_ARRAY32:
        case _MSGPACK_MAP32:
            size = 4;
            break;
        case _MSGPACK_FLOAT64:
        case _MSGPACK_UINT64:
        case _MSGPACK_INT64:
            size = 8;
            break;
        default:
            break;
    }
    return size;
}


/* ----------------------------------------------------------------------------
 register
 ---------------------------------------------------------------------------- */

static PyObject *
_pack_class(PyObject *obj)
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
_pack_singleton(PyObject *obj)
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

    if ((data = PyType_Check(obj) ? _pack_class(obj) : _pack_singleton(obj))) {
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

#define _PyErr_TooLong(o) \
    PyErr_Format(PyExc_OverflowError, \
                 "%.200s too long to convert", Py_TYPE((o))->tp_name)


static inline uint8_t
__pack_bin_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_INVALID;

    if (len < _MSGPACK_UINT8_MAX) {
        type = _MSGPACK_BIN8;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_BIN16;
    }
    else if (len < _MSGPACK_UINT32_MAX) {
        type = _MSGPACK_BIN32;
    }
    else {
        _PyErr_TooLong(obj);
    }
    return type;
}


static inline uint8_t
__pack_str_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_INVALID;

    if (len < _MSGPACK_FIXSTR_LEN_MAX) { // fixstr
        type = _MSGPACK_FIXSTR | (uint8_t)len;
    }
    else if (len < _MSGPACK_UINT8_MAX) {
        type = _MSGPACK_STR8;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_STR16;
    }
    else if (len < _MSGPACK_UINT32_MAX) {
        type = _MSGPACK_STR32;
    }
    else {
        _PyErr_TooLong(obj);
    }
    return type;
}


static inline uint8_t
__pack_array_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_INVALID;

    if (len < _MSGPACK_FIXOBJ_LEN_MAX) { // fixarray
        type = _MSGPACK_FIXARRAY | (uint8_t)len;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_ARRAY16;
    }
    else if (len < _MSGPACK_UINT32_MAX) {
        type = _MSGPACK_ARRAY32;
    }
    else {
        _PyErr_TooLong(obj);
    }
    return type;
}


static inline uint8_t
__pack_map_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_INVALID;

    if (len < _MSGPACK_FIXOBJ_LEN_MAX) { // fixmap
        type = _MSGPACK_FIXMAP | (uint8_t)len;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_MAP16;
    }
    else if (len < _MSGPACK_UINT32_MAX) {
        type = _MSGPACK_MAP32;
    }
    else {
        _PyErr_TooLong(obj);
    }
    return type;
}


static inline uint8_t
__pack_ext_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_INVALID;

    if (len < _MSGPACK_UINT8_MAX) {
        switch (len) {
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
                type = _MSGPACK_EXT8;
                break;
        }
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_EXT16;
    }
    else if (len < _MSGPACK_UINT32_MAX) {
        type = _MSGPACK_EXT32;
    }
    else {
        _PyErr_TooLong(obj);
    }
    return type;
}


/* -------------------------------------------------------------------------- */

#define __pack__(m, s, b) \
    __PyByteArray_Grow(((PyByteArrayObject *)(m)), (s), (b), _Py_MIN_ALLOC)


#define __pack(m, s, b) \
    __pack__((m), (s), (const char *)(b))


#define __pack_value__(s, m, v) \
    do { \
        uint##s##_t bev = htobe##s((v)); \
        return __pack((m), ((s) >> 3), &bev); \
    } while (0)

static inline int
__pack_u8(PyObject *msg, uint8_t value)
{
    return __pack(msg, 1, &value);
}

static inline int
__pack_u16(PyObject *msg, uint16_t value)
{
    __pack_value__(16, msg, value);
}

static inline int
__pack_u32(PyObject *msg, uint32_t value)
{
    __pack_value__(32, msg, value);
}

static inline int
__pack_u64(PyObject *msg, uint64_t value)
{
    __pack_value__(64, msg, value);
}


/* -------------------------------------------------------------------------- */

#define __pack_type __pack_u8

#define __pack_value(s, m, t, v) \
    (__pack_type((m), (t)) ? -1 : __pack_u##s((m), (v)))

#define __pack_8(m, t, v) __pack_value(8, (m), (t), (v))

#define __pack_16(m, t, v) __pack_value(16, (m), (t), (v))

#define __pack_32(m, t, v) __pack_value(32, (m), (t), (v))

#define __pack_64(m, t, v) __pack_value(64, (m), (t), (v))

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


#define __pack_buf(m, s, b) \
    ((s) ? __pack__((m), (s), (b)) : 0)


#define __pack_data(m, t, s, b) \
    (__pack_type((m), (t)) ? -1 : __pack_buf((m), (s), (b)))


/* wrappers ----------------------------------------------------------------- */

static inline int
__pack_signed__(PyObject *msg, int64_t value)
{
    int res = -1;

    if (value < _MSGPACK_INT16_MIN) {
        if (value < _MSGPACK_INT32_MIN) {
            res = __pack_64(msg, _MSGPACK_INT64, (uint64_t)value);
        }
        else {
            res = __pack_32(msg, _MSGPACK_INT32, (uint32_t)value);
        }
    }
    else {
        if (value < _MSGPACK_INT8_MIN) {
            res = __pack_16(msg, _MSGPACK_INT16, (uint16_t)value);
        }
        else {
            res = __pack_8(msg, _MSGPACK_INT8, (uint8_t)value);
        }
    }
    return res;
}


static inline int
__pack_unsigned__(PyObject *msg, int64_t value)
{
    int res = -1;

    if (value < _MSGPACK_UINT16_MAX) {
        if (value < _MSGPACK_UINT8_MAX) {
            res = __pack_8(msg, _MSGPACK_UINT8, (uint8_t)value);
        }
        else {
            res = __pack_16(msg, _MSGPACK_UINT16, (uint16_t)value);
        }
    }
    else {
        if (value < _MSGPACK_UINT32_MAX) {
            res = __pack_32(msg, _MSGPACK_UINT32, (uint32_t)value);
        }
        else {
            res = __pack_64(msg, _MSGPACK_UINT64, (uint64_t)value);
        }
    }
    return res;
}


static int
__pack_long__(PyObject *msg, int64_t value)
{
    int res = -1;

    if (value < _MSGPACK_FIXINT_MIN) {
        res = __pack_signed__(msg, value);
    }
    else if (value < _MSGPACK_FIXINT_MAX) { // fixint
        res = __pack_u8(msg, (uint8_t)value);
    }
    else {
        res = __pack_unsigned__(msg, value);
    }
    return res;
}


/*static int
__pack_float__(PyObject *msg, double value)
{
    // NOTE: this is not the same as FLT_MAX
    static const double float_max = 0x1.ffffffp+127;
    int res = -1;

    if (fabs(value) < float_max) {
        res = __pack_float(msg, (float)value);
    }
    else {
        res = __pack_double(msg, value);
    }
    return res;
}*/


static int
__pack_len__(PyObject *msg, uint8_t type, Py_ssize_t len)
{
    Py_ssize_t size = _type_size(type);
    int res = -1;

    switch (size) {
        case 0:
            res = __pack_type(msg, type);
            break;
        case 1:
            res = __pack_8(msg, type, (uint8_t)len);
            break;
        case 2:
            res = __pack_16(msg, type, (uint16_t)len);
            break;
        case 4:
            res = __pack_32(msg, type, (uint32_t)len);
            break;
        default:
            PyErr_BadInternalCall();
            break;
    }
    return res;
}


static int
__pack_sequence__(PyObject *msg, uint8_t type, Py_ssize_t len, PyObject **items)
{
    Py_ssize_t i;
    int res = -1;

    if (!Py_EnterRecursiveCall(" while packing a sequence")) {
        if (!__pack_len__(msg, type, len)) {
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


static int
__pack_dict__(PyObject *msg, uint8_t type, Py_ssize_t len, PyObject *obj)
{
    Py_ssize_t pos = 0;
    PyObject *key = NULL, *value = NULL;
    int res = -1;

    if (!Py_EnterRecursiveCall(" while packing a dict")) {
        if (!__pack_len__(msg, type, len)) {
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


static int
__pack_set__(PyObject *msg, uint8_t type, Py_ssize_t len, PyObject *obj)
{
    Py_ssize_t pos = 0;
    PyObject *item = NULL;
    Py_hash_t hash;
    int res = -1;

    if (!Py_EnterRecursiveCall(" while packing a set")) {
        if (!__pack_len__(msg, type, len)) {
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


#define __pack_bytes__(m, t, l, b) \
    (__pack_len__((m), (t), (l)) ? -1 : __pack_buf((m), (l), (b)))


#define __pack_ext__(m, t, l, _t, b) \
    (__pack_len__((m), (t), (l)) ? -1 : __pack_data((m), (_t), (l), (b)))


/* types helpers ------------------------------------------------------------ */

static int
_pack_sequence(PyObject *msg, PyObject *obj, PyObject **items)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = Py_SIZE(obj);

    if ((type = __pack_array_type(obj, len)) == _MSGPACK_INVALID) {
        return -1;
    }
    return __pack_sequence__(msg, type, len, items);
}


/* types -------------------------------------------------------------------- */

#define PyNone_Pack(msg) \
    __pack_type((msg), _MSGPACK_NIL)


#define PyFalse_Pack(msg) \
    __pack_type((msg), _MSGPACK_FALSE)


#define PyTrue_Pack(msg) \
    __pack_type((msg), _MSGPACK_TRUE)


static int
PyLong_Pack(PyObject *msg, PyObject *obj)
{
    int overflow = 0;
    int64_t value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if ((value == -1) && PyErr_Occurred()) {
        return -1;
    }
    if (overflow) {
        if (overflow < 0) {
            PyErr_SetString(PyExc_OverflowError, "int too big to convert");
            return -1;
        }
        uint64_t uvalue = PyLong_AsUnsignedLongLong(obj);
        if ((uvalue == (uint64_t)-1) && PyErr_Occurred()) {
            return -1;
        }
        return __pack_64(msg, _MSGPACK_UINT64, uvalue);
    }
    return __pack_long__(msg, value);
}


#define PyFloat_Pack(msg, obj) \
    __pack_double((msg), PyFloat_AS_DOUBLE((obj)))


static int
PyBytes_Pack(PyObject *msg, PyObject *obj)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = Py_SIZE(obj);

    if ((type = __pack_bin_type(obj, len)) == _MSGPACK_INVALID) {
        return -1;
    }
    return __pack_bytes__(msg, type, len, PyBytes_AS_STRING(obj));
}


static int
PyUnicode_Pack(PyObject *msg, PyObject *obj)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len;
    char *buf = NULL;

    if (!(buf = PyUnicode_AsUTF8AndSize(obj, &len)) ||
        ((type = __pack_str_type(obj, len)) == _MSGPACK_INVALID)) {
        return -1;
    }
    return __pack_bytes__(msg, type, len, buf);
}


#define PyTuple_Pack(msg, obj) \
    _pack_sequence((msg), (obj), _PyTuple_ITEMS((obj)))


static int
PyDict_Pack(PyObject *msg, PyObject *obj)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = _PyDict_GET_SIZE(obj);

    if ((type = __pack_map_type(obj, len)) == _MSGPACK_INVALID) {
        return -1;
    }
    return __pack_dict__(msg, type, len, obj);
}


/* extensions helpers ------------------------------------------------------- */

static int
_pack_ext(PyObject *msg, uint8_t _type, PyObject *data)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = Py_SIZE(data);

    if ((type = __pack_ext_type(data, len)) == _MSGPACK_INVALID) {
        return -1;
    }
    return __pack_ext__(msg, type, len, _type, PyByteArray_AS_STRING(data));
}


static int
_pack_timestamp(PyObject *msg, uint64_t seconds, uint32_t nanoseconds)
{
    uint64_t value = 0;

    if ((seconds >> 34) == 0) {
        value = (((uint64_t)nanoseconds << 34) | seconds);
        if ((value & 0xffffffff00000000L) == 0) {
            return __pack_u32(msg, (uint32_t)value);
        }
        return __pack_u64(msg, value);
    }
    return (__pack_u32(msg, nanoseconds) || __pack_u64(msg, seconds)); //XXX
}


static int
_pack_anyset(PyObject *msg, PyObject *obj, uint8_t _type)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = PySet_GET_SIZE(obj);
    PyObject *data = NULL;
    int res = -1;

    if (((type = __pack_array_type(obj, len)) != _MSGPACK_INVALID) &&
        (data = PyByteArray_FromStringAndSize(NULL, 0))) {
        if (!__pack_set__(data, type, len, obj)) {
            res = _pack_ext(msg, _type, data);
        }
        Py_DECREF(data);
    }
    return res;
}


/* extensions --------------------------------------------------------------- */

static int
Timestamp_Pack(PyObject *msg, PyObject *obj)
{
    Timestamp *ts = (Timestamp *)obj;
    PyObject *data = NULL;
    int res = -1;

    if ((data = PyByteArray_FromStringAndSize(NULL, 0))) {
        if (!_pack_timestamp(data, (uint64_t)ts->seconds, ts->nanoseconds)) {
            res = _pack_ext(msg, _MSGPACK_EXT_TIMESTAMP, data);
        }
        Py_DECREF(data);
    }
    return res;
}


static int
PyComplex_Pack(PyObject *msg, PyObject *obj)
{
    Py_complex complex = ((PyComplexObject *)obj)->cval;
    PyObject *data = NULL;
    int res = -1;

    if ((data = PyByteArray_FromStringAndSize(NULL, 0))) {
        if (!__pack_double(data, complex.real) &&
            !__pack_double(data, complex.imag)) {
            res = _pack_ext(msg, _MSGPACK_PYEXT_COMPLEX, data);
        }
        Py_DECREF(data);
    }
    return res;
}


#define PyByteArray_Pack(msg, obj) \
    _pack_ext((msg), _MSGPACK_PYEXT_BYTEARRAY, (obj))


static int
PyList_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = PyByteArray_FromStringAndSize(NULL, 0))) {
        if (!_pack_sequence(data, obj, _PyList_ITEMS(obj))) {
            res = _pack_ext(msg, _MSGPACK_PYEXT_LIST, data);
        }
        Py_DECREF(data);
    }
    return res;
}


#define PySet_Pack(msg, obj) \
    _pack_anyset((msg), (obj), _MSGPACK_PYEXT_SET)


#define PyFrozenSet_Pack(msg, obj) \
    _pack_anyset((msg), (obj), _MSGPACK_PYEXT_FROZENSET)


static int
PyType_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = _pack_class(obj))) {
        res = _pack_ext(msg, _MSGPACK_PYEXT_CLASS, data);
        Py_DECREF(data);
    }
    return res;
}


static int
PyObject_Pack(PyObject *msg, PyObject *obj)
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
                res = _pack_ext(msg, _type, data);
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


/* pack --------------------------------------------------------------------- */

static int
_pack_obj(PyObject *msg, PyObject *obj)
{
    PyTypeObject *type = Py_TYPE(obj);
    int res = -1;

    if (obj == Py_None) {
        res = PyNone_Pack(msg);
    }
    else if (obj == Py_False) {
        res = PyFalse_Pack(msg);
    }
    else if (obj == Py_True) {
        res = PyTrue_Pack(msg);
    }
    else if (type == &PyLong_Type) {
        res = PyLong_Pack(msg, obj);
    }
    else if (type == &PyFloat_Type) {
        res = PyFloat_Pack(msg, obj);
    }
    else if (type == &PyBytes_Type) {
        res = PyBytes_Pack(msg, obj);
    }
    else if (type == &PyUnicode_Type) {
        res = PyUnicode_Pack(msg, obj);
    }
    else if (type == &PyTuple_Type) {
        res = PyTuple_Pack(msg, obj);
    }
    else if (type == &PyDict_Type) {
        res = PyDict_Pack(msg, obj);
    }
    else if (type == &Timestamp_Type) {
        res = Timestamp_Pack(msg, obj);
    }
    else if (type == &PyComplex_Type) {
        res = PyComplex_Pack(msg, obj);
    }
    else if (type == &PyByteArray_Type) {
        res = PyByteArray_Pack(msg, obj);
    }
    else if (type == &PyList_Type) {
        res = PyList_Pack(msg, obj);
    }
    else if (type == &PySet_Type) {
        res = PySet_Pack(msg, obj);
    }
    else if (type == &PyFrozenSet_Type) {
        res = PyFrozenSet_Pack(msg, obj);
    }
    else if (type == &PyType_Type) {
        res = PyType_Pack(msg, obj);
    }
    else {
        res = PyObject_Pack(msg, obj);
    }
    return res;
}


static PyObject *
_pack(PyObject *obj)
{
    PyObject *msg = NULL;

    if ((msg = PyByteArray_FromStringAndSize(NULL, 0)) &&
        _pack_obj(msg, obj)) {
        Py_CLEAR(msg);
    }
    return msg;
}


/* ----------------------------------------------------------------------------
 unpack
 ---------------------------------------------------------------------------- */

#define __unpack_value__(s, b) be##s##toh((*(uint##s##_t *)(b)))

#define __unpack_u8(b) (*(uint8_t *)(b))

#define __unpack_u16(b) __unpack_value__(16, (b))

#define __unpack_u32(b) __unpack_value__(32, (b))

#define __unpack_u64(b) __unpack_value__(64, (b))

static inline double
__unpack_float(const char *buf)
{
    float_value fval = { .u32 = __unpack_u32(buf) };

    return fval.f;
}

static inline double
__unpack_double(const char *buf)
{
    double_value dval = { .u64 = __unpack_u64(buf) };

    return dval.d;
}


/* -------------------------------------------------------------------------- */

static int64_t
__unpack_signed__(const char *buf, Py_ssize_t size)
{
    int64_t value = -1;

    switch (size) {
        case 1:
            value = (int8_t)__unpack_u8(buf);
            break;
        case 2:
            value = (int16_t)__unpack_u16(buf);
            break;
        case 4:
            value = (int32_t)__unpack_u32(buf);
            break;
        case 8:
            value = (int64_t)__unpack_u64(buf);
            break;
        default:
            PyErr_BadInternalCall();
            break;
    }
    return value;
}


static uint64_t
__unpack_unsigned__(const char *buf, Py_ssize_t size)
{
    uint64_t value = (uint64_t)-1;

    switch (size) {
        case 1:
            value = __unpack_u8(buf);
            break;
        case 2:
            value = __unpack_u16(buf);
            break;
        case 4:
            value = __unpack_u32(buf);
            break;
        case 8:
            value = __unpack_u64(buf);
            break;
        default:
            PyErr_BadInternalCall();
            break;
    }
    return value;
}


static double
__unpack_float__(const char *buf, Py_ssize_t size)
{
    double value = -1.0;

    if (size == 4) {
        value = __unpack_float(buf);
    }
    else if (size == 8) {
        value = __unpack_double(buf);
    }
    else {
        PyErr_BadInternalCall();
    }
    return value;
}


/* -------------------------------------------------------------------------- */

static Py_ssize_t
__unpack_offset(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t _off = *off, noff = _off + size;

    if (noff > msg->len) {
        PyErr_SetString(PyExc_EOFError, "Ran out of input");
        return -1;
    }
    *off = noff;
    return _off;
}


static uint8_t
__unpack_type(Py_buffer *msg, Py_ssize_t *off)
{
    Py_ssize_t _off = -1;
    uint8_t type = _MSGPACK_INVALID;

    if ((_off = __unpack_offset(msg, 1, off)) >= 0) {
        if ((type = ((uint8_t *)((msg->buf) + _off))[0]) == _MSGPACK_INVALID) {
            PyErr_Format(PyExc_TypeError,
                         "type '0x%02x' is invalid", _MSGPACK_INVALID);
        }
    }
    return type;
}


static const char *
__unpack_buf(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t _off = -1;
    const char *buf = NULL;

    if ((_off = __unpack_offset(msg, size, off)) >= 0) {
        buf = ((msg->buf) + _off);
    }
    return buf;
}


/* -------------------------------------------------------------------------- */

static Py_ssize_t
__unpack_len(const char *buf, Py_ssize_t size)
{
    Py_ssize_t len = -1;

    switch (size) {
        case 1:
            len = __unpack_u8(buf);
            break;
        case 2:
            len = __unpack_u16(buf);
            break;
        case 4:
            len = __unpack_u32(buf);
            break;
        default:
            PyErr_BadInternalCall();
            break;
    }
    return len;
}


static Py_ssize_t
_unpack_len(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    const char *buf = NULL;
    Py_ssize_t len = -1;

    if ((buf = __unpack_buf(msg, size, off))) {
        len = __unpack_len(buf, size);
    }
    return len;
}


static Py_ssize_t
_unpack_array_len(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = -1;

    if ((type = __unpack_type(msg, off)) != _MSGPACK_INVALID) {
        if ((_MSGPACK_FIXARRAY <= type) && (type <= _MSGPACK_FIXARRAYEND)) {
            len = (type & _MSGPACK_FIXOBJ_BIT);
        }
        else if (type == _MSGPACK_ARRAY16) {
            len = _unpack_len(msg, 2, off);
        }
        else if (type == _MSGPACK_ARRAY32) {
            len = _unpack_len(msg, 4, off);
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "cannot unpack, invalid array type: '0x%02x'", type);
        }
    }
    return len;
}


static PyObject *
_unpack_registered(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    const char *buf = NULL;
    msgpack_state *state = NULL;
    PyObject *result = NULL, *key = NULL;

    if ((buf = __unpack_buf(msg, size, off)) &&
        (state = msgpack_getstate()) &&
        (key = PyBytes_FromStringAndSize(buf, size))) {
        if ((result = PyDict_GetItem(state->registry, key))) { // borrowed
            Py_INCREF(result);
        }
        Py_DECREF(key);
    }
    return result;
}


/* type helpers ------------------------------------------------------------- */

/* PyLong */
static PyObject *
_PyLong_FromSigned(const char *buf, Py_ssize_t size)
{
    int64_t value = __unpack_signed__(buf, size);

    if ((value == -1) && PyErr_Occurred()) {
        return NULL;
    }
    return PyLong_FromLongLong(value);
}

static PyObject *
_PyLong_FromUnsigned(const char *buf, Py_ssize_t size)
{
    uint64_t value = __unpack_unsigned__(buf, size);

    if ((value == (uint64_t)-1) && PyErr_Occurred()) {
        return NULL;
    }
    return PyLong_FromUnsignedLongLong(value);
}

#define PyLong_FromStringAndSize(b, s, is) \
    ((is) ? _PyLong_FromSigned((b), (s)) : _PyLong_FromUnsigned((b), (s)))


/* PyFloat */
static PyObject *
PyFloat_FromStringAndSize(const char *buf, Py_ssize_t size)
{
    double value = __unpack_float__(buf, size);

    if ((value == -1.0) && PyErr_Occurred()) {
        return NULL;
    }
    return PyFloat_FromDouble(value);
}


/* PySequence */
static PyObject *
PySequence_FromBufferAndSize(Py_buffer *msg, Py_ssize_t len, Py_ssize_t *off,
                             int list)
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
PySet_FromBufferAndSize(Py_buffer *msg, Py_ssize_t len, Py_ssize_t *off,
                        int frozen)
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


/* types -------------------------------------------------------------------- */

#define __PyObject_FromBufferAndSize(t, m, s, o, ...) \
    do { \
        const char *b = NULL; \
        if (!(b = __unpack_buf((m), (s), (o)))) { \
            return NULL; \
        } \
        return t##_FromStringAndSize(b, (s), ##__VA_ARGS__); \
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
    return PySequence_FromBufferAndSize((m), (s), (o), 0)

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


/* extensions --------------------------------------------------------------- */

/* _MSGPACK_EXT_TIMESTAMP */
static PyObject *
Timestamp_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;
    const char *buf = NULL;
    uint32_t nanoseconds = 0;
    int64_t seconds = 0;
    uint64_t value = 0;

    if ((buf = __unpack_buf(msg, size, off))) {
        if (size == 4) {
            seconds = (int64_t)__unpack_u32(buf);
        }
        else if (size == 8) {
            value = __unpack_u64(buf);
            nanoseconds = (uint32_t)(value >> 34);
            seconds = (int64_t)(value & 0x00000003ffffffffLL);
        }
        else if (size == 12) {
            nanoseconds = __unpack_u32(buf);
            seconds = (int64_t)__unpack_u64(buf + 4);
        }
        else {
            return PyErr_Format(PyExc_TypeError,
                                "cannot unpack, invalid timestamp size: %zd",
                                size);
        }
        result = _Timestamp_New(&Timestamp_Type, seconds, nanoseconds);
    }
    return result;
}


/* _MSGPACK_PYEXT_COMPLEX */
static double
_unpack_complex_member(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buf = NULL;
    double result = -1.0;

    if ((type = __unpack_type(msg, off)) != _MSGPACK_INVALID) {
        if (type == _MSGPACK_FLOAT64) {
            if ((buf = __unpack_buf(msg, 8, off))) {
                result = __unpack_double(buf);
            }
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "cannot unpack, invalid complex member type: '0x%02x'",
                         type);
        }
    }
    return result;
}

static PyObject *
PyComplex_FromBuffer(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *result = NULL;
    Py_complex complex;

    complex.real = _unpack_complex_member(msg, off);
    complex.imag = _unpack_complex_member(msg, off);
    if (!PyErr_Occurred()) {
        result = PyComplex_FromCComplex(complex);
    }
    return result;
}


/* _MSGPACK_PYEXT_BYTEARRAY */
#define _PyByteArray_FromBufferAndSize(m, s, o) \
    __PyObject_FromBufferAndSize(PyByteArray, (m), (s), (o))


/* _MSGPACK_PYEXT_LIST, _MSGPACK_PYEXT_SET, _MSGPACK_PYEXT_FROZENSET */
#define _PyList_FromBufferAndSize(m, s, o) \
    return PySequence_FromBufferAndSize((m), (s), (o), 1)

#define _PySet_FromBufferAndSize(m, s, o) \
    return PySet_FromBufferAndSize((m), (s), (o), 0)

#define _PyFrozenSet_FromBufferAndSize(m, s, o) \
    return PySet_FromBufferAndSize((m), (s), (o), 1)

#define __PySequence_FromBuffer(t, m, o) \
    do { \
        Py_ssize_t l = -1; \
        if ((l = _unpack_array_len((m), (o))) < 0) { \
            return NULL; \
        } \
        _##t##_FromBufferAndSize((m), l, (o)); \
    } while (0)

#define _PyList_FromBuffer(m, o) \
    __PySequence_FromBuffer(PyList, (m), (o))

#define _PySet_FromBuffer(m, o) \
    __PySequence_FromBuffer(PySet, (m), (o))

#define _PyFrozenSet_FromBuffer(m, o) \
    __PySequence_FromBuffer(PyFrozenSet, (m), (o))


/* _MSGPACK_PYEXT_CLASS */
static void
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
    Py_ssize_t _off = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = _unpack_registered(msg, size, off))) {
        __PyClass_ErrFromBuffer(msg, &_off);
    }
    return result;
}


/* _MSGPACK_PYEXT_SINGLETON */
static void
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
    Py_ssize_t _off = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = _unpack_registered(msg, size, off))) {
        __PySingleton_ErrFromBuffer(msg, &_off);
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

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId_extend, arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return _PySequence_InPlaceConcatOrAdd(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
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

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId_update, arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return _PyObject_Merge(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
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
    Py_ssize_t _off = -1;
    PyObject *result = NULL, *reduce = NULL;

    if (((_off = __unpack_offset(msg, size, off)) >= 0) &&
        (reduce = _unpack_msg(msg, &_off))) {
        result = _PyInstance_New(reduce);
        Py_DECREF(reduce);
    }
    return result;
}


static PyObject *
PyExtension_FromBufferAndSize(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;

    if ((type = __unpack_type(msg, off)) == _MSGPACK_INVALID) {
        return NULL;
    }
    switch (type) {
        case _MSGPACK_EXT_TIMESTAMP:
            return Timestamp_FromBufferAndSize(msg, size, off);
        case _MSGPACK_PYEXT_COMPLEX:
            return PyComplex_FromBuffer(msg, off);
        case _MSGPACK_PYEXT_BYTEARRAY:
            _PyByteArray_FromBufferAndSize(msg, size, off);
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
        default:
            return PyErr_Format(PyExc_TypeError,
                                "cannot unpack, unknown extension type: '0x%02x'",
                                type);
    }
}

#define _PyExtension_FromBufferAndSize(m, s, o) \
    return PyExtension_FromBufferAndSize((m), (s), (o))

#define _PyExtension_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyExtension, (m), (s), (o))


/* unpack ------------------------------------------------------------------- */

static PyObject *
_unpack_msg(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;

    if ((type = __unpack_type(msg, off)) == _MSGPACK_INVALID) {
        return NULL;
    }
    if ((_MSGPACK_FIXPINT <= type) && (type <= _MSGPACK_FIXPINTEND)) {
        return PyLong_FromLong(type);
    }
    if ((_MSGPACK_FIXNINT <= type) && (type <= _MSGPACK_FIXNINTEND)) {
        return PyLong_FromLong((int8_t)type);
    }
    if ((_MSGPACK_FIXSTR <= type) && (type <= _MSGPACK_FIXSTREND)) {
        _PyUnicode_FromBufferAndSize(msg, (type & _MSGPACK_FIXSTR_BIT), off);
    }
    if ((_MSGPACK_FIXARRAY <= type) && (type <= _MSGPACK_FIXARRAYEND)) {
        _PyTuple_FromBufferAndSize(msg, (type & _MSGPACK_FIXOBJ_BIT), off);
    }
    if ((_MSGPACK_FIXMAP <= type) && (type <= _MSGPACK_FIXMAPEND)) {
        _PyDict_FromBufferAndSize(msg, (type & _MSGPACK_FIXOBJ_BIT), off);
    }
    switch (type) {
        case _MSGPACK_NIL:
            Py_RETURN_NONE;
        case _MSGPACK_FALSE:
            Py_RETURN_FALSE;
        case _MSGPACK_TRUE:
            Py_RETURN_TRUE;
        case _MSGPACK_UINT8:
            _PyLong_FromBufferAndSize(msg, 1, off, 0);
        case _MSGPACK_UINT16:
            _PyLong_FromBufferAndSize(msg, 2, off, 0);
        case _MSGPACK_UINT32:
            _PyLong_FromBufferAndSize(msg, 4, off, 0);
        case _MSGPACK_UINT64:
            _PyLong_FromBufferAndSize(msg, 8, off, 0);
        case _MSGPACK_INT8:
            _PyLong_FromBufferAndSize(msg, 1, off, 1);
        case _MSGPACK_INT16:
            _PyLong_FromBufferAndSize(msg, 2, off, 1);
        case _MSGPACK_INT32:
            _PyLong_FromBufferAndSize(msg, 4, off, 1);
        case _MSGPACK_INT64:
            _PyLong_FromBufferAndSize(msg, 8, off, 1);
        case _MSGPACK_FLOAT32:
            _PyFloat_FromBufferAndSize(msg, 4, off);
        case _MSGPACK_FLOAT64:
            _PyFloat_FromBufferAndSize(msg, 8, off);
        case _MSGPACK_BIN8:
            _PyBytes_FromBuffer(msg, 1, off);
        case _MSGPACK_BIN16:
            _PyBytes_FromBuffer(msg, 2, off);
        case _MSGPACK_BIN32:
            _PyBytes_FromBuffer(msg, 4, off);
        case _MSGPACK_STR8:
            _PyUnicode_FromBuffer(msg, 1, off);
        case _MSGPACK_STR16:
            _PyUnicode_FromBuffer(msg, 2, off);
        case _MSGPACK_STR32:
            _PyUnicode_FromBuffer(msg, 4, off);
        case _MSGPACK_ARRAY16:
            _PyTuple_FromBuffer(msg, 2, off);
        case _MSGPACK_ARRAY32:
            _PyTuple_FromBuffer(msg, 4, off);
        case _MSGPACK_MAP16:
            _PyDict_FromBuffer(msg, 2, off);
        case _MSGPACK_MAP32:
            _PyDict_FromBuffer(msg, 4, off);
        case _MSGPACK_EXT8:
            _PyExtension_FromBuffer(msg, 1, off);
        case _MSGPACK_EXT16:
            _PyExtension_FromBuffer(msg, 2, off);
        case _MSGPACK_EXT32:
            _PyExtension_FromBuffer(msg, 4, off);
        case _MSGPACK_FIXEXT1:
            _PyExtension_FromBufferAndSize(msg, 1, off);
        case _MSGPACK_FIXEXT2:
            _PyExtension_FromBufferAndSize(msg, 2, off);
        case _MSGPACK_FIXEXT4:
            _PyExtension_FromBufferAndSize(msg, 4, off);
        case _MSGPACK_FIXEXT8:
            _PyExtension_FromBufferAndSize(msg, 8, off);
        case _MSGPACK_FIXEXT16:
            _PyExtension_FromBufferAndSize(msg, 16, off);
        default:
            return PyErr_Format(PyExc_TypeError,
                                "cannot unpack, unknown type: '0x%02x'", type);
    }
}


/* ----------------------------------------------------------------------------
 ipc packing helpers
 ---------------------------------------------------------------------------- */

static inline uint8_t
__pack_ipc_type(PyObject *obj, Py_ssize_t len)
{
    uint8_t type = _MSGPACK_INVALID;

    if (len < _MSGPACK_UINT8_MAX) {
        type = _MSGPACK_UINT8;
    }
    else if (len < _MSGPACK_UINT16_MAX) {
        type = _MSGPACK_UINT16;
    }
    else if (len < _MSGPACK_UINT32_MAX) {
        type = _MSGPACK_UINT32;
    }
    else {
        _PyErr_TooLong(obj);
    }
    return type;
}


static inline Py_ssize_t
__pack_ipc_max(uint8_t type)
{
    Py_ssize_t max = 0;

    if (type == _MSGPACK_UINT8) {
        max = _MSGPACK_UINT8_MAX;
    }
    else if (type == _MSGPACK_UINT16) {
        max = _MSGPACK_UINT16_MAX;
    }
    else if (type == _MSGPACK_UINT32) {
        max = _MSGPACK_UINT32_MAX;
    }
    else {
        PyErr_Format(PyExc_TypeError, "invalid ipc type: '0x%02x'", type);
    }
    return max;
}


#define __pack_ipc__(m, t, l, b) \
    (__pack_len__((m), (t), (l)) ? -1 : __pack__((m), (l), (b)))


static PyObject *
__pack_ipc(uint8_t type, Py_ssize_t len, PyObject *msg)
{
    PyObject *result = NULL;

    if ((result = PyByteArray_FromStringAndSize(NULL, 0)) &&
        __pack_ipc__(result, type, len, PyByteArray_AS_STRING(msg))) {
        Py_CLEAR(result);
    }
    return result;
}


static PyObject *
__pack_ipc_any__(uint8_t type, Py_ssize_t max, PyObject *obj)
{
    PyObject *result = NULL, *msg = NULL;
    Py_ssize_t len = 0;

    if ((msg = _pack(obj))) {
        if ((len = Py_SIZE(msg)) < max) {
            result = __pack_ipc(type, len, msg);
        }
        else {
            PyErr_Format(PyExc_OverflowError,
                         "msg too long for ipc type: '0x%02x'", type);
        }
        Py_DECREF(msg);
    }
    return result;
}


#define __pack_ipc_any(t, o) \
    __pack_ipc_any__((t), t##_MAX, (o))


static PyObject *
_pack_ipc_any(uint8_t type, PyObject *obj)
{
    Py_ssize_t max = __pack_ipc_max(type);

    return max ? __pack_ipc_any__(type, max, obj) : NULL;
}


static PyObject *
_pack_ipc_obj(PyObject *obj)
{
    PyObject *result = NULL, *msg = NULL;
    uint8_t type = _MSGPACK_INVALID;
    Py_ssize_t len = 0;

    if ((msg = _pack(obj))) {
        len = Py_SIZE(msg);
        if ((type = __pack_ipc_type(obj, len)) != _MSGPACK_INVALID) {
            result = __pack_ipc(type, len, msg);
        }
        Py_DECREF(msg);
    }
    return result;
}


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
    return _pack(obj);
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
    if ((type == _MSGPACK_UINT8) ||
        (type == _MSGPACK_UINT16) ||
        (type == _MSGPACK_UINT32)){
        return PyLong_FromSsize_t(_type_size(type) + 1);
    }
    return PyErr_Format(PyExc_TypeError,
                        "invalid ipc type: '0x%02x'", type);
}


/* msgpack.ipc_pack() */
PyDoc_STRVAR(msgpack_ipc_pack_doc,
"ipc_pack(obj[, type]) -> msg");

static PyObject *
msgpack_ipc_pack(PyObject *module, PyObject *args)
{
    PyObject *result = NULL, *obj = NULL;
    uint8_t type = _MSGPACK_INVALID;

    if (PyArg_ParseTuple(args, "O|b:ipc_pack", &obj, &type)) {
        if (type != _MSGPACK_INVALID) {
            result = _pack_ipc_any(type, obj);
        }
        else {
            result = _pack_ipc_obj(obj);
        }
    }
    return result;
}


/* msgpack.ipc_pack8() */
PyDoc_STRVAR(msgpack_ipc_pack8_doc,
"ipc_pack8(obj) -> msg");

static PyObject *
msgpack_ipc_pack8(PyObject *module, PyObject *obj)
{
    return __pack_ipc_any(_MSGPACK_UINT8, obj);
}


/* msgpack.ipc_pack16() */
PyDoc_STRVAR(msgpack_ipc_pack16_doc,
"ipc_pack16(obj) -> msg");

static PyObject *
msgpack_ipc_pack16(PyObject *module, PyObject *obj)
{
    return __pack_ipc_any(_MSGPACK_UINT16, obj);
}


/* msgpack.ipc_pack32() */
PyDoc_STRVAR(msgpack_ipc_pack32_doc,
"ipc_pack32(obj) -> msg");

static PyObject *
msgpack_ipc_pack32(PyObject *module, PyObject *obj)
{
    return __pack_ipc_any(_MSGPACK_UINT32, obj);
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
              _PyModule_AddType(module, "Timestamp", &Timestamp_Type) ||
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
