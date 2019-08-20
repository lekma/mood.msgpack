/*
#
# Copyright © 2019 Malek Hadj-Ali
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
#endif /* HAVE_ENDIAN_H */


/* for float conversion */
typedef union {
    float f;
    uint32_t i;
} float32_t;

typedef union {
    double f;
    uint64_t i;
} float64_t;


/* forward declarations */
static PyModuleDef msgpack_def;
static PyTypeObject Timestamp_Type;

static int __register_obj(PyObject *registry, PyObject *obj);
static int __pack_obj(PyObject *msg, PyObject *obj);
static PyObject *__unpack_msg(Py_buffer *msg, Py_ssize_t *off);


/* --------------------------------------------------------------------------
   MessagePack definitions (these should probably go in a header)
   see https://github.com/msgpack/msgpack/blob/master/spec.md
   -------------------------------------------------------------------------- */

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
    _MSGPACK_FIXUINT     = 0x00,     //   0
    _MSGPACK_FIXUINTEND  = 0x7f,     // 127
    _MSGPACK_FIXMAP      = 0x80,
    _MSGPACK_FIXMAPEND   = 0x8f,
    _MSGPACK_FIXARRAY    = 0x90,
    _MSGPACK_FIXARRAYEND = 0x9f,
    _MSGPACK_FIXSTR      = 0xa0,
    _MSGPACK_FIXSTREND   = 0xbf,

    _MSGPACK_NIL     = 0xc0,
    _MSGPACK_INVALID = 0xc1,         // invalid type
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

    _MSGPACK_FIXINT    = 0xe0,       // -32
    _MSGPACK_FIXINTEND = 0xff        //  -1
};


/* MessagePack extensions */
enum {
    _MSGPACK_EXT_TIMESTAMP = 0xff    //  -1
};


/* Python MessagePack extensions */
enum {
    _MSGPACK_PYEXT_INVALID = 0x00,   // invalid extension

    _MSGPACK_PYEXT_COMPLEX,
    _MSGPACK_PYEXT_BYTEARRAY,

    _MSGPACK_PYEXT_LIST,
    _MSGPACK_PYEXT_SET,
    _MSGPACK_PYEXT_FROZENSET,

    _MSGPACK_PYEXT_CLASS,
    _MSGPACK_PYEXT_SINGLETON,

    _MSGPACK_PYEXT_INSTANCE = 0x7f   // last
};


/* --------------------------------------------------------------------------
   module state
   -------------------------------------------------------------------------- */

typedef struct {
    PyObject *registry;
} msgpack_state;

#define msgpack_getstate() \
    (msgpack_state *)_PyModuleDef_GetState(&msgpack_def)


static int
msgpack_init_state(PyObject *module)
{
    msgpack_state *state = NULL;

    if (
        !(state = _PyModule_GetState(module)) ||
        !(state->registry = PyDict_New()) ||
        __register_obj(state->registry, Py_NotImplemented) ||
        __register_obj(state->registry, Py_Ellipsis)
       ) {
        return -1;
    }
    return 0;
}


/* --------------------------------------------------------------------------
   Timestamp
   -------------------------------------------------------------------------- */

static int
__cmp(int64_t a, int64_t b)
{
    return (a > b) - (a < b);
}

#define __sig(x) __cmp(x, 0)

#define __round(d) trunc((d < 0) ? (d - .5) : (d + .5))

#define _MSGPACK_NS_MAX 1e9


typedef struct {
    PyObject_HEAD
    int64_t seconds;
    uint32_t nanoseconds;
} Timestamp;


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
    seconds = (int64_t)__round(intpart);
    diff = intpart - (double)seconds;
    if (((intpart < LLONG_MIN) || (LLONG_MAX < intpart)) ||
        ((diff <= -1.0) || (1.0 <= diff))) {
        PyErr_SetString(PyExc_OverflowError,
                        "timestamp out of range for platform");
        return NULL;
    }
    return _Timestamp_New(type, seconds, (uint32_t)__round(fracpart));
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


static PyObject *
_Timestamp_Compare(Timestamp *self, Timestamp *other, int op)
{
    int cmp = 0, res = -1;

    if (!(cmp = __cmp(self->seconds, other->seconds))) {
        cmp = __cmp((__sig(self->seconds) * (int64_t)self->nanoseconds),
                    (__sig(other->seconds) * (int64_t)other->nanoseconds));
    }
    switch (op) {
        case Py_EQ:
            res = (cmp == 0);
            break;
        case Py_NE:
            res = (cmp != 0);
            break;
        case Py_GT:
            res = (cmp == 1);
            break;
        case Py_LE:
            res = (cmp != 1);
            break;
        case Py_LT:
            res = (cmp == -1);
            break;
        case Py_GE:
            res = (cmp != -1);
            break;
        default:
            PyErr_BadArgument();
            return NULL;
    }
    return PyBool_FromLong(res);
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


/* Timestamp_Type.tp_richcompare */
static PyObject *
Timestamp_tp_richcompare(Timestamp *self, PyObject *other, int op)
{
    if (Py_TYPE(other) == &Timestamp_Type) {
        return _Timestamp_Compare(self, (Timestamp *)other, op);
    }
    Py_RETURN_NOTIMPLEMENTED;
}


/* Timestamp.fromtimestamp() */
PyDoc_STRVAR(Timestamp_fromtimestamp_doc,
"@classmethod\n\
fromtimestamp(timestamp) -> Timestamp");

static PyObject *
Timestamp_fromtimestamp(PyObject *cls, PyObject *timestamp)
{
    PyTypeObject *type = (PyTypeObject *)cls;

    if (PyFloat_Check(timestamp)) {
        return _Timestamp_FromPyFloat(type, timestamp);
    }
    if (PyLong_Check(timestamp)) {
        return _Timestamp_FromPyLong(type, timestamp);
    }
    return PyErr_Format(PyExc_TypeError,
                        "expected a 'float' or an 'int', got: '%.200s'",
                        Py_TYPE(timestamp)->tp_name);
}


/* Timestamp.timestamp() */
PyDoc_STRVAR(Timestamp_timestamp_doc,
"timestamp() -> float");

static PyObject *
Timestamp_timestamp(Timestamp *self)
{
    double seconds = (double)self->seconds;
    double nanoseconds = (self->nanoseconds / _MSGPACK_NS_MAX);

    return PyFloat_FromDouble(seconds + copysign(nanoseconds, seconds));
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
    (richcmpfunc)Timestamp_tp_richcompare,    /*tp_richcompare*/
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


/* --------------------------------------------------------------------------
   pack utils
   -------------------------------------------------------------------------- */

static PyObject *
__new_msg(void)
{
    return __PyByteArray_Alloc(_Py_MIN_ALLOC);
}


static int
__pack(PyObject *msg, Py_ssize_t size, const char *bytes)
{
    return __PyByteArray_Grow((PyByteArrayObject *)msg, size, bytes, _Py_MIN_ALLOC);
}


/* --------------------------------------------------------------------------
   register
   -------------------------------------------------------------------------- */

static PyObject *
__pack_class(PyObject *obj)
{
    _Py_IDENTIFIER(__module__);
    _Py_IDENTIFIER(__qualname__);
    PyObject *result = NULL, *module = NULL, *qualname = NULL;

    if ((module = _PyObject_GetAttrId(obj, &PyId___module__)) &&
        (qualname = _PyObject_GetAttrId(obj, &PyId___qualname__)) &&
        (result = __new_msg()) &&
        (__pack_obj(result, module) || __pack_obj(result, qualname))) {
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
        else if ((result = __new_msg()) && __pack_obj(result, reduce)) {
            Py_CLEAR(result);
        }
        Py_DECREF(reduce);
    }
    return result;
}


static int
__register_obj(PyObject *registry, PyObject *obj)
{
    PyObject *data = NULL, *key = NULL;
    int res = -1;

    if ((data = (PyType_Check(obj) ? __pack_class(obj) : __pack_singleton(obj)))) {
        if ((key = PyBytes_FromStringAndSize(PyByteArray_AS_STRING(data),
                                             Py_SIZE(data)))) {
            res = PyDict_SetItem(registry, key, obj);
            Py_DECREF(key);
        }
        Py_DECREF(data);
    }
    return res;
}


/* --------------------------------------------------------------------------
   pack
   -------------------------------------------------------------------------- */

#define __pack_value__(m, s, b) \
    __pack(m, s, (const char *)b)


static inline int
__pack_uint1__(PyObject *msg, uint8_t value)
{
    return __pack_value__(msg, 1, &value);
}

static inline int
__pack_uint2__(PyObject *msg, uint16_t value)
{
    uint16_t bev = htobe16(value);

    return __pack_value__(msg, 2, &bev);
}

static inline int
__pack_uint4__(PyObject *msg, uint32_t value)
{
    uint32_t bev = htobe32(value);

    return __pack_value__(msg, 4, &bev);
}

static inline int
__pack_uint8__(PyObject *msg, uint64_t value)
{
    uint64_t bev = htobe64(value);

    return __pack_value__(msg, 8, &bev);
}


static inline int
__pack_float4__(PyObject *msg, float value)
{
    float32_t val = { .f = value };

    return __pack_uint4__(msg, val.i);
}

static inline int
__pack_float8__(PyObject *msg, double value)
{
    float64_t val = { .f = value };

    return __pack_uint8__(msg, val.i);
}


/* -------------------------------------------------------------------------- */

#define __pack_type __pack_uint1__


#define __pack_bytes(m, s, b) \
    ((s) ? __pack(m, s, b) : 0)


#define __pack_value(m, t, s, v) \
    (__pack_type(m, t) ? -1 : __pack_uint##s##__(m, v))


#define __pack_float(m, t, s, v) \
    (__pack_type(m, t) ? -1 : __pack_float##s##__(m, v))


/* std types helpers -------------------------------------------------------- */

static inline int
__pack_int(PyObject *msg, int64_t value)
{
    int res = -1;

    if (value < _MSGPACK_INT32_MIN) {
        res = __pack_value(msg, _MSGPACK_INT64, 8, value);
    }
    else if (value < _MSGPACK_INT16_MIN) {
        res = __pack_value(msg, _MSGPACK_INT32, 4, value);
    }
    else if (value < _MSGPACK_INT8_MIN) {
        res = __pack_value(msg, _MSGPACK_INT16, 2, value);
    }
    else if (value < _MSGPACK_FIXINT_MIN) {
        res = __pack_value(msg, _MSGPACK_INT8, 1, value);
    }
    else if (value < _MSGPACK_FIXINT_MAX) { // fixint
        res = __pack_type(msg, value);
    }
    else if (value < _MSGPACK_UINT8_MAX) {
        res = __pack_value(msg, _MSGPACK_UINT8, 1, value);
    }
    else if (value < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_UINT16, 2, value);
    }
    else if (value < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_UINT32, 4, value);
    }
    else {
        res = __pack_value(msg, _MSGPACK_UINT64, 8, value);
    }
    return res;
}


#define _PyErr_TooLong(o) \
    PyErr_Format(PyExc_OverflowError, \
                 "%.200s too long to convert", Py_TYPE(o)->tp_name)


static int
__pack_bin_type(PyObject *msg, PyObject *obj, Py_ssize_t size)
{
    int res = -1;

    if (size < _MSGPACK_UINT8_MAX) {
        res = __pack_value(msg, _MSGPACK_BIN8, 1, size);
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_BIN16, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_BIN32, 4, size);
    }
    else {
        _PyErr_TooLong(obj);
    }
    return res;
}


static int
__pack_str_type(PyObject *msg, PyObject *obj, Py_ssize_t size)
{
    int res = -1;

    if (size < _MSGPACK_FIXSTR_LEN_MAX) { // fixstr
        res = __pack_type(msg, (_MSGPACK_FIXSTR | (uint8_t)size));
    }
    else if (size < _MSGPACK_UINT8_MAX) {
        res = __pack_value(msg, _MSGPACK_STR8, 1, size);
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_STR16, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_STR32, 4, size);
    }
    else {
        _PyErr_TooLong(obj);
    }
    return res;
}


static int
__pack_array_type(PyObject *msg, PyObject *obj, Py_ssize_t size)
{
    int res = -1;

    if (size < _MSGPACK_FIXOBJ_LEN_MAX) { // fixarray
        res = __pack_type(msg, (_MSGPACK_FIXARRAY | (uint8_t)size));
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_ARRAY16, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_ARRAY32, 4, size);
    }
    else {
        _PyErr_TooLong(obj);
    }
    return res;
}


static int
__pack_map_type(PyObject *msg, PyObject *obj, Py_ssize_t size)
{
    int res = -1;

    if (size < _MSGPACK_FIXOBJ_LEN_MAX) { // fixmap
        res = __pack_type(msg, (_MSGPACK_FIXMAP | (uint8_t)size));
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_MAP16, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_MAP32, 4, size);
    }
    else {
        _PyErr_TooLong(obj);
    }
    return res;
}


static int
__pack_ext_type(PyObject *msg, PyObject *obj, Py_ssize_t size)
{
    int res = -1;

    if (size < _MSGPACK_UINT8_MAX) {
        switch (size) {
            case 1:
                res = __pack_type(msg, _MSGPACK_FIXEXT1);
                break;
            case 2:
                res = __pack_type(msg, _MSGPACK_FIXEXT2);
                break;
            case 4:
                res = __pack_type(msg, _MSGPACK_FIXEXT4);
                break;
            case 8:
                res = __pack_type(msg, _MSGPACK_FIXEXT8);
                break;
            case 16:
                res = __pack_type(msg, _MSGPACK_FIXEXT16);
                break;
            default:
                res = __pack_value(msg, _MSGPACK_EXT8, 1, size);
                break;
        }
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_EXT16, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_EXT32, 4, size);
    }
    else {
        _PyErr_TooLong(obj);
    }
    return res;
}


/* -------------------------------------------------------------------------- */

static int
__pack_sequence(PyObject *msg, PyObject *obj, PyObject **items)
{
    Py_ssize_t size = Py_SIZE(obj), i;
    int res = -1;

    if (!__pack_array_type(msg, obj, size) &&
        !Py_EnterRecursiveCall(" while packing a sequence")) {
        for (res = 0, i = 0; i < size; ++i) {
            if ((res = __pack_obj(msg, items[i]))) {
                break;
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


/* std types wrappers ------------------------------------------------------- */

static inline int
_pack_long(PyObject *msg, int64_t value)
{
    if ((value == -1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_int(msg, value);
}


static inline int
_pack_ulong(PyObject *msg, uint64_t value)
{
    if ((value == (uint64_t)-1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_value(msg, _MSGPACK_UINT64, 8, value);
}


/* std types ---------------------------------------------------------------- */

#define PyNone_Pack(msg) \
    __pack_type(msg, _MSGPACK_NIL)


#define PyFalse_Pack(msg) \
    __pack_type(msg, _MSGPACK_FALSE)


#define PyTrue_Pack(msg) \
    __pack_type(msg, _MSGPACK_TRUE)


// PyLong
static int
PyLong_Pack(PyObject *msg, PyObject *obj)
{
    int overflow = 0;
    int64_t value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if (overflow) {
        if (overflow < 0) {
            PyErr_SetString(PyExc_OverflowError, "int too big to convert");
            return -1;
        }
        return _pack_ulong(msg, PyLong_AsUnsignedLongLong(obj));
    }
    return _pack_long(msg, value);
}


// PyFloat
#define PyFloat_Pack(msg, obj) \
    __pack_float(msg, _MSGPACK_FLOAT64, 8, PyFloat_AS_DOUBLE(obj))


// PyBytes
static int
PyBytes_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size = Py_SIZE(obj);

    if (__pack_bin_type(msg, obj, size)) {
        return -1;
    }
    return __pack_bytes(msg, size, PyBytes_AS_STRING(obj));
}


// PyUnicode
static int
PyUnicode_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size;
    char *buf = NULL;

    if (!(buf = PyUnicode_AsUTF8AndSize(obj, &size)) ||
        __pack_str_type(msg, obj, size)) {
        return -1;
    }
    return __pack_bytes(msg, size, buf);
}


// PyTuple
#define PyTuple_Pack(msg, obj) \
    __pack_sequence(msg, obj, _PyTuple_ITEMS(obj))


// PyDict
static int
PyDict_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size = _PyDict_GET_SIZE(obj), pos = 0;
    PyObject *key = NULL, *value = NULL;
    int res = -1;

    if (!__pack_map_type(msg, obj, size) &&
        !Py_EnterRecursiveCall(" while packing a dict")) {
        while ((res = PyDict_Next(obj, &pos, &key, &value))) {
            if ((res = __pack_obj(msg, key)) ||
                (res = __pack_obj(msg, value))) {
                break;
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


/* extension types helpers -------------------- ----------------------------- */

#define __pack_ext(m, t, s, b) \
    (__pack_type(m, t) ? -1 : __pack_bytes(m, s, b))


#define __pack_timestamp(m, n, s) \
    ((__pack_uint4__(m, n) || __pack_uint8__(m, s)) ? -1 : 0)


#define __pack_complex(m, r, i) \
    ((__pack_float8__(m, r) || __pack_float8__(m, i)) ? -1 : 0)


static int
__pack_anyset(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size = PySet_GET_SIZE(obj), pos = 0;
    PyObject *item = NULL;
    Py_hash_t hash;
    int res = -1;

    if (!__pack_array_type(msg, obj, size) &&
        !Py_EnterRecursiveCall(" while packing a set")) {
        while ((res = _PySet_NextEntry(obj, &pos, &item, &hash))) {
            if ((res = __pack_obj(msg, item))) {
                break;
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}


/* extension types wrappers ------------------- ----------------------------- */

static int
_pack_ext(PyObject *msg, uint8_t type, PyObject *data)
{
    Py_ssize_t size = Py_SIZE(data);

    if (__pack_ext_type(msg, data, size)) {
        return -1;
    }
    return __pack_ext(msg, type, size, PyByteArray_AS_STRING(data));
}


static int
_pack_timestamp(PyObject *msg, uint64_t seconds, uint32_t nanoseconds)
{
    uint64_t value = 0;

    if ((seconds >> 34) == 0) {
        value = (((uint64_t)nanoseconds << 34) | seconds);
        if ((value & 0xffffffff00000000L) == 0) {
            return __pack_uint4__(msg, (uint32_t)value);
        }
        return __pack_uint8__(msg, value);
    }
    return __pack_timestamp(msg, nanoseconds, seconds);
}


static int
_pack_anyset(PyObject *msg, uint8_t type, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        if (!__pack_anyset(data, obj)) {
            res = _pack_ext(msg, type, data);
        }
        Py_DECREF(data);
    }
    return res;
}


/* extension types ------------------------- -------------------------------- */

// mood.msgpack.Timestamp
static int
Timestamp_Pack(PyObject *msg, PyObject *obj)
{
    Timestamp *ts = (Timestamp *)obj;
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        if (!_pack_timestamp(data, (uint64_t)ts->seconds, ts->nanoseconds)) {
            res = _pack_ext(msg, _MSGPACK_EXT_TIMESTAMP, data);
        }
        Py_DECREF(data);
    }
    return res;
}


// PyComplex
static int
PyComplex_Pack(PyObject *msg, PyObject *obj)
{
    Py_complex complex = ((PyComplexObject *)obj)->cval;
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        if (!__pack_complex(data, complex.real, complex.imag)) {
            res = _pack_ext(msg, _MSGPACK_PYEXT_COMPLEX, data);
        }
        Py_DECREF(data);
    }
    return res;
}


// PyByteArray
#define PyByteArray_Pack(msg, obj) \
    _pack_ext(msg, _MSGPACK_PYEXT_BYTEARRAY, obj)


// PyList
static int
PyList_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        if (!__pack_sequence(data, obj, _PyList_ITEMS(obj))) {
            res = _pack_ext(msg, _MSGPACK_PYEXT_LIST, data);
        }
        Py_DECREF(data);
    }
    return res;
}


// PySet
#define PySet_Pack(msg, obj) \
    _pack_anyset(msg, _MSGPACK_PYEXT_SET, obj)


// PyFrozenSet
#define PyFrozenSet_Pack(msg, obj) \
    _pack_anyset(msg, _MSGPACK_PYEXT_FROZENSET, obj)


// PyType
static int
PyType_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __pack_class(obj))) {
        res = _pack_ext(msg, _MSGPACK_PYEXT_CLASS, data);
        Py_DECREF(data);
    }
    return res;
}


// instances and singletons
static int
PyObject_Pack(PyObject *msg, PyObject *obj)
{
    _Py_IDENTIFIER(__reduce__);
    PyObject *reduce = NULL, *data = NULL;
    uint8_t type = _MSGPACK_PYEXT_INVALID; // 0
    int res = -1;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if (PyTuple_Check(reduce)) {
            type = _MSGPACK_PYEXT_INSTANCE;
        }
        else if (PyUnicode_Check(reduce)) {
            type = _MSGPACK_PYEXT_SINGLETON;
        }
        else {
            PyErr_SetString(PyExc_TypeError,
                            "__reduce__() must return a string or a tuple");
        }
        if (type && (data = __new_msg())) {
            if (!__pack_obj(data, reduce)) {
                res = _pack_ext(msg, type, data);
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
__pack_obj(PyObject *msg, PyObject *obj)
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


/* --------------------------------------------------------------------------
   unpack
   -------------------------------------------------------------------------- */

#define __unpack_uint__(b, s) \
    be##s##toh((*((uint##s##_t *)b)))


#define __unpack_int__(b, s) \
    (int##s##_t)__unpack_uint__(b, s)


/* helpers ------------------------------------------------------------------ */

#define __unpack_uint1(b) (*((uint8_t *)b))
#define __unpack_uint2(b) __unpack_uint__(b, 16)
#define __unpack_uint4(b) __unpack_uint__(b, 32)
#define __unpack_uint8(b) __unpack_uint__(b, 64)


#define __unpack_int1(b) (*((int8_t *)b))
#define __unpack_int2(b) __unpack_int__(b, 16)
#define __unpack_int4(b) __unpack_int__(b, 32)
#define __unpack_int8(b) __unpack_int__(b, 64)


static inline double
__unpack_float4(const char *buffer)
{
    float32_t val = { .i = __unpack_uint__(buffer, 32) };

    return val.f;
}

static inline double
__unpack_float8(const char *buffer)
{
    float64_t val = { .i = __unpack_uint__(buffer, 64) };

    return val.f;
}


static inline const char *
__unpack_buffer(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t poff = *off, noff = poff + size;

    if (noff > msg->len) {
        PyErr_SetString(PyExc_EOFError, "Ran out of input");
        return NULL;
    }
    *off = noff;
    return ((msg->buf) + poff);
}


static inline uint8_t
__unpack_type(Py_buffer *msg, Py_ssize_t *off)
{
    const char *buffer = NULL;
    uint8_t type = _MSGPACK_INVALID;

    if ((buffer = __unpack_buffer(msg, 1, off)) &&
        ((type = __unpack_uint1(buffer)) == _MSGPACK_INVALID)) {
        PyErr_Format(PyExc_TypeError, "invalid type: '0x%02x'", type);
    }
    return type;
}


#define __unpack_len(m, s, o) \
    (((buffer = __unpack_buffer(m, s, o))) ? __unpack_uint##s(buffer) : -1)


#define __PyObject_Unpack(t, m, s, o) \
    (((buffer = __unpack_buffer(m, s, o))) ? t##_FromStringAndSize(buffer, s) : NULL)


#define __PyObject_FromBuffer(t, m, s, o) \
    (((len = __unpack_len(m, s, o)) < 0) ? NULL : t##_Unpack(m, len, o))


/* std types wrappers ------------------------------------------------------- */

// PyLong
#define PyULong_FromStringAndSize(b, s) \
    PyLong_FromUnsignedLongLong(__unpack_uint##s(b))


#define PyLong_FromStringAndSize(b, s) \
    PyLong_FromLongLong(__unpack_int##s(b))


// PyFloat
#define PyFloat_FromStringAndSize(b, s) \
    PyFloat_FromDouble(__unpack_float##s(b))


/* std types ---------------------------------------------------------------- */

// _MSGPACK_UINT
#define PyULong_Unpack(m, s, o) \
    __PyObject_Unpack(PyULong, m, s, o)


// _MSGPACK_INT
#define PyLong_Unpack(m, s, o) \
    __PyObject_Unpack(PyLong, m, s, o)


// _MSGPACK_FLOAT
#define PyFloat_Unpack(m, s, o) \
    __PyObject_Unpack(PyFloat, m, s, o)


// _MSGPACK_BIN
#define PyBytes_Unpack(m, s, o) \
    __PyObject_Unpack(PyBytes, m, s, o)

#define PyBytes_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyBytes, m, s, o)


// _MSGPACK_STR, _MSGPACK_FIXSTR
#define PyUnicode_Unpack(m, s, o) \
    __PyObject_Unpack(PyUnicode, m, s, o)

#define PyUnicode_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyUnicode, m, s, o)


// _MSGPACK_ARRAY, _MSGPACK_FIXARRAY
static PyObject *
PyTuple_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL, *item = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a tuple")) {
        if ((result = PyTuple_New(size))) {
            for (i = 0; i < size; ++i) {
                if (!(item = __unpack_msg(msg, off))) {
                    Py_CLEAR(result);
                    break;
                }
                PyTuple_SET_ITEM(result, i, item);
            }
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define PyTuple_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyTuple, m, s, o)


// _MSGPACK_MAP, _MSGPACK_FIXMAP
static PyObject *
PyDict_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL, *key = NULL, *val = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a dict")) {
        if ((result = PyDict_New())) {
            for (i = 0; i < size; ++i) {
                if (!(key = __unpack_msg(msg, off)) ||
                    !(val = __unpack_msg(msg, off)) ||
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

#define PyDict_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyDict, m, s, o)


/* extension types helpers -------------------------------------------------- */

static Py_ssize_t
__unpack_sequence_len(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;

    if ((type = __unpack_type(msg, off)) != _MSGPACK_INVALID) {
        if ((_MSGPACK_FIXARRAY <= type) && (type <= _MSGPACK_FIXARRAYEND)) {
            len = (type & _MSGPACK_FIXOBJ_BIT);
        }
        else {
            switch (type) {
                case _MSGPACK_ARRAY16:
                    len = __unpack_len(msg, 2, off);
                    break;
                case _MSGPACK_ARRAY32:
                    len = __unpack_len(msg, 4, off);
                    break;
                default:
                    PyErr_Format(PyExc_TypeError,
                                 "invalid array type: '0x%02x'", type);
                    break;
            }
        }
    }
    return len;
}

#define __PySequence_FromBuffer(t, m, o) \
    (((len = __unpack_sequence_len(m, o)) < 0) ? NULL : t##_Unpack(m, len, o))


static PyObject *
__unpack_anyset(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off, int frozen)
{
    PyObject *result = NULL, *item = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a set")) {
        if ((result = (frozen) ? PyFrozenSet_New(NULL) : PySet_New(NULL))) {
            for (i = 0; i < size; ++i) {
                if (!(item = __unpack_msg(msg, off)) ||
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


static PyObject *
__unpack_registered(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    const char *buffer = NULL;
    msgpack_state *state = NULL;
    PyObject *result = NULL, *key = NULL;

    if ((buffer = __unpack_buffer(msg, size, off)) &&
        (state = msgpack_getstate()) &&
        (key = PyBytes_FromStringAndSize(buffer, size))) {
        if ((result = PyDict_GetItem(state->registry, key))) { // borrowed
            Py_INCREF(result);
        }
        Py_DECREF(key);
    }
    return result;
}


/* extension types ---------------------------------------------------------- */

// _MSGPACK_EXT_TIMESTAMP
static PyObject *
Timestamp_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    const char *buffer = NULL;
    uint32_t nanoseconds = 0;
    int64_t seconds = 0;
    uint64_t value = 0;
    PyObject *result = NULL;

    if ((buffer = __unpack_buffer(msg, size, off))) {
        switch (size) {
            case 4:
                seconds = (int64_t)__unpack_uint4(buffer);
                break;
            case 8:
                value = __unpack_uint8(buffer);
                nanoseconds = (uint32_t)(value >> 34);
                seconds = (int64_t)(value & 0x00000003ffffffffLL);
                break;
            case 12:
                nanoseconds = __unpack_uint4(buffer);
                seconds = __unpack_int8((buffer + 4));
                break;
            default:
                return PyErr_Format(PyExc_ValueError,
                                    "invalid timestamp size: %zd", size);
        }
        result = _Timestamp_New(&Timestamp_Type, seconds, nanoseconds);
    }
    return result;
}


// _MSGPACK_PYEXT_COMPLEX
static PyObject *
PyComplex_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_complex complex;
    const char *buffer = NULL;
    PyObject *result = NULL;

    if (size != 16) {
        PyErr_Format(PyExc_ValueError,
                     "invalid complex size: %zd", size);
    }
    else if ((buffer = __unpack_buffer(msg, size, off))) {
        complex.real = __unpack_float8(buffer);
        complex.imag = __unpack_float8((buffer + 8));
        result = PyComplex_FromCComplex(complex);
    }
    return result;
}


// _MSGPACK_PYEXT_BYTEARRAY
#define PyByteArray_Unpack(m, s, o) \
    __PyObject_Unpack(PyByteArray, m, s, o)


// _MSGPACK_PYEXT_LIST
static PyObject *
PyList_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL, *item = NULL;
    Py_ssize_t i;

    if (!Py_EnterRecursiveCall(" while unpacking a list")) {
        if ((result = PyList_New(size))) {
            for (i = 0; i < size; ++i) {
                if (!(item = __unpack_msg(msg, off))) {
                    Py_CLEAR(result);
                    break;
                }
                PyList_SET_ITEM(result, i, item);
            }
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define PyList_FromBuffer(m, o) \
    __PySequence_FromBuffer(PyList, m, o)


// _MSGPACK_PYEXT_SET
#define PySet_Unpack(m, s, o) \
    __unpack_anyset(m, s, o, 0)

#define PySet_FromBuffer(m, o) \
    __PySequence_FromBuffer(PySet, m, o)


// _MSGPACK_PYEXT_FROZENSET
#define PyFrozenSet_Unpack(m, s, o) \
    __unpack_anyset(m, s, o, 1)

#define PyFrozenSet_FromBuffer(m, o) \
    __PySequence_FromBuffer(PyFrozenSet, m, o)


// _MSGPACK_PYEXT_CLASS
static void
__PyClass_ErrFromBuffer(Py_buffer *msg, Py_ssize_t *off)
{
    _Py_IDENTIFIER(builtins);
    PyObject *module = NULL, *qualname = NULL;

    if ((module = __unpack_msg(msg, off)) &&
        (qualname = __unpack_msg(msg, off))) {
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
PyClass_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, size, off))) {
        __PyClass_ErrFromBuffer(msg, &poff);
    }
    return result;
}


// _MSGPACK_PYEXT_SINGLETON
static void
__PySingleton_ErrFromBuffer(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *name = NULL;

    if ((name = __unpack_msg(msg, off))) {
        PyErr_Format(PyExc_TypeError,
                     "cannot unpack '%U'", name);
        Py_DECREF(name);
    }
}

static PyObject *
PySingleton_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, size, off))) {
        __PySingleton_ErrFromBuffer(msg, &poff);
    }
    return result;
}


// _MSGPACK_PYEXT_INSTANCE
// object.__setstate__()
static int
__PyObject_UpdateDict(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(__dict__);
    Py_ssize_t pos = 0;
    PyObject *dict = NULL, *key = NULL, *value = NULL;
    int res = -1;

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
    return res;
}

static int
__PyObject_SetState(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(__setstate__);
    PyObject *result = NULL;

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId___setstate__,
                                                 arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError) &&
            PyDict_Check(arg)) {
            PyErr_Clear();
            return __PyObject_UpdateDict(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
}


// object.extend()
static int
__PyObject_InPlaceConcatOrAdd(PyObject *self, PyObject *arg)
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
                     "cannot extend '%.200s' objects", type->tp_name);
    }
    return res;
}

static int
__PyObject_Extend(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(extend);
    PyObject *result = NULL;

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId_extend,
                                                 arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return __PyObject_InPlaceConcatOrAdd(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
}


// object.update()
static PyObject *
__PySequence_Fast(PyObject *obj, Py_ssize_t len, const char *message)
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
__PyObject_MergeFromIter(PyObject *self, PyObject *iter)
{
    PyObject *item = NULL, *fast = NULL;

    while ((item = PyIter_Next(iter))) {
        if (!(fast = __PySequence_Fast(item, 2, "not a sequence")) ||
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
__PyObject_Merge(PyObject *self, PyObject *arg)
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
        res = __PyObject_MergeFromIter(self, iter);
        Py_DECREF(iter);
    }
    return res;
}

static int
__PyObject_Update(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(update);
    PyObject *result = NULL;

    if (!(result = _PyObject_CallMethodIdObjArgs(self, &PyId_update,
                                                 arg, NULL))) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return __PyObject_Merge(self, arg);
        }
        return -1;
    }
    Py_DECREF(result);
    return 0;
}


// object.__new__()
static int
__PyCallable_Check(PyObject *arg, void *addr)
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

static PyObject *
__PyObject_New(PyObject *reduce)
{
    PyObject *callable, *args;
    PyObject *setstatearg = Py_None, *extendarg = Py_None, *updatearg = Py_None;
    PyObject *self = NULL;

    if (
        PyArg_ParseTuple(reduce, "O&O!|OOO",
                         __PyCallable_Check, &callable, &PyTuple_Type, &args,
                         &setstatearg, &extendarg, &updatearg) &&
        (self = PyObject_CallObject(callable, args)) &&
        (
         (setstatearg != Py_None && __PyObject_SetState(self, setstatearg)) ||
         (extendarg != Py_None && __PyObject_Extend(self, extendarg)) ||
         (updatearg != Py_None && __PyObject_Update(self, updatearg))
        )
       ) {
        Py_CLEAR(self);
    }
    return self;
}


static PyObject *
PyInstance_FromBuffer(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *result = NULL, *reduce = NULL;

    if ((reduce = __unpack_msg(msg, off))) {
        result = __PyObject_New(reduce);
        Py_DECREF(reduce);
    }
    return result;
}


// _MSGPACK_EXT, _MSGPACK_FIXEXT
static PyObject *
PyExtension_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;
    PyObject *result = NULL;

    if ((type = __unpack_type(msg, off)) != _MSGPACK_INVALID) {
        switch (type) {
            case _MSGPACK_EXT_TIMESTAMP:
                result = Timestamp_Unpack(msg, size, off);
                break;
            case _MSGPACK_PYEXT_INVALID:
                PyErr_Format(PyExc_TypeError,
                             "invalid extension type: '0x%02x'", type);
                break;
            case _MSGPACK_PYEXT_COMPLEX:
                result = PyComplex_Unpack(msg, size, off);
                break;
            case _MSGPACK_PYEXT_BYTEARRAY:
                result = PyByteArray_Unpack(msg, size, off);
                break;
            case _MSGPACK_PYEXT_LIST:
                result = PyList_FromBuffer(msg, off);
                break;
            case _MSGPACK_PYEXT_SET:
                result = PySet_FromBuffer(msg, off);
                break;
            case _MSGPACK_PYEXT_FROZENSET:
                result = PyFrozenSet_FromBuffer(msg, off);
                break;
            case _MSGPACK_PYEXT_CLASS:
                result = PyClass_Unpack(msg, size, off);
                break;
            case _MSGPACK_PYEXT_SINGLETON:
                result = PySingleton_Unpack(msg, size, off);
                break;
            case _MSGPACK_PYEXT_INSTANCE:
                result = PyInstance_FromBuffer(msg, off);
                break;
            default:
                PyErr_Format(PyExc_TypeError,
                             "unknown extension type: '0x%02x'", type);
                break;
        }
    }
    return result;
}

#define PyExtension_FromBuffer(m, s, o) \
    __PyObject_FromBuffer(PyExtension, m, s, o)


/* unpack ------------------------------------------------------------------- */

static PyObject *
__unpack_msg(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;
    PyObject *result = NULL;

    if ((type = __unpack_type(msg, off)) != _MSGPACK_INVALID) {
        if ((_MSGPACK_FIXINT <= type) && (type <= _MSGPACK_FIXINTEND)) {
            result = PyLong_FromLong((int8_t)type);
        }
        else if ((_MSGPACK_FIXUINT <= type) && (type <= _MSGPACK_FIXUINTEND)) {
            result = PyLong_FromUnsignedLong(type);
        }
        else if ((_MSGPACK_FIXMAP <= type) && (type <= _MSGPACK_FIXMAPEND)) {
            result = PyDict_Unpack(msg, (type & _MSGPACK_FIXOBJ_BIT), off);
        }
        else if ((_MSGPACK_FIXARRAY <= type) && (type <= _MSGPACK_FIXARRAYEND)) {
            result = PyTuple_Unpack(msg, (type & _MSGPACK_FIXOBJ_BIT), off);
        }
        else if ((_MSGPACK_FIXSTR <= type) && (type <= _MSGPACK_FIXSTREND)) {
            result = PyUnicode_Unpack(msg, (type & _MSGPACK_FIXSTR_BIT), off);
        }
        else {
            switch (type) {
                case _MSGPACK_NIL:
                    result = (Py_INCREF(Py_None), Py_None);
                    break;
                case _MSGPACK_FALSE:
                    result = (Py_INCREF(Py_False), Py_False);
                    break;
                case _MSGPACK_TRUE:
                    result = (Py_INCREF(Py_True), Py_True);
                    break;
                case _MSGPACK_BIN8:
                    result = PyBytes_FromBuffer(msg, 1, off);
                    break;
                case _MSGPACK_BIN16:
                    result = PyBytes_FromBuffer(msg, 2, off);
                    break;
                case _MSGPACK_BIN32:
                    result = PyBytes_FromBuffer(msg, 4, off);
                    break;
                case _MSGPACK_EXT8:
                    result = PyExtension_FromBuffer(msg, 1, off);
                    break;
                case _MSGPACK_EXT16:
                    result = PyExtension_FromBuffer(msg, 2, off);
                    break;
                case _MSGPACK_EXT32:
                    result = PyExtension_FromBuffer(msg, 4, off);
                    break;
                case _MSGPACK_FLOAT32:
                    result = PyFloat_Unpack(msg, 4, off);
                    break;
                case _MSGPACK_FLOAT64:
                    result = PyFloat_Unpack(msg, 8, off);
                    break;
                case _MSGPACK_UINT8:
                    result = PyULong_Unpack(msg, 1, off);
                    break;
                case _MSGPACK_UINT16:
                    result = PyULong_Unpack(msg, 2, off);
                    break;
                case _MSGPACK_UINT32:
                    result = PyULong_Unpack(msg, 4, off);
                    break;
                case _MSGPACK_UINT64:
                    result = PyULong_Unpack(msg, 8, off);
                    break;
                case _MSGPACK_INT8:
                    result = PyLong_Unpack(msg, 1, off);
                    break;
                case _MSGPACK_INT16:
                    result = PyLong_Unpack(msg, 2, off);
                    break;
                case _MSGPACK_INT32:
                    result = PyLong_Unpack(msg, 4, off);
                    break;
                case _MSGPACK_INT64:
                    result = PyLong_Unpack(msg, 8, off);
                    break;
                case _MSGPACK_FIXEXT1:
                    result = PyExtension_Unpack(msg, 1, off);
                    break;
                case _MSGPACK_FIXEXT2:
                    result = PyExtension_Unpack(msg, 2, off);
                    break;
                case _MSGPACK_FIXEXT4:
                    result = PyExtension_Unpack(msg, 4, off);
                    break;
                case _MSGPACK_FIXEXT8:
                    result = PyExtension_Unpack(msg, 8, off);
                    break;
                case _MSGPACK_FIXEXT16:
                    result = PyExtension_Unpack(msg, 16, off);
                    break;
                case _MSGPACK_STR8:
                    result = PyUnicode_FromBuffer(msg, 1, off);
                    break;
                case _MSGPACK_STR16:
                    result = PyUnicode_FromBuffer(msg, 2, off);
                    break;
                case _MSGPACK_STR32:
                    result = PyUnicode_FromBuffer(msg, 4, off);
                    break;
                case _MSGPACK_ARRAY16:
                    result = PyTuple_FromBuffer(msg, 2, off);
                    break;
                case _MSGPACK_ARRAY32:
                    result = PyTuple_FromBuffer(msg, 4, off);
                    break;
                case _MSGPACK_MAP16:
                    result = PyDict_FromBuffer(msg, 2, off);
                    break;
                case _MSGPACK_MAP32:
                    result = PyDict_FromBuffer(msg, 4, off);
                    break;
                default:
                    PyErr_Format(PyExc_TypeError,
                                 "unknown type: '0x%02x'", type);
                    break;
            }
        }
    }
    return result;
}


/* --------------------------------------------------------------------------
   module
   -------------------------------------------------------------------------- */

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
        __register_obj(state->registry, obj)) {
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

    if ((msg = __new_msg()) && __pack_obj(msg, obj)) {
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
    PyObject *result = NULL;
    Py_buffer msg;
    Py_ssize_t off = 0;

    if (PyArg_ParseTuple(args, "y*:unpack", &msg)) {
        result = __unpack_msg(&msg, &off);
        PyBuffer_Release(&msg);
    }
    return result;
}


/* msgpack_def.m_methods */
static PyMethodDef msgpack_m_methods[] = {
    {"register", (PyCFunction)msgpack_register,
     METH_O, msgpack_register_doc},
    {"pack", (PyCFunction)msgpack_pack,
     METH_O, msgpack_pack_doc},
    {"unpack", (PyCFunction)msgpack_unpack,
     METH_VARARGS, msgpack_unpack_doc},
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
              msgpack_init_state(module) ||
              _PyModule_AddType(module, "Timestamp", &Timestamp_Type) ||
              PyModule_AddStringConstant(module, "__version__", PKG_VERSION)
             )
            ) {
        Py_CLEAR(module);
    }
    return module;
}
