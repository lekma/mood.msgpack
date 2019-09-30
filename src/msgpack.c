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

static int __pack_obj(PyObject *msg, PyObject *obj);
static PyObject *__pack_class(PyObject *obj);
static int __register_obj(PyObject *registry, PyObject *obj);
static PyObject *__unpack_msg(Py_buffer *msg, Py_ssize_t *off);


/* misc */
#define _PyErr_ObjTooLong_(n) \
    PyErr_Format(PyExc_OverflowError, \
                 "%s object too long to pack", n)


#define _PyErr_InvalidType_(t) \
    do { \
        if (!PyErr_Occurred()) { \
            PyErr_Format(PyExc_TypeError, \
                         "invalid type: '0x%02x'", t); \
        } \
    } while (0)


/* for use with Py_EnterRecursiveCall */
#define __Where__(a, n) \
    " while " #a " a " n

#define _Packing_(n) \
    __Where__(packing, n)

#define _Unpacking_(n) \
    __Where__(unpacking, n)


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
   MessagePack definitions (these should probably go in a header)
   see https://github.com/msgpack/msgpack/blob/master/spec.md
   -------------------------------------------------------------------------- */

#define _MSGPACK_UINT32_MAX (1LL << 32)
#define _MSGPACK_INT32_MIN -(1LL << 31)

#define _MSGPACK_UINT16_MAX (1LL << 16)
#define _MSGPACK_INT16_MIN -(1LL << 15)

#define _MSGPACK_UINT8_MAX (1LL << 8)
#define _MSGPACK_INT8_MIN -(1LL << 7)

#define _MSGPACK_FIXUINT_MAX (1LL << 7)
#define _MSGPACK_FIXINT_MIN -(1LL << 5)

#define _MSGPACK_FIXSTR_LEN_MAX (1LL << 5)
#define _MSGPACK_FIXSTR_BIT 0x1f

#define _MSGPACK_FIXOBJ_LEN_MAX (1LL << 4)
#define _MSGPACK_FIXOBJ_BIT 0x0f


/* MessagePack types */
enum {
    _MSGPACK_FIXUINT     = 0x00,     //   0
    _MSGPACK_FIXUINT_END = 0x7f,     // 127

    _MSGPACK_FIXMAP     = 0x80,
    _MSGPACK_FIXMAP_END = 0x8f,

    _MSGPACK_FIXARRAY     = 0x90,
    _MSGPACK_FIXARRAY_END = 0x9f,

    _MSGPACK_FIXSTR     = 0xa0,
    _MSGPACK_FIXSTR_END = 0xbf,

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

    _MSGPACK_FIXINT     = 0xe0,      // -32
    _MSGPACK_FIXINT_END = 0xff       //  -1
};


/* Extensions types */
enum {
    _MSGPACK_EXT_INVALID = 0x00,     // invalid extension type

    // Python

    _MSGPACK_EXT_PYCOMPLEX,
    _MSGPACK_EXT_PYBYTEARRAY,

    _MSGPACK_EXT_PYLIST,

    _MSGPACK_EXT_PYSET,
    _MSGPACK_EXT_PYFROZENSET,

    _MSGPACK_EXT_PYCLASS,
    _MSGPACK_EXT_PYSINGLETON,

    _MSGPACK_EXT_PYOBJECT = 0x7f,    // last

    // MessagePack

    _MSGPACK_EXT_TIMESTAMP = 0xff    //  -1
};


/* --------------------------------------------------------------------------
   helpers wrappers
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


#define __pack_bytes__(m, s, b) \
    ((s) ? __pack(m, s, b) : 0)


/* -------------------------------------------------------------------------- */

#define __pack_type __pack_uint1__


#define __pack_value(m, t, s, v) \
    ((__pack_type(m, t)) ? -1 : __pack_uint##s##__(m, v))


#define __pack_float(m, t, s, v) \
    ((__pack_type(m, t)) ? -1 : __pack_float##s##__(m, v))


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
    else if (value < _MSGPACK_FIXUINT_MAX) { // fixint
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

static inline int
__pack_long(PyObject *msg, int64_t value)
{
    if ((value == -1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_int(msg, value);
}

static inline int
__pack_ulong(PyObject *msg, uint64_t value)
{
    if ((value == (uint64_t)-1) && PyErr_Occurred()) {
        return -1;
    }
    return __pack_value(msg, _MSGPACK_UINT64, 8, value);
}


static inline int
__pack_bin_type(PyObject *msg, Py_ssize_t size, const char *name)
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
        _PyErr_ObjTooLong_(name);
    }
    return res;
}

#define __pack_bytes(m, s, b, n) \
    ((__pack_bin_type(m, s, n)) ? -1 : __pack_bytes__(m, s, b))


static inline int
__pack_str_type(PyObject *msg, Py_ssize_t size, const char *name)
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
        _PyErr_ObjTooLong_(name);
    }
    return res;
}

#define __pack_unicode(m, s, b, n) \
    ((__pack_str_type(m, s, n)) ? -1 : __pack_bytes__(m, s, b))


static inline int
__pack_array_type(PyObject *msg, Py_ssize_t size, const char *name)
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
        _PyErr_ObjTooLong_(name);
    }
    return res;
}

static inline int
__pack_sequence__(PyObject *msg, Py_ssize_t size, PyObject **items,
                  const char *name, const char *where)
{
    Py_ssize_t i;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_array_type(msg, size, name)) {
            for (res = 0, i = 0; i < size; ++i) {
                if ((res = __pack_obj(msg, items[i]))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_sequence(m, s, i, n) \
    __pack_sequence__(m, s, i, n, _Packing_(n))


static inline int
__pack_map_type(PyObject *msg, Py_ssize_t size, const char *name)
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
        _PyErr_ObjTooLong_(name);
    }
    return res;
}

static inline int
__pack_dict__(PyObject *msg, PyObject *obj, const char *name, const char *where)
{
    Py_ssize_t pos = 0;
    PyObject *key = NULL, *val = NULL;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_map_type(msg, _PyDict_GET_SIZE(obj), name)) {
            while ((res = PyDict_Next(obj, &pos, &key, &val))) {
                if ((res = __pack_obj(msg, key)) ||
                    (res = __pack_obj(msg, val))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_dict(m, o, n) \
    __pack_dict__(m, o, n, _Packing_(n))


/* Py_None, Py_False, Py_True ----------------------------------------------- */

#define _PyNone_Pack(msg) \
    __pack_type(msg, _MSGPACK_NIL)


#define _PyFalse_Pack(msg) \
    __pack_type(msg, _MSGPACK_FALSE)


#define _PyTrue_Pack(msg) \
    __pack_type(msg, _MSGPACK_TRUE)


/* PyLong ------------------------------------------------------------------- */

static int
_PyLong_Pack(PyObject *msg, PyObject *obj)
{
    int overflow = 0;
    int64_t value = PyLong_AsLongLongAndOverflow(obj, &overflow);

    if (overflow) {
        if (overflow < 0) {
            PyErr_SetString(PyExc_OverflowError, "int too big to convert");
            return -1;
        }
        return __pack_ulong(msg, PyLong_AsUnsignedLongLong(obj));
    }
    return __pack_long(msg, value);
}


/* PyFloat ------------------------------------------------------------------ */

#define _PyFloat_Pack(msg, obj) \
    __pack_float(msg, _MSGPACK_FLOAT64, 8, PyFloat_AS_DOUBLE(obj))


/* PyBytes ------------------------------------------------------------------ */

static int
_PyBytes_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size = PyBytes_GET_SIZE(obj);
    const char *buffer = PyBytes_AS_STRING(obj);

    return __pack_bytes(msg, size, buffer, "bytes");
}


/* PyUnicode ---------------------------------------------------------------- */

static int
_PyUnicode_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size;
    const char *buffer = NULL;

    if (!(buffer = PyUnicode_AsUTF8AndSize(obj, &size))) {
        return -1;
    }
    return __pack_unicode(msg, size, buffer, "str");
}


/* PyTuple ------------------------------------------------------------------ */

static int
_PyTuple_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size = PyTuple_GET_SIZE(obj);
    PyObject **items = _PyTuple_ITEMS(obj);

    return __pack_sequence(msg, size, items, "tuple");
}


/* PyDict ------------------------------------------------------------------- */

#define _PyDict_Pack(msg, obj) \
    __pack_dict(msg, obj, "dict")


/* extension types helpers -------------------------------------------------- */

static inline int
__pack_ext_type(PyObject *msg, Py_ssize_t size, const char *name)
{
    int res = -1;

    if (size < _MSGPACK_UINT8_MAX) {
        if (size == 1) {
            res = __pack_type(msg, _MSGPACK_FIXEXT1);
        }
        else if (size == 2) {
            res = __pack_type(msg, _MSGPACK_FIXEXT2);
        }
        else if (size == 4) {
            res = __pack_type(msg, _MSGPACK_FIXEXT4);
        }
        else if (size == 8) {
            res = __pack_type(msg, _MSGPACK_FIXEXT8);
        }
        else if (size == 16) {
            res = __pack_type(msg, _MSGPACK_FIXEXT16);
        }
        else {
            res = __pack_value(msg, _MSGPACK_EXT8, 1, size);
        }
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, _MSGPACK_EXT16, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, _MSGPACK_EXT32, 4, size);
    }
    else {
        PyErr_Format(PyExc_OverflowError,
                     "%s object extension data too long to pack", name);
    }
    return res;
}

#define __pack_ext__(m, t, s, b) \
    ((__pack_type(m, t)) ? -1 : __pack_bytes__(m, s, b))

#define __pack_ext(m, t, s, b, n) \
    ((__pack_ext_type(m, s, n)) ? -1 : __pack_ext__(m, t, s, b))

static inline int
__pack_extension(PyObject *msg, uint8_t type, PyObject *data, const char *name)
{
    Py_ssize_t size = PyByteArray_GET_SIZE(data);
    const char *buffer = PyByteArray_AS_STRING(data);

    return __pack_ext(msg, type, size, buffer, name);
}


#define __pack_timestamp__(m, n, s) \
    ((__pack_uint4__(m, n) || __pack_uint8__(m, s)) ? -1 : 0)

static inline int
__pack_timestamp(PyObject *msg, Timestamp *ts)
{
    uint64_t value = 0, seconds = (uint64_t)ts->seconds;
    uint32_t nanoseconds = ts->nanoseconds;

    if ((seconds >> 34) == 0) {
        value = (((uint64_t)nanoseconds << 34) | seconds);
        if ((value & 0xffffffff00000000L) == 0) {
            return __pack_uint4__(msg, (uint32_t)value);
        }
        return __pack_uint8__(msg, value);
    }
    return __pack_timestamp__(msg, nanoseconds, seconds);
}


#define __pack_complex(m, c) \
    ((__pack_float8__(m, c.real) || __pack_float8__(m, c.imag)) ? -1 : 0)


static inline int
__pack_anyset__(PyObject *msg, PyObject *obj, const char *name, const char *where)
{
    Py_ssize_t pos = 0;
    PyObject *item = NULL;
    Py_hash_t hash;
    int res = -1;

    if (!Py_EnterRecursiveCall(where)) {
        if (!__pack_array_type(msg, PySet_GET_SIZE(obj), name)) {
            while ((res = _PySet_NextEntry(obj, &pos, &item, &hash))) {
                if ((res = __pack_obj(msg, item))) {
                    break;
                }
            }
        }
        Py_LeaveRecursiveCall();
    }
    return res;
}

#define __pack_anyset(m, o, n) \
    __pack_anyset__(m, o, n, _Packing_(n))


/* -------------------------------------------------------------------------- */

#define __pack_ext_timestamp(m, t, d, ts, n) \
    ((__pack_timestamp(d, ts)) ? -1 : __pack_extension(m, t, d, n))


#define __pack_ext_complex(m, t, d, c, n) \
    ((__pack_complex(d, c)) ? -1 : __pack_extension(m, t, d, n))


#define __pack_ext_sequence(m, t, d, s, i, n) \
    ((__pack_sequence(d, s, i, n)) ? -1 : __pack_extension(m, t, d, n))


#define __pack_ext_anyset(m, t, d, o, n) \
    ((__pack_anyset(d, o, n)) ? -1 : __pack_extension(m, t, d, n))


/* mood.msgpack.Timestamp --------------------------------------------------- */

#define __pack_msgpack_timestamp(m, d, ts) \
    __pack_ext_timestamp(m, _MSGPACK_EXT_TIMESTAMP, d, ts, "msgpack.Timestamp")

static int
_Timestamp_Pack(PyObject *msg, PyObject *obj)
{
    Timestamp *ts = (Timestamp *)obj;
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_msgpack_timestamp(msg, data, ts);
        Py_DECREF(data);
    }
    return res;
}


/* PyComplex ---------------------------------------------------------------- */

#define __pack_py_complex(m, d, c) \
    __pack_ext_complex(m, _MSGPACK_EXT_PYCOMPLEX, d, c, "complex")

static int
_PyComplex_Pack(PyObject *msg, PyObject *obj)
{
    Py_complex complex = ((PyComplexObject *)obj)->cval;
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_py_complex(msg, data, complex);
        Py_DECREF(data);
    }
    return res;
}


/* PyByteArray -------------------------------------------------------------- */

#define _PyByteArray_Pack(msg, obj) \
    __pack_extension(msg, _MSGPACK_EXT_PYBYTEARRAY, obj, "bytearray")


/* PyList ------------------------------------------------------------------- */

#define __pack_py_list(m, d, s, i) \
    __pack_ext_sequence(m, _MSGPACK_EXT_PYLIST, d, s, i, "list")

static int
_PyList_Pack(PyObject *msg, PyObject *obj)
{
    Py_ssize_t size = PyList_GET_SIZE(obj);
    PyObject **items = _PyList_ITEMS(obj);
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_py_list(msg, data, size, items);
        Py_DECREF(data);
    }
    return res;
}


/* PySet -------------------------------------------------------------------- */

#define __pack_py_set(m, d, o) \
    __pack_ext_anyset(m, _MSGPACK_EXT_PYSET, d, o, "set")

static int
_PySet_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_py_set(msg, data, obj);
        Py_DECREF(data);
    }
    return res;
}


/* PyFrozenSet -------------------------------------------------------------- */

#define __pack_py_frozenset(m, d, o) \
    __pack_ext_anyset(m, _MSGPACK_EXT_PYFROZENSET, d, o, "frozenset")

static int
_PyFrozenSet_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_py_frozenset(msg, data, obj);
        Py_DECREF(data);
    }
    return res;
}


/* PyType ------------------------------------------------------------------- */

#define __pack_py_class(m, d) \
    __pack_extension(m, _MSGPACK_EXT_PYCLASS, d, "class")

static int
_PyClass_Pack(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __pack_class(obj))) {
        res = __pack_py_class(msg, data);
        Py_DECREF(data);
    }
    return res;
}


/* instances and singletons ------------------------------------------------- */

static int
_PyObject_Pack(PyObject *msg, PyObject *obj, const char *name)
{
    _Py_IDENTIFIER(__reduce__);
    PyObject *reduce = NULL, *data = NULL;
    uint8_t type = _MSGPACK_EXT_INVALID; // 0
    int res = -1;

    if ((reduce = _PyObject_CallMethodId(obj, &PyId___reduce__, NULL))) {
        if ((data = __new_msg())) {
            if (PyUnicode_CheckExact(reduce)) {
                if (!_PyUnicode_Pack(data, reduce)) {
                    type = _MSGPACK_EXT_PYSINGLETON;
                }
            }
            else if (PyTuple_CheckExact(reduce)) {
                if (!_PyTuple_Pack(data, reduce)) {
                    type = _MSGPACK_EXT_PYOBJECT;
                }
            }
            else {
                PyErr_SetString(PyExc_TypeError,
                                "__reduce__() must return a str or a tuple");
            }
            if (type) {
                res = __pack_extension(msg, type, data, name);
            }
            Py_DECREF(data);
        }
        Py_DECREF(reduce);
    }
    else if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
        PyErr_Clear();
        PyErr_Format(PyExc_TypeError,
                     "cannot pack '%.200s' objects", name);
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
        res = _PyNone_Pack(msg);
    }
    else if (obj == Py_False) {
        res = _PyFalse_Pack(msg);
    }
    else if (obj == Py_True) {
        res = _PyTrue_Pack(msg);
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
    else if (type == &Timestamp_Type) {
        res = _Timestamp_Pack(msg, obj);
    }
    else if (type == &PyComplex_Type) {
        res = _PyComplex_Pack(msg, obj);
    }
    else if (type == &PyByteArray_Type) {
        res = _PyByteArray_Pack(msg, obj);
    }
    else if (type == &PyList_Type) {
        res = _PyList_Pack(msg, obj);
    }
    else if (type == &PySet_Type) {
        res = _PySet_Pack(msg, obj);
    }
    else if (type == &PyFrozenSet_Type) {
        res = _PyFrozenSet_Pack(msg, obj);
    }
    else if (type == &PyType_Type) {
        res = _PyClass_Pack(msg, obj);
    }
    else {
        res = _PyObject_Pack(msg, obj, type->tp_name);
    }
    return res;
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
        (qualname = _PyObject_GetAttrId(obj, &PyId___qualname__))) {
        if (PyUnicode_CheckExact(module) && PyUnicode_CheckExact(qualname)) {
            if ((result = __new_msg()) &&
                (_PyUnicode_Pack(result, module) ||
                 _PyUnicode_Pack(result, qualname))) {
                Py_CLEAR(result);
            }
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "expected strings, got: __module__: %s, __qualname__: %s",
                         Py_TYPE(module)->tp_name, Py_TYPE(qualname)->tp_name);
        }
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
        if (!PyUnicode_CheckExact(reduce)) {
            PyErr_SetString(PyExc_TypeError,
                            "__reduce__() must return a str");
        }
        else if ((result = __new_msg()) && _PyUnicode_Pack(result, reduce)) {
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
                                             PyByteArray_GET_SIZE(data)))) {
            res = PyDict_SetItem(registry, key, obj);
            Py_DECREF(key);
        }
        Py_DECREF(data);
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


/* -------------------------------------------------------------------------- */

static inline const char *
__unpack_buffer(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t poff = *off, noff = poff + size;

    if (noff > msg->len) {
        PyErr_SetString(PyExc_EOFError,
                        "Ran out of input");
        return NULL;
    }
    *off = noff;
    return ((msg->buf) + poff);
}


static inline uint8_t
__unpack_type(Py_buffer *msg, Py_ssize_t *off)
{
    const char *buffer = NULL;

    if (!(buffer = __unpack_buffer(msg, 1, off))) {
        return _MSGPACK_INVALID;
    }
    return __unpack_uint1(buffer);
}


#define __unpack_size(m, s, o) \
    (((buffer = __unpack_buffer(m, s, o))) ? __unpack_uint##s(buffer) : -1)


#define __unpack_object(t, m, s, o) \
    (((buffer = __unpack_buffer(m, s, o))) ? t##_FromStringAndSize(buffer, s) : NULL)


#define __unpack_sized(t, m, s, o) \
    (((size = __unpack_size(m, s, o)) < 0) ? NULL : _##t##_Unpack(m, size, o))


/* std types helpers -------------------------------------------------------- */

// PyLong
#define PyUnsigned_FromStringAndSize(b, s) \
    PyLong_FromUnsignedLongLong(__unpack_uint##s(b))

#define PySigned_FromStringAndSize(b, s) \
    PyLong_FromLongLong(__unpack_int##s(b))


// PyFloat
#define PyFloat_FromStringAndSize(b, s) \
    PyFloat_FromDouble(__unpack_float##s(b))


static inline int
__unpack_sequence(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off,
                  PyObject **items)
{
    PyObject *item = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = (((item = __unpack_msg(msg, off))) ? 0 : -1))) {
            break;
        }
        items[i] = item; // steals ref
    }
    return res;
}


static inline int
__unpack_dict(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off, PyObject *items)
{
    PyObject *key = NULL, *val = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = (((key = __unpack_msg(msg, off)) &&
                     (val = __unpack_msg(msg, off))) ?
                    PyDict_SetItem(items, key, val) : -1))) {
            Py_XDECREF(key);
            Py_XDECREF(val);
            break;
        }
        Py_DECREF(key);
        Py_DECREF(val);
    }
    return res;
}


/* _MSGPACK_UINT ------------------------------------------------------------ */

#define _PyUnsigned_Unpack(m, s, o) \
    __unpack_object(PyUnsigned, m, s, o)


/* _MSGPACK_INT ------------------------------------------------------------- */

#define _PySigned_Unpack(m, s, o) \
    __unpack_object(PySigned, m, s, o)


/* _MSGPACK_FLOAT ----------------------------------------------------------- */

#define _PyFloat_Unpack(m, s, o) \
    __unpack_object(PyFloat, m, s, o)


/* _MSGPACK_BIN ------------------------------------------------------------- */

#define _PyBytes_Unpack(m, s, o) \
    __unpack_object(PyBytes, m, s, o)

#define _PyBytes_UnpackSized(m, s, o) \
    __unpack_sized(PyBytes, m, s, o)


/* _MSGPACK_STR, _MSGPACK_FIXSTR -------------------------------------------- */

#define _PyUnicode_Unpack(m, s, o) \
    __unpack_object(PyUnicode, m, s, o)

#define _PyUnicode_UnpackSized(m, s, o) \
    __unpack_sized(PyUnicode, m, s, o)


/* _MSGPACK_ARRAY, _MSGPACK_FIXARRAY ---------------------------------------- */

static PyObject *
_PyTuple_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("tuple"))) {
        if ((result = PyTuple_New(size)) &&
            __unpack_sequence(msg, size, off, _PyTuple_ITEMS(result))) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyTuple_UnpackSized(m, s, o) \
    __unpack_sized(PyTuple, m, s, o)


/* _MSGPACK_MAP, _MSGPACK_FIXMAP -------------------------------------------- */

static PyObject *
_PyDict_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("dict"))) {
        if ((result = PyDict_New()) &&
            __unpack_dict(msg, size, off, result)) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyDict_UnpackSized(m, s, o) \
    __unpack_sized(PyDict, m, s, o)


/* extension types helpers -------------------------------------------------- */

static Py_ssize_t
__unpack_array_len(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;

    if ((type = __unpack_type(msg, off)) == _MSGPACK_INVALID) {
        _PyErr_InvalidType_(type);
    }
    else if ((_MSGPACK_FIXARRAY <= type) && (type <= _MSGPACK_FIXARRAY_END)) {
        len = (type & _MSGPACK_FIXOBJ_BIT);
    }
    else if (type == _MSGPACK_ARRAY16) {
        len = __unpack_size(msg, 2, off);
    }
    else if (type == _MSGPACK_ARRAY32) {
        len = __unpack_size(msg, 4, off);
    }
    else {
        PyErr_Format(PyExc_TypeError,
                     "invalid array type: '0x%02x'", type);
    }
    return len;
}

#define __unpack_ext_array(t, m, o) \
    (((len = __unpack_array_len(m, o)) < 0) ? NULL : _##t##_Unpack(m, len, o))


static inline int
__unpack_anyset(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off, PyObject *items)
{
    PyObject *item = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = (((item = __unpack_msg(msg, off))) ? PySet_Add(items, item) : -1))) {
            Py_XDECREF(item);
            break;
        }
        Py_DECREF(item);
    }
    return res;
}


static inline PyObject *
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


/* _MSGPACK_EXT_TIMESTAMP --------------------------------------------------- */

static PyObject *
_Timestamp_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    const char *buffer = NULL;
    uint32_t nanoseconds = 0;
    int64_t seconds = 0;
    uint64_t value = 0;
    PyObject *result = NULL;

    if ((buffer = __unpack_buffer(msg, size, off))) {
        if (size == 4) {
            seconds = (int64_t)__unpack_uint4(buffer);
        }
        else if (size == 8) {
            value = __unpack_uint8(buffer);
            nanoseconds = (uint32_t)(value >> 34);
            seconds = (int64_t)(value & 0x00000003ffffffffLL);
        }
        else if (size == 12) {
            nanoseconds = __unpack_uint4(buffer);
            seconds = __unpack_int8((buffer + 4));
        }
        else {
            return PyErr_Format(PyExc_ValueError,
                                "invalid timestamp size: %zd", size);
        }
        result = _Timestamp_New(&Timestamp_Type, seconds, nanoseconds);
    }
    return result;
}


/* _MSGPACK_EXT_PYCOMPLEX --------------------------------------------------- */

static PyObject *
_PyComplex_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
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


/* _MSGPACK_EXT_PYBYTEARRAY ------------------------------------------------- */

#define _PyByteArray_Unpack(m, s, o) \
    __unpack_object(PyByteArray, m, s, o)


/* _MSGPACK_EXT_PYLIST ------------------------------------------------------ */

static PyObject *
_PyList_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("list"))) {
        if ((result = PyList_New(size)) &&
            __unpack_sequence(msg, size, off, _PyList_ITEMS(result))) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyList_UnpackSized(m, o) \
    __unpack_ext_array(PyList, m, o)


/* _MSGPACK_EXT_PYSET ------------------------------------------------------- */

static PyObject *
_PySet_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("set"))) {
        if ((result = PySet_New(NULL)) &&
            __unpack_anyset(msg, size, off, result)) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PySet_UnpackSized(m, o) \
    __unpack_ext_array(PySet, m, o)


/* _MSGPACK_EXT_PYFROZENSET ------------------------------------------------- */

static PyObject *
_PyFrozenSet_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("frozenset"))) {
        if ((result = PyFrozenSet_New(NULL)) &&
            __unpack_anyset(msg, size, off, result)) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyFrozenSet_UnpackSized(m, o) \
    __unpack_ext_array(PyFrozenSet, m, o)


/* _MSGPACK_EXT_PYCLASS ----------------------------------------------------- */

static inline void
__unpack_class_error(Py_buffer *msg, Py_ssize_t *off)
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
_PyClass_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, size, off))) {
        __unpack_class_error(msg, &poff);
    }
    return result;
}


/* _MSGPACK_EXT_PYSINGLETON ------------------------------------------------- */

static inline void
__unpack_singleton_error(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *name = NULL;

    if ((name = __unpack_msg(msg, off))) {
        PyErr_Format(PyExc_TypeError,
                     "cannot unpack '%U'", name);
        Py_DECREF(name);
    }
}

static PyObject *
_PySingleton_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (!(result = __unpack_registered(msg, size, off))) {
        __unpack_singleton_error(msg, &poff);
    }
    return result;
}


/* _MSGPACK_EXT_PYOBJECT ---------------------------------------------------- */

// object.__setstate__()
static inline int
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
static inline int
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

static inline int
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

static inline int
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

static inline PyObject *
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
_PyObject_Unpack(Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *result = NULL, *reduce = NULL;

    if ((reduce = __unpack_msg(msg, off))) {
        result = __PyObject_New(reduce);
        Py_DECREF(reduce);
    }
    return result;
}


/* _MSGPACK_EXT, _MSGPACK_FIXEXT -------------------------------------------- */

static PyObject *
_PyExtension_Unpack(Py_buffer *msg, Py_ssize_t size, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;
    PyObject *result = NULL;

    if ((type = __unpack_type(msg, off)) == _MSGPACK_INVALID) {
        _PyErr_InvalidType_(type);
    }
    else {
        switch (type) {
            case _MSGPACK_EXT_INVALID:
                PyErr_Format(PyExc_TypeError,
                             "invalid extension type: '0x%02x'", type);
                break;
            case _MSGPACK_EXT_TIMESTAMP:
                result = _Timestamp_Unpack(msg, size, off);
                break;
            case _MSGPACK_EXT_PYCOMPLEX:
                result = _PyComplex_Unpack(msg, size, off);
                break;
            case _MSGPACK_EXT_PYBYTEARRAY:
                result = _PyByteArray_Unpack(msg, size, off);
                break;
            case _MSGPACK_EXT_PYLIST:
                result = _PyList_UnpackSized(msg, off);
                break;
            case _MSGPACK_EXT_PYSET:
                result = _PySet_UnpackSized(msg, off);
                break;
            case _MSGPACK_EXT_PYFROZENSET:
                result = _PyFrozenSet_UnpackSized(msg, off);
                break;
            case _MSGPACK_EXT_PYCLASS:
                result = _PyClass_Unpack(msg, size, off);
                break;
            case _MSGPACK_EXT_PYSINGLETON:
                result = _PySingleton_Unpack(msg, size, off);
                break;
            case _MSGPACK_EXT_PYOBJECT:
                result = _PyObject_Unpack(msg, off);
                break;
            default:
                PyErr_Format(PyExc_TypeError,
                             "unknown extension type: '0x%02x'", type);
                break;
        }
    }
    return result;
}

#define _PyExtension_UnpackSized(m, s, o) \
    __unpack_sized(PyExtension, m, s, o)


/* unpack ------------------------------------------------------------------- */

static PyObject *
__unpack_msg(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = _MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t size = -1;
    PyObject *result = NULL;

    if ((type = __unpack_type(msg, off)) == _MSGPACK_INVALID) {
        _PyErr_InvalidType_(type);
    }
    else if ((_MSGPACK_FIXINT <= type) && (type <= _MSGPACK_FIXINT_END)) {
        result = PyLong_FromLong((int8_t)type);
    }
    else if ((_MSGPACK_FIXUINT <= type) && (type <= _MSGPACK_FIXUINT_END)) {
        result = PyLong_FromUnsignedLong(type);
    }
    else if ((_MSGPACK_FIXMAP <= type) && (type <= _MSGPACK_FIXMAP_END)) {
        result = _PyDict_Unpack(msg, (type & _MSGPACK_FIXOBJ_BIT), off);
    }
    else if ((_MSGPACK_FIXARRAY <= type) && (type <= _MSGPACK_FIXARRAY_END)) {
        result = _PyTuple_Unpack(msg, (type & _MSGPACK_FIXOBJ_BIT), off);
    }
    else if ((_MSGPACK_FIXSTR <= type) && (type <= _MSGPACK_FIXSTR_END)) {
        result = _PyUnicode_Unpack(msg, (type & _MSGPACK_FIXSTR_BIT), off);
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
                result = _PyBytes_UnpackSized(msg, 1, off);
                break;
            case _MSGPACK_BIN16:
                result = _PyBytes_UnpackSized(msg, 2, off);
                break;
            case _MSGPACK_BIN32:
                result = _PyBytes_UnpackSized(msg, 4, off);
                break;
            case _MSGPACK_EXT8:
                result = _PyExtension_UnpackSized(msg, 1, off);
                break;
            case _MSGPACK_EXT16:
                result = _PyExtension_UnpackSized(msg, 2, off);
                break;
            case _MSGPACK_EXT32:
                result = _PyExtension_UnpackSized(msg, 4, off);
                break;
            case _MSGPACK_FLOAT32:
                result = _PyFloat_Unpack(msg, 4, off);
                break;
            case _MSGPACK_FLOAT64:
                result = _PyFloat_Unpack(msg, 8, off);
                break;
            case _MSGPACK_UINT8:
                result = _PyUnsigned_Unpack(msg, 1, off);
                break;
            case _MSGPACK_UINT16:
                result = _PyUnsigned_Unpack(msg, 2, off);
                break;
            case _MSGPACK_UINT32:
                result = _PyUnsigned_Unpack(msg, 4, off);
                break;
            case _MSGPACK_UINT64:
                result = _PyUnsigned_Unpack(msg, 8, off);
                break;
            case _MSGPACK_INT8:
                result = _PySigned_Unpack(msg, 1, off);
                break;
            case _MSGPACK_INT16:
                result = _PySigned_Unpack(msg, 2, off);
                break;
            case _MSGPACK_INT32:
                result = _PySigned_Unpack(msg, 4, off);
                break;
            case _MSGPACK_INT64:
                result = _PySigned_Unpack(msg, 8, off);
                break;
            case _MSGPACK_FIXEXT1:
                result = _PyExtension_Unpack(msg, 1, off);
                break;
            case _MSGPACK_FIXEXT2:
                result = _PyExtension_Unpack(msg, 2, off);
                break;
            case _MSGPACK_FIXEXT4:
                result = _PyExtension_Unpack(msg, 4, off);
                break;
            case _MSGPACK_FIXEXT8:
                result = _PyExtension_Unpack(msg, 8, off);
                break;
            case _MSGPACK_FIXEXT16:
                result = _PyExtension_Unpack(msg, 16, off);
                break;
            case _MSGPACK_STR8:
                result = _PyUnicode_UnpackSized(msg, 1, off);
                break;
            case _MSGPACK_STR16:
                result = _PyUnicode_UnpackSized(msg, 2, off);
                break;
            case _MSGPACK_STR32:
                result = _PyUnicode_UnpackSized(msg, 4, off);
                break;
            case _MSGPACK_ARRAY16:
                result = _PyTuple_UnpackSized(msg, 2, off);
                break;
            case _MSGPACK_ARRAY32:
                result = _PyTuple_UnpackSized(msg, 4, off);
                break;
            case _MSGPACK_MAP16:
                result = _PyDict_UnpackSized(msg, 2, off);
                break;
            case _MSGPACK_MAP32:
                result = _PyDict_UnpackSized(msg, 4, off);
                break;
            default:
                PyErr_Format(PyExc_TypeError,
                             "unknown type: '0x%02x'", type);
                break;
        }
    }
    return result;
}


/* --------------------------------------------------------------------------
   ipc
   -------------------------------------------------------------------------- */

static inline int
__pack_ipc_size(PyObject *msg, Py_ssize_t size, const char *name)
{
    int res = -1;

    if (size < _MSGPACK_UINT8_MAX) {
        res = __pack_value(msg, 0x01, 1, size);
    }
    else if (size < _MSGPACK_UINT16_MAX) {
        res = __pack_value(msg, 0x02, 2, size);
    }
    else if (size < _MSGPACK_UINT32_MAX) {
        res = __pack_value(msg, 0x04, 4, size);
    }
    else {
        PyErr_Format(PyExc_OverflowError,
                     "%s object data too long to pack", name);
    }
    return res;
}


#define __pack_ipc__(m, s, b, n) \
    ((__pack_ipc_size(m, s, n)) ? -1 : __pack_bytes__(m, s, b))

static inline int
__pack_ipc(PyObject *msg, PyObject *data, const char *name)
{
    Py_ssize_t size = PyByteArray_GET_SIZE(data);
    const char *buffer = PyByteArray_AS_STRING(data);

    return __pack_ipc__(msg, size, buffer, name);
}


#define __pack_ipc_obj__(m, d, o) \
    ((__pack_obj(d, o)) ? -1 : __pack_ipc(m, d, Py_TYPE(o)->tp_name))

static inline int
__pack_ipc_obj(PyObject *msg, PyObject *obj)
{
    PyObject *data = NULL;
    int res = -1;

    if ((data = __new_msg())) {
        res = __pack_ipc_obj__(msg, data, obj);
        Py_DECREF(data);
    }
    return res;
}


static inline Py_ssize_t
__unpack_ipc_size__(Py_buffer *msg)
{
    Py_ssize_t size = -1, len = msg->len;
    const char * buf = msg->buf;

    if (len == 1) {
        size = __unpack_uint1(buf);
    }
    else if (len == 2) {
        size = __unpack_uint2(buf);
    }
    else if (len == 4) {
        size = __unpack_uint4(buf);
    }
    else {
        PyErr_Format(PyExc_ValueError,
                     "invalid buffer len: %zd", len);
    }
    return size;
}

#define __unpack_ipc_size(m) \
    (((size = __unpack_ipc_size__(m)) < 0) ? NULL : PyLong_FromSsize_t(size))


/* --------------------------------------------------------------------------
   module
   -------------------------------------------------------------------------- */

/* msgpack_def.m_doc */
PyDoc_STRVAR(msgpack_m_doc,
"Python MessagePack implementation");


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


/* msgpack._ipc_pack() */
PyDoc_STRVAR(msgpack__ipc_pack_doc,
"_ipc_pack(obj) -> msg");

static PyObject *
msgpack__ipc_pack(PyObject *module, PyObject *obj)
{
    PyObject *msg = NULL;

    if ((msg = __new_msg()) && __pack_ipc_obj(msg, obj)) {
        Py_CLEAR(msg);
    }
    return msg;
}


/* msgpack._ipc_size() */
PyDoc_STRVAR(msgpack__ipc_size_doc,
"_ipc_size(msg) -> int");

static PyObject *
msgpack__ipc_size(PyObject *module, PyObject *args)
{
    PyObject *result = NULL;
    Py_buffer msg;
    Py_ssize_t size = -1;

    if (PyArg_ParseTuple(args, "y*:_ipc_size", &msg)) {
        result = __unpack_ipc_size(&msg);
        PyBuffer_Release(&msg);
    }
    return result;
}


/* msgpack_def.m_methods */
static PyMethodDef msgpack_m_methods[] = {
    {"pack", (PyCFunction)msgpack_pack,
     METH_O, msgpack_pack_doc},
    {"register", (PyCFunction)msgpack_register,
     METH_O, msgpack_register_doc},
    {"unpack", (PyCFunction)msgpack_unpack,
     METH_VARARGS, msgpack_unpack_doc},
    {"_ipc_pack", (PyCFunction)msgpack__ipc_pack,
     METH_O, msgpack__ipc_pack_doc},
    {"_ipc_size", (PyCFunction)msgpack__ipc_size,
     METH_VARARGS, msgpack__ipc_size_doc},
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
