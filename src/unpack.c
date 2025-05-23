#include "msgpack.h"


#define __PyErr_SetTypeState__(s, d, t) \
    PyErr_Format(PyExc_TypeError, #s " %s type: '0x%02x'", (d) ? d : "\b", t)

#define __PyErr_UnknownType__(d, t) \
    __PyErr_SetTypeState__(unknown, d, t)

#define __PyErr_InvalidType__(d, t) \
    __PyErr_SetTypeState__(invalid, d, t)


#define _PyErr_UnknownType_ __PyErr_UnknownType__

#define _PyErr_InvalidType_(d, t) \
    do { \
        if (!PyErr_Occurred()) { \
            __PyErr_InvalidType__(d, t); \
        } \
    } while (0)


#define _PyErr_InvalidSize_(t, s) \
    PyErr_Format(PyExc_ValueError, "invalid %s size: %zd", t, s)


#define _Unpacking_(n) _While_(unpacking, n)


/* --------------------------------------------------------------------------
   unpack
   -------------------------------------------------------------------------- */

#define be8toh(x) x


#define __unpack_uint__(b, s) be##s##toh((*((uint##s##_t *)b)))

#define __unpack_int__(b, s) (int##s##_t)__unpack_uint__(b, s)


/* -------------------------------------------------------------------------- */

#define __unpack_uint1(b) __unpack_uint__(b, 8)
#define __unpack_uint2(b) __unpack_uint__(b, 16)
#define __unpack_uint4(b) __unpack_uint__(b, 32)
#define __unpack_uint8(b) __unpack_uint__(b, 64)

#define __unpack_int1(b) __unpack_int__(b, 8)
#define __unpack_int2(b) __unpack_int__(b, 16)
#define __unpack_int4(b) __unpack_int__(b, 32)
#define __unpack_int8(b) __unpack_int__(b, 64)


static inline float
__unpack_float4(const char *buffer)
{
    float32_t val = { .i = __unpack_uint4(buffer) };

    return val.f;
}

static inline double
__unpack_float8(const char *buffer)
{
    float64_t val = { .i = __unpack_uint8(buffer) };

    return val.f;
}


/* -------------------------------------------------------------------------- */

static inline const char *
__unpack_buffer(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
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

    if (!(buffer = __unpack_buffer(msg, off, 1))) {
        return MSGPACK_INVALID;
    }
    return (*((uint8_t *)buffer));
}


/* -------------------------------------------------------------------------- */

#define PyUnsignedLong_FromStringAndSize(b, s) \
    PyLong_FromUnsignedLongLong(__unpack_uint##s(b))


#define PyLong_FromStringAndSize(b, s) \
    PyLong_FromLongLong(__unpack_int##s(b))


#define PyFloat_FromStringAndSize(b, s) \
    PyFloat_FromDouble(__unpack_float##s(b))


/* -------------------------------------------------------------------------- */

static inline int
__unpack_sequence(
    PyObject *registry,
    Py_buffer *msg,
    Py_ssize_t *off,
    Py_ssize_t size,
    PyObject **items
)
{
    PyObject *item = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if ((res = ((item = UnpackMessage(registry, msg, off)) ? 0 : -1))) {
            break;
        }
        items[i] = item; // steals ref
    }
    return res;
}


static inline int
__unpack_dict(
    PyObject *registry,
    Py_buffer *msg,
    Py_ssize_t *off,
    Py_ssize_t size,
    PyObject *items
)
{
    PyObject *key = NULL, *val = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if (
            (
                res = (
                    (
                        (key = UnpackMessage(registry, msg, off)) &&
                        (val = UnpackMessage(registry, msg, off))
                    ) ? PyDict_SetItem(items, key, val) : -1
                )
            )
        ) {
            Py_XDECREF(key);
            Py_XDECREF(val);
            break;
        }
        Py_DECREF(key);
        Py_DECREF(val);
    }
    return res;
}


/* -------------------------------------------------------------------------- */

#define __unpack_object(t, m, o, s) \
    ( \
        ( \
            buffer = __unpack_buffer(m, o, s) \
        ) ? t##_FromStringAndSize(buffer, s) : NULL \
    )


#define __unpack_size__(m, o, s) \
    ( \
        ( \
            buffer = __unpack_buffer(m, o, s) \
        ) ? (Py_ssize_t)__unpack_uint##s(buffer) : -1 \
    )


#define __unpack_size(t, m, o, s) \
    (((size = __unpack_size__(m, o, s)) < 0) ? NULL : _##t##_Unpack(m, o, size))


#define __unpack_size_r(t, r, m, o, s) \
    (((size = __unpack_size__(m, o, s)) < 0) ? NULL : _##t##_Unpack(r, m, o, size))


/* MSGPACK_UINT ------------------------------------------------------------- */

#define _PyUnsignedLong_Unpack(m, o, s) \
    __unpack_object(PyUnsignedLong, m, o, s)


/* MSGPACK_INT -------------------------------------------------------------- */

#define _PyLong_Unpack(m, o, s) \
    __unpack_object(PyLong, m, o, s)


/* MSGPACK_FLOAT ------------------------------------------------------------ */

#define _PyFloat_Unpack(m, o, s) \
    __unpack_object(PyFloat, m, o, s)


/* MSGPACK_BIN -------------------------------------------------------------- */

#define _PyBytes_Unpack(m, o, s) \
    __unpack_object(PyBytes, m, o, s)

#define _PyBytes_Unpack_(m, o, s) \
    __unpack_size(PyBytes, m, o, s)


/* MSGPACK_STR, MSGPACK_FIXSTR ---------------------------------------------- */

#define _PyUnicode_Unpack(m, o, s) \
    __unpack_object(PyUnicode, m, o, s)

#define _PyUnicode_Unpack_(m, o, s) \
    __unpack_size(PyUnicode, m, o, s)


/* MSGPACK_ARRAY, MSGPACK_FIXARRAY ------------------------------------------ */

static PyObject *
_PyTuple_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("tuple"))) {
        if (
            (result = PyTuple_New(size)) &&
            __unpack_sequence(registry, msg, off, size, _PyTuple_ITEMS(result))
        ) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyTuple_Unpack_(r, m, o, s) \
    __unpack_size_r(PyTuple, r, m, o, s)


/* MSGPACK_MAP, MSGPACK_FIXMAP ---------------------------------------------- */

static PyObject *
_PyDict_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("dict"))) {
        if (
            (result = PyDict_New()) &&
            __unpack_dict(registry, msg, off, size, result)
        ) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyDict_Unpack_(r, m, o, s) \
    __unpack_size_r(PyDict, r, m, o, s)


/* --------------------------------------------------------------------------
   extensions
   -------------------------------------------------------------------------- */

static inline int
__unpack_anyset(
    PyObject *registry,
    Py_buffer *msg,
    Py_ssize_t *off,
    Py_ssize_t size,
    PyObject *items
)
{
    PyObject *item = NULL;
    Py_ssize_t i;
    int res = 0;

    for (i = 0; i < size; ++i) {
        if (
            (
                res = (
                    (
                        item = UnpackMessage(registry, msg, off)
                    ) ? PySet_Add(items, item) : -1
                )
            )
        ) {
            Py_XDECREF(item);
            break;
        }
        Py_DECREF(item);
    }
    return res;
}


static inline PyObject *
__unpack_registered(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    const char *buffer = NULL;
    PyObject *result = NULL, *key = NULL;

    if (
        (buffer = __unpack_buffer(msg, off, size)) &&
        (key = PyBytes_FromStringAndSize(buffer, size))
    ) {
        if ((result = PyDict_GetItem(registry, key))) { // borrowed
            Py_INCREF(result);
        }
        Py_DECREF(key);
    }
    return result;
}


static Py_ssize_t
__unpack_len__(Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;

    if ((type = __unpack_type(msg, off)) == MSGPACK_INVALID) {
        _PyErr_InvalidType_("array", type);
    }
    else if ((MSGPACK_FIXARRAY <= type) && (type <= MSGPACK_FIXARRAY_END)) {
        len = (type & MSGPACK_FIXOBJ_BIT);
    }
    else if (type == MSGPACK_ARRAY2) {
        len = __unpack_size__(msg, off, 2);
    }
    else if (type == MSGPACK_ARRAY4) {
        len = __unpack_size__(msg, off, 4);
    }
    else {
        __PyErr_InvalidType__("array", type);
    }
    return len;
}

#define __unpack_len(t, r, m, o) \
    (((len = __unpack_len__(m, o)) < 0) ? NULL : _##t##_Unpack(r, m, o, len))


/* MSGPACK_EXT_TIMESTAMP ---------------------------------------------------- */

static PyObject *
_Timestamp_Unpack(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    const char *buffer = NULL;
    uint32_t nanoseconds = 0;
    int64_t seconds = 0;
    uint64_t value = 0;
    PyObject *result = NULL;

    if ((buffer = __unpack_buffer(msg, off, size))) {
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
            return _PyErr_InvalidSize_("timestamp", size);
        }
        result = NewTimestamp(seconds, nanoseconds);
    }
    return result;
}


/* MSGPACK_EXT_PYCOMPLEX ---------------------------------------------------- */

static PyObject *
_PyComplex_Unpack(Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size)
{
    Py_complex complex;
    const char *buffer = NULL;
    PyObject *result = NULL;

    if (size != 16) {
        _PyErr_InvalidSize_("complex", size);
    }
    else if ((buffer = __unpack_buffer(msg, off, size))) {
        complex.real = __unpack_float8(buffer);
        complex.imag = __unpack_float8((buffer + 8));
        result = PyComplex_FromCComplex(complex);
    }
    return result;
}


/* MSGPACK_EXT_PYBYTEARRAY -------------------------------------------------- */

#define _PyByteArray_Unpack(m, o, s) \
    __unpack_object(PyByteArray, m, o, s)


/* MSGPACK_EXT_PYLIST ------------------------------------------------------- */

static PyObject *
_PyList_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("list"))) {
        if (
            (result = PyList_New(size)) &&
            __unpack_sequence(registry, msg, off, size, _PyList_ITEMS(result))
        ) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyList_Unpack_(r, m, o) \
    __unpack_len(PyList, r, m, o)


/* MSGPACK_EXT_PYSET -------------------------------------------------------- */

static PyObject *
_PySet_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("set"))) {
        if (
            (result = PySet_New(NULL)) &&
            __unpack_anyset(registry, msg, off, size, result)
        ) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PySet_Unpack_(r, m, o) \
    __unpack_len(PySet, r, m, o)


/* MSGPACK_EXT_PYFROZENSET -------------------------------------------------- */

static PyObject *
_PyFrozenSet_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    PyObject *result = NULL;

    if (!Py_EnterRecursiveCall(_Unpacking_("frozenset"))) {
        if (
            (result = PyFrozenSet_New(NULL)) &&
            __unpack_anyset(registry, msg, off, size, result)
        ) {
            Py_CLEAR(result);
        }
        Py_LeaveRecursiveCall();
    }
    return result;
}

#define _PyFrozenSet_Unpack_(r, m, o) \
    __unpack_len(PyFrozenSet, r, m, o)


/* MSGPACK_EXT_PYCLASS ------------------------------------------------------ */

static inline void
__unpack_class_error(PyObject *registry, Py_buffer *msg, Py_ssize_t *off)
{
    _Py_IDENTIFIER(builtins);
    PyObject *module = NULL, *qualname = NULL;

    if (
        (module = UnpackMessage(registry, msg, off)) &&
        (qualname = UnpackMessage(registry, msg, off))
    ) {
        if (!_PyUnicode_EqualToASCIIId(module, &PyId_builtins)) {
            PyErr_Format(
                PyExc_TypeError, "cannot unpack <class '%U.%U'>",
                module, qualname
            );
        }
        else {
            PyErr_Format(
                PyExc_TypeError, "cannot unpack <class '%U'>", qualname
            );
        }
    }
    Py_XDECREF(qualname);
    Py_XDECREF(module);
}

static PyObject *
_PyClass_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (
        !(result = __unpack_registered(registry, msg, off, size)) &&
        !PyErr_Occurred()
    ) {
        __unpack_class_error(registry, msg, &poff);
    }
    return result;
}


/* MSGPACK_EXT_PYSINGLETON -------------------------------------------------- */

static inline void
__unpack_singleton_error(PyObject *registry, Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *name = NULL;

    if ((name = UnpackMessage(registry, msg, off))) {
        PyErr_Format(PyExc_TypeError, "cannot unpack '%U'", name);
        Py_DECREF(name);
    }
}

static PyObject *
_PySingleton_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    Py_ssize_t poff = *off; // keep the original offset in case of error
    PyObject *result = NULL;

    if (
        !(result = __unpack_registered(registry, msg, off, size)) &&
        !PyErr_Occurred()
    ) {
        __unpack_singleton_error(registry, msg, &poff);
    }
    return result;
}


/* MSGPACK_EXT_PYOBJECT ----------------------------------------------------- */

static PyObject *
_PyObject_Unpack_(PyObject *registry, Py_buffer *msg, Py_ssize_t *off)
{
    PyObject *result = NULL, *reduce = NULL;

    if ((reduce = UnpackMessage(registry, msg, off))) {
        result = __PyObject_New(reduce);
        Py_DECREF(reduce);
    }
    return result;
}


/* MSGPACK_EXT, MSGPACK_FIXEXT ---------------------------------------------- */

static PyObject *
_Extension_Unpack(
    PyObject *registry, Py_buffer *msg, Py_ssize_t *off, Py_ssize_t size
)
{
    uint8_t type = MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t len = -1;
    PyObject *result = NULL;

    switch ((type = __unpack_type(msg, off))) {
        case MSGPACK_INVALID:
        case MSGPACK_EXT_INVALID:
            _PyErr_InvalidType_("extension", type);
            break;
        case MSGPACK_EXT_TIMESTAMP:
            result = _Timestamp_Unpack(msg, off, size);
            break;
        case MSGPACK_EXT_PYCOMPLEX:
            result = _PyComplex_Unpack(msg, off, size);
            break;
        case MSGPACK_EXT_PYBYTEARRAY:
            result = _PyByteArray_Unpack(msg, off, size);
            break;
        case MSGPACK_EXT_PYLIST:
            result = _PyList_Unpack_(registry, msg, off);
            break;
        case MSGPACK_EXT_PYSET:
            result = _PySet_Unpack_(registry, msg, off);
            break;
        case MSGPACK_EXT_PYFROZENSET:
            result = _PyFrozenSet_Unpack_(registry, msg, off);
            break;
        case MSGPACK_EXT_PYCLASS:
            result = _PyClass_Unpack(registry, msg, off, size);
            break;
        case MSGPACK_EXT_PYSINGLETON:
            result = _PySingleton_Unpack(registry, msg, off, size);
            break;
        case MSGPACK_EXT_PYOBJECT:
            result = _PyObject_Unpack_(registry, msg, off);
            break;
        default:
            _PyErr_UnknownType_("extension", type);
            break;
    }
    return result;
}

#define _Extension_Unpack_(r, m, o, s) \
    __unpack_size_r(Extension, r, m, o, s)


/* --------------------------------------------------------------------------
   interface
   -------------------------------------------------------------------------- */

PyObject *
UnpackMessage(PyObject *registry, Py_buffer *msg, Py_ssize_t *off)
{
    uint8_t type = MSGPACK_INVALID;
    const char *buffer = NULL;
    Py_ssize_t size = -1;
    PyObject *result = NULL;

    if ((type = __unpack_type(msg, off)) == MSGPACK_INVALID) {
        _PyErr_InvalidType_(NULL, type);
    }
    else if ((MSGPACK_FIXINT <= type) && (type <= MSGPACK_FIXINT_END)) {
        result = PyLong_FromLong((int8_t)type);
    }
    else if ((MSGPACK_FIXUINT <= type) && (type <= MSGPACK_FIXUINT_END)) {
        result = PyLong_FromUnsignedLong(type);
    }
    else if ((MSGPACK_FIXMAP <= type) && (type <= MSGPACK_FIXMAP_END)) {
        result = _PyDict_Unpack(registry, msg, off, (type & MSGPACK_FIXOBJ_BIT));
    }
    else if ((MSGPACK_FIXARRAY <= type) && (type <= MSGPACK_FIXARRAY_END)) {
        result = _PyTuple_Unpack(registry, msg, off, (type & MSGPACK_FIXOBJ_BIT));
    }
    else if ((MSGPACK_FIXSTR <= type) && (type <= MSGPACK_FIXSTR_END)) {
        result = _PyUnicode_Unpack(msg, off, (type & MSGPACK_FIXSTR_BIT));
    }
    else {
        switch (type) {
            case MSGPACK_NIL:
                result = Py_NewRef(Py_None);
                break;
            case MSGPACK_FALSE:
                result = Py_NewRef(Py_False);
                break;
            case MSGPACK_TRUE:
                result = Py_NewRef(Py_True);
                break;
            case MSGPACK_BIN1:
                result = _PyBytes_Unpack_(msg, off, 1);
                break;
            case MSGPACK_BIN2:
                result = _PyBytes_Unpack_(msg, off, 2);
                break;
            case MSGPACK_BIN4:
                result = _PyBytes_Unpack_(msg, off, 4);
                break;
            case MSGPACK_EXT1:
                result = _Extension_Unpack_(registry, msg, off, 1);
                break;
            case MSGPACK_EXT2:
                result = _Extension_Unpack_(registry, msg, off, 2);
                break;
            case MSGPACK_EXT4:
                result = _Extension_Unpack_(registry, msg, off, 4);
                break;
            case MSGPACK_FLOAT4:
                result = _PyFloat_Unpack(msg, off, 4);
                break;
            case MSGPACK_FLOAT8:
                result = _PyFloat_Unpack(msg, off, 8);
                break;
            case MSGPACK_UINT1:
                result = _PyUnsignedLong_Unpack(msg, off, 1);
                break;
            case MSGPACK_UINT2:
                result = _PyUnsignedLong_Unpack(msg, off, 2);
                break;
            case MSGPACK_UINT4:
                result = _PyUnsignedLong_Unpack(msg, off, 4);
                break;
            case MSGPACK_UINT8:
                result = _PyUnsignedLong_Unpack(msg, off, 8);
                break;
            case MSGPACK_INT1:
                result = _PyLong_Unpack(msg, off, 1);
                break;
            case MSGPACK_INT2:
                result = _PyLong_Unpack(msg, off, 2);
                break;
            case MSGPACK_INT4:
                result = _PyLong_Unpack(msg, off, 4);
                break;
            case MSGPACK_INT8:
                result = _PyLong_Unpack(msg, off, 8);
                break;
            case MSGPACK_FIXEXT1:
                result = _Extension_Unpack(registry, msg, off, 1);
                break;
            case MSGPACK_FIXEXT2:
                result = _Extension_Unpack(registry, msg, off, 2);
                break;
            case MSGPACK_FIXEXT4:
                result = _Extension_Unpack(registry, msg, off, 4);
                break;
            case MSGPACK_FIXEXT8:
                result = _Extension_Unpack(registry, msg, off, 8);
                break;
            case MSGPACK_FIXEXT16:
                result = _Extension_Unpack(registry, msg, off, 16);
                break;
            case MSGPACK_STR1:
                result = _PyUnicode_Unpack_(msg, off, 1);
                break;
            case MSGPACK_STR2:
                result = _PyUnicode_Unpack_(msg, off, 2);
                break;
            case MSGPACK_STR4:
                result = _PyUnicode_Unpack_(msg, off, 4);
                break;
            case MSGPACK_ARRAY2:
                result = _PyTuple_Unpack_(registry, msg, off, 2);
                break;
            case MSGPACK_ARRAY4:
                result = _PyTuple_Unpack_(registry, msg, off, 4);
                break;
            case MSGPACK_MAP2:
                result = _PyDict_Unpack_(registry, msg, off, 2);
                break;
            case MSGPACK_MAP4:
                result = _PyDict_Unpack_(registry, msg, off, 4);
                break;
            default:
                _PyErr_UnknownType_(NULL, type);
                break;
        }
    }
    return result;
}
