#ifndef Py_MOOD_MSGPACK_H
#define Py_MOOD_MSGPACK_H


#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#include "helpers/helpers.h"


#ifdef __cplusplus
extern "C" {
#endif


/* we need a 64bit type */
#if !defined(HAVE_LONG_LONG)
#error "mood.msgpack needs a long long integer type"
#endif /* HAVE_LONG_LONG */


/* endian.h is the POSIX way(?) */
#if defined(HAVE_ENDIAN_H)
#include <endian.h>
#elif defined(HAVE_SYS_ENDIAN_H)
#include <sys/endian.h>
#else
#error "mood.msgpack needs <endian.h> or <sys/endian.h>"
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


/* for use with Py_EnterRecursiveCall */
#define _While_(a, n) " while " #a " a " n


/* Timestamp */
typedef struct {
    PyObject_HEAD
    int64_t seconds;
    uint32_t nanoseconds;
} Timestamp;

extern PyTypeObject Timestamp_Type;


/* module state */
typedef struct {
    PyObject *registry;
} module_state;


/* interface */
PyObject *NewMessage(void);
int RegisterObject(PyObject *registry, PyObject *obj);
int PackObject(PyObject *msg, PyObject *obj);

PyObject *__PyObject_New(PyObject *reduce);
PyObject *UnpackMessage(PyObject *module, Py_buffer *msg, Py_ssize_t *off);

PyObject *NewTimestamp(int64_t seconds, uint32_t nanoseconds);


/* --------------------------------------------------------------------------
   msgpack definitions
   see https://github.com/msgpack/msgpack/blob/master/spec.md
   -------------------------------------------------------------------------- */

#define MSGPACK_UINT4_MAX (1LL << 32)
#define MSGPACK_INT4_MIN -(1LL << 31)

#define MSGPACK_UINT2_MAX (1LL << 16)
#define MSGPACK_INT2_MIN -(1LL << 15)

#define MSGPACK_UINT1_MAX (1LL << 8)
#define MSGPACK_INT1_MIN -(1LL << 7)

#define MSGPACK_FIXUINT_MAX (1LL << 7)
#define MSGPACK_FIXINT_MIN -(1LL << 5)

#define MSGPACK_FIXSTR_MAX (1LL << 5)
#define MSGPACK_FIXSTR_BIT 0x1f

#define MSGPACK_FIXOBJ_MAX (1LL << 4)
#define MSGPACK_FIXOBJ_BIT 0x0f


/* msgpack types */
enum {
    MSGPACK_FIXUINT     = 0x00,  //   0
    MSGPACK_FIXUINT_END = 0x7f,  // 127

    MSGPACK_FIXMAP     = 0x80,
    MSGPACK_FIXMAP_END = 0x8f,

    MSGPACK_FIXARRAY     = 0x90,
    MSGPACK_FIXARRAY_END = 0x9f,

    MSGPACK_FIXSTR     = 0xa0,
    MSGPACK_FIXSTR_END = 0xbf,

    MSGPACK_NIL     = 0xc0,
    MSGPACK_INVALID = 0xc1,      // invalid type
    MSGPACK_FALSE   = 0xc2,
    MSGPACK_TRUE    = 0xc3,

    MSGPACK_BIN1 = 0xc4,
    MSGPACK_BIN2 = 0xc5,
    MSGPACK_BIN4 = 0xc6,

    MSGPACK_EXT1 = 0xc7,
    MSGPACK_EXT2 = 0xc8,
    MSGPACK_EXT4 = 0xc9,

    MSGPACK_FLOAT4 = 0xca,
    MSGPACK_FLOAT8 = 0xcb,

    MSGPACK_UINT1 = 0xcc,
    MSGPACK_UINT2 = 0xcd,
    MSGPACK_UINT4 = 0xce,
    MSGPACK_UINT8 = 0xcf,

    MSGPACK_INT1 = 0xd0,
    MSGPACK_INT2 = 0xd1,
    MSGPACK_INT4 = 0xd2,
    MSGPACK_INT8 = 0xd3,

    MSGPACK_FIXEXT1  = 0xd4,
    MSGPACK_FIXEXT2  = 0xd5,
    MSGPACK_FIXEXT4  = 0xd6,
    MSGPACK_FIXEXT8  = 0xd7,
    MSGPACK_FIXEXT16 = 0xd8,

    MSGPACK_STR1 = 0xd9,
    MSGPACK_STR2 = 0xda,
    MSGPACK_STR4 = 0xdb,

    MSGPACK_ARRAY2 = 0xdc,
    MSGPACK_ARRAY4 = 0xdd,

    MSGPACK_MAP2 = 0xde,
    MSGPACK_MAP4 = 0xdf,

    MSGPACK_FIXINT     = 0xe0,   // -32
    MSGPACK_FIXINT_END = 0xff    //  -1
};


/* msgpack extensions types */
enum {
    MSGPACK_EXT_INVALID = 0x00,     // invalid extension type

    // python

    MSGPACK_EXT_PYCOMPLEX   = 0x01,
    MSGPACK_EXT_PYBYTEARRAY = 0x02,

    MSGPACK_EXT_PYLIST = 0x03,

    MSGPACK_EXT_PYSET       = 0x04,
    MSGPACK_EXT_PYFROZENSET = 0x05,

    MSGPACK_EXT_PYCLASS     = 0x06,
    MSGPACK_EXT_PYSINGLETON = 0x07,

    MSGPACK_EXT_PYOBJECT = 0x7f,    // last

    // msgpack

    MSGPACK_EXT_TIMESTAMP = 0xff    //  -1
};


#ifdef __cplusplus
}
#endif


#endif // !Py_MOOD_MSGPACK_H
