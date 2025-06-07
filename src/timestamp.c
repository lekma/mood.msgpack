/*
https://en.cppreference.com/w/c/chrono/timespec
and
https://pubs.opengroup.org/onlinepubs/007908799/xsh/realtime.html

    Clocks and Timers
    ...
    Time Value Specification Structures
    ...
    The tv_nsec member is only valid if greater than or equal to zero, and less
    than the number of nanoseconds in a second (1000 million).
*/


#include "msgpack.h"


#define MSGPACK_NSECS_MAX 1e9


static inline int
_cmp_(int64_t a, int64_t b)
{
    return (a > b) - (a < b);
}


/* --------------------------------------------------------------------------
   Timestamp
   -------------------------------------------------------------------------- */

static PyObject *
_Timestamp_New(PyTypeObject *type, int64_t seconds, uint32_t nanoseconds)
{
    Timestamp *self = NULL;

    if (nanoseconds < MSGPACK_NSECS_MAX) {
        if ((self = PyObject_GC_NEW(Timestamp, type))) {
            self->seconds = seconds;
            self->nanoseconds = nanoseconds;
            PyObject_GC_Track(self);
        }
    }
    else {
        PyErr_SetString(
            PyExc_OverflowError, "argument 'nanoseconds' greater than maximum"
        );
    }
    return _PyObject_CAST(self);
}


static PyObject *
_Timestamp_FromPyFloat(PyObject *type, PyObject *timestamp)
{
    static const double _int64_max_ = INT64_MAX; // INT64_MAX + 1
    static const double _int64_min_ = INT64_MIN;
    double value = PyFloat_AS_DOUBLE(timestamp), seconds;
    volatile double nanoseconds;

    nanoseconds = round(modf(value, &seconds) * MSGPACK_NSECS_MAX);
    if (nanoseconds >= MSGPACK_NSECS_MAX) {
        nanoseconds -= MSGPACK_NSECS_MAX;
        seconds += 1.0;
    }
    else if (nanoseconds < 0) {
        nanoseconds += MSGPACK_NSECS_MAX;
        seconds -= 1.0;
    }
    if ((seconds < _int64_min_) || (_int64_max_ <= seconds)) {
        PyErr_SetString(PyExc_OverflowError, "timestamp out of range");
        return NULL;
    }
    return NewTimestamp(type, (int64_t)seconds, (uint32_t)nanoseconds);
}


static PyObject *
_Timestamp_FromPyLong(PyObject *type, PyObject *timestamp)
{
    int64_t seconds = 0;

    if (((seconds = PyLong_AsLongLong(timestamp)) == -1) && PyErr_Occurred()) {
        return NULL;
    }
    return NewTimestamp(type, seconds, 0);
}


static PyObject *
_Timestamp_Compare(Timestamp *self, Timestamp *other, int op)
{
    int cmp = 0, res = -1;

    if (!(cmp = _cmp_(self->seconds, other->seconds))) {
        cmp = _cmp_(self->nanoseconds, other->nanoseconds);
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

/* Timestamp_Type.tp_new */
static PyObject *
Timestamp_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"seconds", "nanoseconds", NULL};
    int64_t seconds;
    uint32_t nanoseconds = 0;

    if (
        !PyArg_ParseTupleAndKeywords(
            args, kwargs, "L|I:__new__", kwlist, &seconds, &nanoseconds
        )
    ) {
        return NULL;
    }
    return _Timestamp_New(type, seconds, nanoseconds);
}


/* Timestamp_Type.tp_traverse */
static int
Timestamp_tp_traverse(Timestamp *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self)); // heap type
    return 0;
}


/* Timestamp_Type.tp_clear */
static int
Timestamp_tp_clear(Timestamp *self)
{
    return 0;
}


/* Timestamp_Type.tp_dealloc */
static void
Timestamp_tp_dealloc(Timestamp *self)
{
    PyObject_GC_UnTrack(self);
    Timestamp_tp_clear(self);
    PyTypeObject *type = Py_TYPE(self);
    PyObject_GC_Del(self);
    Py_XDECREF(type); // heap type
}


/* Timestamp_Type.tp_repr */
static PyObject *
Timestamp_tp_repr(Timestamp *self)
{
    return PyUnicode_FromFormat(
        "%s(seconds=%lld, nanoseconds=%09u)",
        Py_TYPE(self)->tp_name, self->seconds, self->nanoseconds
    );
}


/* Timestamp_Type.tp_richcompare */
static PyObject *
Timestamp_tp_richcompare(Timestamp *self, PyObject *other, int op)
{
    /*if (Py_TYPE(other) == &Timestamp_Type) {*/
    if (Py_TYPE(self) == Py_TYPE(other)) {
        return _Timestamp_Compare(self, (Timestamp *)other, op);
    }
    Py_RETURN_NOTIMPLEMENTED;
}


/* Timestamp.fromtimestamp() */
PyDoc_STRVAR(Timestamp_fromtimestamp_doc,
"@classmethod\n\
fromtimestamp(timestamp) -> Timestamp");

static PyObject *
Timestamp_fromtimestamp(PyObject *type, PyObject *timestamp)
{
    PyObject *result = NULL;

    if (PyFloat_Check(timestamp)) {
        result = _Timestamp_FromPyFloat(type, timestamp);
    }
    else if (PyLong_Check(timestamp)) {
        result = _Timestamp_FromPyLong(type, timestamp);
    }
    else {
        PyErr_Format(
            PyExc_TypeError, "expected a 'float' or an 'int', got: '%.200s'",
            Py_TYPE(timestamp)->tp_name
        );
    }
    return result;
}


/* Timestamp.timestamp() */
PyDoc_STRVAR(Timestamp_timestamp_doc,
"timestamp() -> float");

static PyObject *
Timestamp_timestamp(Timestamp *self)
{
    return PyFloat_FromDouble(
        self->seconds + (self->nanoseconds / MSGPACK_NSECS_MAX)
    );
}


/* TimestampType.tp_methods */
static PyMethodDef Timestamp_tp_methods[] = {
    {
        "fromtimestamp", (PyCFunction)Timestamp_fromtimestamp,
        METH_O | METH_CLASS, Timestamp_fromtimestamp_doc
    },
    {
        "timestamp", (PyCFunction)Timestamp_timestamp,
        METH_NOARGS, Timestamp_timestamp_doc
    },
    {NULL}  /* Sentinel */
};


/* Timestamp_Type.tp_members */
static PyMemberDef Timestamp_tp_members[] = {
    {
        "seconds", T_LONGLONG, offsetof(Timestamp, seconds),
        READONLY, NULL
    },
    {
        "nanoseconds", T_UINT, offsetof(Timestamp, nanoseconds),
        READONLY, NULL
    },
    {NULL}  /* Sentinel */
};


static PyType_Slot Timestamp_Slots[] = {
    {Py_tp_doc, "Timestamp(seconds[, nanoseconds=0])"},
    {Py_tp_new, Timestamp_tp_new},
    {Py_tp_traverse, Timestamp_tp_traverse},
    {Py_tp_clear, Timestamp_tp_clear},
    {Py_tp_dealloc, Timestamp_tp_dealloc},
    {Py_tp_repr, Timestamp_tp_repr},
    {Py_tp_richcompare, Timestamp_tp_richcompare},
    {Py_tp_methods, Timestamp_tp_methods},
    {Py_tp_members, Timestamp_tp_members},
    {0, NULL}
};


PyType_Spec Timestamp_Spec = {
    .name = "mood.msgpack.Timestamp",
    .basicsize = sizeof(Timestamp),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
    .slots = Timestamp_Slots
};


/* --------------------------------------------------------------------------
   interface
   -------------------------------------------------------------------------- */

PyObject *
NewTimestamp(PyObject *type, int64_t seconds, uint32_t nanoseconds)
{
    return _Timestamp_New((PyTypeObject *)type, seconds, nanoseconds);
}
