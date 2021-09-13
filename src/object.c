/*
#
# Copyright © 2021 Malek Hadj-Ali
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


#include "msgpack.h"


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
                PyErr_Format(
                    PyExc_TypeError,
                    "expected state key to be unicode, not '%.200s'",
                    Py_TYPE(key)->tp_name
                );
                Py_DECREF(key);
                break;
            }
            PyUnicode_InternInPlace(&key);
            /* __dict__ can be a dictionary or other mapping object
               https://docs.python.org/3.8/library/stdtypes.html#object.__dict__ */
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

    if (
        !(
            result = _PyObject_CallMethodIdObjArgs(
                self, &PyId___setstate__, arg, NULL
            )
        )
    ) {
        if (
            PyErr_ExceptionMatches(PyExc_AttributeError) &&
            PyDict_Check(arg)
        ) {
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

    if (
        (seq_methods = type->tp_as_sequence) && seq_methods->sq_inplace_concat
    ) {
        if ((result = seq_methods->sq_inplace_concat(self, arg))) {
            res = 0;
            Py_DECREF(result);
        }
    }
    else if (
        (num_methods = type->tp_as_number) && num_methods->nb_inplace_add
    ) {
        if ((result = num_methods->nb_inplace_add(self, arg))) {
            if (result != Py_NotImplemented) {
                res = 0;
            }
            Py_DECREF(result);
        }
    }
    if (res && !PyErr_Occurred()) {
        PyErr_Format(
            PyExc_TypeError, "cannot extend '%.200s' objects", type->tp_name
        );
    }
    return res;
}

static int
__PyObject_Extend(PyObject *self, PyObject *arg)
{
    _Py_IDENTIFIER(extend);
    PyObject *result = NULL;

    if (
        !(
            result = _PyObject_CallMethodIdObjArgs(
                self, &PyId_extend, arg, NULL
            )
        )
    ) {
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

    if (
        (result = PySequence_Fast(obj, message)) &&
        (PySequence_Fast_GET_SIZE(result) != len)
    ) {
        PyErr_Format(PyExc_ValueError, "expected a sequence of len %zd", len);
        Py_CLEAR(result);
    }
    return result;
}

static inline int
__PyObject_MergeFromIter(PyObject *self, PyObject *iter)
{
    PyObject *item = NULL, *fast = NULL;

    while ((item = PyIter_Next(iter))) {
        if (
            !(fast = __PySequence_Fast(item, 2, "not a sequence")) ||
            PyObject_SetItem(
                self,
                PySequence_Fast_GET_ITEM(fast, 0),
                PySequence_Fast_GET_ITEM(fast, 1)
            )
        ) {
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

    if (
        !(
            result = _PyObject_CallMethodIdObjArgs(
                self, &PyId_update, arg, NULL
            )
        )
    ) {
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
    PyErr_Format(
        PyExc_TypeError, "argument 1 must be a callable, not %.200s",
        Py_TYPE(arg)->tp_name
    );
    return 0;
}

PyObject *
__PyObject_New(PyObject *reduce)
{
    PyObject *callable, *args;
    PyObject *setstatearg = Py_None, *extendarg = Py_None, *updatearg = Py_None;
    PyObject *self = NULL;

    if (
        PyArg_ParseTuple(
            reduce, "O&O!|OOO",
            __PyCallable_Check, &callable, &PyTuple_Type, &args,
            &setstatearg, &extendarg, &updatearg
        ) &&
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
