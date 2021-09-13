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


/* --------------------------------------------------------------------------
   module
   -------------------------------------------------------------------------- */

/* msgpack.pack() */
PyDoc_STRVAR(msgpack_pack_doc,
"pack(obj) -> msg");

static PyObject *
msgpack_pack(PyObject *module, PyObject *obj)
{
    PyObject *msg = NULL;

    if ((msg = NewMessage()) && PackObject(msg, obj)) {
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
    module_state *state = NULL;

    if (
        !(state = _PyModule_GetState(module)) ||
        RegisterObject(state->registry, obj)
    ) {
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
        result = UnpackMessage(&msg, &off);
        PyBuffer_Release(&msg);
    }
    return result;
}


/* msgpack_def.m_methods */
static PyMethodDef msgpack_m_methods[] = {
    {
        "pack", (PyCFunction)msgpack_pack,
        METH_O, msgpack_pack_doc
    },
    {
        "register", (PyCFunction)msgpack_register,
        METH_O, msgpack_register_doc
    },
    {
        "unpack", (PyCFunction)msgpack_unpack,
        METH_VARARGS, msgpack_unpack_doc
    },
    {NULL} /* Sentinel */
};


/* msgpack_def.m_traverse */
static int
msgpack_m_traverse(PyObject *module, visitproc visit, void *arg)
{
    module_state *state = NULL;

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
    module_state *state = NULL;

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
    .m_name = "msgpack",
    .m_doc = "Python MessagePack implementation",
    .m_size = sizeof(module_state),
    .m_methods = msgpack_m_methods,
    .m_traverse = (traverseproc)msgpack_m_traverse,
    .m_clear = (inquiry)msgpack_m_clear,
    .m_free = (freefunc)msgpack_m_free,
};


/* get module state */
module_state *
module_get_state(void)
{
    return (module_state *)_PyModuleDef_GetState(&msgpack_def);
}


static inline int
_module_state_init(PyObject *module)
{
    module_state *state = NULL;

    if (
        !(state = _PyModule_GetState(module)) ||
        !(state->registry = PyDict_New()) ||
        RegisterObject(state->registry, Py_NotImplemented) ||
        RegisterObject(state->registry, Py_Ellipsis)
    ) {
        return -1;
    }
    return 0;
}


static inline int
_module_init(PyObject *module)
{
    if (
        _module_state_init(module) ||
        PyModule_AddStringConstant(module, "__version__", PKG_VERSION) ||
        _PyModule_AddType(module, "Timestamp", &Timestamp_Type)
    ) {
        return -1;
    }
    return 0;
}


/* module initialization */
PyMODINIT_FUNC
PyInit_msgpack(void)
{
    PyObject *module = NULL;

    if ((module = PyState_FindModule(&msgpack_def))) {
        Py_INCREF(module);
    }
    else if ((module = PyModule_Create(&msgpack_def)) && _module_init(module)) {
        Py_CLEAR(module);
    }
    return module;
}
