package numbers

/*
#cgo darwin CFLAGS: -I/opt/homebrew/opt/python@3.12/Frameworks/Python.framework/Headers
#cgo darwin LDFLAGS: -L/opt/homebrew/opt/python@3.12/Frameworks/Python.framework/Versions/Current/lib -lpython3.12
#include <Python.h>
#include <stdlib.h>

PyObject* convert_to_python_string(const char* str) {
    return PyUnicode_FromString(str);
}

char* call_lossy_add(const char* tokens) {
    PyObject *pName, *pModule, *pFunc, *pArgs, *pValue;
    char* result = NULL;

    Py_Initialize();

    PyRun_SimpleString("from decimal import Decimal\n"
                       "def lossyAdd(tokens: str):\n"
                       "    big_amount = float(tokens)\n"
                       "    div = 0.999999999999999\n"
                       "    res = big_amount * div\n"
                       "    res_str = \"{}\".format(res)\n"
                       "    return \"{}\".format(int(Decimal(res_str)))\n");

    pName = PyUnicode_FromString("__main__");
    pModule = PyImport_Import(pName);
    Py_DECREF(pName);

    if (pModule != NULL) {
        pFunc = PyObject_GetAttrString(pModule, "lossyAdd");
        if (pFunc && PyCallable_Check(pFunc)) {
            pArgs = PyTuple_New(1);
            PyTuple_SetItem(pArgs, 0, convert_to_python_string(tokens));
            pValue = PyObject_CallObject(pFunc, pArgs);
            Py_DECREF(pArgs);
            if (pValue != NULL) {
                const char* temp = PyUnicode_AsUTF8(pValue);
                result = strdup(temp);
                Py_DECREF(pValue);
            }
        }
        Py_XDECREF(pFunc);
        Py_DECREF(pModule);
    }

    Py_Finalize();
    return result;
}
*/
import "C"
import (
	"unsafe"
)

// PreNileTokensPerDay calculates the tokens per day for pre-nile rewards, rounded to 15 sigfigs
//
// Not gonna lie, this is pretty annoying that it has to be this way, but in order to support backwards compatibility
// with the current/old rewards system where postgres was lossy, we have to do this.
func PreNileTokensPerDay(tokensPerDay string) (string, error) {
	cTokens := C.CString(tokensPerDay)
	defer C.free(unsafe.Pointer(cTokens))

	result := C.call_lossy_add(cTokens)
	defer C.free(unsafe.Pointer(result))

	return C.GoString(result), nil
}
