#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdlib.h>
#include <stdio.h>
#include "calculations.h"
SQLITE_EXTENSION_INIT1

typedef struct {
    PyThreadState *tstate;
    PyObject *pModule;
} PythonConnectionData;

static void pythonConnectionDestructor(void *p) {
    PythonConnectionData *data = (PythonConnectionData*)p;
    if (data) {
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(data->pModule);
        PyThreadState_Swap(data->tstate);
        Py_EndInterpreter(data->tstate);
        PyThreadState_Swap(NULL);
        PyGILState_Release(gstate);
        sqlite3_free(data);
    }
}

// Function to get or create the PythonConnectionData for the current connection
static PythonConnectionData* getPythonConnectionData(sqlite3 *db) {
    static int aux_index = 0;
    PythonConnectionData *data = sqlite3_get_auxdata(db, aux_index);

    if (!data) {
        data = sqlite3_malloc(sizeof(PythonConnectionData));
        if (!data) return NULL;

        PyGILState_STATE gstate = PyGILState_Ensure();
        PyThreadState *mainThreadState = PyThreadState_Get();
        data->tstate = Py_NewInterpreter();
        PyThreadState_Swap(mainThreadState);

        const char *module_name = "calculations";
        PyThreadState_Swap(data->tstate);
        PyObject *pName = PyUnicode_DecodeFSDefault(module_name);
        data->pModule = PyImport_Import(pName);
        Py_DECREF(pName);
        PyThreadState_Swap(mainThreadState);

        if (!data->pModule) {
            PyThreadState_Swap(data->tstate);
            PyErr_Print();
            PyThreadState_Swap(mainThreadState);
            Py_EndInterpreter(data->tstate);
            sqlite3_free(data);
            PyGILState_Release(gstate);
            return NULL;
        }

        PyGILState_Release(gstate);
        sqlite3_set_auxdata(db, aux_index, data, pythonConnectionDestructor);
    }

    return data;
}

char* call_python_func(sqlite3 *db, const char* func_name, const char* arg1, const char* arg2) {
    char *result = NULL;
    PyObject *pFunc, *pArgs, *pValue;

    PythonConnectionData *data = getPythonConnectionData(db);
    if (!data) return NULL;

    PyGILState_STATE gstate = PyGILState_Ensure();
    PyThreadState *mainThreadState = PyThreadState_Swap(data->tstate);

    pFunc = PyObject_GetAttrString(data->pModule, func_name);

    if (pFunc && PyCallable_Check(pFunc)) {
        if (arg2 == NULL) {
            pArgs = PyTuple_Pack(1, PyUnicode_DecodeFSDefault(arg1));
        } else {
            pArgs = PyTuple_Pack(2, PyUnicode_DecodeFSDefault(arg1), PyUnicode_DecodeFSDefault(arg2));
        }

        pValue = PyObject_CallObject(pFunc, pArgs);
        Py_DECREF(pArgs);

        if (pValue != NULL) {
            PyObject *str_obj = PyObject_Str(pValue);
            const char *str_result = PyUnicode_AsUTF8(str_obj);
            result = strdup(str_result);
            Py_DECREF(str_obj);
            Py_DECREF(pValue);
        }
    }
    Py_XDECREF(pFunc);

    if (PyErr_Occurred()) {
        PyErr_Print();
    }

    PyThreadState_Swap(mainThreadState);
    PyGILState_Release(gstate);
    return result;
}
int call_bool_python_func(sqlite3 *db, const char* func_name, const char* arg1, const char* arg2) {
    int result = 0;
    PyObject *pFunc, *pArgs, *pValue;

    PythonConnectionData *data = getPythonConnectionData(db);
    if (!data) return result;

    PyGILState_STATE gstate = PyGILState_Ensure();
    PyThreadState *mainThreadState = PyThreadState_Swap(data->tstate);

    pFunc = PyObject_GetAttrString(data->pModule, func_name);

    if (pFunc && PyCallable_Check(pFunc)) {
        if (arg2 == NULL) {
            pArgs = PyTuple_Pack(1, PyUnicode_DecodeFSDefault(arg1));
        } else {
            pArgs = PyTuple_Pack(2, PyUnicode_DecodeFSDefault(arg1), PyUnicode_DecodeFSDefault(arg2));
        }

        pValue = PyObject_CallObject(pFunc, pArgs);
        Py_DECREF(pArgs);

        if (pValue != NULL) {
            if (PyObject_IsTrue(pValue)) {
                result = 1;
            } else {
                result = 0;
            }
            Py_DECREF(pValue);
        }
    }
    Py_XDECREF(pFunc);

    if (PyErr_Occurred()) {
        PyErr_Print();
    }

    PyThreadState_Swap(mainThreadState);
    PyGILState_Release(gstate);
    return result;
}

// _pre_nile_tokens_per_day is a non-sqlite3 function that is called by pre_nile_tokens_per_day
char* _pre_nile_tokens_per_day(sqlite3 *db, const char* tokens) {
    return call_python_func(db, "preNileTokensPerDay", tokens, NULL);
}
void pre_nile_tokens_per_day(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "pre_nile_tokens_per_day() requires exactly one argument", -1);
        return;
    }
    const char* input = (const char*)sqlite3_value_text(argv[0]);
    if (!input) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _pre_nile_tokens_per_day(db, input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
    free(tokens);
}

char* _amazon_staker_token_rewards(sqlite3 *db, const char* sp, const char* tpd) {
    return call_python_func(db, "amazonStakerTokenRewards", sp, tpd);
}
void amazon_staker_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "amazon_staker_token_rewards() requires two arguments", -1);
        return;
    }
    const char* sp = (const char*)sqlite3_value_text(argv[0]);
    if (!sp) {
        sqlite3_result_null(context);
        return;
    }

    const char* tpd = (const char*)sqlite3_value_text(argv[1]);
    if (!tpd) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _amazon_staker_token_rewards(db, sp, tpd);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _nile_staker_token_rewards(sqlite3 *db, const char* sp, const char* tpd) {
    return call_python_func(db, "nileStakerTokenRewards", sp, tpd);
}
void nile_staker_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "nile_staker_token_rewards() requires two arguments", -1);
        return;
    }
    const char* sp = (const char*)sqlite3_value_text(argv[0]);
    if (!sp) {
        sqlite3_result_null(context);
        return;
    }

    const char* tpd = (const char*)sqlite3_value_text(argv[1]);
    if (!tpd) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _nile_staker_token_rewards(db, sp, tpd);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _staker_token_rewards(sqlite3 *db, const char* sp, const char* tpd) {
    return call_python_func(db, "stakerTokenRewards", sp, tpd);
}
void staker_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "staker_token_rewards() requires two arguments", -1);
        return;
    }
    const char* sp = (const char*)sqlite3_value_text(argv[0]);
    if (!sp) {
        sqlite3_result_null(context);
        return;
    }

    const char* tpd = (const char*)sqlite3_value_text(argv[1]);
    if (!tpd) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _staker_token_rewards(db, sp, tpd);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _amazon_operator_token_rewards(sqlite3 *db, const char* totalStakerOperatorTokens) {
    return call_python_func(db, "amazonOperatorTokenRewards", totalStakerOperatorTokens, NULL);
}
void amazon_operator_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "amazon_operator_token_rewards() requires exactly one argument", -1);
        return;
    }
    const char* input = (const char*)sqlite3_value_text(argv[0]);
    if (!input) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _amazon_operator_token_rewards(db, input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _nile_operator_token_rewards(sqlite3 *db, const char* totalStakerOperatorTokens) {
    return call_python_func(db, "nileOperatorTokenRewards", totalStakerOperatorTokens, NULL);
}
void nile_operator_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "nile_operator_token_rewards() requires exactly one argument", -1);
        return;
    }
    const char* input = (const char*)sqlite3_value_text(argv[0]);
    if (!input) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _nile_operator_token_rewards(db, input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _operator_token_rewards(sqlite3 *db, const char* totalStakerOperatorTokens) {
    return call_python_func(db, "operatorTokenRewards", totalStakerOperatorTokens, NULL);
}
void operator_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "operator_token_rewards() requires exactly one argument", -1);
        return;
    }
    const char* input = (const char*)sqlite3_value_text(argv[0]);
    if (!input) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _operator_token_rewards(db, input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


int _big_gt(sqlite3 *db, const char* a, const char* b) {
    return call_bool_python_func(db, "bigGt", a, b);
}
void big_gt(sqlite3_context *context, int argc, sqlite3_value **argv){
    if (argc != 2) {
        sqlite3_result_error(context, "big_gt() requires exactly two arguments", -1);
        return;
    }
    const char* sp = (const char*)sqlite3_value_text(argv[0]);
    if (!sp) {
        sqlite3_result_null(context);
        return;
    }

    const char* tpd = (const char*)sqlite3_value_text(argv[1]);
    if (!tpd) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    int is_greater = _big_gt(db, sp, tpd);

    sqlite3_result_int(context, is_greater ? 1 : 0);
}


char* _sum_big_c(sqlite3 *db, const char* a, const char* b) {
    return call_python_func(db, "sumBigC", a, b);
}
void sum_big_c(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "_sum_big_c() requires two arguments", -1);
        return;
    }
    const char* a = (const char*)sqlite3_value_text(argv[0]);
    if (!a) {
        sqlite3_result_null(context);
        return;
    }

    const char* b = (const char*)sqlite3_value_text(argv[1]);
    if (!b) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _sum_big_c(db, a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _numeric_multiply_c(sqlite3 *db, const char* a, const char* b) {
    return call_python_func(db, "numericMultiplyC", a, b);
}
void numeric_multiply_c(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "_numeric_multiply_c() requires two arguments", -1);
        return;
    }
    const char* a = (const char*)sqlite3_value_text(argv[0]);
    if (!a) {
        sqlite3_result_null(context);
        return;
    }

    const char* b = (const char*)sqlite3_value_text(argv[1]);
    if (!b) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _numeric_multiply_c(db, a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

typedef struct SumContext {
    char* current_sum;
} SumContext;

static void sum_big_step(sqlite3_context* context, int argc, sqlite3_value** argv) {
    SumContext* ctx = (SumContext*)sqlite3_aggregate_context(context, sizeof(SumContext));

    if (argc != 1) {
        sqlite3_result_error(context, "sum_big_c() requires one argument", -1);
        return;
    }

    const char* value = (const char*)sqlite3_value_text(argv[0]);
    if (!value) {
        return; // Skip NULL values
    }

    if (!ctx->current_sum) {
        ctx->current_sum = strdup(value);
    } else {
        sqlite3 *db = sqlite3_context_db_handle(context);
        char* new_sum = _sum_big_c(db, ctx->current_sum, value);
        free(ctx->current_sum);
        ctx->current_sum = new_sum;
    }
}

static void sum_big_finalize(sqlite3_context* context) {
    SumContext* ctx = (SumContext*)sqlite3_aggregate_context(context, sizeof(SumContext));

    if (ctx && ctx->current_sum) {
        sqlite3_result_text(context, ctx->current_sum, -1, SQLITE_TRANSIENT);
        free(ctx->current_sum);
    } else {
        sqlite3_result_null(context);
    }
}


char* _calculate_staker_proportion(sqlite3 *db, const char* stakerWeight, const char* totalWeight) {
    return call_python_func(db, "calculateStakerProportion", stakerWeight, totalWeight);
}
void calculate_staker_proportion(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "_numeric_multiply_c() requires two arguments", -1);
        return;
    }
    const char* stakerWeight = (const char*)sqlite3_value_text(argv[0]);
    if (!stakerWeight) {
        sqlite3_result_null(context);
        return;
    }

    const char* totalWeight = (const char*)sqlite3_value_text(argv[1]);
    if (!totalWeight) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _calculate_staker_proportion(db, stakerWeight, totalWeight);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _subtract_big(sqlite3 *db, const char* a, const char* b) {
    return call_python_func(db, "subtractBig", a, b);
}
void subtract_big(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "subtract_big() requires two arguments", -1);
        return;
    }
    const char* a = (const char*)sqlite3_value_text(argv[0]);
    if (!a) {
        sqlite3_result_null(context);
        return;
    }

    const char* b = (const char*)sqlite3_value_text(argv[1]);
    if (!b) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _subtract_big(db, a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _add_big(sqlite3 *db, const char* a, const char* b) {
    return call_python_func(db, "addBig", a, b);
}
void add_big(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "subtract_big() requires two arguments", -1);
        return;
    }
    const char* a = (const char*)sqlite3_value_text(argv[0]);
    if (!a) {
        sqlite3_result_null(context);
        return;
    }

    const char* b = (const char*)sqlite3_value_text(argv[1]);
    if (!b) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _add_big(db, a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _calc_tokens_per_day(sqlite3 *db, const char* amount, const char* duration) {
    return call_python_func(db, "calcTokensPerDay", amount, duration);
}
void calc_tokens_per_day(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "calc_tokens_per_day() requires two arguments", -1);
        return;
    }
    const char* amount = (const char*)sqlite3_value_text(argv[0]);
    if (!amount) {
        sqlite3_result_null(context);
        return;
    }

    const char* duration = (const char*)sqlite3_value_text(argv[1]);
    if (!duration) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _calc_tokens_per_day(db, amount, duration);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}


char* _calc_tokens_per_day_decimal(sqlite3 *db, const char* amount, const char* duration) {
    return call_python_func(db, "calcTokensPerDayDecimal", amount, duration);
}
void calc_tokens_per_day_decimal(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "calc_tokens_per_day_decimal() requires two arguments", -1);
        return;
    }
    const char* amount = (const char*)sqlite3_value_text(argv[0]);
    if (!amount) {
        sqlite3_result_null(context);
        return;
    }

    const char* duration = (const char*)sqlite3_value_text(argv[1]);
    if (!duration) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(context);
    char* tokens = _calc_tokens_per_day(db, amount, duration);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

int sqlite3_calculations_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);

    if (!Py_IsInitialized()) {
        Py_Initialize();
        PyEval_InitThreads();  // This is called automatically in Python 3.7+
    }

    int rc;
    rc = sqlite3_create_function(db, "pre_nile_tokens_per_day", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, pre_nile_tokens_per_day, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "amazon_staker_token_rewards", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, amazon_staker_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "nile_staker_token_rewards", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, nile_staker_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "staker_token_rewards", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, staker_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "amazon_operator_token_rewards", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, amazon_operator_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "nile_operator_token_rewards", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, nile_operator_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "operator_token_rewards", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, operator_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "big_gt", 2, SQLITE_DETERMINISTIC, 0, big_gt, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "numeric_multiply_c", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, numeric_multiply_c, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "sum_big_c", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, NULL, sum_big_step, sum_big_finalize);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "calculate_staker_proportion", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, calculate_staker_proportion, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "subtract_big", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, subtract_big, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "add_big", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, add_big, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "calc_tokens_per_day", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, calc_tokens_per_day, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "calc_tokens_per_day_decimal", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, calc_tokens_per_day_decimal, 0, 0);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("Failed to create function: %s", sqlite3_errmsg(db));
        return rc;
    }

    return SQLITE_OK;
}

void sqlite3_calculations_shutdown(void) {
    if (Py_IsInitialized()) {
        PyGILState_Ensure();
        Py_Finalize();
    }
}
