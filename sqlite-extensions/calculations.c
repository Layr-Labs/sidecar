#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdatomic.h>
#include "calculations.h"
SQLITE_EXTENSION_INIT1

static PyObject *pModule = NULL;
atomic_int python_initialized;

void init_python_initialized(void) {
    atomic_init(&python_initialized, 0);
}

int ensure_python_initialized() {
    int expected = 0;
    if (atomic_compare_exchange_strong(&python_initialized, &expected, 1)) {
        Py_Initialize();
        // As of Python 3.7, the GIL is created automatically,
        // and threads are initialized by default
    }
    return atomic_load(&python_initialized);
    //if (!python_initialized) {
    //    Py_Initialize();
    //    // As of Python 3.7, the GIL is created automatically,
    //    // and threads are initialized by default
    //    python_initialized = 1;
    //}
    //return python_initialized;
}

void finalize_python() {
    //// Reacquire the GIL before finalizing
    //PyGILState_STATE gstate = PyGILState_Ensure();
    //Py_XDECREF(pModule);
    //Py_Finalize();
    int expected = 1;
    if (atomic_compare_exchange_strong(&python_initialized, &expected, 0)) {
        // Reacquire the GIL before finalizing
        PyGILState_STATE gstate = PyGILState_Ensure();
        Py_XDECREF(pModule);
        Py_Finalize();
        PyGILState_Release(gstate);
    }
}

char* call_python_func(const char* func_name, const char* arg1, const char* arg2) {
    const char *module_name = "calculations";
    char *result = NULL;
    PyGILState_STATE gstate;
    PyObject *pName, *pModule, *pFunc, *pArgs, *pValue;

    if (!ensure_python_initialized()) {
        return NULL;
    }

    gstate = PyGILState_Ensure();

    pName = PyUnicode_DecodeFSDefault(module_name);
    pModule = PyImport_Import(pName);
    Py_DECREF(pName);

    if (pModule != NULL) {
        pFunc = PyObject_GetAttrString(pModule, func_name);

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
        Py_DECREF(pModule);
    }

    if (PyErr_Occurred()) {
        PyErr_Print();
    }

    PyGILState_Release(gstate);
    return result;
}
int call_bool_python_func(const char* func_name, const char* arg1, const char* arg2) {
    const char *module_name = "calculations";
    int result = 0;
    PyGILState_STATE gstate;
    PyObject *pName, *pModule, *pFunc, *pArgs, *pValue;

    if (!ensure_python_initialized()) {
        return 0;
    }

    gstate = PyGILState_Ensure();

    pName = PyUnicode_DecodeFSDefault(module_name);
    pModule = PyImport_Import(pName);
    Py_DECREF(pName);

    if (pModule != NULL) {
        pFunc = PyObject_GetAttrString(pModule, func_name);

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
        Py_DECREF(pModule);
    }

    if (PyErr_Occurred()) {
        PyErr_Print();
    }

    PyGILState_Release(gstate);
    return result;
}

// _pre_nile_tokens_per_day is a non-sqlite3 function that is called by pre_nile_tokens_per_day
char* _pre_nile_tokens_per_day(const char* tokens) {
    return call_python_func("preNileTokensPerDay", tokens, NULL);
}

// Your custom function
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

    char* tokens = _pre_nile_tokens_per_day(input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _amazon_staker_token_rewards(const char* sp, const char* tpd) {
    return call_python_func("amazonStakerTokenRewards", sp, tpd);
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

    char* tokens = _amazon_staker_token_rewards(sp, tpd);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _nile_staker_token_rewards(const char* sp, const char* tpd) {
    return call_python_func("nileStakerTokenRewards", sp, tpd);
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

    char* tokens = _nile_staker_token_rewards(sp, tpd);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _staker_token_rewards(const char* sp, const char* tpd) {
    return call_python_func("stakerTokenRewards", sp, tpd);
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

    char* tokens = _staker_token_rewards(sp, tpd);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _amazon_operator_token_rewards(const char* totalStakerOperatorTokens) {
    return call_python_func("amazonOperatorTokenRewards", totalStakerOperatorTokens, NULL);
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

    char* tokens = _amazon_operator_token_rewards(input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _nile_operator_token_rewards(const char* totalStakerOperatorTokens) {
    return call_python_func("nileOperatorTokenRewards", totalStakerOperatorTokens, NULL);
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

    char* tokens = _nile_operator_token_rewards(input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _operator_token_rewards(const char* totalStakerOperatorTokens) {
    return call_python_func("operatorTokenRewards", totalStakerOperatorTokens, NULL);
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

    char* tokens = _operator_token_rewards(input);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

int _big_gt(const char* a, const char* b) {
    return call_bool_python_func("bigGt", a, b);
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

    int is_greater = _big_gt(sp, tpd);

    sqlite3_result_int(context, is_greater ? 1 : 0);
}

char* _sum_big_c(const char* a, const char* b) {
    return call_python_func("sumBigC", a, b);
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

    char* tokens = _sum_big_c(a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _numeric_multiply_c(const char* a, const char* b) {
    return call_python_func("numericMultiplyC", a, b);
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

    char* tokens = _numeric_multiply_c(a, b);
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
        char* new_sum = _sum_big_c(ctx->current_sum, value);
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

char* _calculate_staker_proportion(const char* stakerWeight, const char* totalWeight) {
    return call_python_func("calculateStakerProportion", stakerWeight, totalWeight);
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

    char* tokens = _calculate_staker_proportion(stakerWeight, totalWeight);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _subtract_big(const char* a, const char* b) {
    return call_python_func("subtractBig", a, b);
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

    char* tokens = _subtract_big(a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _add_big(const char* a, const char* b) {
    return call_python_func("addBig", a, b);
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

    char* tokens = _add_big(a, b);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _calc_tokens_per_day(const char* amount, const char* duration) {
    return call_python_func("calcTokensPerDay", amount, duration);
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

    char* tokens = _calc_tokens_per_day(amount, duration);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

char* _calc_tokens_per_day_decimal(const char* amount, const char* duration) {
    return call_python_func("calcTokensPerDayDecimal", amount, duration);
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

    char* tokens = _calc_tokens_per_day(amount, duration);
    if (!tokens) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_text(context, tokens, -1, SQLITE_TRANSIENT);
}

int sqlite3_calculations_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);

    init_python_initialized();

    int rc;
    rc = sqlite3_create_function(db, "pre_nile_tokens_per_day", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, pre_nile_tokens_per_day, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "amazon_staker_token_rewards", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, amazon_staker_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "nile_staker_token_rewards", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, nile_staker_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "staker_token_rewards", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, staker_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "amazon_operator_token_rewards", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, amazon_operator_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "nile_operator_token_rewards", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, nile_operator_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "operator_token_rewards", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, operator_token_rewards, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "big_gt", 2, SQLITE_DETERMINISTIC, 0, big_gt, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "numeric_multiply_c", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, numeric_multiply_c, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "sum_big_c", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, NULL, sum_big_step, sum_big_finalize);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "calculate_staker_proportion", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, calculate_staker_proportion, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "subtract_big", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, subtract_big, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "add_big", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, add_big, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "calc_tokens_per_day", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, calc_tokens_per_day, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_create_function(db, "calc_tokens_per_day_decimal", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, calc_tokens_per_day_decimal, 0, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create function: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    return SQLITE_OK;
}

void sqlite3_calculations_shutdown(void) {
    if (python_initialized) {
        PyGILState_Ensure();
        Py_Finalize();
        python_initialized = 0;
    }
}
