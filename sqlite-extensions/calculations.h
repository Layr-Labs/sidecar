#ifndef CALCULATIONS_H
#define CALCULATIONS_H

#include <sqlite3ext.h>

extern atomic_int python_initialized;
int ensure_python_initialized(void);
void finalize_python(void);

char* call_python_func(const char* func_name, const char* arg1, const char* arg2);
int call_bool_python_func(const char* func_name, const char* arg1, const char* arg2);

char* _pre_nile_tokens_per_day(const char* tokens);
void pre_nile_tokens_per_day(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _amazon_staker_token_rewards(const char* sp, const char* tpd);
void amazon_staker_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _nile_staker_token_rewards(const char* sp, const char* tpd);
void nile_staker_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _staker_token_rewards(const char* sp, const char* tpd);
void staker_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _amazon_operator_token_rewards(const char* totalStakerOperatorTokens);
void amazon_operator_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _nile_operator_token_rewards(const char* totalStakerOperatorTokens);
void nile_operator_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _operator_token_rewards(const char* totalStakerOperatorTokens);
void operator_token_rewards(sqlite3_context *context, int argc, sqlite3_value **argv);

int _big_gt(const char* a, const char* b);
void big_gt(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _sum_big_c(const char* a, const char* b);
void sum_big_c(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _numeric_multiply_c(const char* a, const char* b);
void numeric_multiply_c(sqlite3_context *context, int argc, sqlite3_value **argv);

static void sum_big_step(sqlite3_context* context, int argc, sqlite3_value** argv);
static void sum_big_finalize(sqlite3_context* context);

char* _calculate_staker_proportion(const char* stakerWeight, const char* totalWeight);
void calculate_staker_proportion(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _subtract_big(const char* a, const char* b);
void subtract_big(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _add_big(const char* a, const char* b);
void add_big(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _calc_tokens_per_day(const char* a, const char* b);
void calc_tokens_per_day(sqlite3_context *context, int argc, sqlite3_value **argv);

char* _calc_tokens_per_day_decimal(const char* a, const char* b);
void calc_tokens_per_day_decimal(sqlite3_context *context, int argc, sqlite3_value **argv);

void sqlite3_calculations_shutdown(void);

#endif // CALCULATIONS_H
