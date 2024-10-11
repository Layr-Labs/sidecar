from decimal import Decimal, getcontext, ROUND_HALF_UP, ROUND_UP, ROUND_DOWN, Context, ROUND_HALF_EVEN
import math

def preNileTokensPerDay(tokens: str) -> str:
    big_amount = float(tokens)
    div = 0.999999999999999
    res = big_amount * div

    res_str = "{}".format(res)
    return "{}".format(int(Decimal(res_str)))

def amazonStakerTokenRewards(sp:str, tpd:str) -> str:
    # Set precision to 38 to match DECIMAL(38,0)
    getcontext().prec = 15

    # Convert string inputs to Decimal, preserving original precision
    proportion = Decimal(sp)
    tokens = Decimal(tpd)

    # Perform the multiplication
    result = proportion * tokens

    # Convert to string, ensuring no scientific notation
    return "{}".format(int(result))

# Former sql query: (staker_proportion * tokens_per_day)::text::decimal(38,0)
def nileStakerTokenRewards(sp:str, tpd:str) -> str:
    getcontext().prec = 15
    getcontext().rounding = ROUND_HALF_UP
    # Convert string inputs to Decimal, preserving original precision
    proportion = Decimal(sp)
    tokens = Decimal(tpd)

    # Perform the multiplication
    result = proportion * tokens
    print("result: {}".format(result))

    getcontext().prec = 38
    res_decimal = result.quantize(Decimal('1'), rounding=ROUND_UP)

    # Convert to string, ensuring no scientific notation
    return "{}".format(res_decimal, 'f')

def stakerTokenRewards(sp:str, tpd:str) -> str:
    getcontext().prec = 38
    getcontext().rounding = ROUND_HALF_EVEN
    stakerProportion = Decimal(sp)
    tokensPerDay = Decimal(tpd)

    decimal_res = stakerProportion * tokensPerDay

    floored = decimal_res.quantize(Decimal('1'), rounding=ROUND_DOWN)
    return "{}".format(floored, 'f')

# Former sql query: cast(total_staker_operator_payout * 0.10 AS DECIMAL(38,0))
# This is the same as the amazonStakerTokens function, just with different inputs
def amazonOperatorTokenRewards(totalStakerOperatorTokens:str) -> str:
    # Set precision to 38 to match DECIMAL(38,0)
    getcontext().prec = 15

    # Perform the multiplication
    result = Decimal(totalStakerOperatorTokens) * Decimal('.1')

    getcontext().prec = 38
    res_floor = result.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    # Convert to string, ensuring no scientific notation
    return "{}".format(res_floor)


# Former sql query: (total_staker_operator_payout * 0.10)::text::decimal(38,0)
# This is the same as the nileStakerTokenRewards function, just with different inputs
def nileOperatorTokenRewards(tsot:str) -> str:
    getcontext().prec = 15
    getcontext().rounding = ROUND_HALF_UP

    # Perform the multiplication
    result = Decimal(tsot) * Decimal('.1')
    print("result: {}".format(result))

    getcontext().prec = 38
    res_decimal = result.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    # Convert to string, ensuring no scientific notation
    return "{}".format(res_decimal, 'f')


# Former sql query: floor(total_staker_operator_payout * 0.10)
# This is the same as the stakerTokenRewards function, just with different inputs
def operatorTokenRewards(totalStakerOperatorTokens:str) -> str:
    return stakerTokenRewards(totalStakerOperatorTokens, '0.10')


def bigGt(a:str, b:str) -> bool:
    return Decimal(a) > Decimal(b)

def sumBigC(a:str, b:str) -> str:
    sum = Decimal(a) + Decimal(b)
    return format(sum, 'f')

def numericMultiplyC(a:str, b:str) -> str:
    product = Decimal(a) * Decimal(b)

    return format(product, 'f')

def calculateStakerProportion(stakerWeightStr: str, totalWeightStr: str) -> str:
    getcontext().prec = 15
    getcontext().rounding = ROUND_DOWN

    stakerWeight = Decimal(stakerWeightStr)
    totalWeight = Decimal(totalWeightStr)

    preProportion = stakerWeight / totalWeight

    preProportion1 = math.floor(float(preProportion * Decimal('1000000000000000')))

    preProportion2 = preProportion1 / Decimal('1000000000000000')

    return "{}".format(preProportion2, 'f')

def subtractBig(a:str, b:str) -> str:
    diff = Decimal(a) - Decimal(b)
    return format(diff, 'f')

def addBig(a:str, b:str) -> str:
    diff = Decimal(a) + Decimal(b)
    return format(diff, 'f')

def calcTokensPerDay(amountStr:str, durationStr:str) -> str:
    amount = Decimal(amountStr)
    duration = int(durationStr)

    perDay = Decimal(duration / 86400)

    getcontext().prec = 22
    tpd = Decimal(amount / perDay)

    return "{}".format(tpd, 'f')

def calcTokensPerDayDecimal(amountStr:str, durationStr:str) -> str:
    amount = Decimal(amountStr)
    duration = int(durationStr)

    perDay = Decimal(duration / 86400)

    tpd = Decimal(amount / perDay).quantize(Decimal('1'), rounding=ROUND_DOWN)

    return "{}".format(tpd, 'f')
