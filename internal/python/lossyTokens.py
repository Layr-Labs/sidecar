from decimal import Decimal

def lossyAdd(tokens: str):
    big_amount = float(tokens)
    div = 0.999999999999999
    res = big_amount * div

    res_str = "{}".format(res)
    return "{}".format(int(Decimal(res_str)))


print(lossyAdd("1428571428571428571428571428571428571.4142857142857143"))
