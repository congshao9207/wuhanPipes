# @Time : 2/5/21 10:53 AM 
# @Author : lixiaobo
# @File : strategy_config.py 
# @Software: PyCharm
from config import OPEN_STRATEGY_URL, STRATEGY_URL

open_strategy_support_products = [
    "07001",  # 征信报告
    "07002",  # 征信拦截
    "08001",  # 流水报告
    "08002",  # 流水拦截
    "08003",  # 轻量版流水报告
    "07003",  # 企业征信报告
    "07004",  # 企业征信拦截
    "11002",  #财务报表
    "08004",  #流水贷
]


def obtain_strategy_url(product_code=None):
    return [STRATEGY_URL, OPEN_STRATEGY_URL][
        product_code is not None and product_code in open_strategy_support_products]
