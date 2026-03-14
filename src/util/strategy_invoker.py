# @Time : 12/11/20 11:48 AM 
# @Author : lixiaobo
# @File : strategy_invoker.py 
# @Software: PyCharm
import requests
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from product.p_utils import _build_request
from strategy_config import obtain_strategy_url

logger = LoggerUtil().logger(__name__)


def invoke_strategy(variables, product_code, req_no):
    strategy_request = _build_request(req_no, product_code, variables)
    logger.info("strategy_request:%s", strategy_request)
    strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
    logger.debug("strategy_response%s", strategy_response.text)
    if strategy_response.status_code != 200:
        raise Exception("strategyOne错误:" + strategy_response.text)
    strategy_resp = strategy_response.json()
    error = jsonpath(strategy_resp, '$..Error')
    if error:
        raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
    return strategy_resp
