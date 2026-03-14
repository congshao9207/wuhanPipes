# @Time : 2020/4/21 11:28 AM 
# @Author : lixiaobo
# @File : p07001.py 
# @Software: PyCharm

import json
import traceback

import requests
from flask import request
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each
from mapping.mapper import translate_for_strategy
from mapping.utils.np_encoder import NpEncoder
from product.generate import Generate
from product.p_config import product_codes_dict
from product.p_utils import _relation_risk_subject, _append_rules, score_to_int, _get_biz_types, _build_request
from strategy_config import obtain_strategy_url
from mapping.t11002 import T11002


logger = LoggerUtil().logger(__name__)


# 财务通用报告产品处理
class P11001(Generate):
    def shake_hand_process(self):
        raise NotImplementedError()

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1. 财务通用报告：获取映射结果，流程开启, 入参为：%s", json.dumps(json_data))
            strategy_param = json_data.get('strategyParam')
            product_code = strategy_param.get('productCode')
            table_no=strategy_param.get('financeReportNo')
            data_repository = {'table_no':table_no,'product_code':product_code}
            variables, out_decision_code = translate_for_strategy(product_code, ['11002'], origin_data=None,
                                                                  data_repository=data_repository, full_msg=json_data)

            self.response = {"response": variables}
            logger.info(self.response)
            logger.info("2. 财务报表分析报告，应答：%s", json.dumps(self.response, cls=NpEncoder))
        except Exception as err:
            self.response = {"response": T11002().variables}
            logger.error(traceback.format_exc())
            # raise ServerException(code=500, description=str(err))

    def strategy(self, data, product_code, req_no):
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = product_codes_dict[product_code]
        base_type = 'U_COMPANY'
        biz_types = codes.copy()
        data_repository = {}
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, origin_data=data, data_repository=data_repository)
        origin_input = {'out_strategyBranch': ','.join(codes)}
        # 合并新的转换变量
        origin_input.update(variables)
        logger.info("1. 财务报表-开始策略引擎封装入参")
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.info("2. 财务报表-策略引擎封装入参:%s", strategy_request)
        strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
        logger.info("3. 财务报表-策略引擎返回结果：%s", strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        logger.info("4. 财务报表-策略引擎调用成功 %s", strategy_resp)
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)

        biz_types, categories = _get_biz_types(strategy_resp)
        logger.info(biz_types)

        data['bizType'] = biz_types
        data['strategyInputVariables'] = variables
        resp = {
            'product_code':product_code,
            'strategyInputVariables':variables,
            'strategyResult':strategy_resp,
            'queryData':data
        }
        resp

        data_repository.clear()
        del data_repository
        return resp


