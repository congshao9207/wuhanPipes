# -*- coding: utf-8 -*-
# @Time : 2020/2/21 1:39 PM
# @Author : lixiaobo
# @Site : 
# @File : strategy_service.py.py
# @Software: PyCharm
import requests
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy
from product.p_utils import _build_request, score_to_int, _get_biz_types, _relation_risk_subject, _append_rules
from strategy_config import obtain_strategy_url
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail

logger = LoggerUtil().logger(__name__)


class StrategyService(object):
    def __init__(self):
        pass

    def strategy(self, df_client, data, product_code, req_no):
        origin_input = data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = data.get('bizType')
        base_type = data.get('baseType')
        relation = data.get('relation')
        fundratio = data.get('fundratio')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, df_client, data)
        origin_input['out_strategyBranch'] = ','.join(codes)
        # 合并新的转换变量
        origin_input.update(variables)
        logger.info("1. 开始策略引擎封装入参")
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.info("2. 策略引擎封装入参:%s", strategy_request)
        strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
        logger.info("3. 策略引擎返回结果：%s", strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        logger.info("4. 策略引擎调用成功 %s", strategy_resp)
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        logger.info(biz_types)

        resp = {}
        self._strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                        resp, strategy_resp, user_name, user_type, variables)
        return resp

    @staticmethod
    def _strategy_second_loop_resp(base_type, biz_types, data, id_card_no, out_decision_code, phone, product_code,
                                   resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        :param base_type:
        :param biz_types:
        :param data:
        :param id_card_no:
        :param out_decision_code:
        :param phone:
        :param product_code:
        :param resp:
        :param strategy_resp:
        :param user_name:
        :param user_type:
        :param variables:
        :return:
        """
        data['bizType'] = biz_types
        data['strategyInputVariables'] = variables
        # 最后返回报告详情
        if STRATEGE_DONE in biz_types:
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                 base_type, data)
            resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data