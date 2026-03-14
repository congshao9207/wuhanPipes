import json
import traceback

import requests
from flask import request, jsonify
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy

from mapping.t00000 import T00000
from product.generate import Generate
from product.p_utils import _build_request, _get_biz_types, _append_rules, score_to_int, _relation_risk_subject
from strategy_config import obtain_strategy_url
from view.mapper_detail import STRATEGE_DONE, translate_for_report_detail

logger = LoggerUtil().logger(__name__)


def step_log(step, msg):
    logger.info("%s～～～～～～～～～～%s", step, msg)


class P002(Generate):

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            step_log(1, "一级企业详版报告 defensor invoke pipes 获取bizTypes，流程开启")
            step_log("1-1", json_data)
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data = json_data.get('queryData')[0]
            user_name = query_data.get('name')
            id_card_no = query_data.get('idno')
            phone = query_data.get('phone')
            user_type = query_data.get('userType')
            variables = T00000().run(user_name, id_card_no, phone, user_type, 'COMPANY', query_data)['variables']
            # 决策要求一直要加上00000，用户基础信息。
            variables["product_code"] = "002"
            variables['out_strategyBranch'] = '00000'
            step_log(2, "开始策略引擎封装入参")
            strategy_request = _build_request(req_no, product_code, variables=variables)
            step_log("2-1", strategy_request)
            step_log(3, "开始调用策略引擎")
            response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
            if response.status_code != 200:
                raise Exception("strategyOne错误:" + response.text)
            resp_json = response.json()
            step_log(4, "策略引擎调用成功")
            step_log("4-1", resp_json)
            error = jsonpath(resp_json, '$..Error')
            if error:
                raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(resp_json, '$..Description')))
            biz_types, categories = _get_biz_types(resp_json)

            query_data = {
                'name': user_name,
                'idno': id_card_no,
                'phone': phone,
                'userType': user_type,
                'bizType': biz_types,
                'rules': _append_rules(biz_types),
                'categories': categories
            }

            resp = {
                'productCode': json_data.get('productCode'),
                'reqNo': json_data.get('reqNo'),
                'queryData': [query_data]
            }
            self.response = resp
            step_log(5, "流程结束 pipes 回调 defensor")
            step_log("5-1", self.response)
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        try:
            json_data = request.get_json()
            step_log(1, "一级企业详版报告 defensor invoke pipes 获取策略引擎结果，流程开启")
            step_log("1-1", json_data)
            strategy_param = json_data.get('strategyParam')
            origin_input = json_data.get('strategyInputVariables')
            if origin_input is None:
                origin_input = {}
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            query_data = strategy_param.get('queryData')[0]
            user_name = query_data.get('name')
            id_card_no = query_data.get('idno')
            phone = query_data.get('phone')
            user_type = query_data.get('userType')
            codes = query_data.get('bizType')
            biz_types = codes.copy()
            biz_types.append('00000')
            variables, out_decision_code = translate_for_strategy("002", biz_types, user_name, id_card_no, phone, user_type,
                                                                  'COMPANY', self.df_client, query_data)
            origin_input['out_strategyBranch'] = ','.join(codes)
            # 合并新的转换变量
            origin_input.update(variables)
            step_log(2, "开始策略引擎封装入参")
            strategy_request = _build_request(req_no, product_code, origin_input)
            step_log("2-1", strategy_request)
            step_log(3, "开始调用策略引擎")
            strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
            if strategy_response.status_code != 200:
                raise Exception("strategyOne错误:" + strategy_response.text)
            strategy_resp = strategy_response.json()
            step_log(4, "策略引擎调用成功")
            step_log("4-1", strategy_resp)
            error = jsonpath(strategy_resp, '$..Error')
            if error:
                raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
            score_to_int(strategy_resp)
            biz_types, categories = _get_biz_types(strategy_resp)
            logger.info(biz_types)
            for subjectNode in strategy_param["queryData"]:
                subjectNode['bizType'] = biz_types
            # 最后返回报告详情
            if STRATEGE_DONE in biz_types:
                detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                     'COMPANY')
                json_data['reportDetail'] = [detail]
            # 处理关联人
            _relation_risk_subject(strategy_resp, out_decision_code)
            json_data['strategyResult'] = strategy_resp
            json_data['strategyInputVariables'] = variables
            json_data['rules'] = _append_rules(biz_types)
            self.response = json_data
            step_log(5, "流程结束 pipes 回调 defensor")
            step_log("5-1", self.response)
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))
