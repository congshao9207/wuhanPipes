import traceback

import requests
from flask import request
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy
from mapping.t00000 import T00000
from product.generate import Generate
from product.p_utils import _build_request, score_to_int, _get_biz_types, _append_rules, _relation_risk_subject
from strategy_config import obtain_strategy_url

logger = LoggerUtil().logger(__name__)


def step_log(step, msg):
    logger.info("%s～～～～～～～～～～%s", step, msg)


# 移出灰名单操作
class P005(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            step_log("灰名单移除决策SharkHand-01", json_data)
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            response_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                response_array.append(self._shark_hand_response(data, product_code, req_no))
            resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData': response_array
            }
            self.response = resp
            step_log("灰名单移除决策SharkHand结束-02", self.response)
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            step_log("灰名单移除决策strategy请求开始", json_data)
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            step_req_no = strategy_param.get('stepReqNo')
            version_no = strategy_param.get('versionNo')
            query_data_array = strategy_param.get('queryData')
            subject = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                resp = self._strategy_second_hand(data, product_code, req_no)
                subject.append(resp)
            # 封装最终返回json
            resp_end = self._create_strategy_resp(product_code, req_no, step_req_no,
                                                  version_no, subject)
            self.response = resp_end
            step_log("灰名单移除决策 流程结束 pipes 回调 defensor:", self.response)
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _create_strategy_resp(self, product_code, req_no, step_req_no, version_no, subject):
        resp = {
            'reqNo': req_no,
            'product_code': product_code,
            'stepReqNo': step_req_no,
            'versionNo': version_no,
            'subject': subject
        }
        return resp

    def _strategy_second_hand(self, data, product_code, req_no):
        resp = {}
        array = {}
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
        fund_ratio = data.get('fundratio')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, self.df_client, data)
        origin_input['out_strategyBranch'] = ','.join(codes)
        # 合并新的转换变量
        origin_input.update(variables)
        logger.info("productCode:005-_strategy_second_hand begin")
        strategy_request = _build_request(req_no, product_code, origin_input)
        logger.info("productCode:005-_strategy_second_hand strategy_request:%s", strategy_request)
        strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
        logger.debug(strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        logger.info("productCode:005-_strategy_second_hand,strategy_resp:%s", strategy_resp)
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        logger.info(biz_types)
        self._strategy_second_loop_resp(biz_types, data, out_decision_code,
                                        resp, strategy_resp, variables)
        return resp

    def _strategy_second_loop_resp(self, biz_types, data, out_decision_code,
                                   resp, strategy_resp, variables):
        data['bizType'] = biz_types
        data['strategyInputVariables'] = variables
        # 最后返回报告详情
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    def _shark_hand_response(self, data, product_code, req_no):
        resp = {}
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        extra_param = data.get("extraParam")
        user_type = data.get('userType')
        auth_status = data.get('authorStatus')
        fund_ratio = data.get('fundratio')
        relation = data.get('relation')
        # 获取base_type
        base_type = self._get_base_type(fund_ratio, auth_status, phone, relation, user_type)
        variables = T00000().run(user_name, id_card_no, phone, user_type, base_type, data)['variables']
        # 决策要求一直要加上00000，用户基础信息。
        variables['out_strategyBranch'] = '00000'
        variables["product_code"] = product_code
        logger.info("productCode:005, 开始策略引擎封装入参")
        strategy_request = _build_request(req_no, product_code, variables=variables)
        step_log("2-1", strategy_request)
        # 调用决策引擎
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
        rules = _append_rules(biz_types)
        resp['name'] = user_name
        resp['idno'] = id_card_no
        resp['phone'] = phone
        resp['extraParam'] = extra_param
        resp['userType'] = user_type
        resp['authStatus'] = auth_status
        resp['fundratio'] = fund_ratio
        resp['baseType'] = base_type
        resp['relation'] = relation
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories
        return resp

    def _get_base_type(self, fund_ratio, auth_status, phone, relation, user_type):
        if relation == 'GUARANTOR':
            if user_type == 'COMPANY':
                return 'G_COMPANY'
            elif auth_status == 'AUTHORIZED':
                return 'G_PERSONAL'
            else:
                return 'G_S_PERSONAL'
        if user_type == 'PERSONAL':
            if auth_status == 'AUTHORIZED' and phone is not None and phone != '':
                return 'U_PERSONAL'
            else:
                return 'U_S_PERSONAL'
        if user_type == 'COMPANY':
            if relation == 'CONTROLLER':
                return 'U_COMPANY'
            elif relation == 'LEGAL':
                return 'U_COMPANY'
            elif relation == 'MAIN':
                return 'U_COMPANY'
            elif relation == 'SHAREHOLDER' and float(fund_ratio) >= 0.50:
                return 'U_COMPANY'
            else:
                return 'U_S_COMPANY'
