# @Time : 2021-05-07 12:09:00
# @Author : lixiaobo
# @Software: PyCharm
import traceback

from flask import request

from exceptions import ServerException
from logger.logger_util import LoggerUtil

from product.gamt.gamt_common_flow_executor_gy import GAmtCommonFlowExecutorGY
from product.generate import Generate
from product.microloan.gy_amt_flow_executor import SegmentGyAmtFlowExecutor
from product.p_utils import _get_biz_types, _append_rules, \
    _get_resp_field_value
from service.base_type_service_v2 import BaseTypeServiceV2
from util.strategy_invoker import invoke_strategy

logger = LoggerUtil().logger(__name__)


class P09002(Generate):
    def __init__(self):
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            app_id = json_data.get('appId')
            req_no = json_data.get('reqNo')
            step_req_no = json_data.get("stepReqNo")
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            industry = json_data.get("industry")
            extra_param = json_data.get("extraParam")
            base_type_service = BaseTypeServiceV2(query_data_array)

            response_array = []
            for data in query_data_array:
                response_array.append(self._shake_hand_response(base_type_service, data, product_code, req_no))
            resp = {
                'appId': app_id,
                'productCode': product_code,
                'reqNo': req_no,
                'step_req_no': step_req_no,
                'industry': industry,
                'queryData': response_array,
                'extraParam': extra_param
            }
            self.response = resp
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            strategy_param = json_data.get('strategyParam')
            query_data_array = strategy_param.get('queryData')
            extra_param = strategy_param.get("extraParam")

            segment_handler = self.find_process_flow(json_data, query_data_array)
            segment_handler.execute()
            self.response = segment_handler.response
            self.response["extraParam"] = extra_param
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def find_process_flow(self, json_data, query_data_array):
        executor = None
        for item in query_data_array:
            segment_name = item.get("nextSegmentName")
            if segment_name == "loan_amt":
               executor = SegmentGyAmtFlowExecutor(json_data)
               break
            # elif segment_name == "device_fraud":
            #     executor = SegmentGyDeviceFraudExecutor(json_data)
            #     break

        if not executor:
            executor = GAmtCommonFlowExecutorGY(json_data)

        executor.sql_db = self.sql_db
        executor.df_client = self.df_client
        return executor

    def _shake_hand_response(self, base_type_service, data, product_code, req_no):
        """
        和决策交互，封装response
        :param data:
        :param product_code:
        :param req_no:
        :return:
        """
        # user_name = data.get('name')
        # id_card_no = data.get('idno')
        # phone = data.get('phone')
        # user_type = data.get('userType')
        # 获取base_type
        base_type = base_type_service.parse_base_type(data)
        # variables = T00000().run(user_name, id_card_no, phone, user_type, base_type, data)['variables']
        variables = {"product_code": product_code, 'out_strategyBranch': '00000'}
        # 决策要求一直要加上00000，用户基础信息。
        logger.info("variables:%s", variables)

        resp_json = invoke_strategy(variables, product_code, req_no)
        biz_types, categories = _get_biz_types(resp_json)
        segment_name = _get_resp_field_value(resp_json, "$..segment_name")
        rules = _append_rules(biz_types)

        resp = {}
        resp.update(data)
        resp['baseType'] = base_type
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories
        resp['segmentName'] = "HAND_SHAKE"
        resp['nextSegmentName'] = segment_name

        return resp

