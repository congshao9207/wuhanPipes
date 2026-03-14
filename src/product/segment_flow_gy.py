# @Time : 12/11/20 10:12 AM 
# @Author : lixiaobo
# @File : micro_loan_flowable.py 
# @Software: PyCharm

from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each
from mapping.mapper import translate_for_strategy
from mapping.mapper_gy import translate_for_strategy_gy
from product.p_utils import score_to_int, _get_biz_types, _relation_risk_subject, _append_rules, \
    _get_resp_field_value
from util.strategy_invoker import invoke_strategy
from view.grouped_mapper_detail import view_variables_scheduler
from view.mapper_detail import STRATEGE_DONE

logger = LoggerUtil().logger(__name__)


class SegmentFlowGY(object):
    def __init__(self, json_data):
        self.sql_db = None
        self.df_client = None
        self.json_data = json_data
        self.strategy_param = self.json_data.get('strategyParam')
        self.req_no = self.strategy_param.get('reqNo')
        self.step_req_no = self.strategy_param.get('stepReqNo')
        self.version_no = self.strategy_param.get('versionNo')
        self.product_code = self.strategy_param.get('productCode')
        self.query_data_array = self.strategy_param.get('queryData')

    @staticmethod
    def _create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data):
        resp = {
            'commonDetail': common_detail,
            'subject': subject
        }

        if strategy_resp:
            resp['strategyResult'] = strategy_resp
        if variables:
            resp['strategyInputVariables'] = variables
        return resp

    def _strategy_hand(self, json_data, data, product_code, req_no):
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
        variables, out_decision_code = translate_for_strategy_gy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, self.df_client, data, None, json_data)
        origin_input = data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        origin_input['out_strategyBranch'] = ','.join(filter(lambda e: e != "00000", codes))
        # 合并新的转换变量
        variables["segment_name"] = data.get("nextSegmentName")
        origin_input["tracking_" + variables["segment_name"]] = 1
        origin_input.update(variables)

        strategy_resp = invoke_strategy(origin_input, product_code, req_no)
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        segment_name = _get_resp_field_value(strategy_resp, "$..segment_name")
        data["segmentName"] = data.get("nextSegmentName")
        data["nextSegmentName"] = segment_name
        data["bizType"] = biz_types

        resp = {}
        self.resp_vars_to_input_vars(strategy_resp, origin_input)
        self._calc_view_variables(base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone,
                                  product_code,
                                  resp, strategy_resp, user_name, user_type, origin_input)

        array = self._get_strategy_second_array(data, fund_ratio, relation, strategy_resp, user_name, user_type)
        return array, resp

    def _get_strategy_second_array(self, data, fundratio, relation, strategy_resp, user_name, user_type):
        array = {
            'name': user_name,
            'idno': data.get('idno'),
            'userType': user_type
        }

        if fundratio is None or fundratio == '':
            array['fundratio'] = 0.00
        else:
            array['fundratio'] = float(fundratio)

        array['relation'] = relation
        array["id"] = data.get("id")
        array["parentId"] = data.get("parentId")

        resp_vars = jsonpath(strategy_resp, "$..Variables")
        if resp_vars and len(resp_vars) > 0:
            array.update(resp_vars[0])

        return array

    @staticmethod
    def _get_json_path_value(strategy_resp, path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
            return 0

    def _calc_view_variables(self, base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone, product_code,
                             resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        """
        data['strategyInputVariables'] = variables
        if STRATEGE_DONE in biz_types:
            detail = view_variables_scheduler(product_code, json_data, user_name, id_card_no, phone, user_type,
                                              base_type,
                                              data, invoke_each)
            resp['reportDetail'] = detail
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    def resp_vars_to_input_vars(self, strategy_resp, variables):
        pass


