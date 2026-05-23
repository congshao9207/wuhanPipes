# @Time : 12/11/20 9:57 AM 
# @Author : lixiaobo
# @File : micro_loan_amt_flow_executor.py 
# @Software: PyCharm
import json

from logger.logger_util import LoggerUtil
from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from mapping.p09003_m.limit_model_param import LimitModel
from mapping.utils.np_encoder import NpEncoder
from product.microloan.abs_micro_loan_flow import MicroLoanFlow
from product.p_utils import score_to_int, _get_biz_types, _get_resp_field_value
from util.strategy_invoker import invoke_strategy
from util.type_converter import format_var

logger = LoggerUtil().logger(__name__)


class ELoanAmtFlowExecutor(MicroLoanFlow):
    required_vars = ["model_pred", "flow_limit_amt"]

    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        subject = []
        model_variables = {}
        td_score = 0
        # 遍历query_data_array获取借款人+配偶同盾分 max
        for data in self.query_data_array:
            strategyInputVariables = data.get("strategyInputVariables")
            if "score_fin" in strategyInputVariables.keys():
                score_fin = strategyInputVariables['score_fin']
                if score_fin >= td_score:
                    td_score = score_fin

        # 遍历query_data_array调用strategy
        strategy_resp = None
        for data in self.query_data_array:
            segment_name = data.get("nextSegmentName")
            if segment_name == "loan_amt":
                # 封装第二次调用参数
                main_data = list(filter(lambda e: e["relation"] == "MAIN", self.query_data_array))[0]

                cached_data = {"query_data_array": self.query_data_array}
                dp = DataPreparedProcessor()
                dp.init(model_variables, main_data["name"], main_data["idno"], main_data, cached_data)
                dp.process()

                lm = LimitModel()
                lm.init(model_variables, main_data["name"], main_data["idno"], main_data, cached_data)
                lm.process()

                model_variables["segment_name"] = "loan_amt"
                model_variables["tracking_loan_amt"] = 1
                model_variables["td_score"] = td_score
                data['strategyInputVariables'] = model_variables

                strategy_resp = invoke_strategy(model_variables, self.product_code, self.req_no)
                score_to_int(strategy_resp)
                biz_types, categories = _get_biz_types(strategy_resp)
                segment_name = _get_resp_field_value(strategy_resp, "$..segment_name")
                data["segmentName"] = data.get("nextSegmentName")
                data["nextSegmentName"] = segment_name
                data["bizType"] = biz_types

                resp = {
                    'reportDetail': {},
                    'strategyResult': strategy_resp,
                    'queryData': data
                }
                subject.append(resp)
            else:
                subject.append({"queryData": data})

        resp_end = self._create_strategy_resp(strategy_resp, None, None, subject, self.json_data)

        format_var(None, None, -1, resp_end)
        logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
        self.response = resp_end

    @staticmethod
    def fetch_input_variables(data):
        array = {}
        var_list = data.get("strategyInputVariables")
        if var_list:
            for var_name in ELoanAmtFlowExecutor.required_vars:
                val = var_list.get(var_name)
                if val:
                    array[var_name] = val
        return array

