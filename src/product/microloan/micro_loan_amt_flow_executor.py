# @Time : 12/11/20 9:57 AM 
# @Author : lixiaobo
# @File : micro_loan_amt_flow_executor.py 
# @Software: PyCharm
import json

from logger.logger_util import LoggerUtil
from mapping.tp0002 import Tp0002
from mapping.utils.np_encoder import NpEncoder
from product.microloan.abs_micro_loan_flow import MicroLoanFlow
from util.strategy_invoker import invoke_strategy
from util.type_converter import format_var

logger = LoggerUtil().logger(__name__)


class MicroLoanAmtFlowExecutor(MicroLoanFlow):
    required_vars = ["model_pred", "flow_limit_amt"]
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        subject = []
        cache_array = []

        # 遍历query_data_array调用strategy
        for data in self.query_data_array:
            segment_name = data.get("nextSegmentName")
            if segment_name == "loan_amt":
                previous_vars = self.fetch_input_variables(data)
                array, resp = self._strategy_hand(self.json_data, data, self.product_code, self.req_no)
                curr_vars = self.fetch_input_variables(data)
                subject.append(resp)
                if len(array) > 0:
                    array.update(previous_vars)
                    array.update(curr_vars)
                cache_array.append(array)
            else:
                subject.append(data)

        # 封装第二次调用参数
        trans_result = Tp0002().run(None, None, None, None, None, cache_array, None, self.json_data)

        variables = trans_result.get("variables")
        variables["segment_name"] = "loan_amt"
        variables["tracking_loan_amt"] = 1
        variables["call_type"] = "aggregate"

        strategy_resp = invoke_strategy(variables, self.product_code, self.req_no)
        resp_end = self._create_strategy_resp(strategy_resp, variables, None, subject, self.json_data)

        format_var(None, None, -1, resp_end)
        logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
        self.response = resp_end

    @staticmethod
    def fetch_input_variables(data):
        array = {}
        var_list = data.get("strategyInputVariables")
        if var_list:
            for var_name in MicroLoanAmtFlowExecutor.required_vars:
                val = var_list.get(var_name)
                if val:
                    array[var_name] = val
        return array

