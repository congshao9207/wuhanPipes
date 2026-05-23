# @Time : 12/11/20 9:53 AM 
# @Author : lixiaobo
# @File : micro_loan_default_flow_executor.py 
# @Software: PyCharm
import json

from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from mapping.tp0001 import Tp0001
from mapping.utils.np_encoder import NpEncoder
from product.microloan.abs_micro_loan_flow import MicroLoanFlow
from util.strategy_invoker import invoke_strategy
from util.type_converter import format_var

logger = LoggerUtil().logger(__name__)


class MicroLoanDefaultFlowExecutor(MicroLoanFlow):
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        subject = []
        cache_array = []

        # 遍历query_data_array调用strategy
        for data in self.query_data_array:
            segment_name = data.get("nextSegmentName")
            if segment_name == "default":
                array, resp = self._strategy_hand(self.json_data, data, self.product_code, self.req_no)
                subject.append(resp)
                cache_array.append(array)
            else:
                subject.append(data)

        # 封装第二次调用参数
        trans_result = Tp0001().run(None, None, None, None, None, cache_array, None, self.json_data)
        variables = trans_result.get("variables")
        variables["segment_name"] = "default"
        variables["tracking_default"] = 1

        strategy_resp = invoke_strategy(variables, self.product_code, self.req_no)
        resp_end = self._create_strategy_resp(strategy_resp, variables, None, subject, self.json_data)

        format_var(None, None, -1, resp_end)
        logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
        self.response = resp_end

    def resp_vars_to_input_vars(self, strategy_resp, variables):
        resp_vars = jsonpath(strategy_resp, "$..Variables")
        if resp_vars and len(resp_vars) > 0:
            variables.update(resp_vars[0])



