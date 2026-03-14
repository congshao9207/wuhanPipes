# @Time : 12/11/20 9:58 AM 
# @Author : lixiaobo
# @File : micro_loan_common_flow_executor.py 
# @Software: PyCharm
import json

from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from mapping.utils.np_encoder import NpEncoder
from product.microloan.abs_micro_loan_flow import MicroLoanFlow
from util.type_converter import format_var

logger = LoggerUtil().logger(__name__)


class MicroLoanCommonFlowExecutor(MicroLoanFlow):
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        subject = []
        # 遍历query_data_array调用strategy
        for data in self.query_data_array:
            array, resp = self._strategy_hand(self.json_data, data, self.product_code, self.req_no)
            subject.append(resp)
            # 最后返回报告详情

        # 封装最终返回json
        resp_end = self._create_strategy_resp(None, None, None, subject, self.json_data)
        format_var(None, None, -1, resp_end)
        logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
        self.response = resp_end

    def resp_vars_to_input_vars(self, strategy_resp, variables):
        resp_vars = jsonpath(strategy_resp, "$..Variables")
        if resp_vars and len(resp_vars) > 0:
            variables.update(resp_vars[0])
