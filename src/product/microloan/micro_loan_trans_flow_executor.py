# @Time : 12/11/20 9:52 AM 
# @Author : lixiaobo
# @File : micro_loan_trans_flow_executor.py 
# @Software: PyCharm
import json
import traceback

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from portrait.transflow.single_portrait import SinglePortrait
from portrait.transflow.union_portrait import UnionPortrait
from product.microloan.abs_micro_loan_flow import MicroLoanFlow
from product.p08001 import P08001
from product.p_config import product_codes_dict
from product.p_utils import _get_resp_field_value
from service.base_type_service_v3 import BaseTypeServiceV3

logger = LoggerUtil().logger(__name__)


class MicroLoanTransFlowExecutor(MicroLoanFlow):
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        self._prepared()
        # 获取请求参数
        try:
            strategy_param = self.json_data.get('strategyParam')
            pre_report_req_no = strategy_param.get('preReportReqNo')

            # 遍历query_data_array调用strategy
            base_type_service = BaseTypeServiceV3(self.query_data_array)
            main_query_data = None
            subjects = []
            for data in self.query_data_array:
                data["preReportReqNo"] = pre_report_req_no
                data["baseTypeDetail"] = base_type_service.parse_base_type(data)
                subjects.append(data)

                if data.get("relation") == "MAIN":
                    main_query_data = data

            # 决策调用及view变量清洗
            p = P08001()
            resp = p.strategy(False, self.df_client, subjects, main_query_data, self.product_code, self.req_no, code_info=["51001"],
                              clean_view_var=False)
            item_data_list = []
            for subject in subjects:
                item_data = {
                    "queryData": subject
                }

                if subject.get("relation") == "MAIN":
                    del subject["strategyInputVariables"]["single"]
                    item_data.update(resp)
                    subject["segmentName"] = subject.get("nextSegmentName")
                    subject["nextSegmentName"] = _get_resp_field_value(resp, "$..segment_name")

                item_data_list.append(item_data)

            self.response = p.create_strategy_resp(self.product_code, self.req_no, self.step_req_no, self.version_no,
                                                   item_data_list)

            logger.info("小额快贷-流水策略，应答：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _prepared(self):
        base_type_service = BaseTypeServiceV3(self.query_data_array)

        main_node = None
        response_array = []
        for data in self.query_data_array:
            base_type = base_type_service.parse_base_type(data)
            data["baseTypeDetail"] = base_type
            if data.get("relation") == "MAIN":
                main_node = data
            else:
                response_array.append(data)

        cached_data = {}
        user_name = main_node.get('name')
        id_card_no = main_node.get('idno')
        phone = main_node.get('phone')
        user_type = main_node.get('userType')
        base_type = main_node.get("baseType")

        strategy_param = self.json_data.get('strategyParam')
        report_req_no = strategy_param.get('preReportReqNo')
        extra_param = strategy_param.get('extraParam')

        public_param = {
            "reqNo": self.req_no,
            "reportReqNo": report_req_no,
            "productCode": self.product_code,
            "isSingle": False,
            "outApplyNo": extra_param.get("outApplyNo"),
            "applyAmt": self.json_data.get("applyAmt"),
            "renewLoans": self.json_data.get("renewLoans"),
            "historicalBiz": self.json_data.get("historicalBiz")
        }

        var_item = {
            "bizType": product_codes_dict.get(self.product_code)
        }

        var_item.update(main_node)
        single, union = SinglePortrait(), UnionPortrait()

        single.sql_db = self.sql_db
        single.init(var_item, self.query_data_array, user_name, user_type, base_type,
                    id_card_no, phone, main_node, public_param, cached_data)
        single.process()

        union.sql_db = self.sql_db
        union.init(var_item, self.query_data_array, user_name, user_type, base_type,
                   id_card_no, phone, main_node, public_param, cached_data)
        union.process()

