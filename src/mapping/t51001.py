
# @Time : 2020/6/19 1:52 PM
# @Author : lixiaobo
# @File : t51001.py.py 
# @Software: PyCharm
import time
from logger.logger_util import LoggerUtil
from mapping.p08001_m.app_amt_predication import ApplyAmtPrediction
from mapping.p08001_m.get_variable_in_db import GetVariableInDB
from mapping.p08001_m.get_variable_in_flow import GetVariableInFlow
from mapping.p08001_m.flow_strategy_processor import FlowStrategyProcessor
from mapping.p08001_m.loan_amt import LoanAmt
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class T51001(Transformer):
    """
    流水报告决策入参及变量清洗调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'report_req_no': 0,
            'interest_half': 0,
            'interest_year': 0,
            'balance_half': 0,
            'balance_year': 0,
            'mean_half': 0,
            'mean_year': 0,
            'normal_income_amt': 0,
            'normal_expense_amt': 0,
            'net_income_amt': 0,
            # 'large_income_cnt': 0,
            # 'large_income_period': 0,
            'top_three_opponent': 0,
            'unusual_trans_cnt': 0,
            'trans_cnt': 0,
            'unusual_trans_rate': 0,
            'quarter_1_amt': 0,
            'quarter_2_amt': 0,
            'quarter_3_amt': 0,
            'quarter_4_amt': 0,
            'mean_quarter_amt': 0,
            'std_quarter_amt': 0,
            'rate_quarter': 0,
            'income_loanable': 0,
            'cost_loanable': 0,
            'a1': 0,
            'a2': 0,
            # 'b11': 0,
            # 'b12': 0,
            'b2': 0,
            'b3': 0,
            'b4': 0,
            'funds_max': 0,
            'funds_min': 0,
            'c': 0,
            'credit': 0,
            'trans_flow_src_type': 0
        }

    def transform(self):
        """
        input_param 为所有关联关系的入参
        [
            {
                "applyAmo":66600,
                "authorStatus":"AUTHORIZED",
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                },
                "fundratio":0,
                "id":11843,
                "idno":"31011519910503253X",
                "name":"韩骁頔",
                "parentId":0,
                "phone":"13611647802",
                "relation":"CONTROLLER",
                "userType":"PERSONAL",
                "preReportReqNo":"PR472454663971700736",
                "baseTypeDetail":"U_COM_CT_PERSONAL"
            },
            {
                "applyAmo":66600,
                "extraParam":{
                    "bankName":"银行名",
                    "bankAccount":"银行账户",
                    "totalSalesLastYear":23232,
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonable":"1",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10"
                },
                "fundratio":0,
                "id":11844,
                "idno":"91440300MA5EEJUR92",
                "name":"磁石供应链商业保理（深圳）有限公司",
                "parentId":0,
                "phone":"021-1234567",
                "relation":"MAIN",
                "userType":"COMPANY",
                "preReportReqNo":"PR472454663971700736",
                "baseTypeDetail":"U_COMPANY"
            }
        ]
        """
        logger.info("input_param:%s", self.cached_data.get("input_param"))

        handle_list = [
            GetVariableInFlow(),
            GetVariableInDB(),
            # ApplyAmtPrediction(),
            FlowStrategyProcessor(),
            LoanAmt()
        ]
        handler_name_mapping = {
            GetVariableInFlow: "决策流水变量处理1",
            GetVariableInDB: "决策流水变量处理2",
            ApplyAmtPrediction: "决策申请金额预测",
            FlowStrategyProcessor: "决策流水策略处理",
            LoanAmt: "决策流水额度模型"
        }

        for handler in handle_list:
            start = time.time()
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
            cost = round(time.time() - start, 2)
            logger.info(f"{self.origin_data['preReportReqNo']}:{handler_name_mapping[handler.__class__]} 处理完成，耗时 {cost:.2f} 秒")
