#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :t51004.py.py
# @Time      :2025/2/20 18:18
# @Author    :chenwen


from logger.logger_util import LoggerUtil
from mapping.p08004.get_mtr_variable_in_flow import GetMtrVariableInFlow
from mapping.p08004.mtr_loan_amt import MtrLoanAmt
from mapping.p08004.mtr_loan_rate import MtrLoanRate
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class T51004(Transformer):
    """
    流水贷报告决策入参及变量清洗调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "mtr_bank_duration_under_150_days": 0,  # 收单流水时间跨度未满150天
            "mtr_bank_date_gap_over_32_days": 0,  # 收单流水拉取截止日期距申请日期超过32天
            "bank_is_non_borrower_or_spouse_account": 0,  # 流水账户非借款人、配偶、主营企业
            "bank_duration_under_150_days": 0,  # 流水时间跨度未满150天
            "bank_date_gap_over_32_days": 0,  # 流水拉取截止日期距申请日期超过32天
            "combined_repayment_ratio": 0.0,  # 流水中融资还款的比例超过60%
            "net_operational_income": 0.00,  # 流水经营性净进账金额较低（剔除退款等出账），风险较高
            "transactions_gap_over_32_days": 0,  # 流水交易记录中，两笔交易记录时间相距超过32天，结息除外
            "recent_loan_institution_count": 0,  # 该客户近期贷款机构类型过多，风险较高
            "recent_non_bank_repayments_count": 0,  # 该客户近期还款非银机构类型较多，风险较高
            "low_daily_balance": 0.00,  # 近12个月客户银行流水余额日均较低，风险较高
            "weak_fund_mobilization_ability": 0.00,  # 该客户资金调动能力弱，风险较高
            "recent_funding_pressure_trans_count": 0,  # 该客户近期资金紧张，风险较高
            "low_weighted_balance": 0.00,  # 余额日均低，风险较高
            "final_rate": 0.0542  # 额度利率
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
            GetMtrVariableInFlow(),
            MtrLoanAmt(),
            MtrLoanRate()
        ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
