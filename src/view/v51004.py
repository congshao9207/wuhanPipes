#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :v51004.py.py
# @Time      :2025/2/20 16:05
# @Author    :chenwen


from mapping.tranformer import Transformer
from view.p08004_v.json_mtr_counterparty_portrait import JsonMtrCounterpartyPortrait
from view.p08004_v.json_mtr_title import JsonMtrTitle
from view.p08004_v.json_not_mtr_normal_income_portrait import JsonNotMtrNormalIncomePortrait
from view.p08004_v.json_mtr_summary_portrait import JsonMtrSummaryPortrait
from view.p08004_v.json_mtr_normal_income_portrait import JsonMtrNormalIncomePortrait
from view.p08004_v.json_u_mtr_refund_analysis import JsonUnionMtrRefundAnalysis
from view.p08004_v.json_u_mtr_fraud_risk_analysis import JsonUnionMtrFraudRiskAnalysis
from view.p08004_v.json_mtr_salary_income_analysis import JsonMtrSalaryIncomeAnalysis


class V51004(Transformer):
    """
    流水报告变量清洗
    """

    def __init__(self) -> None:
        super().__init__()

    def transform(self):
        view_handle_list = [
        ]

        if self.cached_data["single"]:
            view_handle_list = []
        else:
            view_handle_list.append(JsonMtrTitle())
            view_handle_list.append(JsonMtrCounterpartyPortrait())
            view_handle_list.append(JsonNotMtrNormalIncomePortrait())
            view_handle_list.append(JsonMtrSummaryPortrait())
            view_handle_list.append(JsonMtrNormalIncomePortrait())
            view_handle_list.append(JsonUnionMtrRefundAnalysis())
            view_handle_list.append(JsonUnionMtrFraudRiskAnalysis())
            view_handle_list.append(JsonMtrSalaryIncomeAnalysis())

        for view in view_handle_list:
            view.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            view.process()
