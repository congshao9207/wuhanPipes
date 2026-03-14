#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :mtr_loan_rate.py
# @Time      :2025/4/25 15:35
# @Author    :chenwen

from mapping.trans_module_processor import TransModuleProcessor


class MtrLoanRate(TransModuleProcessor):

    def __init__(self):
        super().__init__()

    def process(self):
        self._cal_loan_rate()

    def _cal_loan_rate(self):
        has_bank_flow = self.variables.get('trans_flow_src_type')
        if has_bank_flow != 0:
            return

        inter_mean = self.variables.get('a12')
        bal_mean = self.variables.get('a11')
        loan_amt = self.variables.get('final_limit')

        # 计算调整系数y
        y = self._calculate_adjustment_factor(inter_mean, bal_mean, loan_amt)

        # 计算最终利率
        base_rate = 0.0542
        final_rate = base_rate * y
        final_rate = round(max(0.0385, min(0.07, final_rate)), 4)

        self.variables['final_rate'] = final_rate

    def _calculate_adjustment_factor(self, inter_mean, bal_mean, loan_amt):
        """计算利率调整系数"""
        # 计算结息余额日均
        daily_interest_balance = self._get_daily_interest_balance(inter_mean, bal_mean)

        if daily_interest_balance is None or daily_interest_balance == 0:
            return 1.2

        if loan_amt == 0 or loan_amt is None:
            return 1.0

        ratio = daily_interest_balance / loan_amt

        if ratio >= 1 / 3:
            return 0.8
        elif ratio <= 1 / 5:
            return 1.1
        else:
            return 1.0

    @staticmethod
    def _get_daily_interest_balance(inter_mean, bal_mean):
        """获取结息余额日均值"""
        if inter_mean is None and bal_mean is None:
            return None

        valid_values = []
        if inter_mean is not None and inter_mean != 0:
            valid_values.append(inter_mean)
        if bal_mean is not None and bal_mean != 0:
            valid_values.append(bal_mean)

        return min(valid_values) if valid_values else None
