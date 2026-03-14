#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :get_mtr_variable_in_flow.py
# @Time      :2025/3/14 11:26
# @Author    :chenwen

from datetime import datetime
from pandas.tseries import offsets
from mapping.trans_module_processor import TransModuleProcessor
import numpy as np


class GetMtrVariableInFlow(TransModuleProcessor):

    def __init__(self):
        super().__init__()
        self.apply_date = datetime.now().date()  # 申请日期设为当前日期

    def process(self):
        self._clean_duration_gap()
        self._clean_financing_ratio()
        self._clean_operational_income()
        self._clean_balance_metrics()
        self._clean_fund_capacity()

        # 设置默认值函数
        self.variables["bank_is_non_borrower_or_spouse_account"] = 0

    def _get_date_metrics(self, df, days_threshold=150, gap_threshold=32):
        """时间跨度计算通用函数"""
        if df is None or df.empty:
            return 0, 0

        max_date = df['trans_date'].max()
        min_date = df['trans_date'].min()

        duration = (max_date - min_date).days
        date_gap = (self.apply_date - max_date).days

        return int(duration < days_threshold), int(date_gap > gap_threshold)

    def _clean_duration_gap(self):
        """清洗交易天数和时间跨度指标"""
        # 银行流水指标
        bank_duration_flag, bank_gap_flag = self._get_date_metrics(self.trans_u_flow_portrait)
        self.variables.update({
            "bank_duration_under_150_days": bank_duration_flag,
            "bank_date_gap_over_32_days": bank_gap_flag
        })

        # 收单流水指标
        mtr_duration_flag, mtr_gap_flag = self._get_date_metrics(self.mtr_trans_flow_portrait)
        self.variables.update({
            "mtr_bank_duration_under_150_days": mtr_duration_flag,
            "mtr_bank_date_gap_over_32_days": mtr_gap_flag
        })

        # 交易间隔指标
        if self.trans_u_flow_portrait is not None:
            df = self.trans_u_flow_portrait.sort_values('trans_date')
            gaps = df['trans_date'].diff().dt.days.dropna()
            self.variables["transactions_gap_over_32_days"] = int(any(gaps > 32))

    def _clean_financing_ratio(self):
        """清洗融资还款相关指标"""
        if self.trans_u_flow_portrait is None:
            return

        df = self.trans_u_flow_portrait
        # 经营性收入计算
        operational_income = df.loc[
            (df['relationship'].isna()) &
            (df['loan_type'].isna()) &
            (df['unusual_trans_type'].isna()) &
            (df['trans_amt'] > 0),
            'trans_amt'
        ].sum()

        # 融资还款计算
        loan_repayment = df.loc[
            df['loan_type'].notna() &
            (df['trans_amt'] < 0),
            'trans_amt'
        ].abs().sum()

        # 还款比例计算（保留2位小数）
        repayment_ratio = loan_repayment / operational_income if operational_income != 0 else 0.0
        self.variables["combined_repayment_ratio"] = round(repayment_ratio, 2)

        # 贷款机构统计
        loan_inst_count = len(df[df['loan_type'].notna()]['loan_type'].unique())
        self.variables["recent_loan_institution_count"] = loan_inst_count

        # 非银机构统计
        non_bank_count = df[
            (df['trans_amt'] < 0) &
            (df['loan_type'] != "银行") &
            (df['loan_type'].notna())
            ]['loan_type'].nunique()
        self.variables["recent_non_bank_repayments_count"] = non_bank_count

        # 资金压力统计
        pressure_trans_count = df[
            (df['trans_amt'] < 0) &
            (df['loan_type'] == "消金")
            ].shape[0]
        self.variables["recent_funding_pressure_trans_count"] = pressure_trans_count

    def _clean_operational_income(self):
        """清洗经营性收入指标"""
        if self.trans_u_flow_portrait is None:
            return

        df = self.trans_u_flow_portrait
        condition = (
            # 每个字段都检查两种空值情况
            (df['relationship'].isna() | (df['relationship'] == '')) &
            (df['loan_type'].isna() | (df['loan_type'] == '')) &
            (df['unusual_trans_type'].isna() | (df['unusual_trans_type'] == ''))
        )
        # 进账出账计算
        income = df.loc[condition & (df['trans_amt'] > 0), 'trans_amt'].sum()

        expense = df.loc[condition & (df['trans_amt'] < 0), 'trans_amt'].sum()

        # 计算月均净值
        net_income = income + expense  # expense为负值，相加即为净收入
        days = (df['trans_date'].max() - df['trans_date'].min()).days or 1
        monthly_avg = net_income * 30 / days
        self.variables["net_operational_income"] = float(round(monthly_avg, 2))

    def _clean_balance_metrics(self):
        """清洗余额相关指标"""
        # 余额日均
        if self.trans_u_flow_portrait is not None:
            self.daily_bal_mean(self.trans_u_flow_portrait)
            self.variables["low_daily_balance"] = round(self.variables.get("daily_bal_mean", 0), 2)

        # 加权余额
        if self.trans_u_portrait is not None and not self.trans_u_portrait.empty:
            self.variables["low_weighted_balance"] = float(self.trans_u_portrait['balance_weight_max'].iloc[0])

    def _clean_fund_capacity(self):
        """清洗资金调动能力指标"""
        if self.trans_u_flow_portrait is None:
            self.variables["weak_fund_mobilization_ability"] = 0.0
            return

        # 使用loc筛选并转换为NumPy数组
        positive_income = self.trans_u_flow_portrait.loc[
            (self.trans_u_flow_portrait['trans_amt'] > 0) &
            (self.trans_u_flow_portrait['trans_amt'].notna()),
            'trans_amt'
        ]
        if positive_income.empty:
            self.variables["weak_fund_mobilization_ability"] = 0.0
            return
        # 转换为NumPy数组（过滤空值）
        income_array = positive_income.to_numpy(copy=True)
        # 使用numpy.percentile计算（等效于Pandas的quantile(0.9)）
        quantile_90 = np.percentile(income_array, 90, method='linear')

        # 结果处理
        self.variables["weak_fund_mobilization_ability"] = round(float(quantile_90), 2)

    def daily_bal_mean(self, df):
        """优化后的余额日均计算"""
        balance_amt = 0
        try:
            for account_id in df['account_id'].unique():
                account_df = df[df['account_id'] == account_id]
                end_date = account_df['trans_date'].max()
                start_date = end_date - offsets.DateOffset(months=12)

                balance_df = account_df[account_df['trans_date'].between(start_date, end_date)]
                if balance_df.empty:
                    continue

                # 填充缺失日期
                balance_df = balance_df.set_index('trans_date').resample('D').last().ffill()
                total_days = len(balance_df)
                balance_amt += balance_df['account_balance'].sum() / total_days

            self.variables['daily_bal_mean'] = round(balance_amt, 2)
        except Exception as e:
            print(f"Error calculating daily balance: {str(e)}")
            self.variables['daily_bal_mean'] = 0
