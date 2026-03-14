#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :json_u_mtr_fraud_risk_analysis.py.py
# @Time      :2025/2/25 9:49
# @Author    :chenwen

import pandas as pd
from pandas.tseries.offsets import *
from view.TransFlow import TransFlow


class JsonUnionMtrFraudRiskAnalysis(TransFlow):
    """收单流水欺诈风险分析模块"""

    def __init__(self):
        super().__init__()

    def process(self):
        self._init_output_structure()
        if self.mtr_trans_flow_portrait is None:
            return
        df = self._get_processed_data()

        if df.empty:
            return

        self._generate_form(df)
        # self._generate_risk_tips(df)

    def _init_output_structure(self):
        self.variables['mtr_fraud_risk_analysis'] = {
            "mtr_fraud_form_detail": [],
            "mtr_fraud_risk_tips": "",
            "mtr_fraud_total_cnt": 0
        }

    def _get_processed_data(self):
        """数据预处理"""
        df = self.mtr_trans_flow_portrait.copy()

        # 时间处理
        df['trans_time'] = pd.to_datetime(df['trans_time'], errors='coerce')
        df = df[df['trans_time'].notnull()]

        # 新增近一年数据筛选
        if df.shape[0] > 0:
            max_time = df['trans_time'].max()
            start_date = max_time - DateOffset(years=1)
            df = df[df['trans_time'] >= start_date]

        df['year_month'] = df['trans_time'].dt.strftime('%Y-%m')
        df['day'] = df['trans_time'].dt.date
        df['hour'] = df['trans_time'].dt.hour

        # 金额处理
        df['trans_amt'] = pd.to_numeric(df['trans_amt'], errors='coerce')
        df = df[df['trans_amt'].notnull()]

        return df

    def _generate_form(self, df):
        """向量化表单生成"""
        # 基础聚合
        base_df = df.groupby('year_month', observed=True).agg(
            total_cnt=('trans_amt', 'count')
        ).reset_index()

        # 大额交易向量化计算
        avg_amt = df['trans_amt'].mean()
        large_amt_threshold = avg_amt * 20 if avg_amt > 0 else 0
        large_mask = df['trans_amt'] >= large_amt_threshold
        large_cnt = df[large_mask].groupby('year_month', observed=True).size()
        base_df['mtr_large_amt_cnt'] = base_df['year_month'].map(large_cnt).fillna(0).astype(int)

        # 整额交易向量化计算
        round_values = [100, 1000, 10000, 100000]
        for i, value in enumerate(round_values):
            round_mask = (df['trans_amt'] % value == 0)
            round_cnt = df[round_mask].groupby('year_month', observed=True).size()
            base_df[f'mtr_{["hundred","thousand","10k","100k"][i]}_cnt'] = (
                base_df['year_month'].map(round_cnt).fillna(0).astype(int)
            )

        # 时间段交易向量化计算
        midnight_mask = df['hour'].between(0, 4, inclusive='left')
        night_mask = df['hour'].between(22, 24, inclusive='right')
        base_df['mtr_midnight_cnt'] = (
            df[midnight_mask].groupby('year_month', observed=True).size()
            .reindex(base_df['year_month'], fill_value=0).values
        )
        base_df['mtr_night_cnt'] = (
            df[night_mask].groupby('year_month', observed=True).size()
            .reindex(base_df['year_month'], fill_value=0).values
        )

        # 集中交易天数计算
        daily_counts = df.groupby(['year_month', 'day'], observed=True).size()
        avg_daily = daily_counts.groupby('year_month', observed=True).transform('mean')
        threshold = avg_daily * 20
        cluster_days = (daily_counts > threshold).groupby('year_month', observed=True).sum()
        base_df['mtr_cluster_cnt'] = base_df['year_month'].map(cluster_days).fillna(0).astype(int)

        # 风险交易笔数
        mtr_fraud_cnt = 0
        mtr_fraud_list = ['mtr_large_amt_cnt', 'mtr_midnight_cnt', 'mtr_night_cnt', 'mtr_cluster_cnt']
        for col in mtr_fraud_list:
            mtr_fraud_cnt += base_df[col].sum()

        # 结果格式化
        self.variables['mtr_fraud_risk_analysis']['mtr_fraud_form_detail'] = base_df.to_dict('records')
        self.variables['mtr_fraud_risk_analysis']['mtr_fraud_total_cnt'] = int(mtr_fraud_cnt)

        if mtr_fraud_cnt > 0:
            self.variables['mtr_fraud_risk_analysis']['mtr_fraud_risk_tips'] = \
                "请关注客户大额交易、凌晨交易、夜间交易、集中交易等指标变化，警惕流水刷单行为"

    @staticmethod
    def _count_large_amt(df, year_month, threshold):
        """统计大额交易"""
        return df[(df['year_month'] == year_month) & (df['trans_amt'] >= threshold)].shape[0] if threshold > 0 else 0

    @staticmethod
    def _count_round_amt(df, year_month, base_value):
        """统计整额交易"""
        return df[(df['year_month'] == year_month) & (df['trans_amt'] % base_value == 0)].shape[0]

    @staticmethod
    def _count_time_range(df, year_month, time_range):
        """统计时间段交易"""
        start, end = time_range
        return df[(df['year_month'] == year_month) & (df['hour'] >= start) & (df['hour'] <= end)].shape[0]

    @staticmethod
    def _count_cluster_days(df, year_month):
        """统计集中交易天数"""
        month_df = df[df['year_month'] == year_month]
        daily_cnt = month_df.groupby('day').size()
        avg_daily = daily_cnt.mean()
        threshold = avg_daily * 20 if avg_daily > 0 else 0
        return (daily_cnt > threshold).sum()

    def _generate_risk_tips(self, df):
        """风险提示"""
        latest_month = pd.Period(df['year_month'].max(), freq='M')
        recent_months = [latest_month - i for i in range(3)]
        recent_months = [str(m) for m in recent_months if m in df['year_month'].unique()]

        alert_flags = [
            any([
                self.variables['mtr_fraud_risk_analysis']['mtr_fraud_form_detail'][m]['mtr_large_amt_cnt'] > 0,
                self.variables['mtr_fraud_risk_analysis']['mtr_fraud_form_detail'][m]['mtr_hundred_cnt'] > 0,
                self.variables['mtr_fraud_risk_analysis']['mtr_fraud_form_detail'][m]['mtr_cluster_cnt'] > 0
            ])
            for m in range(len(self.variables['mtr_fraud_risk_analysis']['mtr_fraud_form_detail']))
            if self.variables['mtr_fraud_risk_analysis']['mtr_fraud_form_detail'][m]['year_month'] in recent_months
        ]

        if any(alert_flags):
            self.variables['mtr_fraud_risk_analysis']['mtr_fraud_risk_tips'] = \
                "近三个月大额交易、整额交易、集中交易指标变化大，警惕流水刷单情况"
