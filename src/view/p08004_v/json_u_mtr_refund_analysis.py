#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :json_u_mtr_refund_analysis.py.py
# @Time      :2025/2/24 17:50
# @Author    :chenwen

import pandas as pd
from pandas import DateOffset

from view.TransFlow import TransFlow


class JsonUnionMtrRefundAnalysis(TransFlow):
    """售后订单分析模块"""

    def __init__(self):
        super().__init__()

    def process(self):
        self._init_output_structure()
        if self.mtr_trans_flow_portrait is None:
            return
        df = self.mtr_trans_flow_portrait.copy()

        # 时间格式转换和清洗
        df['trans_time'] = pd.to_datetime(df['trans_time'], errors='coerce')
        refund_df = df[df['trans_time'].notnull() & (df['trans_status_label'] == 1) & (df['trans_amt'] < 0)]

        if refund_df.shape[0] > 0:
            refund_df['trans_amt'] = refund_df['trans_amt'].abs()
            end_date = df['trans_time'].max()
            end_date = min(end_date, pd.Timestamp.now())
            self._build_refund_form(df, refund_df, end_date)
            self._generate_risk_tips(df, refund_df, end_date)

    def _init_output_structure(self):
        self.variables['mtr_after_sales_order_analysis'] = {
            "mtr_after_sales_order_form_detail": [],
            "mtr_after_sales_order_risk_tips": ""
        }

    def _build_refund_form(self, df, refund_df, end_date):
        metrics = [
            {
                'name': '退款次数（次）',
                'field': 'cnt',
                'agg': 'count',
                'format': lambda x: int(x) if pd.notnull(x) else 0
            },
            {
                'name': '退款金额（元）',
                'field': 'amt',
                'agg': 'sum',
                'format': lambda x: round(float(x), 2) if pd.notnull(x) else 0.0
            },
            {
                'name': '退款金额率（%）',
                'field': 'rate',
                'agg': self.__class__._calc_refund_rate,
                'format': lambda x: f"{round(float(x), 1)}%" if pd.notnull(x) else "0.0%"
            }
        ]

        form_detail = []
        for metric in metrics:
            row = {"sales_feature_name": metric['name']}
            for period in [1, 3, 6, 12]:
                # 月份首日处理逻辑
                def get_month_start(date, delta_months=0):
                    return (pd.Timestamp(date) - pd.offsets.DateOffset(months=delta_months)) \
                        .to_period('M').to_timestamp()

                current_start = get_month_start(end_date, delta_months=period - 1)
                earliest_date = df['trans_time'].min()

                # 边界保护：当计算日期早于最早数据时取最早数据
                start_date = max(current_start, earliest_date)

                period_mask = (df['trans_time'] >= start_date) & (df['trans_time'] <= end_date)
                refund_period_mask = (refund_df['trans_time'] >= start_date) & (refund_df['trans_time'] <= end_date)
                period_df = df.loc[period_mask]
                refund_period_df = refund_df.loc[refund_period_mask]

                total_amt = period_df['trans_amt'].sum()
                value = 0

                if period_df.shape[0] > 0:
                    if metric['field'] == 'rate':
                        value = metric['agg'](refund_period_df, total_amt)
                    else:
                        value = refund_period_df['trans_amt'].agg(metric['agg'])

                row[f'sales_last_{period}m'] = metric['format'](value)

            form_detail.append(row)

        self.variables['mtr_after_sales_order_analysis']['mtr_after_sales_order_form_detail'] = form_detail

    @staticmethod
    def _calc_refund_rate(refund_period_df, total_amt):
        """静态方法计算退款率"""
        refund_amt = refund_period_df['trans_amt'].abs().sum()
        if total_amt <= 0 or pd.isnull(total_amt):
            return 0.0
        return (refund_amt / total_amt) * 100

    def _generate_risk_tips(self, df, refund_df, end_date):
        """风险结论"""
        yearly_start = end_date - DateOffset(years=1)
        yearly_df = df[df.trans_time.between(yearly_start, end_date)]

        if yearly_df.shape[0] > 0:
            total_amt = yearly_df['trans_amt'].sum()
            refund_rate = self.__class__._calc_refund_rate(refund_df, total_amt)
            if refund_rate > 5:
                self.variables['mtr_after_sales_order_analysis']['mtr_after_sales_order_risk_tips'] = \
                    f"<b>退单率较高</b>，近12个月退款金额比率{round(refund_rate, 2)}%，客户满意度较低，关注商户经营能力，是否存在刷单行为"
        else:
            self.variables['mtr_after_sales_order_analysis']['mtr_after_sales_order_risk_tips'] = \
                "近12个月无交易数据"