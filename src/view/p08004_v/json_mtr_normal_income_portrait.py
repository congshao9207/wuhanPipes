# -*- coding: utf-8 -*-
# @Time    : 2022/6/10 15:21
# @Author  : chenwen
# @IDE     : PyCharm
# @Name    : json_mtr_normal_income_portrait.py


import pandas as pd
from view.TransFlow import TransFlow
from pandas.tseries.offsets import *


class JsonMtrNormalIncomePortrait(TransFlow):
    """
        收单流水经营性收入模块
        author:陈文
        created_time:20220610
    """

    def __init__(self):
        super().__init__()
        self.df = pd.DataFrame()
        self.period = 0

    def process(self):
        self.get_mtr_normal_detail()

    def get_mtr_normal_detail(self):
        if self.mtr_trans_flow_portrait is None:
            # 初始化
            self.variables['mtr_operation_analysis'] = []
            return
        df = self.mtr_trans_flow_portrait.copy()
        if df.shape[0] > 0:
            year_ago = pd.to_datetime(df['trans_time']).max() - DateOffset(months=12)
            # 新增交易年-月列
            df['year_month'] = df.trans_time.apply(lambda x: x.strftime('%Y-%m'))
            # 筛选近一年经营性数据
            df = df.loc[(pd.to_datetime(df.trans_time) >= year_ago)]
            # 初始化输出结构
            self.variables['mtr_operation_analysis'] = {
                "mtr_operation_form_detail": [],
                "mtr_income_month": 0,
                "mtr_unit_price": 0.0,
                "mtr_operation_trend_chart": [],
                "mtr_operation_risk_tips": ""
            }

            if df.shape[0] > 0:
                # 生成基础数据
                monthly_data = self._generate_monthly_data(df)
                # 填充表单数据
                self._fill_form_detail(monthly_data)
                # 计算统计指标
                self._calculate_monthly_stats(df, monthly_data)
                # 构建趋势图表
                self._build_trend_chart(df)
                # 生成专家建议
                self._generate_expert_tips(df)

    @staticmethod
    def _generate_monthly_data(df):
        """生成月度聚合数据"""
        return df.groupby('year_month').agg(
            trans_amt=('trans_amt', 'sum'),
            trans_cnt=('trans_amt', 'count'),
            trans_amt_avg=('trans_amt', 'mean')
        ).reset_index()

    def _fill_form_detail(self, monthly_data):
        """优化后的表单填充方法"""
        # 使用向量化操作替代逐行处理
        monthly_data = monthly_data.assign(
            trans_amt=lambda x: round(x.trans_amt / 10000, 2),
            trans_cnt=lambda x: x.trans_cnt.astype(int),
            trans_amt_avg=lambda x: round(x.trans_amt_avg, 2)
        ).rename(columns={'year_month': 'month'})

        self.variables['mtr_operation_analysis']['mtr_operation_form_detail'] = monthly_data[
            ['month', 'trans_amt', 'trans_cnt', 'trans_amt_avg']
        ].to_dict('records')

    def _calculate_monthly_stats(self, df, monthly_data):
        """计算月均统计指标"""
        total_amt = df['trans_amt'].sum()
        total_cnt = df.shape[0]
        month_count = len(monthly_data)

        # 月均收入
        self.variables['mtr_operation_analysis']['mtr_income_month'] = int(
            round(total_amt / 10000 / (month_count or 1), 0))

        # 客单价
        self.variables['mtr_operation_analysis']['mtr_unit_price'] = round(
            (total_amt / total_cnt) if total_cnt > 0 else 0, 1)

    def _build_trend_chart(self, df):
        """趋势分析图表构建"""

        def safe_aggregate(data, column, aggregate_func):
            """安全聚合方法"""
            if data.empty:
                return 0.0
            # 处理不同的聚合函数类型
            if isinstance(aggregate_func, str):
                return data[column].agg(aggregate_func)
            return aggregate_func(data[column])

        def calc_rate(current_metric, previous_metric):
            """比率计算"""
            # 统一转换为数值类型
            current = float(current_metric) if not isinstance(current_metric, (int, float)) else current_metric
            previous = float(previous_metric) if not isinstance(previous_metric, (int, float)) else previous_metric

            # 处理分母为零的情况
            if previous == 0:
                # return "0%" if current == 0 else "∞"
                return "0%"
            rate = (current - previous) / previous * 100
            return f"{rate:.2f}%"

        metrics = {
            '交易次数环比变动率': ('trans_amt', 'count'),
            '交易天数环比变动率': ('trans_time', lambda x: x.dt.date.nunique()),  # 明确转换为日期去重
            '交易额环比变动率': ('trans_amt', 'sum')
        }

        trend_data = []
        end_date = df['trans_time'].max()

        def get_month_start(date, delta_months=0):
            """获取指定月份数的首日"""
            return (pd.Timestamp(date) - pd.offsets.DateOffset(months=delta_months)) \
                .to_period('M').to_timestamp()

        for feature_name, (col, agg_func) in metrics.items():
            row_data = {"operation_feature_name": feature_name}

            for period in ['1m', '3m', '6m']:
                months = int(period[0])
                current_start = get_month_start(end_date, delta_months=months-1)
                prev_start = get_month_start(current_start, delta_months=months)

                earliest_date = df['trans_time'].min()
                current_start = max(current_start, earliest_date)
                prev_start = max(prev_start, earliest_date)

                # 使用安全筛选方法（新增边界判断）
                current_df = df[(df.trans_time >= current_start) & (df.trans_time <= end_date)]
                prev_df = df[(df.trans_time >= prev_start) & (df.trans_time < current_start)]

                # 使用安全聚合
                current_val = safe_aggregate(current_df, col, agg_func)
                prev_val = safe_aggregate(prev_df, col, agg_func)

                row_data[f"operation_last_{period}"] = calc_rate(current_val, prev_val)

            trend_data.append(row_data)

        self.variables['mtr_operation_analysis']['mtr_operation_trend_chart'] = trend_data

    def _generate_expert_tips(self, df):
        """生成专家建议"""
        total_amt = df['trans_amt'].sum()
        total_cnt = df.shape[0]
        total_amt_wan = round(total_amt / 10000, 2)
        unit_price = round(total_amt / total_cnt, 2) if total_cnt > 0 else 0

        # 基础提示
        expert_tips = [f"收单流水收入总额{total_amt_wan}万元，总订单数{total_cnt}笔，客单价{unit_price}元"]

        # 风险提示
        end_date = df['trans_time'].max()
        six_month_amt = df[df['trans_time'] >= (end_date - DateOffset(months=6))]['trans_amt'].sum()
        prev_six_month_amt = df[df['trans_time'].between(end_date - DateOffset(months=12),
                                                         end_date - DateOffset(months=6))]['trans_amt'].sum()

        if prev_six_month_amt > 0 and ((six_month_amt - prev_six_month_amt) / prev_six_month_amt) > 0.2:
            rate = round((six_month_amt - prev_six_month_amt) / prev_six_month_amt * 100, 2)
            expert_tips.append(f"近6个月交易环比变动率{rate}%，关注是否为淡旺季波动")

        self.variables['mtr_operation_analysis']['mtr_operation_risk_tips'] = ";".join(expert_tips)
