#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :mtr_loan_amt.py
# @Time      :2025/3/14 16:11
# @Author    :chenwen

import pandas as pd
from mapping.trans_module_processor import TransModuleProcessor


class MtrLoanAmt(TransModuleProcessor):
    """
    流水贷额度计算核心处理器
    输入数据：
    - 银行/第三方流水（交易明细+月度汇总）
    - 收单流水（交易明细含对手信息）
    输出指标：
    a11(余额日均) | a12(结息日均) | a2(经营净收入) | mtr_monthly_avg(收单月均)
    → adjust_coeff(调额系数) → final_limit(最终额度)

    核心逻辑：
    1. 数据组合计算
    2. 基础额度(a)计算
    3. 最终额度
    """

    def __init__(self):
        super().__init__()
        # 新增流水类型标识
        self.has_bank = False  # 是否含银行流水
        self.has_third_party = False  # 是否含微信支付宝
        self.has_mtr = False  # 是否含收单流水
        self.flow_data = None
        self.union_summary = None
        self.mtr_flow = None  # 新增收单流水属性
        self.variables = {}

    def process(self):
        self.get_trans_flow_detail()
        self.get_mtr_flow_detail()
        self._determine_flow_types()  # 新增流水类型判断

        self.calculate_base_indicators()
        self._calculate_adjustment_coefficient()  # 新增调额系数计算
        self.calculate_a()
        self.calculate_final()

    def _determine_flow_types(self):
        """判断流水类型"""
        # 银行/微信支付宝流水判断
        if not self.flow_data.empty:
            src_types = self.flow_data['trans_flow_src_type'].unique().tolist()
            self.has_bank = True if 1 in src_types or 0 in src_types else False
            self.has_third_party = True if 2 in src_types else False

        # 收单流水判断
        self.has_mtr = not self.mtr_flow.empty if self.mtr_flow is not None else False

        # 新增流水类型判断逻辑
        if self.has_bank:  # 存在银行流水
            trans_flow_src_type = 0
        elif self.has_third_party and not self.has_mtr:  # 仅微信支付宝
            trans_flow_src_type = 1
        elif self.has_mtr and not self.has_third_party:  # 仅收单流水
            trans_flow_src_type = 2
        elif self.has_third_party and self.has_mtr:  # 同时有微信支付宝和收单流水
            trans_flow_src_type = 3
        else:  # 其他未知情况
            trans_flow_src_type = 0

        self.variables['trans_flow_src_type'] = trans_flow_src_type

    @staticmethod
    def _calculate_operational_income(flow_df):
        """统一计算经营性收入"""
        condition = (
                (flow_df['relationship'].isna() | (flow_df['relationship'] == '')) &
                (flow_df['loan_type'].isna() | (flow_df['loan_type'] == '')) &
                (flow_df['unusual_trans_type'].isna() | (flow_df['unusual_trans_type'] == '')) &
                (flow_df['trans_amt'] > 0)
        )
        return flow_df[condition]['trans_amt'].sum()

    def calculate_a(self):
        """重构后的基础额度计算"""
        # 根据流水类型选择计算方式
        if self.has_bank:
            # 原有银行流水计算逻辑
            a11 = self.variables.get('a11', 0)
            a12 = self.variables.get('a12', 0)
            a1 = (min(a11, a12) if a12 else a11) * 6
            a2 = self.variables.get('a2', 0)
            a = 0.1 * a1 + 0.9 * a2 if a1 else 0
        else:
            # 新逻辑：微信支付宝/收单组合计算
            income = 0
            if self.has_third_party:
                income += self._calculate_operational_income(self.flow_data)
            if self.has_mtr:
                income += self.mtr_flow['trans_amt'].sum()

            rate = 0.35 if not self.has_third_party else 0.25
            a = income * rate

        self.variables['a'] = float(round(a, 2))

    def _calculate_adjustment_coefficient(self):
        """新增调额系数计算"""
        ratio = 0.0

        if (self.has_bank or self.has_third_party) and not self.flow_data.empty:
            total_income = self.flow_data[self.flow_data['trans_amt'] > 0]['trans_amt'].sum()
            top10 = self.flow_data.groupby('opponent_name')['trans_amt'].sum().nlargest(10).sum()
            ratio = top10 / total_income if total_income > 0 else 0.0
        elif self.has_mtr and not self.mtr_flow.empty:
            total_income = self.mtr_flow['trans_amt'].sum()
            top10 = self.mtr_flow.groupby('opponent_name')['trans_amt'].sum().nlargest(10).sum()
            ratio = top10 / total_income if total_income > 0 else 0.0
        else:
            # 当没有有效流水时仍然记录比例值
            self.variables['adjust_coeff'] = 1.0
            self.variables['top_10_income_amt_prop'] = 0.0  # 确保指标存在
            return

        # 统一设置指标
        self.variables['top_10_income_amt_prop'] = float(round(ratio, 4))
        if ratio <= 0.3:
            coeff = 1.1
        elif ratio <= 0.6:
            coeff = 1.05
        elif ratio <= 0.95:
            coeff = 1.0
        else:
            coeff = 0.9 if (self.has_bank or self.has_third_party) else 1.0

        self.variables['adjust_coeff'] = float(coeff)
        # self.variables['a'] *= coeff  # 直接应用系数调整

    def calculate_base_indicators(self):
        self._calculate_a11_a12()
        self._calculate_a2()
        self._calculate_mtr_avg()  # 收单日均计算
        self._calculate_monthly_avg()  # 计算月均
        self._calculate_compensation_metrics()  # 新增补偿指标计算

    def _calculate_compensation_metrics(self):
        """补偿额度指标计算"""
        # 指标1：净收入>0的月份占比
        net_income_month_cnt_prop = 0.0
        if not self.flow_data.empty:
            # 生成月份序列
            monthly_net = (self.flow_data.groupby(pd.Grouper(key='trans_date', freq='M'))
                           .apply(lambda x: x[x['trans_amt'] > 0]['trans_amt'].sum()
                                            - x[x['trans_amt'] < 0]['trans_amt'].abs().sum()))
            positive_months = monthly_net[monthly_net > 0].count()
            total_months = monthly_net.count()
            net_income_month_cnt_prop = round(positive_months / total_months, 4) if total_months > 0 else 0.0

        # 指标2：经营性进账金额
        normal_income = 0.0
        # 合并银行流水和收单流水
        all_income_data = pd.concat([self.flow_data, self.mtr_flow], ignore_index=True) if self.mtr_flow is not None else self.flow_data

        if not self.flow_data.empty:
            condition = (
                    (all_income_data['relationship'].isna() | (all_income_data['relationship'] == '')) &
                    (all_income_data['loan_type'].isna() | (all_income_data['loan_type'] == '')) &
                    (all_income_data['unusual_trans_type'].isna() | (all_income_data['unusual_trans_type'] == '')) &
                    (all_income_data['trans_amt'] > 0)
            )
            normal_income = all_income_data[condition]['trans_amt'].sum()
        else:
            normal_income = self.mtr_flow['trans_amt'].sum()

        self.variables.update({
            'net_income_amt_prop': float(net_income_month_cnt_prop),
            'normal_income_amt': float(round(normal_income, 2))
        })

    def _calculate_monthly_avg(self):
        """统一计算月均指标"""
        if self.has_bank:
            # 银行流水优先计算（含微信支付宝）
            filtered_flow = self._filter_operational_flow(self.flow_data)
            monthly_avg = self._calc_flow_monthly_avg(filtered_flow)
        elif self.has_third_party and self.has_mtr:
            # 微信支付宝+收单组合计算
            wechat_flow = self._filter_operational_flow(self.flow_data)
            combined_amt = wechat_flow['trans_amt'].sum() + self.mtr_flow['trans_amt'].sum()
            monthly_avg = self._calc_combined_monthly_avg(wechat_flow, self.mtr_flow, combined_amt)
        elif self.has_third_party:
            # 仅微信支付宝
            filtered_flow = self._filter_operational_flow(self.flow_data)
            monthly_avg = self._calc_flow_monthly_avg(filtered_flow)
        else:
            # 仅收单流水或没有流水
            monthly_avg = self.variables.get('mtr_monthly_avg', 0)

        self.variables['monthly_avg'] = float(round(monthly_avg, 2))

    def _calculate_mtr_avg(self):
        """计算收单月均(按自然月计算)"""
        if self.mtr_flow is None or self.mtr_flow.empty:
            self.variables['mtr_monthly_avg'] = 0.0
            return

        # 计算实际天数占比
        max_date = self.mtr_flow['trans_date'].max()
        min_date = self.mtr_flow['trans_date'].min()
        total_days = (max_date - min_date).days + 1
        monthly_avg = (self.mtr_flow['trans_amt'].sum() / total_days) * 30
        self.variables['mtr_monthly_avg'] = round(monthly_avg, 2)

    @staticmethod
    def _filter_operational_flow(df):
        """筛选经营性流水（银行/微信支付宝）"""
        condition = (
                (df['relationship'].isna() | (df['relationship'] == '')) &
                (df['loan_type'].isna() | (df['loan_type'] == '')) &
                (df['unusual_trans_type'].isna() | (df['unusual_trans_type'] == '')) &
                (df['trans_amt'] > 0)
        )
        return df[condition]

    def _calc_flow_monthly_avg(self, df):
        """单一流水月均计算"""
        if df.empty:
            return 0
        return self._calculate_general_monthly_avg(df, 'trans_date')

    @staticmethod
    def _calc_combined_monthly_avg(df1, df2, total_amt):
        """组合流水月均计算"""
        # 统一转换为Timestamp类型
        df1['trans_date'] = pd.to_datetime(df1['trans_date'])
        df2['trans_date'] = pd.to_datetime(df2['trans_date'])

        dates = pd.concat([df1['trans_date'], df2['trans_date']])
        max_date = dates.max()
        min_date = dates.min()
        total_days = (max_date - min_date).days + 1
        return (total_amt / total_days) * 30

    def calculate_final(self):
        """重构后的最终额度计算"""
        a = self.variables.get('a', 0)

        # 统一计算月均指标
        if self.has_bank or self.has_third_party:
            monthly_avg = self._calculate_general_monthly_avg(self.flow_data, 'trans_date')
        else:
            monthly_avg = self.variables.get('mtr_monthly_avg', 0)

        # 分档逻辑保持不变（添加浮点型处理）
        if monthly_avg >= 10000.0:  # 改为浮点型比较
            final = min(300000.0, max(a, 50000.0))
        elif monthly_avg >= 2000.0:  # 改为浮点型比较
            final = min(300000.0, max(a, monthly_avg * 12 * 0.4))
        else:
            final = a

        # 整万处理逻辑
        final = int(final // 10000) * 10000 if final >= 10000 else 0
        self.variables['final_limit'] = final

    @staticmethod
    def _calculate_general_monthly_avg(df, date_col):
        """通用月均计算"""
        if df.empty:
            return 0
        max_date = df[date_col].max()
        min_date = df[date_col].min()
        total_days = (max_date - min_date).days + 1
        return (df['trans_amt'].sum() / total_days) * 30

    def get_trans_flow_detail(self):
        flow_data = self.trans_u_flow_portrait
        union_summary = self.trans_u_summary_portrait
        if flow_data is None or flow_data.empty:
            self.flow_data = pd.DataFrame()
            self.union_summary = pd.DataFrame()
            return
        flow_data['trans_flow_src_type'].fillna(0, inplace=True)
        self.flow_data = flow_data
        self.union_summary = union_summary

    # 新增收单流水处理
    def get_mtr_flow_detail(self):
        if self.mtr_trans_flow_portrait is not None:
            self.mtr_flow = self.mtr_trans_flow_portrait.copy()

    def _calculate_a11_a12(self):
        """计算余额日均(a11)和结息日均(a12)"""
        # 判断是否存在银行流水
        if not self.has_bank:
            return
        if self.union_summary is None:
            self.variables.update({'a11': None, 'a12': None})
            return
        # 清理无效月份数据（新增过滤逻辑）
        union_summary = self.union_summary.loc[
            ~self.union_summary.month.str.contains(r'\d', regex=True)
        ].reset_index(drop=True)
        # 近一年数据优先
        a11 = self._get_balance(union_summary, 'year') or self._get_balance(union_summary, 'half_year')
        a12 = self._get_interest(union_summary, 'year') or self._get_interest(union_summary, 'half_year')

        self.variables.update({
            'a11': float(round(a11, 2)) if a11 else 0,
            'a12': float(round(a12, 2)) if a12 else 0
        })

    @staticmethod
    def _get_balance(union_summary, period):
        """获取指定周期的余额日均"""
        if union_summary is None or union_summary.empty:
            return None
        mask = union_summary.month.str.startswith(f'{period}')
        if mask.any():
            return union_summary.loc[mask, 'balance_amt'].values[0]
        return None

    @staticmethod
    def _get_interest(union_summary, period):
        """获取指定周期的结息日均"""
        if union_summary is None or union_summary.empty:
            return None
        mask = union_summary.month.str.startswith(f'{period}')
        if mask.any():
            return union_summary.loc[mask, 'interest_amt'].values[0]
        return None

    def _calculate_a2(self):
        """计算近一年经营性净收入"""
        # 初始化变量
        operational_net_income = 0.0
        mtr_income = 0.0
        # 指标1：净收入>0的月份占比
        net_income_month_cnt_prop = 0.0
        # 指标2：经营性进账金额
        normal_income = 0.0

        # 计算银行/第三方流水的经营性净收入
        if self.flow_data is not None and not self.flow_data.empty:
            condition = (
                    (self.flow_data['relationship'].isna() | (self.flow_data['relationship'] == '')) &
                    (self.flow_data['loan_type'].isna() | (self.flow_data['loan_type'] == '')) &
                    (self.flow_data['unusual_trans_type'].isna() | (self.flow_data['unusual_trans_type'] == ''))
            )
            operational_net_income = self.flow_data[condition]['trans_amt'].sum()

            # 生成月份序列
            self.flow_data['trans_date'] = pd.to_datetime(self.flow_data['trans_date'])
            monthly_net = (self.flow_data[condition].groupby(pd.Grouper(key='trans_date', freq='M'))
                           .apply(lambda x: x['trans_amt'].sum()))
            positive_months = monthly_net[monthly_net > 0].count()
            total_months = monthly_net.count()
            net_income_month_cnt_prop = round(positive_months / total_months, 4) if total_months > 0 else 0.0

        # 计算收单流水的全量收入
        if self.has_mtr and self.mtr_flow is not None and not self.mtr_flow.empty:
            mtr_income = self.mtr_flow['trans_amt'].sum()

        # 计算最终a2值
        total_income = operational_net_income + mtr_income
        a2 = min(operational_net_income * 0.25, 1000000) if operational_net_income > 0 else min(mtr_income * 0.35, 1000000)

        # 存入变量
        self.variables['a2'] = float(round(a2, 2))
        self.variables['net_income_amt'] = float(round(total_income, 2))

        """补偿额度指标计算"""

        if not self.flow_data.empty:
            condition2 = (
                    (self.flow_data['relationship'].isna() | (self.flow_data['relationship'] == '')) &
                    (self.flow_data['loan_type'].isna() | (self.flow_data['loan_type'] == '')) &
                    (self.flow_data['unusual_trans_type'].isna() | (self.flow_data['unusual_trans_type'] == '')) &
                    (self.flow_data['trans_amt'] > 0)
            )
            operational_income = self.flow_data[condition2]['trans_amt'].sum()
            normal_income = operational_income + mtr_income

        self.variables.update({
            'net_income_month_cnt_prop': float(net_income_month_cnt_prop),
            'normal_income_amt': round(normal_income, 2)
        })
