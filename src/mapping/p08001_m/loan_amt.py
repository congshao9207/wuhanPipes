import pandas as pd
from datetime import datetime
import numpy as np
from pandas.tseries.offsets import *

from mapping.trans_module_processor import TransModuleProcessor


class LoanAmt(TransModuleProcessor):
    """
    20220822更新：
        1.若只有微信支付宝流水，则稳定性系数各类指标给默认值1
        2.若同时有银行流水、微信支付宝流水，则稳定性系数部分、资金调动能力部分，均只考虑银行流水，微信支付宝流水不参与计算
        3.重构文件，不再通过DataFrame更新各类指标，而是直接计算各类指标
    """

    def __init__(self):
        super().__init__()
        self.flow_data = None
        self.union_summary = None
        self.trans_flow_src_type = 0

    def process(self):
        self.get_file_src_type()
        self.get_trans_flow_detail()
        if self.flow_data is not None:
            self.calculate_feature()
            # self.calculate_credit_amt()

    def get_file_src_type(self):
        # 判断流水文件类型，若只有微信支付宝文件，则给文件类型参数trans_flow_src_type赋值1
        flow_data = self.trans_u_flow_portrait
        if not flow_data.empty:
            flow_data['trans_flow_src_type'].fillna(0, inplace=True)
            trans_flow_src_type_list = flow_data.trans_flow_src_type.unique().tolist()
            if len(trans_flow_src_type_list) == 1 and 1 in trans_flow_src_type_list:
                self.trans_flow_src_type = 1
                self.variables['trans_flow_src_type'] = 1

    # def calculate_credit_amt(self):
    #     # 计算基础额度
    #     # 计算基础额度中的结息和余额日均a1
    #     if pd.notna(self.mean_year):
    #         self.variables['a1'] = self.mean_year
    #     elif pd.notna(self.mean_half):
    #         self.variables['a1'] = self.mean_half
    #
    #     # 计算基础额度中的经营性收入a2
    #     a2 = self.calculate_amt(self.normal_income_amt, self.net_income_amt)
    #
    #     # 计算b11
    #     if 'large_income_cnt' in temp_df.columns.tolist():
    #         large_income_cnt = temp_df.large_income_cnt.values[0]
    #         if large_income_cnt < 5:
    #             b11 = 0.7
    #         elif large_income_cnt < 10:
    #             b11 = 0.9
    #         elif large_income_cnt < 20:
    #             b11 = 1
    #         elif large_income_cnt >= 20:
    #             b11 = 1.1
    #         else:
    #             b11 = 0
    #
    #         self.credit_df.loc[temp_df.index, 'b11'] = b11
    #         # self.variable['b11'] = b11
    #
    #     # 计算b12
    #     if 'large_income_period' in temp_df.columns.tolist():
    #         large_income_period = temp_df.large_income_period.values[0]
    #         if large_income_period <= 2:
    #             b12 = 0.7
    #         elif large_income_period <= 3:
    #             b12 = 0.9
    #         elif large_income_period <= 5:
    #             b12 = 1
    #         elif large_income_period <= 7:
    #             b12 = 1.1
    #         else:
    #             b12 = 0
    #
    #         self.credit_df.loc[temp_df.index, 'b12'] = b12
    #         # self.variable['b12'] = b12
    #
    #     # 计算大额进账占比 b2
    #     top_opponent = temp_df.top_three_opponent.values[0]
    #     if top_opponent <= 0.4:
    #         b2 = 0.9
    #     elif top_opponent <= 0.6:
    #         b2 = 1
    #     elif top_opponent <= 0.8:
    #         b2 = 1.1
    #     elif top_opponent <= 1:
    #         b2 = 0.9
    #     else:
    #         b2 = 0
    #
    #     self.credit_df.loc[temp_df.index, 'b2'] = b2
    #
    #     # 计算异常交易占比 b3
    #     unusual_trans_rate = temp_df.unusual_trans_rate.values[0]
    #     if unusual_trans_rate < 0.1:
    #         b3 = 1.1
    #     elif unusual_trans_rate < 0.3:
    #         b3 = 1
    #     elif unusual_trans_rate <= 1:
    #         b3 = 0.9
    #     else:
    #         b3 = 0
    #
    #     self.credit_df.loc[temp_df.index, 'b3'] = b3
    #
    #     # 计算季度进账变异系数 b4
    #     rate_quarter = temp_df.rate_quarter.values[0]
    #     if 0 <= rate_quarter < 0.5:
    #         b4 = 1.1
    #     elif rate_quarter <= 1:
    #         b4 = 1
    #     elif rate_quarter > 1:
    #         b4 = 0.9
    #     else:
    #         b4 = 0
    #
    #     self.credit_df.loc[temp_df.index, 'b4'] = b4
    #
    #     # 计算资金调动能力
    #     income_loanable = temp_df.income_loanable.values[0]
    #     if income_loanable >= 100 * 10000:
    #         income_loanable = 100 * 10000.
    #     cost_loanable = abs(temp_df.cost_loanable.values[0])
    #     if cost_loanable >= 100 * 10000:
    #         cost_loanable = 100 * 10000.
    #     funds_max = max(income_loanable, cost_loanable)
    #     # 成本项支出方式
    #     if funds_max <= 2 * 10000:
    #         funds_min = 0.
    #     elif funds_max <= 10 * 10000:
    #         funds_min = funds_max - 2 * 10000
    #     elif funds_max <= 40 * 10000:
    #         funds_min = funds_max - 5 * 10000
    #     elif funds_max > 40 * 10000:
    #         funds_min = funds_max - 10 * 10000
    #     else:
    #         funds_min = 0.
    #
    #     self.credit_df.loc[temp_df.index, 'funds_max'] = funds_max
    #     self.credit_df.loc[temp_df.index, 'funds_min'] = funds_min
    #
    #     # 计算基础额度
    #     if 'a1' in self.credit_df.columns.tolist():
    #         a = 0.4 * a1 * 6 + 0.6 * a2
    #     else:
    #         a = a2
    #     self.variables['a'] = a
    #
    #     # 计算稳定性系数
    #     if 'large_income_cnt' in self.credit_df.columns.tolist():
    #         b1 = 0.6 * b11 + 0.4 * b12
    #         b = 0.35 * b1 + 0.35 * b2 + 0.15 * b3 + 0.15 * b4
    #         self.variables['b1'] = b1
    #     else:
    #         b = 0.5 * b2 + 0.25 * b3 + 0.25 * b4
    #     self.variables['b'] = b
    #
    #     # 计算额度
    #     c = a * b
    #
    #     self.credit_df.loc[temp_df.index, 'c'] = c
    #
    #     # 额度范围比较
    #
    #     if c <= funds_min:
    #         credit = funds_min
    #     elif c <= funds_max:
    #         credit = c
    #     elif c > funds_max:
    #         credit = funds_max
    #     else:
    #         credit = 0
    #
    #     self.credit_df.loc[temp_df.index, 'credit'] = credit

    def calculate_feature(self):
        self.base_amount()
        self.large_income()
        self.top_opponent()
        self.unusual_trans()
        self.quarter_income()
        self.loanable_funds()

    @staticmethod
    # 定义一个net_income与income占比关系的函数，返回一个系数
    def cor_income_cost(income, net_income):
        if net_income < (-1 * income):
            cor = 0.4
        elif net_income < (-0.5 * income):
            cor = 0.6
        elif net_income < 0:
            cor = 0.8
        elif net_income <= income:
            cor = 1
        else:
            cor = None
        return cor

    @staticmethod
    # 定义一个计算经营性收入的函数
    def calculate_amt(income, net_income):
        if 0 <= income <= 500 * 10000:
            a = income * 0.05 * LoanAmt.cor_income_cost(income, net_income)
        elif income <= 1000 * 10000:
            a = income * 0.05 * LoanAmt.cor_income_cost(income, net_income)
            if a > 40 * 10000:
                a = 40 * 10000
        elif income <= 2000 * 10000:
            a = income * 0.04 * LoanAmt.cor_income_cost(income, net_income)
            if a > 79 * 10000:
                a = 70 * 10000
        elif income <= 3000 * 10000:
            a = income * 0.035 * LoanAmt.cor_income_cost(income, net_income)
            if a > 100 * 10000:
                a = 100 * 10000
        elif income > 3000 * 10000:
            a = 100 * 10000 * LoanAmt.cor_income_cost(income, net_income)
        else:
            a = None
        return a

    def base_amount(self):
        """
        基础额度部分，得到结息和余额，经营性进账，出账，成本
        :return:
        """
        trans_flow, union_summary = self.flow_data, self.union_summary
        if union_summary is not None:
            use_col = ['month', 'interest_amt', 'balance_amt']
            union_summary = union_summary[use_col]
            union_summary = union_summary.loc[~union_summary.month.str.contains('0|1|2|3|4|5|6|7|8|9')].reset_index(
                drop=True)
            if not union_summary.empty:
                col = union_summary['month'].tolist()
                if 'year' not in col:
                    interest_half = union_summary.loc[0, 'interest_amt']
                    interest_year = None
                    balance_half = union_summary.loc[0, 'balance_amt'] if pd.notna(
                        union_summary.loc[0, 'balance_amt']) else None
                    balance_year = None
                else:
                    interest_half = union_summary.loc[union_summary.month == 'half_year', 'interest_amt'].values[0]
                    interest_year = union_summary.loc[union_summary.month == 'year', 'interest_amt'].values[0]
                    balance_half = union_summary.loc[union_summary.month == 'half_year', 'balance_amt'].values[0]
                    balance_year = union_summary.loc[union_summary.month == 'year', 'balance_amt'].values[0]

                # 计算结息日均和余额日均
                if pd.notna(interest_year) and pd.notna(balance_year):
                    mean_half = min(interest_half, balance_half)
                    mean_year = min(interest_year, balance_year)
                elif pd.notna(interest_year) and pd.notna(balance_half):
                    mean_half = min(interest_half, balance_half)
                    mean_year = interest_year
                elif pd.notna(interest_half) and pd.notna(balance_half):
                    mean_year = None
                    mean_half = min(interest_half, balance_half)
                elif pd.notna(interest_half):
                    mean_half = interest_half
                    mean_year = None
                else:
                    mean_half = None
                    mean_year = None

                in_bal_list = ['interest_half', 'interest_year', 'balance_half', 'balance_year', 'mean_half',
                               'mean_year']
                for i in in_bal_list:
                    if i is not None:
                        self.variables[i] = eval(i)

                # 计算a1
                if pd.notna(mean_year):
                    self.variables['a1'] = mean_year
                elif pd.notna(mean_half):
                    self.variables['a1'] = mean_half

        if trans_flow is not None:
            not_sensitive_df = trans_flow[pd.isnull(trans_flow.relationship) &
                                          pd.isna(trans_flow.loan_type) &
                                          pd.isna(trans_flow.unusual_trans_type)]
            cost_df = trans_flow[
                pd.notnull(trans_flow.cost_type) & (~trans_flow.cost_type.astype(str).str.contains('到期贷款'))]

            # 经营性进账、出账、净收入
            normal_income_amt = not_sensitive_df.loc[not_sensitive_df.trans_amt >= 0]['trans_amt'].sum()
            normal_expense_amt = cost_df.trans_amt.sum()
            net_income_amt = normal_income_amt + normal_expense_amt

            self.variables['normal_income_amt'] = normal_income_amt
            self.variables['normal_expense_amt'] = normal_expense_amt
            self.variables['net_income_amt'] = net_income_amt

    def large_income(self):
        # 大额进账版块
        trans_flow = self.flow_data
        # 考虑经营性收入，且账户类型为普通银行流水
        if trans_flow is not None:
            large_income_data = trans_flow[(pd.isnull(trans_flow.relationship)) &
                                           pd.isna(trans_flow.loan_type) &
                                           pd.isna(trans_flow.unusual_trans_type) &
                                           (trans_flow.trans_flow_src_type == 0) &
                                           (trans_flow.trans_date > (
                                                   trans_flow.trans_date.max() - DateOffset(months=6)))]
            if not large_income_data.empty:
                # 近6个月大额进账，近6个月大额进账周期
                if pd.notna(self.variables['mean_year']):
                    base_amt = self.variables['mean_year']
                elif pd.notna(self.variables['mean_half']):
                    base_amt = self.variables['mean_half']
                else:
                    base_amt = None
                if base_amt is not None:
                    self.variables['large_income_cnt'] = large_income_data.loc[
                        large_income_data.trans_amt > (base_amt * 0.8)].shape[0]
                    self.variables['large_income_period'] = len(
                        set(large_income_data.loc[large_income_data.trans_amt > (base_amt * 0.8)].trans_date.dt.month))

    def top_opponent(self):
        """
        近一年前三大交易对手进账金额占比
        用正常交易流水，剔除异常、关联交易数据
        筛选交易对手非空数据，再计算排名前n交易对手交易金额占比
        20220823更新:只考虑银行流水，微信支付宝流水给默认值
        :return:
        """
        trans_flow = self.flow_data
        if trans_flow is not None:
            if self.trans_flow_src_type == 0:
                operational_data = trans_flow[(pd.isnull(trans_flow.relationship))
                                              & pd.isna(trans_flow.loan_type)
                                              & pd.isna(trans_flow.unusual_trans_type)
                                              & (trans_flow.trans_amt >= 0)
                                              & (trans_flow.trans_flow_src_type == 0)
                                              & (trans_flow.trans_date > (
                        trans_flow.trans_date.max() - DateOffset(years=1)))]
                if not operational_data.empty:
                    opponent_data = operational_data.groupby('opponent_name').agg({'trans_amt': sum}).sort_values(
                        'trans_amt', ascending=False).reset_index()
                    self.variables['top_three_opponent'] = round(
                        opponent_data.trans_amt[:3].sum() / opponent_data.trans_amt.sum(), 2)
            else:
                self.variables['top_three_opponent'] = 0.5

    def unusual_trans(self):
        """
        异常交易占比，取近一年数据即可。
        :return:
        """
        trans_flow = self.flow_data
        if trans_flow is not None:
            if self.trans_flow_src_type == 0:
                unusual_trans_data = trans_flow[
                    (trans_flow.trans_date > (trans_flow.trans_date.max() - DateOffset(years=1)))
                    & (trans_flow.trans_flow_src_type == 0)]
                if not unusual_trans_data.empty:
                    unusual_trans_cnt = unusual_trans_data.loc[pd.notna(unusual_trans_data['unusual_trans_type']) & (unusual_trans_data['unusual_trans_type'] != '')].shape[0]
                    trans_cnt = unusual_trans_data.shape[0]
                    self.variables['unusual_trans_cnt'] = unusual_trans_cnt
                    self.variables['trans_cnt'] = trans_cnt
                    self.variables['unusual_trans_rate'] = round(unusual_trans_cnt / trans_cnt, 2)
            else:
                self.variables['unusual_trans_rate'] = 0.2

    def quarter_income(self):
        """
        季度进账变异系数
        1、四个季度的经营性进账，用这四个数据的标准差除以均值
        用近一年数据，季度用自然季度，如一季度就是1-3月，不同年份同一季度也合并到一起
        :return:
        """
        trans_flow = self.flow_data
        if trans_flow is not None:
            if self.trans_flow_src_type == 0:
                arr = []
                quarter_trans_data = trans_flow[(trans_flow.trans_amt >= 0) &
                                                (trans_flow.trans_flow_src_type == 0) &
                                                (trans_flow.trans_date > (
                                                        trans_flow.trans_date.max() - DateOffset(years=1)))]
                if not quarter_trans_data.empty:
                    quarter_trans_data['trans_date_m'] = quarter_trans_data['trans_date'].dt.month
                    quarter_income_data = quarter_trans_data.groupby('trans_date_m').agg({'trans_amt': 'sum'})
                    quarter_1_amt = round(quarter_income_data[quarter_income_data.index <= 3]['trans_amt'].sum(), 2)
                    quarter_2_amt = round(
                        quarter_income_data[(quarter_income_data.index > 3) & (quarter_income_data.index <= 6)][
                            'trans_amt'].sum(), 2)
                    quarter_3_amt = round(
                        quarter_income_data[(quarter_income_data.index > 6) & (quarter_income_data.index <= 9)][
                            'trans_amt'].sum(), 2)
                    quarter_4_amt = round(
                        quarter_income_data[(quarter_income_data.index > 9) & (quarter_income_data.index <= 12)][
                            'trans_amt'].sum(), 2)
                    for i in [quarter_1_amt, quarter_2_amt, quarter_3_amt, quarter_4_amt]:
                        arr.append(i)
                    mean_quarter_amt = round(np.mean(arr), 2)
                    std_quarter_amt = round(np.std(arr, ddof=1), 2)
                    self.variables['quarter_1_amt'] = quarter_1_amt
                    self.variables['quarter_2_amt'] = quarter_2_amt
                    self.variables['quarter_3_amt'] = quarter_3_amt
                    self.variables['quarter_4_amt'] = quarter_4_amt
                    self.variables['mean_quarter_amt'] = mean_quarter_amt
                    self.variables['std_quarter_amt'] = std_quarter_amt
                    self.variables['rate_quarter'] = round(std_quarter_amt / mean_quarter_amt, 2)
            else:
                self.variables['rate_quarter'] = 0.5

    def loanable_funds(self):
        """
        经营性流水
        :return:
        """
        trans_flow = self.flow_data
        if trans_flow is not None:
            df = trans_flow[pd.isnull(trans_flow.relationship) &
                            pd.isna(trans_flow.loan_type) &
                            pd.isna(trans_flow.unusual_trans_type)]
            if not df.empty:
                normal_income_data = df.loc[df.trans_amt >= 0 & (pd.isnull(df.is_financing))]
                cost_data = df.loc[(df.trans_amt < 0) & pd.notna(df.cost_type)
                                   & (~df.cost_type.astype(str).str.contains('到期贷款')) & (pd.isnull(df.is_financing))]

                if normal_income_data.empty:
                    income_loanable = 0.
                else:
                    income_loanable = np.nanpercentile(normal_income_data.trans_amt, 90, interpolation='linear')
                if cost_data.empty:
                    cost_loanable = 0.
                else:
                    cost_data.trans_amt = cost_data.trans_amt.abs()
                    cost_loanable = np.nanpercentile(cost_data.trans_amt, 90, interpolation='linear')

                # 计算资金调动能力
                if income_loanable >= 100 * 10000:
                    income_loanable = 100 * 10000.
                if cost_loanable >= 100 * 10000:
                    cost_loanable = 100 * 10000.
                funds_max = max(income_loanable, cost_loanable)
                # 成本项支出方式
                if funds_max <= 2 * 10000:
                    funds_min = 0.
                elif funds_max <= 10 * 10000:
                    funds_min = funds_max - 2 * 10000
                elif funds_max <= 40 * 10000:
                    funds_min = funds_max - 5 * 10000
                elif funds_max > 40 * 10000:
                    funds_min = funds_max - 10 * 10000
                else:
                    funds_min = 0.

                self.variables['funds_max'] = funds_max
                self.variables['funds_min'] = funds_min

    def get_trans_flow_detail(self):
        flow_data = self.trans_u_flow_portrait
        union_summary = self.trans_u_summary_portrait
        if flow_data.empty:
            return
        # 给trans_flow_src_type为空的填充0
        flow_data['trans_flow_src_type'].fillna(0, inplace=True)
        flow_data.trans_date = pd.to_datetime(flow_data.trans_date)

        self.flow_data = flow_data
        self.union_summary = union_summary
