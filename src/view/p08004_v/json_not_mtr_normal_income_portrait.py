# -*- coding: utf-8 -*-
# @Time    : 2022/6/10 15:21
# @Author  : chenwen
# @IDE     : PyCharm
# @Name    : json_not_mtr_normal_income_portrait.py


import pandas as pd
from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df

from pandas.tseries.offsets import *
import datetime
import numpy as np


class JsonNotMtrNormalIncomePortrait(TransFlow):
    """
        经营性收入模块
        author:陈文
        created_time:20220610
        updated_time_v2:20220718 重构代码，取值直接从trans_u_flow_portrait取值
    """

    def __init__(self):
        super().__init__()
        self.df = pd.DataFrame()
        self.period = 0
        self.balance_df = pd.DataFrame()

    def process(self):
        # self.read_u_summary_pt()
        self.get_u_normal_detail()
        self.process_union_income()

    def get_u_normal_detail(self):
        # 从单账户取非完整月
        sql2 = """
                select account_id,not_full_month from trans_single_portrait where report_req_no = %(report_req_no)s
        """
        if self.trans_u_flow_portrait is None:
            return
        df = self.trans_u_flow_portrait.copy()
        df_not_full_month = sql_to_df(sql=sql2, params={"report_req_no": self.reqno})
        if df.shape[0] > 0:
            # 新增非完整月列
            df = pd.merge(df, df_not_full_month, how='left', on='account_id')
            # 获取一年前的日期
            year_ago = pd.to_datetime(df['trans_date']).max() - DateOffset(months=12)
            # 新增交易年-月列
            df['year_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
            # 筛选近一年经营性数据
            df = df.loc[(pd.to_datetime(df.trans_date) >= year_ago)
                        & pd.isna(df.loan_type) & pd.isna(df.unusual_trans_type) & pd.isna(df.relationship)]
            if df.shape[0] > 0:
                # 20220914 新增联合账户余额数据
                self.balance_df = df
                # 20220919 出账取所有经营性出账，不再仅取成本项
                normal_income_df = df.loc[df.trans_amt >= 0]
                # 取成本支出项
                expense_income_df = df.loc[df.trans_amt < 0]
                temp_df = df.groupby(['account_id', 'year_month']).agg(
                    {'not_full_month': lambda x: ','.join(list(map(str, x.unique()))), 'trans_flow_src_type': 'min'})
                temp_df['normal_income_amt'] = normal_income_df.groupby(['account_id', 'year_month'])[
                    'trans_amt'].sum()
                temp_df['normal_expense_amt'] = expense_income_df.groupby(['account_id', 'year_month'])[
                    'trans_amt'].sum().abs()
                temp_df['net_income_amt'] = temp_df['normal_income_amt'].sub(temp_df['normal_expense_amt'],
                                                                             fill_value=0)
                temp_df['salary_cost_amt'] = \
                    expense_income_df.loc[expense_income_df.cost_type == '工资'].groupby(['account_id', 'year_month'])[
                        'trans_amt'].sum().abs()
                temp_df['living_cost_amt'] = \
                    expense_income_df.loc[expense_income_df.cost_type == '水电'].groupby(['account_id', 'year_month'])[
                        'trans_amt'].sum().abs()
                temp_df['tax_cost_amt'] = \
                    expense_income_df.loc[expense_income_df.cost_type == '税费'].groupby(['account_id', 'year_month'])[
                        'trans_amt'].sum().abs()
                temp_df['rent_cost_amt'] = \
                    expense_income_df.loc[expense_income_df.cost_type == '房租'].groupby(['account_id', 'year_month'])[
                        'trans_amt'].sum().abs()
                temp_df['insurance_cost_amt'] = \
                    expense_income_df.loc[expense_income_df.cost_type == '保险'].groupby(['account_id', 'year_month'])[
                        'trans_amt'].sum().abs()
                temp_df['variable_cost_amt'] = \
                    expense_income_df.loc[expense_income_df.cost_type == '可变成本'].groupby(['account_id', 'year_month'])[
                        'trans_amt'].sum().abs()
                temp_df = temp_df.reset_index().fillna(0.).rename(columns={0: 'trans_flow_src_type'})

                self.df = temp_df
                # 判断是否有近6个月数据，确定是否展示下面的专家经验
                # 用交易时间去判定是否有6个月数据
                if df['trans_date'].max() - DateOffset(months=6) >= pd.to_datetime(df['trans_date'].min()):
                    self.period = (df['trans_date'].max().year - df['trans_date'].min().year) * 12 + \
                                  (df['trans_date'].max().month - df['trans_date'].min().month) + 1

    def process_union_income(self):
        # 图表信息
        operational_trend_chart = []
        operational_form_detail = []
        # 专家经验
        risk_tips_union = ""
        risk_tips_season = ""
        risk_tips_payment = ""
        sug_month, suggest_tips = [], ""
        sug_month_balance, suggest_tips_balance = [], ''
        suggest_detail = []
        suggest_balance = pd.DataFrame(columns=['month', 'account_balance'])
        if self.balance_df.shape[0] == 0:
            self.variables['operational_analysis'] = {
                'operational_trend_chart': [],
                'operational_form_detail': [],
                'risk_tips_union': "",
                'risk_tips_season': "",
                'risk_tips_payment': ""
            }
            return
        # 仅银行流水有根据余额判断建议还款月模块
        balance_df = self.balance_df[self.balance_df['trans_flow_src_type'] == 0]
        if balance_df.shape[0] > 0:
            # 计算单账户每个月的账户余额，按月度加总余额
            balance_df.sort_values(by=['account_id', 'trans_date', 'trans_time'], ascending=True, inplace=True)
            temp_df = balance_df[['account_id', 'account_balance', 'year_month']]
            temp_df.drop_duplicates(subset=['account_id', 'year_month'], keep='last', inplace=True)
            temp_df1 = temp_df.groupby('year_month', as_index=False).agg({'account_balance': 'sum'}).sort_values(
                'account_balance', ascending=False)
            sug_month_balance_list = temp_df1.year_month.tolist()[:3]
            now = datetime.datetime.now()
            sug_month_balance = sorted(
                [format(datetime.datetime.now() + DateOffset(months=(int(x[-2:]) - now.month - 1) % 12 + 1), '%Y-%m')
                 for x in sug_month_balance_list])
            month_str_balance = [f"{x[:4]}年{int(x[-2:])}月" for x in sug_month_balance]
            if len(month_str_balance) > 0:
                suggest_tips_balance = f"建议本金还款月：{'，'.join(month_str_balance)};" \
                               f"注：本栏建议本金还款月基于客户上传的流水数据月底余额进行判断，实际操作中还需结合其他尽调结果进行综合评估"
            suggest_balance = temp_df1[['year_month', 'account_balance']]
            suggest_balance.rename(columns={'year_month': 'month'}, inplace=True)
        flow_df = self.df
        if flow_df.shape[0] > 0:
            # 新增月度进账，支付宝微信按80%计
            """
            20220718 更新，此处新增判断，若纯微信支付宝或纯银行数据，不用考虑80%进账处理
            """
            if flow_df.trans_flow_src_type.nunique() == 2:
                flow_df['u_normal_income_amt'] = flow_df.apply(
                    lambda x: x.normal_income_amt * 1 if x.trans_flow_src_type == 0 else x.normal_income_amt * 0.8,
                    axis=1)
            else:
                flow_df['u_normal_income_amt'] = flow_df.normal_income_amt
            # 是否完整月，0为非完整月，1为完整月
            flow_df['u_not_full_month'] = flow_df.apply(
                lambda x: 0 if pd.notna(x.not_full_month) and x.year_month in x.not_full_month else 1, axis=1)
            # 计算月均
            flow_df['u_normal_income_mean'] = round(flow_df['u_normal_income_amt'].sum() /
                                                    flow_df['year_month'].nunique(), 2)
            # 处理联合版块趋势图数据
            # u_not_full_month取最大，逻辑为只要有任一单账户某月为完整月，则当月联合账户判定为完整月
            def func1(x): return x[x.index.isin(flow_df.query('trans_flow_src_type==1').index.tolist())].sum()
            def func2(x): return x[x.index.isin(flow_df.query('trans_flow_src_type==0').index.tolist())].sum()
            temp_df = flow_df.groupby('year_month', as_index=False).agg({
                'normal_income_amt': ['sum', func1, func2],
                'normal_expense_amt': ['sum', func1, func2],
                'net_income_amt': 'sum',
                'u_normal_income_amt': 'sum',
                'u_normal_income_mean': np.mean,
                'u_not_full_month': max,
                'variable_cost_amt': 'sum',
                'salary_cost_amt': 'sum',
                'living_cost_amt': 'sum',
                'tax_cost_amt': 'sum',
                'rent_cost_amt': 'sum',
                'insurance_cost_amt': 'sum'
            })
            temp_df.columns = ['year_month', 'income_month', 'wx_month_amt', 'month_amt', 'payment_month',
                               'wx_month_expense_amt', 'month_expense_amt', 'net_income_month', 'u_normal_income_amt',
                               'u_normal_income_mean', 'u_not_full_month', 'vari_cost', 'vages', 'hydropower',
                               'taxation', 'rent', 'insurance']
            # 交易周期两端月份打上非完整月标签,0为非完整月
            temp_df.sort_values('year_month', ascending=True).reset_index(inplace=True)
            if temp_df.shape[0] == 1:
                temp_df.loc[0, 'u_not_full_month'] = 0
            else:
                temp_df.loc[[0, temp_df.index.max()], 'u_not_full_month'] = 0
            # 0701 优化旺季判定：旺季不判定是否完整月
            temp_df['season'] = temp_df.apply(
                lambda x: '旺季' if (x.u_normal_income_amt >= x.u_normal_income_mean * 1.5) else (
                    '淡季' if x.u_normal_income_amt < x.u_normal_income_mean * 0.5 and x.u_not_full_month == 1
                    else '平季'), axis=1)
            # 建议还款月
            sug_month = temp_df[temp_df['season'] == '旺季'].sort_values(
                by='income_month', ascending=False)['year_month'].tolist()[:3]
            now = datetime.datetime.now()
            sug_month = sorted(
                [format(datetime.datetime.now() + DateOffset(months=(int(x[-2:]) - now.month - 1) % 12 + 1), '%Y-%m')
                 for x in sug_month])
            month_str = [f"{x[:4]}年{int(x[-2:])}月" for x in sug_month]
            # 20220914 专家经验修改为“建议本金还款月”
            if len(month_str) > 0:
                suggest_tips = f"建议本金还款月：{'，'.join(month_str)};" \
                               f"注：本栏建议本金还款月基于客户上传的流水数据月度进出帐进行判断，实际操作中还需结合其他尽调结果进行综合评估"
            # 微信支付宝、银行净出账
            temp_df['net_income'] = temp_df['month_amt'].sub(temp_df['month_expense_amt'], fill_value=0.)
            temp_df['wx_net_income'] = temp_df['wx_month_amt'].sub(temp_df['wx_month_expense_amt'], fill_value=0.)
            # 固定成本，总成本
            fixed_cost_col = ['vages', 'hydropower', 'taxation', 'rent', 'insurance']
            temp_df['fix_cost'] = temp_df.apply(lambda x: x[fixed_cost_col].sum(), axis=1)
            # 20220919 新增 成本项为所有经营性支出成本
            # temp_df['total_cost_amt'] = temp_df['vari_cost'] + temp_df['fix_cost']
            temp_df['total_cost_amt'] = temp_df['payment_month']
            temp_df['wx_cost_amt'] = temp_df['wx_month_expense_amt']
            temp_df['cost_amt'] = temp_df['month_expense_amt']
            temp_df['total_month_amt'] = temp_df['income_month']
            # # 重命名列
            temp_df.rename(columns={'year_month': 'month'}, inplace=True)
            # 专家经验
            # 经营规模
            if self.period >= 6:
                risk_tips_union = \
                    f"近{self.period}个月经营性收入为{temp_df['income_month'].sum() / 10000:.2f}万，" \
                    f"月均经营性收入为{temp_df['income_month'].sum() / self.period / 10000:.2f}万"
            # 经营旺季
            temp_df.sort_values('month', ascending=True, inplace=True)
            risk_tips_season1 = '、'.join(temp_df.loc[temp_df.season == '旺季']['month'].unique().tolist())
            if len(risk_tips_season1) > 0:
                risk_tips_season += risk_tips_season1 + "月是经营收款旺季;"
            # 淡季
            risk_tips_season3 = '、'.join(temp_df.loc[temp_df.season == '淡季']['month'].unique().tolist())
            if len(risk_tips_season3) > 0:
                risk_tips_season += risk_tips_season3 + "月是经营收款淡季"
            union_col = ['month', 'income_month', 'payment_month', 'net_income_month', 'season']
            overview_sub_tips = ''
            if temp_df['month_amt'].sum() > 0 or temp_df['month_expense_amt'].sum() > 0:
                overview_sub_tips += f"银行账户经营性总进账{temp_df['month_amt'].sum() / 10000:.2f}万元、" \
                                     f"总出账{temp_df['month_expense_amt'].sum() / 10000:.2f}万元、" \
                                     f"净进账{temp_df['net_income'].sum() / 10000:.2f}万元;"
            if temp_df['wx_month_amt'].sum() > 0 or temp_df['wx_month_expense_amt'].sum() > 0:
                overview_sub_tips += f"微信支付宝账户经营性总进账{temp_df['wx_month_amt'].sum() / 10000:.2f}万元、" \
                                     f"总出账{temp_df['wx_month_expense_amt'].sum() / 10000:.2f}万元、" \
                                     f"净进账{temp_df['wx_net_income'].sum() / 10000:.2f}万元"
            for col in temp_df.select_dtypes(include=['float64', 'float32', 'float']).columns.tolist():
                temp_df[col] = temp_df[col].apply(lambda x: '%.2f' % x)
            temp_df.sort_values('month', ascending=True, inplace=True)

            # 图表信息
            operational_trend_chart = temp_df[union_col].to_dict('records')
            # 表单信息
            # 表单需要用到的列
            form_detail_col = ['month', 'total_month_amt', 'wx_month_amt', 'month_amt', 'total_cost_amt',
                               'wx_cost_amt', 'cost_amt', 'vari_cost', 'fix_cost', 'vages', 'hydropower',
                               'taxation', 'rent', 'insurance', 'union_net_income', 'wx_net_income', 'net_income']
            temp_df.rename({'net_income_month': 'union_net_income'}, axis=1, inplace=True)
            # 二级及以下标题，全为0则不展示
            cost_type_list = ['wx_month_amt', 'month_amt', 'wx_cost_amt', 'cost_amt', 'vari_cost', 'fix_cost',
                              'wx_net_income', 'net_income', 'vages', 'hydropower', 'taxation', 'rent', 'insurance']
            for i in cost_type_list:
                temp_df[i] = temp_df[i].astype(float)
                if temp_df[i].sum() == 0:
                    temp_df[i] = None
            # 建议还款月
            suggest_income = temp_df[['month', 'u_normal_income_amt']]
            suggest_df = pd.merge(suggest_income, suggest_balance, on='month', how='left').fillna('')
            suggest_detail = suggest_df.to_dict('records')

            # 20220919 根据青岛农商行需求，将各类子分类成本项赋空
            cost_type = ['vari_cost', 'fix_cost', 'vages', 'hydropower', 'taxation', 'rent', 'insurance']
            for i in cost_type:
                temp_df[i] = None
            operational_form_detail = temp_df[form_detail_col].to_dict('records')
        self.variables['operational_analysis'] = {
            'operational_trend_chart': operational_trend_chart,
            'operational_form_detail': operational_form_detail,
            'risk_tips_union': risk_tips_union,
            'risk_tips_season': risk_tips_season,
            'risk_tips_payment': risk_tips_payment
        }
