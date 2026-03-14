import pandas as pd
from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
from pandas import DateOffset


class JsonUnionFundsSummaryPortrait(TransFlow):
    """
    资金全貌版块，从trans_u_flow_portrait取值计算。包含银行流水概貌、银行流水进出帐分析、融资分析、微信支付宝流水概貌
    """

    def __init__(self):
        super().__init__()
        self.df = pd.DataFrame()
        self.period = 0

    def process(self):
        self.variables['trans_report_fullview'] = {}
        self.get_u_funds_summary_detail()
        self.bank_summary()
        self.bank_account_summary()
        self.bank_trans_type()
        self.loan_analyse()
        self.wxzfb_summary()

    def get_u_funds_summary_detail(self):
        # 取联合版块的画像数据
        df = self.trans_u_flow_portrait.copy()
        if not df.empty:
            # 获取一年前的日期
            year_ago = pd.to_datetime(df['trans_date']).max() - DateOffset(months=12)
            # 新增交易年-月列
            df['trans_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
            # 筛选近一年数据
            df = df.loc[pd.to_datetime(df.trans_date) >= year_ago]
            if not df.empty:
                self.df = df
                # 判断是否有近6个月数据，确定是否展示下面的专家经验
                # 用交易时间去判定是否有6个月数据
                if df['trans_date'].max() - DateOffset(months=6) >= pd.to_datetime(df['trans_date'].min()):
                    self.period = (df['trans_date'].max().year - df['trans_date'].min().year) * 12 + \
                                  (df['trans_date'].max().month - df['trans_date'].min().month) + 1

    @staticmethod
    def get_trans_account_detail(id_list):
        sql = """select * from trans_account where id in %(id_list)s"""
        df = sql_to_df(sql=sql, params={"id_list": id_list})
        return df

    def bank_summary(self):
        """
        银行流水全貌，包含银行经营性流水月进账、月出账、月净进账
        :return:
        """
        bank_summary = []
        bank_summary_tips = ""
        bank_df = self.df.copy()
        if not bank_df.empty:
            # 判断是否只有一种流水类型，即纯银行流水或纯微信、支付宝流水
            if bank_df.trans_flow_src_type.nunique() == 2:
                # 银行流水经营性进账，剔除多头、特殊、关联关系
                bank_df = bank_df.loc[(bank_df.trans_flow_src_type == 0)
                                      & pd.isna(bank_df.loan_type)
                                      & pd.isna(bank_df.unusual_trans_type)
                                      & pd.isna(bank_df.relationship)]
                if not bank_df.empty:
                    def func1(x):
                        return x[x.index.isin(bank_df.query('trans_amt >= 0').index.tolist())].sum()

                    def func2(x):
                        return x[x.index.isin(bank_df.loc[bank_df.trans_amt < 0].index.tolist())].abs().sum()

                    temp_df = bank_df.groupby('trans_month', as_index=False).agg({"trans_amt": [func1, func2]})
                    temp_df.columns = ['trans_month', 'normal_income_amt', 'expense_income_amt']
                    temp_df['net_income_amt'] = temp_df['normal_income_amt'].sub(temp_df['expense_income_amt'],
                                                                                 fill_value=0)
                    # 专家经验 流水不足6个月，不展示专家经验
                    if self.period >= 6:
                        total_normal_income_amt = temp_df.normal_income_amt.sum()
                        mean_normal_income_amt = temp_df.normal_income_amt.mean()
                        total_expense_income_amt = temp_df.expense_income_amt.sum()
                        total_net_income_amt = temp_df.net_income_amt.sum()
                        # 1.1、银行流水总进账XX万，总出账XX万，净流入XX万。月均进账XX万。
                        bank_summary_tips = f"银行流水经营性总进账{total_normal_income_amt / 10000:.2f}万，" \
                                            f"总出账{total_expense_income_amt / 10000:.2f}万，" \
                                            f"净流入{total_net_income_amt / 10000:.2f}万，" \
                                            f"月均进账{mean_normal_income_amt / 10000:.2f}万;"
                        # 收入支出版块，考虑收入为0情况
                        if total_normal_income_amt == 0:
                            proportion = -1.0
                        else:
                            proportion = total_net_income_amt / total_normal_income_amt
                        # 1.2、若收支差额 >= 50 %，提示“收支差额xx %，建议核实：1.行业特征，2.客户是否有营业外收入，3.客户是否有流水外支出”
                        # 1.3、若收支差额 >= 20 %，提示“收支差额xx %，略有盈余”
                        # 1.4、若收支差额 >= -5 %，提示“收支差额xx %，收支平衡”
                        # 1.5、若收支差额 >= -5 %，提示“收支差额 - xx %，建议核实：1.客户现金流情况，2.流水收集完整性”
                        if proportion >= 0.5:
                            bank_summary_tips += f"收支差额{proportion:.1%}，建议核实：" \
                                                 f"1.行业特征，2.客户是否有营业外收入，3.客户是否有流水外支出"
                        elif proportion >= 0.2:
                            bank_summary_tips += f"收支差额{proportion:.1%}，略有盈余"
                        elif proportion >= -0.05:
                            bank_summary_tips += f"收支差额{proportion:.1%}，收支平衡"
                        else:
                            bank_summary_tips += f"收支差额{proportion:.1%}，建议核实：1.客户现金流情况，2.流水收集完整性"
                    # 表单数据
                    for col in temp_df.select_dtypes(include=['float64', 'float32', 'float']).columns.tolist():
                        temp_df[col] = temp_df[col].apply(lambda x: '%.2f' % x)
                    bank_summary = temp_df.to_dict('records')
        self.variables['trans_report_fullview']['bank_summary'] = {
            "bank_summary_trend_chart": bank_summary,
            "bank_summary_tips": bank_summary_tips
        }

    def bank_account_summary(self):
        """
        分账户汇总经营性收入和支出，以及分账户按月汇总经营性收入和支出
        :return:
        """
        account_summary = []
        account_monthly_summary = []
        all_df = self.df.copy()
        if not all_df.empty:
            # 筛选经营性数据：剔除多头、特殊、关联关系
            normal_df = all_df.loc[
                pd.isna(all_df.loan_type)
                & pd.isna(all_df.unusual_trans_type)
                & pd.isna(all_df.relationship)
            ]
            if not normal_df.empty:
                # 分别计算进账和出账
                income_df = normal_df[normal_df.trans_amt >= 0]
                expense_df = normal_df[normal_df.trans_amt < 0]

                # ========== 1. 分账户汇总经营性收入和支出 ==========
                income_by_account = income_df.groupby('account_id').agg(
                    {'trans_amt': 'sum'}).rename(columns={'trans_amt': 'normal_income_amt'})
                expense_by_account = expense_df.groupby('account_id').agg(
                    {'trans_amt': lambda x: x.abs().sum()}).rename(columns={'trans_amt': 'normal_expense_amt'})
                account_agg_df = income_by_account.join(expense_by_account, how='outer').fillna(0).reset_index()
                account_agg_df['net_income_amt'] = account_agg_df['normal_income_amt'] - account_agg_df[
                    'normal_expense_amt']

                # 计算各账户收入和支出占比
                total_income = account_agg_df['normal_income_amt'].sum()
                total_expense = account_agg_df['normal_expense_amt'].sum()
                account_agg_df['income_proportion'] = round(
                    account_agg_df['normal_income_amt'] / total_income, 4) if total_income > 0 else 0
                account_agg_df['expense_proportion'] = round(
                    account_agg_df['normal_expense_amt'] / total_expense, 4) if total_expense > 0 else 0

                # 匹配账户详情
                account_id_list = account_agg_df['account_id'].tolist()
                account_detail_df = self.get_trans_account_detail(account_id_list)
                account_agg_df = account_agg_df.merge(
                    account_detail_df[['id', 'account_name', 'bank', 'account_no']],
                    how='left', left_on='account_id', right_on='id')
                # 同一账户可能存在多条记录（不同file_id），需合并
                account_agg_df = account_agg_df.groupby(
                    ['account_name', 'bank', 'account_no'], as_index=False).agg(
                    {'normal_income_amt': 'sum', 'normal_expense_amt': 'sum',
                     'net_income_amt': 'sum', 'income_proportion': 'sum',
                     'expense_proportion': 'sum'})
                # 银行账号保留后4位
                account_agg_df['account_no'] = account_agg_df['account_no'].astype(str).str[-4:]
                # 金额保留2位小数
                for col in ['normal_income_amt', 'normal_expense_amt', 'net_income_amt']:
                    account_agg_df[col] = account_agg_df[col].apply(lambda x: round(x, 2))
                account_agg_df.rename(columns={
                    'account_name': 'user_name', 'bank': 'bank_name', 'account_no': 'bank_no'
                }, inplace=True)
                account_summary = account_agg_df[
                    ['user_name', 'bank_name', 'bank_no', 'normal_income_amt', 'normal_expense_amt',
                     'net_income_amt', 'income_proportion', 'expense_proportion']].to_dict('records')

                # ========== 2. 分账户按月汇总经营性收入和支出 ==========
                income_by_acct_month = income_df.groupby(['account_id', 'trans_month']).agg(
                    {'trans_amt': 'sum'}).rename(columns={'trans_amt': 'normal_income_amt'})
                expense_by_acct_month = expense_df.groupby(['account_id', 'trans_month']).agg(
                    {'trans_amt': lambda x: x.abs().sum()}).rename(columns={'trans_amt': 'normal_expense_amt'})
                monthly_df = income_by_acct_month.join(
                    expense_by_acct_month, how='outer').fillna(0).reset_index()
                monthly_df['net_income_amt'] = monthly_df['normal_income_amt'] - monthly_df['normal_expense_amt']

                # 匹配账户详情
                monthly_df = monthly_df.merge(
                    account_detail_df[['id', 'account_name', 'bank', 'account_no']],
                    how='left', left_on='account_id', right_on='id')
                # 同一账户同月合并
                monthly_df = monthly_df.groupby(
                    ['account_name', 'bank', 'account_no', 'trans_month'], as_index=False).agg(
                    {'normal_income_amt': 'sum', 'normal_expense_amt': 'sum',
                     'net_income_amt': 'sum'})
                monthly_df['account_no'] = monthly_df['account_no'].astype(str).str[-4:]
                for col in ['normal_income_amt', 'normal_expense_amt', 'net_income_amt']:
                    monthly_df[col] = monthly_df[col].apply(lambda x: round(x, 2))
                monthly_df.sort_values(['account_name', 'bank', 'account_no', 'trans_month'], inplace=True)
                monthly_df.rename(columns={
                    'account_name': 'user_name', 'bank': 'bank_name', 'account_no': 'bank_no'
                }, inplace=True)
                account_monthly_summary = monthly_df[
                    ['user_name', 'bank_name', 'bank_no', 'trans_month',
                     'normal_income_amt', 'normal_expense_amt', 'net_income_amt']].to_dict('records')

        self.variables['trans_report_fullview']['bank_account_summary'] = {
            'account_summary': account_summary,
            'account_monthly_summary': account_monthly_summary
        }

    @staticmethod
    def _unusual_trans_type(df, label_list):
        res_list = []
        for label in label_list:
            temp_df = df[df.unusual_trans_type.astype(str).str.contains(label)]
            if label == '博彩' and temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -1e4:
                res_list.append('博彩')
            elif label == '娱乐' and temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -1e4:
                res_list.append('娱乐')
            elif label == '案件纠纷' and temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -5e4:
                res_list.append('案件纠纷')
            elif label == '治安罚款' and temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -1e3:
                res_list.append('治安罚款')
            elif label == '保险理赔' and (temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -5e4 or
                                          temp_df[temp_df.trans_amt > 0].trans_amt.sum() > 5e4):
                res_list.append('保险理赔')
            elif label == '股票期货' and (temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -2e5 or
                                          temp_df[temp_df.trans_amt > 0].trans_amt.sum() > 2e5):
                res_list.append('股票期货')
            elif label == '医院' and temp_df[temp_df.trans_amt < 0].trans_amt.sum() < -1e4:
                res_list.append('医院')
            elif label in ['贷款异常', '对外担保异常', '典当']:
                res_list.append(label)
        return '、'.join(res_list)

    def bank_trans_type(self):
        """
        此版块包含银行流水各类型进出帐金额及占比，金额返回单位：元，保留2位小数，比例为小数，保留了4位小数
        :return:
        """
        self.variables['trans_report_fullview']['bank_trans_type'] = {}
        all_df = self.df.copy()
        if all_df.shape[0] > 0:
            for file_type in ['bank', 'wxzfb', 'all']:
                if file_type == 'bank':
                    temp_df = self.df.loc[(self.df.trans_flow_src_type == 0)]
                    collect, suggest_bank_tips = self.trans_type_profill(temp_df, file_type)
                    self.variables['trans_report_fullview']['bank_trans_type'].update(collect)
                    self.variables['trans_report_overview']['trans_general_info']['bank_trans_income_type'][
                        'risk_tips'] = collect['bank_normal']['bank_normal_trans_tips']
                    self.variables['trans_report_overview']['trans_general_info']['bank_trans_expense_type'][
                        'risk_tips'] = collect['bank_expense']['bank_expense_trans_tips']
                    self.variables['suggestion_and_guide']['trans_general_info']['bank_trans_type'][
                        'risk_tips'] = suggest_bank_tips
                elif file_type == 'wxzfb':
                    temp_df = self.df.loc[(self.df.trans_flow_src_type == 1)]
                    collect, suggest_bank_tips = self.trans_type_profill(temp_df, file_type)
                    self.variables['trans_report_fullview']['bank_trans_type'].update(collect)
                else:
                    collect, suggest_bank_tips = self.trans_type_profill(all_df, file_type)
                    self.variables['trans_report_fullview']['bank_trans_type'].update(collect)

    def trans_type_profill(self, temp_df, file_type):
        bank_normal_trans_type, bank_expense_trans_type = [], []
        bank_normal_trans_tips, bank_expense_trans_tips = "", ""
        bank_normal_distribute, bank_expense_distribute = [], []
        suggest_bank_tips = ""
        temp_normal_df = temp_df[temp_df['trans_amt'] >= 0]
        temp_expense_df = temp_df[temp_df['trans_amt'] < 0]
        # 银行流水各类型进账金额及占比
        if temp_normal_df.shape[0] > 0:
            total_income_amt = round(temp_normal_df['trans_amt'].sum(), 2)
            if total_income_amt != 0:
                normal_income_amt = round(
                    temp_normal_df.loc[pd.isna(temp_normal_df.loan_type)
                                       & pd.isna(temp_normal_df.unusual_trans_type)
                                       & pd.isna(temp_normal_df.relationship)]['trans_amt'].sum(), 2)
                normal_income_amt_proportion = round(normal_income_amt / total_income_amt, 4)
                relationship_income_amt = round(
                    temp_normal_df.loc[pd.notna(temp_normal_df.relationship)]['trans_amt'].sum(), 2)
                relationship_income_amt_proportion = round(relationship_income_amt / total_income_amt, 4)
                # 保证互斥，特殊需剔除关联关系
                unusual_income_amt = round(
                    temp_normal_df.loc[pd.notna(temp_normal_df.unusual_trans_type)
                                       & pd.isna(temp_normal_df.relationship)]['trans_amt'].sum(), 2)
                unusual_income_amt_proportion = round(unusual_income_amt / total_income_amt, 4)
                # 保证互斥，多头需要剔除特殊及关联关系
                loan_income_amt = round(
                    temp_normal_df.loc[pd.notna(temp_normal_df.loan_type)
                                       & pd.isna(temp_normal_df.relationship)
                                       & pd.isna(temp_normal_df.unusual_trans_type)]['trans_amt'].sum(), 2)
                loan_income_amt_proportion = round(loan_income_amt / total_income_amt, 4)

                bank_normal_trans_type.append({'normal_income_amt': normal_income_amt,
                                               'normal_income_amt_proportion': normal_income_amt_proportion,
                                               'relationship_income_amt': relationship_income_amt,
                                               'relationship_income_amt_proportion': relationship_income_amt_proportion,
                                               'unusual_income_amt': unusual_income_amt,
                                               'unusual_income_amt_proportion': unusual_income_amt_proportion,
                                               'loan_income_amt': loan_income_amt,
                                               'loan_income_amt_proportion': loan_income_amt_proportion})
                # 专家经验 流水不足6个月，不展示专家经验
                if self.period >= 6 and file_type == 'bank':
                    # 2.1、经营性进账金额占比 < 30 %，提示"经营性进账金额占比xx%，疑似非主要经营性流水"
                    if normal_income_amt_proportion < 0.3:
                        bank_normal_trans_tips = f"经营性进账金额占比{normal_income_amt_proportion:.1%}，疑似非主要经营性流水;"
                    # 2.2、关联人进账占比 > 40 %，提示"关联人进账占比xx%，建议核实：1.是否存在虚增流水现象，2.是否存在资金汇集账户，3.流水收集完整性"
                    if relationship_income_amt_proportion > 0.4:
                        bank_normal_trans_tips += f"关联人进账占比{relationship_income_amt_proportion:.1%}，建议核实：1.是否存在虚增流水现象，2.是否存在资金汇集账户，3.流水收集完整性;"
                    # 2.3、多头进账金额占比 > 40 %，提示"多头进账金额占比xx%，融资规模较大"
                    if loan_income_amt_proportion > 0.4:
                        bank_normal_trans_tips += f"多头进账金额占比{loan_income_amt_proportion:.1%}，融资规模较大;"
                    # 2.4、存在特殊交易，提示"存在特殊交易：博彩、期货"
                    if unusual_income_amt > 0:
                        unusual_type = ';'.join(list(map(str, temp_normal_df.loc[
                            pd.notna(temp_normal_df.unusual_trans_type)]['unusual_trans_type'].unique().tolist())))
                        unusual_list = list(set(unusual_type.split(';')))
                        res_type = self._unusual_trans_type(temp_normal_df, unusual_list)
                        if res_type != '':
                            bank_normal_trans_tips += f"流水进账中存在特殊交易：{res_type}"

            # 20230920 新增经营性进账流水账户占比
            normal_income_df = temp_normal_df.loc[pd.isna(temp_normal_df.loan_type)
                                                  & pd.isna(temp_normal_df.unusual_trans_type)
                                                  & pd.isna(temp_normal_df.relationship)]
            if normal_income_df.shape[0] > 0:
                account_df = normal_income_df.groupby('account_id').agg({'trans_amt': 'sum'}).reset_index()
                account_df['trans_amt_prop'] = round(account_df['trans_amt'] / account_df['trans_amt'].sum(), 4)
                # 金额万元，保留2位小数
                account_df['trans_amt'] = round(account_df['trans_amt'], 2)
                account_id_list = account_df['account_id'].tolist()
                # 匹配trans_account表中数据
                account_detail_df = self.get_trans_account_detail(account_id_list)
                bank_normal_distribute_df = account_df.merge(account_detail_df, how='left', left_on='account_id',
                                                             right_on='id')
                use_col = ['account_name', 'bank', 'account_no', 'trans_amt', 'trans_amt_prop']
                new_col_name = ['user_name', 'bank_name', 'bank_no', 'income_amt', 'income_amt_prop']
                bank_normal_distribute_df = bank_normal_distribute_df[use_col].groupby(
                    ['account_name', 'bank', 'account_no'], as_index=False).agg(
                    {'trans_amt': sum, 'trans_amt_prop': sum}).reset_index(drop=True)
                bank_normal_distribute_df.columns = new_col_name
                # 银行账号保留后4位
                bank_normal_distribute_df['bank_no'] = bank_normal_distribute_df['bank_no'].astype(str).str[-4:]
                bank_normal_distribute = bank_normal_distribute_df.to_dict(orient='records')

        # 银行流水各类型出账金额及占比
        if temp_expense_df.shape[0] > 0:
            total_expense_amt = round(temp_expense_df.trans_amt.abs().sum(), 2)
            if total_expense_amt != 0:
                # 可变成本 与 固定成本
                # variable_cost_amt = round(bank_expense_df.loc[bank_expense_df.cost_type == "可变成本"][
                #                               'trans_amt'].abs().sum(), 2)
                # variable_cost_amt_proportion = round(variable_cost_amt / total_expense_amt, 4)
                # fixed_cost_amt = round(
                #     bank_expense_df.loc[bank_expense_df.cost_type.astype(str).str.contains("工资|水电|税费|房租|保险")][
                #         'trans_amt'].abs().sum())
                # fixed_cost_amt_proportion = round(fixed_cost_amt / total_expense_amt, 4)
                variable_cost_amt = None
                variable_cost_amt_proportion = None
                fixed_cost_amt = None
                fixed_cost_amt_proportion = None
                # 20220919 成本项改为所有经营性出账
                normal_expense_amt = round(
                    temp_expense_df.loc[pd.isna(temp_expense_df.loan_type)
                                        & pd.isna(temp_expense_df.unusual_trans_type)
                                        & pd.isna(temp_expense_df.relationship)]['trans_amt'].abs().sum(), 2)
                normal_expense_amt_proportion = round(normal_expense_amt / total_expense_amt, 4)
                # 关联人出账、银行融资与非银行融资出账、异常出账
                relationship_expense_amt = round(
                    temp_expense_df.loc[pd.notna(temp_expense_df.relationship)]['trans_amt'].abs().sum(), 2)
                relationship_expense_amt_proportion = round(relationship_expense_amt / total_expense_amt, 4)
                unusual_expense_amt = round(
                    temp_expense_df.loc[pd.notna(temp_expense_df.unusual_trans_type)
                                        & pd.isna(temp_expense_df.relationship)]['trans_amt'].abs().sum(), 2)
                unusual_expense_amt_proportion = round(unusual_expense_amt / total_expense_amt, 4)
                loan_expense_amt = round(
                    temp_expense_df.loc[pd.notna(temp_expense_df.loan_type) & pd.isna(temp_expense_df.relationship)
                                        & pd.isna(temp_expense_df.unusual_trans_type)][
                        'trans_amt'].abs().sum(), 2)
                loan_expense_amt_proportion = round(loan_expense_amt / total_expense_amt, 4)
                bank_expense_trans_type.append({'variable_cost_amt': variable_cost_amt,
                                                'variable_cost_amt_proportion': variable_cost_amt_proportion,
                                                'fixed_cost_amt': fixed_cost_amt,
                                                'fixed_cost_amt_proportion': fixed_cost_amt_proportion,
                                                'normal_expense_amt': normal_expense_amt,
                                                'normal_expense_amt_proportion': normal_expense_amt_proportion,
                                                'relationship_expense_amt': relationship_expense_amt,
                                                'relationship_expense_amt_proportion': relationship_expense_amt_proportion,
                                                'unusual_expense_amt': unusual_expense_amt,
                                                'unusual_expense_amt_proportion': unusual_expense_amt_proportion,
                                                'loan_expense_amt': loan_expense_amt,
                                                'loan_expense_amt_proportion': loan_expense_amt_proportion})
                # 专家经验 流水不足6个月，不展示专家经验
                if self.period >= 6 and file_type == 'bank':
                    # 3.1、多头出账金额占比为 > 40 %，提示"多头出账金额占比为xx%，还款压力较大"
                    if loan_expense_amt_proportion > 0.4:
                        bank_expense_trans_tips = f"多头出账金额占比为{loan_expense_amt_proportion:.1%}，还款压力较大;"
                    # 3.2、存在特殊交易，提示"存在特殊交易：博彩、期货"
                    if unusual_expense_amt > 0:
                        unusual_type = ';'.join(list(map(str, temp_expense_df.loc[
                            pd.notna(temp_expense_df.unusual_trans_type)]['unusual_trans_type'].unique().tolist())))
                        unusual_list = list(set(unusual_type.split(';')))
                        res_type = self._unusual_trans_type(temp_expense_df, unusual_list)
                        if res_type != '':
                            bank_expense_trans_tips += f"流水出账中存在特殊交易：{res_type}"
                    if temp_expense_df.loc[temp_expense_df.cost_type.astype(str).str.contains("工资")].shape[0] > 0:
                        suggest_bank_tips += "营销建议：流水中存在工资支出，可进行代发工资业务营销;"
                    if temp_expense_df.loc[temp_expense_df.cost_type.astype(str).str.contains("水电")].shape[0] > 0:
                        suggest_bank_tips += "营销建议：流水中存在水电费支出，可进行绑定行内账户代扣代缴业务营销;"
            # 20230920 新增经营性出账流水账户占比
            expense_income_df = temp_expense_df.loc[pd.isna(temp_expense_df.loan_type)
                                                    & pd.isna(temp_expense_df.unusual_trans_type)
                                                    & pd.isna(temp_expense_df.relationship)]
            if expense_income_df.shape[0] > 0:
                # 金额转为正值
                expense_income_df['trans_amt'] = expense_income_df['trans_amt'].abs()
                account_df = expense_income_df.groupby('account_id').agg({'trans_amt': 'sum'}).reset_index()
                account_df['trans_amt_prop'] = round(account_df['trans_amt'] / account_df['trans_amt'].sum(), 4)
                # 金额万元，保留2位小数
                account_df['trans_amt'] = round(account_df['trans_amt'], 2)
                account_id_list = account_df['account_id'].tolist()
                # 匹配trans_account表中数据
                account_detail_df = self.get_trans_account_detail(account_id_list)
                bank_expense_distribute_df = account_df.merge(account_detail_df, how='left', left_on='account_id',
                                                              right_on='id')
                use_col = ['account_name', 'bank', 'account_no', 'trans_amt', 'trans_amt_prop']
                new_col_name = ['user_name', 'bank_name', 'bank_no', 'expense_amt', 'expense_amt_prop']
                bank_expense_distribute_df = bank_expense_distribute_df[use_col].groupby(
                    ['account_name', 'bank', 'account_no'], as_index=False).agg(
                    {'trans_amt': sum, 'trans_amt_prop': sum}).reset_index(drop=True)
                bank_expense_distribute_df.columns = new_col_name
                # 银行账号保留后4位
                bank_expense_distribute_df['bank_no'] = bank_expense_distribute_df['bank_no'].astype(str).str[-4:]
                bank_expense_distribute = bank_expense_distribute_df.to_dict(orient='records')

        collection = {
            f"{file_type}_normal": {
                "bank_normal_trans_circular_chart": bank_normal_trans_type,
                "bank_normal_trans_tips": bank_normal_trans_tips,
                "bank_normal_distribute": bank_normal_distribute
            },
            f"{file_type}_expense": {
                "bank_expense_trans_circular_chart": bank_expense_trans_type,
                "bank_expense_trans_tips": bank_expense_trans_tips,
                "bank_expense_distribute": bank_expense_distribute
            },
        }
        return collection, suggest_bank_tips

    def loan_analyse(self):
        """
        融资分析版块，即多头机构分析。图表返回的值，单位：元，需前端转为万元
        :return:
        """
        loan_analyse_detail = []
        loan_analyse_tips = ""
        overview_analyse_tips = ""
        bank_df = self.df.copy()
        if not bank_df.empty:
            # 经营性数据
            normal_income_df = bank_df.loc[pd.isna(bank_df.relationship) & pd.isna(bank_df.unusual_trans_type)
                                           & pd.isna(bank_df.loan_type)]
            normal_income_amt = normal_income_df.loc[normal_income_df.trans_amt >= 0]['trans_amt'].sum()
            loan_df = bank_df.loc[pd.notna(bank_df.loan_type)]
            if not loan_df.empty:
                def func3(x): return x[x >= 0].sum()
                def func4(x): return x[x < 0].abs().sum()
                temp_df = loan_df.groupby('trans_month', as_index=False).agg({'trans_amt': [func3, func4, 'sum']})
                temp_df.columns = ['trans_month', 'loan_amt', 'repay_amt', 'net_income_amt']
                # 专家经验 流水不足6个月，不展示专家经验
                if self.period >= 6:
                    if temp_df.loan_amt.sum() != 0:
                        # 4.1、融资出账金额与经营性进账占比 >= 20 %，提示"经营性进账用于融资还款的比例为xx%，相对于经营规模，客户的融资比例较高"
                        if normal_income_amt == 0:
                            proportion = 1.
                        else:
                            proportion = temp_df.repay_amt.sum() / normal_income_amt
                        overview_analyse_tips = \
                            f"经营性进账用于融资还款的比例为{proportion:.1%};" if proportion > 0.01 else ""
                        if proportion >= 0.2:
                            loan_analyse_tips = \
                                f"经营性进账用于融资还款的比例为{proportion:.1%}，相对于经营规模，客户的融资比例较高;"
                        # 4.2、贷款净增总额≥ 年经营性收入 * 20 %，提示"近n个月贷款额增加XX万，负债增加较多。"
                        if temp_df.net_income_amt.sum() >= 0.2 * normal_income_amt:
                            overview_analyse_tips += \
                                f"近{self.period}个月贷款额增加{temp_df.net_income_amt.sum() / 10000:.2f}万；"
                            loan_analyse_tips += \
                                f"近{self.period}个月贷款额增加{temp_df.net_income_amt.sum() / 10000:.2f}万，负债增加较多"
                # 趋势图数据
                for col in temp_df.select_dtypes(include=['float64', 'float32', 'float']).columns.tolist():
                    temp_df[col] = temp_df[col].apply(lambda x: '%.2f' % x)
                loan_analyse_detail = temp_df.to_dict('records')
        self.variables['trans_report_fullview']['loan_analyse'] = {
            "loan_analyse_trend_chart": loan_analyse_detail,
            "loan_analyse_tips": loan_analyse_tips
        }
        self.variables['trans_report_overview']['loan_analyse']['risk_tips'] = overview_analyse_tips
        self.variables['suggestion_and_guide']['loan_analyse']['risk_tips'] = loan_analyse_tips

    def wxzfb_summary(self):
        """
        微信支付宝流水概貌，返回表单金额单位：万元
        :return:
        """
        wxzfb_detail = []
        wxzfb_tips = ""
        df = self.df.copy()
        if not df.empty:
            # 判断是否只有一种流水类型，即纯银行流水或纯微信、支付宝流水
            if df.trans_flow_src_type.nunique() == 2:
                wxzfb_df = df.loc[(df.trans_flow_src_type == 1)
                                  & pd.isna(df.loan_type)
                                  & pd.isna(df.relationship)
                                  & pd.isna(df.unusual_trans_type)]
                bank_df = df.loc[(df.trans_flow_src_type == 0)
                                 & pd.isna(df.loan_type)
                                 & pd.isna(df.relationship)
                                 & pd.isna(df.unusual_trans_type)]
                if not wxzfb_df.empty:
                    def func5(x): return x[x >= 0].sum()
                    def func6(x): return x[x.index.isin(wxzfb_df.loc[wxzfb_df.trans_amt < 0].index.tolist())].abs().sum()
                    temp_df = wxzfb_df.groupby('trans_month', as_index=False).agg({'trans_amt': [func5, func6]})
                    temp_df.columns = ['trans_month', 'loan_amt', 'repay_amt']
                    temp_df['net_income_amt'] = temp_df['loan_amt'].sub(temp_df['repay_amt'], fill_value=0)
                    # 专家经验
                    if self.period >= 6:
                        # 1、近一年微信、支付宝总进账金额＞银行一年总进账金额，提示：“微信、支付宝是申请人经营的主要收款方式”。
                        if bank_df.empty:
                            wxzfb_tips = "微信、支付宝是申请人经营的主要收款方式"
                        elif temp_df.loan_amt.sum() > bank_df.query('trans_amt >= 0')['trans_amt'].sum():
                            wxzfb_tips = "微信、支付宝是申请人经营的主要收款方式"
                        # 2、近一年微信、支付宝总进账金额＜银行一年总进账金额，提示：“银行转账是申请人经营的主要收款方式”。
                        elif temp_df.loan_amt.sum() < bank_df.query('trans_amt >= 0')['trans_amt'].sum():
                            wxzfb_tips = "银行转账是申请人经营的主要收款方式"
                    # 趋势图数据
                    for col in temp_df.select_dtypes(include=['float64', 'float32', 'float']).columns.tolist():
                        temp_df[col] = temp_df[col].apply(lambda x: '%.2f' % x)
                    wxzfb_detail = temp_df.to_dict('records')
        self.variables['trans_report_fullview']['wxzfb_summary'] = {
            "wxzfb_summary_trend_chart": wxzfb_detail,
            "wxzfb_summary_tips": wxzfb_tips
        }
