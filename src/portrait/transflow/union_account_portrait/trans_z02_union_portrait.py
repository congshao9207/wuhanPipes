
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
from util.mysql_reader import sql_to_df
import pandas as pd
import datetime


class UnionTransProtrait:
    """
    联合账户画像表汇总信息
    author:汪腾飞
    created_time:20200708
    updated_time_v1:20201126,修复int值nan的问题
    """

    def __init__(self, trans_flow):
        self.db = trans_flow.db
        self.app_no = trans_flow.app_no
        self.report_req_no = trans_flow.report_req_no
        self.trans_flow_portrait_df = trans_flow.trans_u_flow_portrait_df
        self.role = {}

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._analyse_time()
        self._trans_amt()
        self._relationship_risk()
        self._income_detail()

        transform_class_str([self.role], 'TransUPortrait')
        # self.db.session.bulk_save_objects([role])
        # self.db.session.add(role)
        # self.db.session.commit()

    def _single_portrait(self):
        sql = """select * from trans_single_portrait where report_req_no='%s'""" % self.report_req_no
        single_u_df = sql_to_df(sql)
        return single_u_df

    def _analyse_time(self):
        flow_df = self.trans_flow_portrait_df
        self.role['apply_no'] = self.app_no
        self.role['report_req_no'] = self.report_req_no
        max_date = flow_df['trans_date'].max()
        min_date = flow_df['trans_date'].min()

        not_full_month = []
        if min_date.day != 1:
            not_full_month.append(datetime.datetime.strftime(min_date, '%Y-%m'))

        if (max_date + datetime.timedelta(days=1)).day != 1:
            not_full_month.append(datetime.datetime.strftime(max_date, '%Y-%m'))
        self.role['analyse_start_time'] = datetime.datetime.strftime(min_date, '%Y-%m-%d')
        self.role['analyse_end_time'] = datetime.datetime.strftime(max_date, '%Y-%m-%d')
        self.role['not_full_month'] = ','.join(not_full_month)

    def _trans_amt(self):
        flow_df = self.trans_flow_portrait_df
        df = flow_df[((pd.isnull(flow_df.relationship)) & (flow_df.is_sensitive != 1)) &
                     (flow_df.trans_amt > 0)]
        df['trans_year'] = df['trans_date'].apply(lambda x: x.year)
        df['trans_month'] = df['trans_date'].apply(lambda x: x.month)
        self.role['normal_income_amt'] = df.trans_amt.sum() if df.shape[0] > 0 else 0
        self.role['normal_income_cnt'] = df.shape[0]
        self.role['normal_income_mean'] = df.trans_amt.mean() if df.shape[0] > 0 else 0
        # self.role['normal_income_d_mean'] = \
        #     df.groupby(by='trans_date').agg({'trans_amt': sum})['trans_amt'].mean() if df.shape[0] > 0 else 0
        self.role['normal_income_d_mean'] = round(
            df.trans_amt.sum() / ((df.trans_date.max() - df.trans_date.min()).days
                                  if df.trans_date.max() != df.trans_date.min() else 1), 4) if df.shape[0] > 0 else 0
        month_mean_income = df.groupby(by=['trans_year', 'trans_month']).\
            agg({'trans_amt': sum})['trans_amt'].mean()
        if pd.isnull(month_mean_income):
            month_mean_income = 0
        self.role['normal_income_m_mean'] = month_mean_income
        self.role['income_amt_y_pred'] = month_mean_income * 12
        normal_income_m_std = df.groupby(by=['trans_year', 'trans_month']).agg({'trans_amt': sum})['trans_amt'].std()
        self.role['normal_income_m_std'] = normal_income_m_std if pd.notnull(normal_income_m_std) else 0

        expense_df = flow_df[pd.notnull(flow_df['cost_type'])]
        self.role['normal_expense_amt'] = expense_df.trans_amt.sum() if expense_df.shape[0] > 0 else 0
        self.role['normal_expense_cnt'] = expense_df.shape[0]

    def _relationship_risk(self):
        flow_df = self.trans_flow_portrait_df
        related_income_amt = flow_df[(flow_df.trans_amt > 0) & (pd.notnull(flow_df.relationship))]['trans_amt'].sum()
        unrelated_income_amt = flow_df[(flow_df.trans_amt > 0) & (pd.isnull(flow_df.relationship))]['trans_amt'].sum()
        self.role['relationship_risk'] = 1 if unrelated_income_amt < related_income_amt else 0

    def _income_detail(self):
        flow_df = self.trans_flow_portrait_df
        single_u_df = self._single_portrait()
        income_list = [0, 5, 10, 30, 50, 100, 200]
        temp_df = flow_df.copy()
        temp_df['str_date'] = temp_df[['account_id', 'trans_date']].apply(
            lambda x: str(x['account_id']) + datetime.datetime.strftime(x['trans_date'], '%Y-%m-%d'), axis=1)
        temp_df.drop_duplicates(subset='str_date', keep='last', inplace=True)
        income_weight_max = 0
        income_weight_min = 0
        balance_weight_max = 0
        balance_weight_min = 0
        income_total_cnt = 0
        balance_total_cnt = 0
        for i in range(7):
            if i != 6:
                income_variable = 'income_' + str(income_list[i]) + '_to_' + str(income_list[i+1]) + '_cnt'
                balance_variable = 'balance_' + str(income_list[i]) + '_to_' + str(income_list[i+1]) + '_day'
                # if i == 5:
                #     income_variable = income_variable[1:]
                left = income_list[i] * 10000
                right = income_list[i+1] * 10000
                income_df = flow_df[(flow_df.trans_amt > left) & (flow_df.trans_amt <= right)]
                balance_df = temp_df[(temp_df.account_balance > left) & (temp_df.account_balance <= right)]
            else:
                income_variable = 'income_above_' + str(income_list[i]) + '_cnt'
                balance_variable = 'balance_above_' + str(income_list[i]) + '_day'
                left = income_list[i] * 10000
                income_df = flow_df[flow_df.trans_amt > left]
                balance_df = temp_df[temp_df.account_balance > left]

            temp_income_max = income_df['trans_amt'].max() if len(income_df) > 0 else 0
            temp_income_min = income_df['trans_amt'].min() if len(income_df) > 0 else 0
            temp_income_cnt = single_u_df[income_variable].sum()
            income_weight_max += temp_income_max * temp_income_cnt
            income_weight_min += temp_income_min * temp_income_cnt
            income_total_cnt += temp_income_cnt

            temp_balance_max = balance_df['account_balance'].max() if len(balance_df) > 0 else 0
            temp_balance_min = balance_df['account_balance'].min() if len(balance_df) > 0 else 0
            temp_balance_cnt = single_u_df[balance_variable].sum()
            balance_weight_max += temp_balance_max * temp_balance_cnt
            balance_weight_min += temp_balance_min * temp_balance_cnt
            balance_total_cnt += temp_balance_cnt

            self.role[income_variable] = temp_income_cnt
            self.role[balance_variable] = temp_balance_cnt
        self.role['income_weight_max'] = income_weight_max / income_total_cnt if income_total_cnt != 0 else 0
        self.role['income_weight_min'] = income_weight_min / income_total_cnt if income_total_cnt != 0 else 0
        self.role['balance_weight_max'] = balance_weight_max / balance_total_cnt if balance_total_cnt != 0 else 0
        self.role['balance_weight_min'] = balance_weight_min / balance_total_cnt if balance_total_cnt != 0 else 0
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.role['create_time'] = create_time
        self.role['update_time'] = create_time
