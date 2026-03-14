from datetime import datetime

from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_year_date, before_n_month_date, date_to_timestamp
import numpy as np
import pandas as pd

from util.common_util import format_timestamp, replace_nan


def get_credit_min_repay(df, repay_amount, amout_replay_amount):
    if df[amout_replay_amount] is None:
        return "否"
    return ["否", "是"][df[repay_amount] * 2 > df[amout_replay_amount]]


def get_credit_usage_rate(df, quota_used, avg_overdraft_balance_6, loan_amount):
    if df[loan_amount] > 0:
        temp = [df[avg_overdraft_balance_6], df[quota_used]][df[quota_used] > df[avg_overdraft_balance_6]]
        return round(temp / df[loan_amount], 2)
    else:
        return 0


class PcreditLoanView(ModuleProcessor):

    def process(self):
        self._get_loan_msg()

    def _get_loan_msg(self):
        credit_info_df = self.cached_data.get("pcredit_info")
        if credit_info_df is not None and not credit_info_df.empty:
            undestroy_limit = self._check_is_null(credit_info_df.loc[0,'undestroy_limit'])
            undestory_semi_limit = self._check_is_null(credit_info_df.loc[0,'undestory_semi_limit'])
            undestory_used_limit = self._check_is_null(credit_info_df.loc[0, 'undestory_used_limit'])
            undestory_semi_overdraft = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_overdraft'])
            undestory_avg_use = self._check_is_null(credit_info_df.loc[0, 'undestory_avg_use'])
            undestory_semi_avg_overdraft = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_avg_overdraft'])
            total_credit_card_limit = undestroy_limit + undestory_semi_limit
            self.variables['total_credit_card_limit'] = total_credit_card_limit
            total_credit_quota_used = undestory_used_limit + undestory_semi_overdraft
            self.variables['total_credit_quota_used'] = total_credit_quota_used
            total_credit_avg_used_6m = undestory_avg_use + undestory_semi_avg_overdraft
            self.variables['total_credit_avg_used_6m'] = total_credit_avg_used_6m
            if total_credit_card_limit > 0:
                total_credit_usage_rate= max(total_credit_quota_used,total_credit_avg_used_6m)/total_credit_card_limit
                self.variables['total_credit_usage_rate'] = total_credit_usage_rate
                self.variables['total_credit_used_rate'] = "%d%%" % (total_credit_usage_rate*100)
        loan_df = self.cached_data.get("pcredit_loan")
        if loan_df is None or loan_df.empty:
            return
        loan_df = loan_df[pd.notnull(loan_df['loan_date'])]
        loan_df['loan_date'] = loan_df['loan_date'].apply(lambda x: date_to_timestamp(x))
        credit_base_info_df = self.cached_data.get("credit_base_info")

        pcredit_acc_speculate_df = self.cached_data.get("pcredit_acc_speculate")
        pcredit_acc_speculate_df["year"] = pcredit_acc_speculate_df.repay_month.apply(lambda x: int(x[:4]))
        pcredit_acc_speculate_df["month"] = pcredit_acc_speculate_df.repay_month.apply(lambda x: int(x[5:]))

        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_3_year = before_n_year_date(report_time, 3)
        report_time_before_2_year = before_n_year_date(report_time, 2)
        report_time_before_1_year = before_n_year_date(report_time, 1)
        report_time_before_3_month = before_n_month_date(report_time, 3)
        report_time_before_6_month = before_n_month_date(report_time, 6)
        report_time_before_12_month = before_n_month_date(report_time, 12)


        # 经营、消费、按揭类贷款
        loan_account_type_df = loan_df[loan_df['account_type'].isin(['01', '02', '03'])]
        if not loan_account_type_df.empty:
            # 个人信息-固定资产
            self._loan_mort(loan_account_type_df)

            report_time_before_2_year_df = loan_account_type_df[(loan_account_type_df['loan_type'].isin(
                ['01', '04', '07', '99'])) & (loan_account_type_df['loan_date'] > report_time_before_2_year)]
            if not report_time_before_2_year_df.empty:
                self._report_time_after_2_year(report_time_before_2_year_df, pcredit_acc_speculate_df)

            # 信贷交易信息-贷款信息-贷款类型余额分布
            self._loan_balance_distribution(loan_account_type_df)

            # 信贷交易信息-贷款信息-贷款申请新增机构
            self._variable_loan_account_org(loan_account_type_df, report_time_before_12_month,
                                            report_time_before_2_year, report_time_before_3_month,
                                            report_time_before_6_month)

            loan_type_df = loan_account_type_df[(loan_account_type_df['loan_type'].isin(['01', '07', '99'])) |
                                                (loan_account_type_df['loan_type'].str.contains('融资租赁')) |
                                                ((loan_account_type_df['loan_type'] == '04') & (
                                                            loan_account_type_df['loan_amount'] >= 200000))]
            if not loan_type_df.empty:
                # 信贷交易信息-贷款信息-近三年机构申请总变化
                self._org_change_recent_3_year(loan_type_df, report_time, report_time_before_1_year,
                                               report_time_before_2_year, report_time_before_3_year,
                                               pcredit_acc_speculate_df)

            temp_df = loan_account_type_df[(~loan_account_type_df['loan_type'].isin(['02', '03', '05', '06'])) &
                                           (loan_account_type_df['loan_guarantee_type'].isin(
                                               ['1', '2', '3', '4', '5', '6', '7'])) &
                                           (loan_account_type_df['loan_balance'] > 0)]
            gua_mort_df = temp_df[temp_df['loan_guarantee_type'].isin(['1', '2'])]
            gua_credit_df = temp_df[temp_df['loan_guarantee_type'].isin(['3', '4', '7'])]
            gua_com_df = temp_df[temp_df['loan_guarantee_type'].isin(['5', '6'])]
            # 信贷交易信息-贷款信息-担保方式余额分布-担保类型
            guar_type_list = []
            # 信贷交易信息-贷款信息-担保方式余额分布-目前余额
            # mort_balance = 0
            # credit_balance = 0
            # com_balance = 0
            total_balance = temp_df['loan_balance'].sum()
            guar_type_balance_list = []
            # 信贷交易信息-贷款信息-担保方式余额分布-目前笔数
            guar_type_cnt_list = []
            # 信贷交易信息-贷款信息-担保方式余额分布-余额占比
            guar_type_balance_prop_list = []
            if not gua_mort_df.empty:
                guar_type_list.append("抵质押类")
                mort_balance = gua_mort_df.loc[:, 'loan_balance'].sum()
                guar_type_balance_list.append(mort_balance)
                guar_type_cnt_list.append(gua_mort_df[gua_mort_df['loan_balance'] > 0].shape[0])
                guar_type_balance_prop_list.append('%.4f' % (mort_balance / total_balance) if total_balance > 0 else 0)
            if not gua_credit_df.empty:
                guar_type_list.append("保证信用类")
                credit_balance = gua_credit_df.loc[:, 'loan_balance'].sum()
                guar_type_balance_list.append(credit_balance)
                guar_type_cnt_list.append(gua_credit_df[gua_credit_df['loan_balance'] > 0].shape[0])
                guar_type_balance_prop_list.append(
                    '%.4f' % (credit_balance / total_balance) if total_balance > 0 else 0)
            if not gua_com_df.empty:
                guar_type_list.append("组合类")
                com_balance = gua_com_df.loc[:, 'loan_balance'].sum()
                guar_type_balance_list.append(com_balance)
                guar_type_cnt_list.append(gua_com_df[gua_com_df['loan_balance'] > 0].shape[0])
                guar_type_balance_prop_list.append('%.4f' % (com_balance / total_balance) if total_balance > 0 else 0)
            # if total_balance != 0:
            #     guar_type_balance_prop_list.append('%.4f' % (mort_balance / total_balance) if total_balance > 0 else 0)
            #     guar_type_balance_prop_list.append(
            #         '%.4f' % (credit_balance / total_balance) if total_balance > 0 else 0)
            #     guar_type_balance_prop_list.append('%.4f' % (com_balance / total_balance) if total_balance > 0 else 0)
            self.variables["guar_type"] = guar_type_list
            self.variables["guar_type_balance"] = replace_nan(guar_type_balance_list)
            self.variables["guar_type_cnt"] = replace_nan(guar_type_cnt_list)
            self.variables["guar_type_balance_prop"] = replace_nan(guar_type_balance_prop_list)
            # 信贷交易信息-贷款信息-担保方式余额分布保证类最大金额
            ensure_max_principal = gua_credit_df['loan_balance'].max()
            self.variables["ensure_max_principal"] = ensure_max_principal
            # 信贷交易信息-贷款信息-担保方式余额分布抵押类最大金额
            mort_max_principal = gua_mort_df['loan_balance'].max()
            self.variables["mort_max_principal"] = mort_max_principal
            apply_amount = self.origin_data.get("applyAmo")
            if apply_amount:
                # 信贷交易信息-贷款信息-担保方式余额分布保证类最大金额是我司申请金额倍数
                self.variables["ensure_principal_multi_apply"] = round(ensure_max_principal / apply_amount,
                                                                       2) if apply_amount > 0 else 0
                # 信贷交易信息-贷款信息-担保方式余额分布抵押类最大金额
                self.variables["mort_principal_multi_apply"] = round(mort_max_principal / apply_amount,
                                                                     2) if apply_amount > 0 else 0

            loan_account_type_df_status03 = loan_account_type_df[
                (pd.notnull(loan_account_type_df['loan_status_time'])) &
                (loan_account_type_df['loan_status'] == '04') &
                (loan_account_type_df['loan_status_time'] > report_time_before_6_month)]
            if not loan_account_type_df_status03.empty:
                # 信贷交易信息-资金压力解析-提示-已结清贷款机构名
                self.variables["settle_account_org"] = loan_account_type_df_status03.loc[:, 'account_org'].tolist()
                # 信贷交易信息-资金压力解析-提示-已结清贷款申请时间
                self.variables['settle_loan_date'] = loan_account_type_df_status03.loc[:, 'loan_date'].apply(lambda x: datetime.strftime(x, "%Y-%m-%d")).tolist()
                # 信贷交易信息-资金压力解析-提示-结清时间
                self.variables["settle_date"] = loan_account_type_df_status03.loc[:, 'loan_status_time'].apply(
                    lambda x: format_timestamp(x)).tolist()
                # 信贷交易信息-资金压力解析-提示-结清贷款金额
                self.variables["settle_loan_amount"] = loan_account_type_df_status03.loc[:, 'loan_amount'].fillna(
                    0).tolist()

        # 担保类贷款
        loan_gua_df = loan_df[loan_df['account_type'] == '06'].sort_values(by='loan_date')
        if not loan_gua_df.empty:
            self._gua_loan(loan_gua_df)

        # 贷记卡
        loan_credit_df = loan_df[(loan_df['account_type'].isin(['04', '05'])) &
                                 (pd.notnull(loan_df['loan_date'])) &
                                 ((loan_df['currency'].isin(['人民币元', 'CNY', 'RMB'])) |
                                  (pd.isna(loan_df['currency'])))]
        if not loan_credit_df.empty:
            self._get_credit_loan_variables(loan_credit_df, report_time_before_1_year, report_time_before_2_year,
                                            report_time_before_3_year,report_time)




        # 征信不良信息-严重预警信息-五级分类状态
        self.variables["category"] = loan_df[(loan_df['account_type'].isin(['01', '02', '03', '04', '05']))
                                             & (loan_df['category'] != '01')].loc[:, 'category'].fillna(
            '--').unique().tolist()

        # 信贷交易信息-资金压力解析-提示
        record_id_list = pcredit_acc_speculate_df[pcredit_acc_speculate_df['account_status'] == '1'].loc[:,
                         'record_id'].tolist()
        record_id_list = list(set(record_id_list))
        loan_pressure_df = loan_df[
            (loan_df['account_type'].isin(['01', '02', '03'])) &
            (~loan_df['id'].isin(record_id_list)) &
            (loan_df['loan_status'] != '04')]
        # 信贷交易信息-资金压力解析-提示-机构名
        self.variables["hint_account_org"] = loan_pressure_df.loc[:, 'account_org'].tolist()
        # 信贷交易信息-资金压力解析-提示-发放时间
        self.variables["hint_loan_date"] = loan_pressure_df.loc[:, 'loan_date'].apply(
            lambda x: format_timestamp(x)).tolist()
        # 信贷交易信息-资金压力解析-提示-贷款金额
        self.variables["hint_principal_amount"] = loan_pressure_df.loc[:, 'loan_amount'].fillna(0).tolist()

        house_loan_pre_settle_df = loan_df[
            (loan_df['account_type'].isin(['01', '02', '03'])) &
            (loan_df['loan_type'].isin(['03', '05', '06'])) &
            (loan_df['loan_status'] == '04') &
            (loan_df['loan_status_time'] < loan_df['end_date'])]
        # 信贷交易信息-贷款信息-房贷提前结清机构名
        self.variables['house_loan_pre_settle_org'] = house_loan_pre_settle_df['account_org'].to_list()
        self.variables['house_loan_pre_settle_date'] = house_loan_pre_settle_df['loan_status_time'].to_list()

    @staticmethod
    def _check_is_null(value):
        return 0 if pd.isnull(value) else value

    def _get_credit_loan_variables(self, loan_credit_df, report_time_before_1_year, report_time_before_2_year,
                                   report_time_before_3_year,report_time):
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总发卡机构
        self.variables["credit_org"] = loan_credit_df.loc[:, 'account_org'].tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总-开户时间
        self.variables["credit_loan_date"] = loan_credit_df.loc[:, 'loan_date'].apply(
            lambda x: format_timestamp(x)).tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总-账户状态
        self.variables["credit_loan_status"] = loan_credit_df.loc[:, 'loan_status'].tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总-授信额度
        self.variables["credit_principal_amount"] = loan_credit_df.loc[:, 'loan_amount'].fillna(0).tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总-已使用额度
        self.variables["credit_quota_used"] = loan_credit_df.loc[:, 'quota_used'].fillna(0).tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总最近6个月平均使用额度
        self.variables["credit_avg_used_6m"] = loan_credit_df.loc[:, 'avg_overdraft_balance_6'].fillna(0).tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总贷记卡使用率
        loan_credit_df[['quota_used', 'avg_overdraft_balance_6', 'repay_amount']] = loan_credit_df[
            ['quota_used', 'avg_overdraft_balance_6', 'repay_amount']].fillna(0)
        loan_credit_df['credit_usage_rate'] = loan_credit_df.apply(get_credit_usage_rate, axis=1, args=(
            'quota_used', 'avg_overdraft_balance_6', 'loan_amount'))
        self.variables["credit_usage_rate"] = loan_credit_df.loc[:, 'credit_usage_rate'].fillna(0).tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总是否为最低还款
        loan_credit_df['credit_min_repay'] = loan_credit_df.apply(get_credit_min_repay, axis=1,
                                                                  args=('repay_amount', 'amout_replay_amount'))
        self.variables['credit_min_repay'] = loan_credit_df.loc[:, 'credit_min_repay'].fillna(0).tolist()
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总发卡机构个数
        self.variables['credit_org_cnt'] = len(set(loan_credit_df.loc[:, 'account_org'].tolist()))
        # 信贷交易信息-贷记卡信息-贷记卡信息汇总最低还款张数
        # 按照机构去重，得到机构数量
        credit_min_repay_cnt = loan_credit_df[loan_credit_df['credit_min_repay'] == "是"]['account_org'].nunique()
        self.variables["credit_min_repay_cnt"] = credit_min_repay_cnt
        self.variables["total_credit_min_repay_cnt"] = credit_min_repay_cnt
        credit_limit_3, credit_cnt_3 = self._get_total_credit_limit_ny_ago(loan_credit_df, report_time_before_3_year,report_time_before_2_year)
        # 信贷交易信息-贷记卡信息-贷记卡额度及张数变化3年前总额度
        self.variables["total_credit_limit_3y_ago"] = credit_limit_3
        # 信贷交易信息-贷记卡信息-贷记卡额度及张数变化3年前总张数
        self.variables["total_credit_cnt_3y_ago"] = credit_cnt_3
        credit_limit_2, credit_cnt_2 = self._get_total_credit_limit_ny_ago(loan_credit_df, report_time_before_2_year,report_time_before_1_year)
        # 信贷交易信息-贷记卡信息-贷记卡额度及张数变化2年前总额度
        self.variables["total_credit_limit_2y_ago"] = credit_limit_2
        # 信贷交易信息-贷记卡信息-贷记卡额度及张数变化2年前总张数
        self.variables["total_credit_cnt_2y_ago"] = credit_cnt_2
        credit_limit_1, credit_cnt_1 = self._get_total_credit_limit_ny_ago(loan_credit_df, report_time_before_1_year,report_time)
        # 信贷交易信息-贷记卡信息-贷记卡额度及张数变化1年前总额度
        self.variables["total_credit_limit_1y_ago"] = credit_limit_1
        # 信贷交易信息-贷记卡信息-贷记卡额度及张数变化1年前总张数
        self.variables["total_credit_cnt_1y_ago"] = credit_cnt_1
        #贷记卡最低还款张数

    def _get_total_credit_limit_ny_ago(self, loan_credit_df, date1,date2):
        credit_limit, credit_cnt = 0, 0
        df = loan_credit_df[(loan_credit_df['loan_date'] < date2) & (loan_credit_df['loan_date'] >= date1) ]
        if not df.empty:
            credit_limit = df.loc[:, 'loan_amount'].sum()
            credit_cnt = df.shape[0]
        return credit_limit, credit_cnt

    def _org_change_recent_3_year(self, loan_type_df, report_time, report_time_before_1_year, report_time_before_2_year,
                                  report_time_before_3_year, pcredit_acc_speculate_df):
        account_org_list = []
        df = loan_type_df[loan_type_df['loan_date'] >= report_time_before_3_year]
        if not df.empty:
            account_org_list = df.sort_values(by='loan_date').loc[:, 'account_org'].drop_duplicates().values.tolist()
            # 信贷交易信息-贷款信息-近三年机构申请总变化机构名称
            self.variables["account_org"] = account_org_list
            total_principal_list_3, max_terms_list_3, max_interest_rate_3y_ago_list = self._total_principal(
                account_org_list, loan_type_df,
                report_time_before_2_year,
                report_time_before_3_year, pcredit_acc_speculate_df)
            self.variables["total_principal_3y_ago"] = replace_nan(total_principal_list_3)
            self.variables["max_terms_3y_ago"] = [int(x) for x in replace_nan(max_terms_list_3)]
            self.variables["max_interest_rate_3y_ago"] = replace_nan(max_interest_rate_3y_ago_list)

            total_principal_list_2, max_terms_list_2, max_interest_rate_2y_ago_list = self._total_principal(
                account_org_list, loan_type_df,
                report_time_before_1_year,
                report_time_before_2_year, pcredit_acc_speculate_df)
            self.variables["total_principal_2y_ago"] = replace_nan(total_principal_list_2)
            self.variables["max_terms_2y_ago"] = [int(x) for x in replace_nan(max_terms_list_2)]
            self.variables["max_interest_rate_2y_ago"] = replace_nan(max_interest_rate_2y_ago_list)

            total_principal_list_1, max_terms_list_1, max_interest_rate_1y_ago_list = self._total_principal(
                account_org_list, loan_type_df,
                report_time,
                report_time_before_1_year, pcredit_acc_speculate_df)
            self.variables["total_principal_1y_ago"] = replace_nan(total_principal_list_1)
            self.variables["max_terms_1y_ago"] = [int(x) for x in replace_nan(max_terms_list_1)]
            self.variables["max_interest_rate_1y_ago"] = replace_nan(max_interest_rate_1y_ago_list)

            # df_temp = pd.merge(loan_type_df, pcredit_acc_speculate_df, left_on='id', right_on='record_id')
            # df_temp_2_year = self.util_get_acc_speculate_n_year_before(df_temp, report_time_before_2_year, None, None)
            # if not df_temp_2_year.empty:
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前个数
            #     self.variables["busi_org_cnt_2y_ago"] = df_temp_2_year.loc[:, 'account_org'].drop_duplicates().size
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前余额
            #     # self.variables["busi_org_balance_2y_ago"]=df_temp_2_year.loc[:,'loan_balance'].sum()
            #
            # df_temp_1_year = self.util_get_acc_speculate_n_year_before(df_temp, report_time_before_2_year, None, None)
            # if not df_temp_1_year.empty:
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前个数
            #     self.variables["busi_org_cnt_1y_ago"] = df_temp_1_year.loc[:, 'account_org'].drop_duplicates().size
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前余额
            #     # self.variables["busi_org_balance_1y_ago"]=df_temp_1_year.loc[:,'loan_balance'].sum()
            #
            # df_temp_3_year = self.util_get_acc_speculate_n_year_before(df_temp, report_time_before_3_year, None, None)
            # if not df_temp_3_year.empty:
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前个数
            #     self.variables["busi_org_cnt_3y_ago"] = df_temp_3_year.loc[:, 'account_org'].drop_duplicates().size
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前余额
            #     # self.variables["busi_org_balance_3y_ago"] = df_temp_3_year.loc[:, 'loan_balance'].sum()
            #
            # df_temp_0_year = self.util_get_acc_speculate_n_year_before(df_temp, report_time, None, None)
            # if not df_temp_0_year.empty:
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前个数
            #     self.variables["busi_org_cnt_now"] = df_temp_0_year.loc[:, 'account_org'].drop_duplicates().size
            #     # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前余额
            #     # self.variables["busi_org_balance_now"] = df_temp_0_year.loc[:, 'loan_balance'].sum()

    def util_get_acc_speculate_n_year_before(self, df, date, param, param_value_list):
        if not df.empty:
            year = date.year
            month = date.month
            df = df[(df['year'] == year) & (df['month'] == month)]
            if param is not None:
                df = df[df[param].isin(param_value_list)]
        return df

    def _total_principal(self, account_org_list, loan_type_df, date1, date2, pcredit_acc_speculate_df):
        total_principal_list = []
        max_terms_list = []
        max_interest_rate_ago_list = []
        df = loan_type_df[(loan_type_df['loan_date'] <= date1)
                          & (loan_type_df['loan_date'] > date2)].sort_values(by='loan_date', ascending=False)
        for org in account_org_list:
            df_temp = df[df['account_org'] == org].reset_index(drop=True)
            df_temp = df_temp.fillna(0)
            if not df_temp.empty:
                loan_amount = df_temp.loc[:, 'loan_amount'].sum()
                repay_period = df_temp.loc[:, 'repay_period'].max()
                df_temp1 = pd.merge(df_temp, pcredit_acc_speculate_df, left_on='id', right_on='record_id')
                if not df_temp1.empty:
                    max_interest_rate_ago_list.append(df_temp1.loc[:, 'nominal_interest_rate'].max())
                else:
                    max_interest_rate_ago_list.append(0)
                total_principal_list.append(loan_amount)
                max_terms_list.append(repay_period)
            else:
                total_principal_list.append(0)
                max_terms_list.append(0)
                max_interest_rate_ago_list.append(0)
        return total_principal_list, max_terms_list, max_interest_rate_ago_list

    def _variable_loan_account_org(self, loan_account_type_df, report_time_before_12_month, report_time_before_2_year,
                                   report_time_before_3_month, report_time_before_6_month):
        # 信贷交易信息-贷款信息-贷款申请新增机构-前3个月
        account_org_list_3, loan_type_list_3, principal_amount_list_3 = self._util_loan_account_org(
            loan_account_type_df,
            report_time_before_2_year,
            report_time_before_3_month)
        self.variables["new_org_3m_ago"] = replace_nan(account_org_list_3)
        self.variables["loan_type_3m_ago"] = replace_nan(loan_type_list_3)
        self.variables["principal_amount_3m_ago"] = replace_nan(principal_amount_list_3)
        # 信贷交易信息-贷款信息-贷款申请新增机构-前6个月
        account_org_list_6, loan_type_list_6, principal_amount_list_6 = self._util_loan_account_org(
            loan_account_type_df,
            report_time_before_2_year,
            report_time_before_6_month)
        self.variables["new_org_6m_ago"] = replace_nan(account_org_list_6)
        self.variables["loan_type_6m_ago"] = replace_nan(loan_type_list_6)
        self.variables["principal_amount_6m_ago"] = replace_nan(principal_amount_list_6)
        # 信贷交易信息-贷款信息-贷款申请新增机构-前12个月
        account_org_list_12, loan_type_list_12, principal_amount_list_12 = self._util_loan_account_org(
            loan_account_type_df,
            report_time_before_2_year,
            report_time_before_12_month)
        self.variables["new_org_12m_ago"] = replace_nan(account_org_list_12)
        self.variables["loan_type_12m_ago"] = replace_nan(loan_type_list_12)
        self.variables["principal_amount_12m_ago"] = replace_nan(principal_amount_list_12)

    def _gua_loan(self, loan_gua_df):
        # 担保信息-担保信息明细-管理机构
        self.variables["guar_acc_org"] = loan_gua_df.loc[:, 'account_org'].tolist()
        # 担保信息-担保信息明细-业务种类
        self.variables["guar_loan_type"] = loan_gua_df.loc[:, 'loan_type'].tolist()
        # 担保信息-担保信息明细-到期日期
        self.variables["guar_end_date"] = loan_gua_df.loc[:, 'end_date'].apply(lambda x: format_timestamp(x)).tolist()
        # 担保信息-担保信息明细-担保金额
        self.variables["guar_principal_amount"] = loan_gua_df.loc[:, 'loan_amount'].fillna(0).tolist()
        # 担保信息-担保信息明细-担保余额
        self.variables["guar_loan_balance"] = loan_gua_df.loc[:, 'loan_balance'].fillna(0).tolist()
        # 担保信息-担保信息明细-五级分类
        self.variables["guar_latest_category"] = loan_gua_df.loc[:, 'category'].tolist()
        # 担保信息-担保信息明细-管理机构个数
        self.variables["guar_acc_org_cnt"] = loan_gua_df.loc[:, 'account_org'].drop_duplicates().size
        # 担保信息-担保信息明细-担保金额总额
        self.variables["total_guar_principal_amount"] = loan_gua_df.loc[:, 'loan_amount'].sum()
        # 担保信息-担保信息明细-担保余额总额
        self.variables["total_guar_loan_balance"] = loan_gua_df.loc[:, 'loan_balance'].sum()

    def _report_time_after_2_year(self, report_time_before_2_year_df, pcredit_acc_speculate_df):
        # 信贷交易信息-贷款信息-贷款趋势变化图-贷款机构
        self.variables['each_loan_account_org'] = report_time_before_2_year_df.loc[:, 'account_org'].tolist()
        # 信贷交易信息-贷款信息-贷款趋势变化图-发放时间
        self.variables["each_loan_date"] = report_time_before_2_year_df.loc[:, 'loan_date'].apply(
            lambda x: format_timestamp(x)).tolist()
        # 信贷交易信息-贷款信息-贷款趋势变化图-贷款发放额
        self.variables["each_principal_amount"] = report_time_before_2_year_df.loc[:, 'loan_amount'].fillna(0).tolist()
        # 信贷交易信息-贷款信息-贷款趋势变化图-贷款利率
        each_interest_rate_list = []
        # 信贷交易信息-贷款信息-贷款趋势变化图-贷款类型
        each_loan_type_list = []
        for index, row in report_time_before_2_year_df.iterrows():
            loan_type = row['loan_type']
            loan_amount = row['loan_amount']
            id = row['id']
            if loan_type in ['01', '07', '99'] or (loan_type == '04' and loan_amount >= 200000):
                each_loan_type_list.append('经营性贷款')
            else:
                each_loan_type_list.append('消费性贷款')
            if pcredit_acc_speculate_df is not None or not pcredit_acc_speculate_df.empty:
                df_temp = pcredit_acc_speculate_df[pcredit_acc_speculate_df['record_id'] == id]
                if not df_temp.empty:
                    each_interest_rate_list.append(df_temp.loc[:, 'nominal_interest_rate'].tolist()[0])
                else:
                    each_interest_rate_list.append(0)
            else:
                each_interest_rate_list.append(0)
        self.variables["each_loan_type"] = each_loan_type_list
        self.variables["each_interest_rate"] = [x if pd.notna(x) else 0 for x in each_interest_rate_list]
        # 信贷交易信息-贷款信息-贷款趋势变化图-账号状态
        self.variables["each_loan_status"] = report_time_before_2_year_df.loc[:, 'loan_status'].tolist()
        # 信贷交易信息-贷款信息-贷款趋势变化图最大贷款金额
        max_principal_amount = report_time_before_2_year_df.loc[:, 'loan_amount'].max()
        self.variables["max_principal_amount"] = max_principal_amount
        # 信贷交易信息-贷款信息-贷款趋势变化图最小贷款金额
        min_principal_amount = report_time_before_2_year_df.loc[:, 'loan_amount'].min()
        self.variables["min_principal_amount"] = min_principal_amount
        # 信贷交易信息-贷款信息-贷款趋势变化图贷款金额极差
        self.variables["rng_principal_amount"] = max_principal_amount - min_principal_amount
        # 信贷交易信息-贷款信息-贷款趋势变化图贷款金额比值
        self.variables["multiple_principal_amount"] = round((max_principal_amount / min_principal_amount),
                                                            2) if min_principal_amount > 0 else 0
        # 信贷交易信息-贷款信息-贷款额度区间分布-0-20万笔数
        loan_principal_0_20w_cnt = report_time_before_2_year_df[(report_time_before_2_year_df['loan_amount'] > 0)
                                                                & (report_time_before_2_year_df[
                                                                       'loan_amount'] <= 200000)].shape[0]
        self.variables["loan_principal_0_20w_cnt"] = loan_principal_0_20w_cnt
        # 信贷交易信息-贷款信息-贷款额度区间分布-20-50万笔数
        loan_principal_20_50w_cnt = \
            report_time_before_2_year_df[(report_time_before_2_year_df['loan_amount'] > 200000)
                                         & (report_time_before_2_year_df['loan_amount'] <= 500000)].shape[0]
        self.variables["loan_principal_20_50w_cnt"] = loan_principal_20_50w_cnt
        # 信贷交易信息-贷款信息-贷款额度区间分布-50-100万笔数
        loan_principal_50_100w_cnt = \
            report_time_before_2_year_df[(report_time_before_2_year_df['loan_amount'] > 500000)
                                         & (report_time_before_2_year_df['loan_amount'] <= 1000000)].shape[0]
        self.variables["loan_principal_50_100w_cnt"] = loan_principal_50_100w_cnt
        # 信贷交易信息-贷款信息-贷款额度区间分布-100-200万笔数
        loan_principal_100_200w_cnt = \
            report_time_before_2_year_df[(report_time_before_2_year_df['loan_amount'] > 1000000)
                                         & (report_time_before_2_year_df['loan_amount'] <= 2000000)].shape[0]
        self.variables["loan_principal_100_200w_cnt"] = loan_principal_100_200w_cnt
        # 信贷交易信息-贷款信息-贷款额度区间分布-大于200万笔数
        oan_principal_200w_cnt = \
            report_time_before_2_year_df[(report_time_before_2_year_df['loan_amount'] > 2000000)].shape[0]
        self.variables["oan_principal_200w_cnt"] = oan_principal_200w_cnt
        # 信贷交易信息-贷款信息-贷款额度区间分布-总数
        loan_principal_total_cnt = \
            report_time_before_2_year_df[(report_time_before_2_year_df['loan_amount'] > 0)].shape[0]
        self.variables["loan_principal_total_cnt"] = loan_principal_total_cnt
        # 信贷交易信息-贷款信息-贷款额度区间分布-0-20万占比
        self.variables["loan_principal_0_20w_prop"] = round(loan_principal_0_20w_cnt / loan_principal_total_cnt,
                                                            2) if loan_principal_total_cnt > 0 else 0
        # 信贷交易信息-贷款信息-贷款额度区间分布-20-50万占比
        self.variables["loan_principal_20_50w_prop"] = round(loan_principal_20_50w_cnt / loan_principal_total_cnt,
                                                             2) if loan_principal_total_cnt > 0 else 0
        # 信贷交易信息-贷款信息-贷款额度区间分布-50-100万占比
        self.variables["loan_principal_50_100w_prop"] = round(loan_principal_50_100w_cnt / loan_principal_total_cnt,
                                                              2) if loan_principal_total_cnt > 0 else 0
        # 信贷交易信息-贷款信息-贷款额度区间分布-100-200万占比
        self.variables["loan_principal_100_200w_prop"] = round(
            loan_principal_100_200w_cnt / loan_principal_total_cnt, 2) if loan_principal_total_cnt > 0 else 0
        # 信贷交易信息-贷款信息-贷款额度区间分布-大于200万占比
        self.variables["loan_principal_200w_prop"] = round((oan_principal_200w_cnt / loan_principal_total_cnt),
                                                           2) if loan_principal_total_cnt > 0 else 0

    def _loan_mort(self, loan_account_type_df):
        # 个人信息-固定资产-按揭已归还
        self.variables["mort_settle_loan_date"] = loan_account_type_df[
                                                      (loan_account_type_df['loan_type'].isin(['03', '05', '06']))
                                                      & (loan_account_type_df['loan_status'] == '04')].loc[:,
                                                  'loan_date'].apply(lambda x: format_timestamp(x)).tolist()
        # 个人信息-固定资产-按揭未结清
        self.variables["mort_no_settle_loan_date"] = loan_account_type_df[(loan_account_type_df['loan_type'].isin(
            ['03', '05', '06'])) & (loan_account_type_df['loan_status'] != '04')].loc[:, 'loan_date'].apply(
            lambda x: format_timestamp(x)).tolist()

    def _loan_balance_distribution(self, loan_account_type_df):
        # 信贷交易信息-贷款信息-贷款类型余额分布-贷款类型
        loan_type_list = []
        # 信贷交易信息-贷款信息-贷款类型余额分布-目前余额
        loan_type_balance_list = []
        # 信贷交易信息-贷款信息-贷款类型余额分布-目前笔数
        loan_type_cnt_list = []
        # 信贷交易信息-贷款信息-贷款类型余额分布-余额占比
        loan_type_balance_prop_list = []
        loan_busi_df = loan_account_type_df[
            (loan_account_type_df['loan_type'].isin(['01', '07', '99'])) |
            (loan_account_type_df['loan_type'].str.contains('融资租赁')) |
            ((loan_account_type_df['loan_type'] == '04') &
             (loan_account_type_df['loan_amount'] >= 200000))]
        loan_con_df = loan_account_type_df[
            (loan_account_type_df['loan_type'] == '04') & (loan_account_type_df['loan_amount'] < 200000)]
        loan_mor_df = loan_account_type_df[loan_account_type_df['loan_type'].isin(['03', '05', '06'])]
        loan_busi_balance = 0
        loan_con_balance = 0
        loan_mor_balance = 0
        if not loan_busi_df.empty:
            loan_type_list.append("经营性贷款")
            loan_busi_balance = loan_busi_df.loc[:, 'loan_balance'].sum()
            loan_type_balance_list.append(loan_busi_balance)
            loan_type_cnt_list.append(loan_busi_df[loan_busi_df['loan_balance'] > 0].shape[0])
        if not loan_con_df.empty:
            loan_type_list.append("消费性贷款")
            loan_con_balance = loan_con_df.loc[:, 'loan_balance'].sum()
            loan_type_balance_list.append(loan_con_balance)
            loan_type_cnt_list.append(loan_con_df[loan_con_df['loan_balance'] > 0].shape[0])
        if not loan_mor_df.empty:
            loan_type_list.append("按揭类贷款")
            loan_mor_balance = loan_mor_df.loc[:, 'loan_balance'].sum()
            loan_type_balance_list.append(loan_mor_balance)
            loan_type_cnt_list.append(loan_mor_df[loan_mor_df['loan_balance'] > 0].shape[0])
        loan_total_balance = loan_busi_balance + loan_con_balance + loan_mor_balance
        if not loan_busi_df.empty:
            loan_type_balance_prop_list.append(
                '%.4f' % (loan_busi_balance / loan_total_balance) if loan_total_balance > 0 else 0)
        if not loan_con_df.empty:
            loan_type_balance_prop_list.append(
                '%.4f' % (loan_con_balance / loan_total_balance) if loan_total_balance > 0 else 0)
        if not loan_mor_df.empty:
            loan_type_balance_prop_list.append(
                '%.4f' % (loan_mor_balance / loan_total_balance) if loan_total_balance > 0 else 0)
        self.variables["loan_type"] = loan_type_list
        self.variables["loan_type_balance"] = replace_nan(loan_type_balance_list)
        self.variables["loan_type_cnt"] = replace_nan(loan_type_cnt_list)
        self.variables["loan_type_balance_prop"] = replace_nan(loan_type_balance_prop_list)

    # 单查询条件，获取对应的结果
    def _get_one_query_condition_max(self, df, query_field, query_list, filter_field, method):
        df_temp = df[df[query_field].isin(query_list)]
        value = 0
        if not df_temp.empty:
            if method == 'max':
                value = df_temp.loc[:, filter_field].max()
            elif method == 'min':
                value = df_temp.loc[:, filter_field].min()
            elif method == 'sum':
                value = df_temp.loc[:, filter_field].sum()
        if pd.notna(value):
            return value
        return 0

    def _util_loan_account_org(self, loan_account_type_df, date1, date2):
        account_org_list = []
        loan_type_list = []
        principal_amount_list = []
        loan_account_type_df = loan_account_type_df.sort_values(by='loan_date', ascending=False)
        df1 = loan_account_type_df[
            (loan_account_type_df['loan_date'] >= date1) & (loan_account_type_df['loan_date'] < date2)]
        df2 = loan_account_type_df[loan_account_type_df['loan_date'] >= date2]
        account_org_list_temp = list(
            set(df2.loc[:, 'account_org'].tolist()).difference(set(df1.loc[:, 'account_org'].tolist())))
        if len(account_org_list_temp) > 0:
            df3=df2[df2['account_org'].isin(account_org_list_temp)]
            account_org_list = df3.loc[:, 'account_org'].tolist()
            loan_type_list = df3.loc[:, 'loan_type'].tolist()
            principal_amount_list = df3.loc[:, 'loan_amount'].tolist()
        return account_org_list, loan_type_list, principal_amount_list
