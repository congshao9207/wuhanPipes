import pandas as pd
from mapping.module_processor import ModuleProcessor
from pandas.tseries import offsets


class PcreditRule(ModuleProcessor):

    def __init__(self):
        super().__init__()
        self.report_time = None

    def credit_rule(self):
        df = self.cached_data.get('pcredit_loan')
        if df is None or df.shape[0] == 0:
            return
        loan_df = df[(pd.notnull(df['loan_amount'])) & (df['account_type'].isin(['01', '02', '03']))]
        credit_df = df[(df['account_type'].isin(['04', '05'])) &
                       (df['currency'].isin(['人民币元', 'CNY', 'RMB']))]
        guar_df = df[df['account_type'].isin(['06'])]

        # 征信白户
        self.variables['no_loan'] = \
            1 if df[df['account_type'].isin(['01', '02', '03', '04', '05', '06'])].shape[0] == 0 else 0

        cons_df = loan_df[(loan_df['loan_type'] == '04') & (loan_df['loan_amount'] < 200000)]

        # 逾期信息
        repay_df = self.cached_data.get("pcredit_repayment")
        repay_df = pd.merge(repay_df, loan_df[['id', 'loan_amount', 'account_org']], how='left', left_on='record_id',
                            right_on='id', sort=False)
        # 特殊交易信息
        special_df = self.cached_data.get('pcredit_special')
        # 查询信息
        query_info = self.cached_data.get('pcredit_query_record')
        # 资产处置信息
        default_info_df = self.cached_data.get("pcredit_default_info")

        # 5年内贷款存在本金逾期（仅指先息后本）
        self.variables['loan_principal_overdue_cnt'] = repay_df[
            (repay_df['record_id'].isin(loan_df['id'].tolist())) &
            (repay_df['repayment_amt'] > repay_df['loan_amount'])]['record_id'].nunique()

        # 存在呆账、资产处置、保证人代偿
        self.variables['public_sum_count'] = default_info_df[
            ((default_info_df['default_type'] == '01') &
             (default_info_df['default_subtype'] == '0103')) |
            (default_info_df['default_type'] == '02')].shape[0]

        # 贷款五级分类存在“关注、次级、可疑、损失”
        self.variables['loan_fiveLevel_a_level_cnt'] = loan_df[
            loan_df['category'].isin(['2', '3', '4', '5'])].shape[0]

        # 5年内还款方式为等额本息分期偿还的贷款连续逾期2期
        self.variables['business_loan_average_overdue_cnt'] = self._business_loan_average_overdue_cnt(df, repay_df)

        # 贷记卡当前逾期（单笔当前逾期＞500或当前逾期发卡机构＞1家）
        self.variables['credit_now_overdue_money'] = 1 if credit_df[
            credit_df['overdue_amount'] > 500].shape[0] > 0 or credit_df[
            credit_df['overdue_amount'] > 0]['account_org'].nunique() > 1 else 0

        # 贷款有当前逾期
        self.variables['loan_now_overdue_money'] = loan_df['overdue_amount'].sum()

        # 单张贷记卡（信用卡）、单笔贷款5年内出现连续逾期≥3期不准入（单笔逾期金额在＜500元的除外）
        self.variables['single_credit_or_loan_5year_overdue_max_month'] = \
            self._single_credit_or_loan_5year_overdue_max_month(df, repay_df)

        # 单张贷记卡（信用卡）近2年内存在5次以上逾期（单笔逾期金额在＜500元的除外）
        self.variables['single_credit_overdue_2year_cnt'] = self._single_credit_overdue_2year_cnt(credit_df, repay_df)

        # 单笔贷款近2年内存在5次以上逾期
        self.variables['single_loan_overdue_2year_cnt'] = self._single_loan_overdue_2year_cnt(loan_df, repay_df)

        # 总计贷记卡（信用卡）2年内逾期超过10次
        self.variables['credit_overdue_2year'] = repay_df[
            (repay_df['record_id'].isin(credit_df['id'].tolist())) &
            ((repay_df['jhi_year'] >= self.report_time.year - 1) |
             ((repay_df['jhi_year'] == self.report_time.year - 2) &
             (repay_df['month'] >= self.report_time.month))) &
            (repay_df['status'].str.isdigit()) &
            (repay_df['repayment_amt'] > 500)].shape[0]

        # 总计贷款2年内逾期超过8次
        self.variables['loan_consume_overdue_2year'] = repay_df[
            (repay_df['record_id'].isin(loan_df['id'].tolist())) &
            ((repay_df['jhi_year'] >= self.report_time.year - 1) |
             ((repay_df['jhi_year'] == self.report_time.year - 2) &
              (repay_df['month'] >= self.report_time.month))) &
            (repay_df['status'].str.isdigit())].shape[0]

        # 对外担保五级分类存在“关注、次级、可疑、损失”
        self.variables['loan_scured_five_a_level_abnormality_cnt'] = guar_df[
            guar_df['category'].isin(['2', '3', '4', '5'])].shape[0]

        # 存在展期
        self.variables['extension_number'] = special_df[
            (special_df['record_id'].isin(loan_df['id'].tolist())) &
            (special_df['special_type'].isin(['1']))]['record_id'].nunique()

        # 贷记卡账户2年内出现过“呆账”
        self.variables['credit_status_bad_cnt_2y'] = repay_df[
            (repay_df['record_id'].isin(credit_df['id'].tolist())) &
            ((repay_df['jhi_year'] >= self.report_time.year - 1) |
             ((repay_df['jhi_year'] == self.report_time.year - 2) &
              (repay_df['month'] >= self.report_time.month))) &
            (repay_df['status'].isin(['B']))]['record_id'].nunique()

        # 贷记卡账户状态存在“司法追偿”
        self.variables['credit_status_legal_cnt'] = special_df[
            (special_df['record_id'].isin(credit_df['id'].tolist())) &
            (special_df['special_type'].isin(['8']))]['record_id'].nunique()

        # 贷记卡账户状态存在“止付、冻结”
        self.variables['credit_status_b_level_cnt'] = credit_df[
            (credit_df['loan_status'].isin(['05', '06']))].shape[0]

        # 贷款账户状态存在“呆账”
        self.variables['loan_status_bad_cnt'] = loan_df[
            (loan_df['loan_status'].isin(['03']))].shape[0]

        # 贷款账户状态存在“司法追偿”
        self.variables['loan_status_legal_cnt'] = special_df[
            (special_df['record_id'].isin(loan_df['id'].tolist())) &
            (special_df['special_type'].isin(['8']))]['record_id'].nunique()

        # 贷款账户状态存在“银行止付、冻结”
        self.variables['loan_status_b_level_cnt'] = loan_df[
            (loan_df['loan_status'].isin(['05', '06']))].shape[0]

        # 近三个月征信查询（贷款审批及贷记卡审批等）
        self.variables['loan_credit_query_3month_cnt'] = query_info[
            (query_info['reason'].isin(['01', '02', '08', '09', '11', '12', '13', '20'])) &
            (query_info['jhi_time'] >= (self.report_time - offsets.DateOffset(months=3)).date())]['operator'].nunique()

        # 贷记卡总透支率达80%且存在2张贷记卡最低额还款且已激活贷记卡张数＞3
        self.variables['credit_overdrawn_min_payed_cnt'] = self._credit_overdrawn_min_payed_cnt(credit_df)

        # 经营性贷款在贷机构超过6家
        self.variables['unsettled_busLoan_agency_number'] = loan_df[
            (pd.notnull(loan_df['loan_amount'])) &
            (loan_df['account_type'].isin(['01', '02', '03'])) &
            ((loan_df['loan_type'].isin(['01', '07', '99', '15', '16'])) |
             ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] >= 200000) &
              (loan_df['loan_balance'] - 0.2 * loan_df['loan_amount'] > 0))) &
            (loan_df['loan_balance'] > 0)]['account_org'].nunique()

        # 消费性贷款在贷机构家数
        self.variables['unsettled_consume_agency_cnt'] = cons_df[cons_df['loan_balance'] > 0]['account_org'].nunique()

        # 征信在贷5万以下小额贷款机构家数
        small_loan_df = loan_df[
            loan_df['loan_balance'] > 0].groupby('account_org', as_index=False).agg({'loan_amount': 'sum'})
        self.variables['unsettled_small_loan_org_cnt'] = small_loan_df[
            small_loan_df['loan_amount'] < 50000].shape[0]

        # 征信在贷机构家数
        self.variables['unsettled_loan_agency_number'] = loan_df[
            (~loan_df['loan_type'].isin(["02", "03", "05", "06"])) &
            (loan_df['loan_balance'] > 0)]['account_org'].nunique()

        # 信用卡发卡机构激活的机构数
        self.variables['uncancelled_credit_organization_number'] = credit_df[
            (~credit_df['loan_status'].isin(['07', '08']))]['account_org'].nunique()

        # 贷记卡五级分类异常
        self.variables['credit_fiveLevel_abnormal_cnt'] = credit_df[
            credit_df['category'].isin(['2', '3', '4', '5'])].shape[0]

    # 5年内还款方式为等额本息分期偿还的贷款连续逾期2期
    def _business_loan_average_overdue_cnt(self, df, overdue_df):
        loan_df = df[((pd.notna(df['repay_period'])) |
                      ((pd.notna(df['loan_date'])) &
                       (pd.notna(df['loan_end_date'])))) &
                     (df['account_type'].isin(['01', '02', '03'])) &
                     ((df['loan_type'].isin(['01', '07', '99', '15', '16'])) |
                      ((df['loan_type'] == '04') &
                       (df['loan_amount'] >= 200000)))]
        if loan_df.shape[0] == 0:
            return 0
        loan_df['loan_date'] = pd.to_datetime(loan_df['loan_date'])
        loan_df['loan_end_date'] = pd.to_datetime(loan_df['loan_end_date'])
        loan_df['repay_period'] = loan_df.apply(
            lambda x: x['repay_period'] if pd.notna(x['repay_period']) else
            (x['loan_end_date'].year - x['loan_date'].year) * 12 + x['loan_end_date'].month - x['loan_date'].month
            + (x['loan_end_date'].day - x['loan_date'].day - 1) // 100 + 1, axis=1)
        loan_df['avg_loan_amount'] = loan_df.apply(
            lambda x: x['loan_amount'] / x['repay_period'] if pd.notna(x['repay_period']) and x['repay_period'] != 0
            else None, axis=1)
        loan_overdue_df = overdue_df[overdue_df['record_id'].isin(list(set(loan_df['id'].tolist())))]
        loan_overdue_df = pd.merge(loan_overdue_df, loan_df[['id', 'avg_loan_amount']], how='left',
                                   left_on='record_id', right_on='id', sort=False)
        # 筛选还款记录表中的年份、月份在报告查询日期3年内的记录
        temp_overdue_df = loan_overdue_df[(loan_overdue_df['repayment_amt'] > loan_overdue_df['avg_loan_amount']) &
                                          (loan_overdue_df['repayment_amt'] < loan_overdue_df['loan_amount'] / 3)]
        if temp_overdue_df.shape[0] == 0:
            self.variables["business_loan_average_3year_overdue_cnt"] = 0
            return
        temp_overdue_df.sort_values(by=['record_id', 'jhi_year', 'month'], inplace=True)
        temp_overdue_df.reset_index(drop=True, inplace=True)
        temp_overdue_df.loc[0, 'conti_month'] = 1
        if temp_overdue_df.shape[0] > 1:
            last_repayment_year = temp_overdue_df.loc[0, 'jhi_year']
            last_repayment_month = temp_overdue_df.loc[0, 'month']
            last_status = temp_overdue_df.loc[0, 'status']
            last_amt = temp_overdue_df.loc[0, 'repayment_amt']
            last_record_id = temp_overdue_df.loc[0, 'record_id']
            for index in temp_overdue_df.index[1:]:
                this_repayment_year = temp_overdue_df.loc[index, 'jhi_year']
                this_repayment_month = temp_overdue_df.loc[index, 'month']
                this_status = temp_overdue_df.loc[index, 'status']
                this_amt = temp_overdue_df.loc[index, 'repayment_amt']
                this_record_id = temp_overdue_df.loc[index, 'record_id']
                diff_month = (int(this_repayment_year) - int(last_repayment_year)) * 12 + \
                    int(this_repayment_month) - int(last_repayment_month)
                # 此处表示必须满足条件：1.同一笔贷款；2.间隔一个月份；3.逾期状态递增且只增加1或者逾期金额近乎倍增；才被视为连续逾期
                if this_record_id == last_record_id and diff_month == 1 and \
                        (this_amt > last_amt * 1.9 or (last_status.isdigit() and this_status.isdigit() and
                                                       int(this_status) - int(last_status) == 1)):
                    temp_overdue_df.loc[index, 'conti_month'] = temp_overdue_df.loc[index - 1, 'conti_month'] + 1
                else:
                    temp_overdue_df.loc[index, 'conti_month'] = 1
                last_repayment_year = this_repayment_year
                last_repayment_month = this_repayment_month
                last_status = this_status
                last_amt = this_amt
                last_record_id = this_record_id
        business_loan_average_overdue_cnt = temp_overdue_df[temp_overdue_df['conti_month'] == 2].shape[0]
        return business_loan_average_overdue_cnt

    # 单张贷记卡（信用卡）、单笔贷款5年内出现连续逾期≥3期不准入（单笔逾期金额在＜500元的除外）
    def _single_credit_or_loan_5year_overdue_max_month(self, df, overdue_df):
        repayment_df = overdue_df[
            (overdue_df['jhi_year'] >= self.report_time.year - 4) |
            ((overdue_df['jhi_year'] == self.report_time.year - 5) &
             (overdue_df['month'] >= self.report_time.month))]
        credit_overdue_max_month = 0
        if df is None or df.empty or repayment_df is None or repayment_df.empty:
            credit_overdue_max_month = 0
        else:
            credit_loan_df = df.query('account_type in ["04", "05"]')
            credit_repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
            credit_repayment_df = credit_repayment_df[credit_repayment_df['repayment_amt'] > 500]

            if not credit_repayment_df.empty:
                status_list = []
                for index, row in credit_repayment_df.iterrows():
                    if row["status"] and row["status"].isdigit():
                        status_list.append(int(row["status"]))
                credit_overdue_max_month = 0 if len(status_list) == 0 else max(status_list)

        loan_overdue_max_month = 0
        loan_df = df[df['account_type'].isin(['01', '02', '03'])]
        loan_repayment_df = repayment_df.query('record_id in ' + str(list(df.id)))
        if loan_df.shape[0] == 0:
            loan_overdue_max_month = 0
        else:
            if not loan_repayment_df.empty:
                status_list = []
                for index, row in loan_repayment_df.iterrows():
                    if pd.notna(row["status"]) and row["status"].isdigit() and \
                            pd.notna(row['repayment_amt']) and row['repayment_amt'] > 0:
                        status_list.append(int(row["status"]))
                loan_overdue_max_month = 0 if len(status_list) == 0 else max(status_list)
        return max(credit_overdue_max_month, loan_overdue_max_month)

    # 单张贷记卡（信用卡）近2年内存在5次以上逾期（单笔逾期金额在＜500元的除外）
    def _single_credit_overdue_2year_cnt(self, credit_loan_df, repayment_df):
        repayment_df = repayment_df[
            (repayment_df['jhi_year'] >= self.report_time.year - 1) |
            ((repayment_df['jhi_year'] == self.report_time.year - 2) &
             (repayment_df['month'] >= self.report_time.month))]
        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None and not repayment_df.empty:
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 500:
                    status_list.append(int(row["status"]))
            return 0 if len(status_list) == 0 else max(status_list)

    # 单笔贷款近2年内存在5次以上逾期
    def _single_loan_overdue_2year_cnt(self, credit_loan_df, repayment_df):
        repayment_df = repayment_df[
            (repayment_df['jhi_year'] >= self.report_time.year - 1) |
            ((repayment_df['jhi_year'] == self.report_time.year - 2) &
             (repayment_df['month'] >= self.report_time.month))]
        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        count = 0
        if not repayment_df.empty:
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 0:
                    count = count + 1
        return count

    def _credit_overdrawn_min_payed_cnt(self, credit_loan_df):
        credit_info = self.cached_data.get('pcredit_info')
        large_info = self.cached_data.get('pcredit_large_scale')
        credit_df = pd.merge(credit_loan_df, large_info[['record_id', 'large_scale_quota']], how='left',
                             left_on='id', right_on='record_id', sort=False)
        total_used = max(credit_info.loc[:, ['undestory_used_limit', 'undestory_semi_overdraft']].sum().sum(),
                         credit_info.loc[:, ['undestory_avg_use', 'undestory_semi_avg_overdraft']].sum().sum())
        total_limit = credit_info.loc[:, ['undestroy_limit', 'undestory_semi_limit']].sum().sum()
        p1 = total_used / total_limit if total_limit > 0 else 0

        p2 = credit_df[((credit_df['large_scale_quota'] > 0) &
                        (pd.notna(credit_df['repay_amount'])) &
                        (pd.notna(credit_df['amout_replay_amount'])) &
                        (credit_df['repay_amount'] - credit_df['amout_replay_amount'] > 0) &
                        (credit_df['repay_amount'] - 0.015 * credit_df['large_scale_quota'] > 0)) |
                       (((pd.isna(credit_df['large_scale_quota'])) | (credit_df['large_scale_quota'] == 0)) &
                        (((credit_df['loan_amount'] > 0) &
                          (pd.notna(credit_df['repay_amount'])) &
                          (pd.notna(credit_df['amout_replay_amount'])) &
                          (credit_df['repay_amount'] * 2 - credit_df['amout_replay_amount'] > 0) &
                          (credit_df['repay_amount'] - 0.015 * credit_df['loan_amount'] > 0)) |
                         ((credit_df['loan_amount'] > 0) &
                          (pd.notna(credit_df['repay_amount'])) &
                          (pd.notna(credit_df['amout_replay_amount'])) &
                          (credit_df['repay_amount'] - credit_df['amout_replay_amount'] > 0))))].shape[0]
        p3 = credit_df[(~credit_df['loan_status'].isin(['07', '08']))].shape[0]
        res = 1 if p1 >= 0.8 and p2 >= 2 and p3 > 3 else 0
        return res

    def process(self):
        self.report_time = pd.to_datetime(self.cached_data.get('report_time'))
        self.credit_rule()
        for k, v in self.variables.items():
            if isinstance(v, float):
                self.variables[k] = round(v, 2)
