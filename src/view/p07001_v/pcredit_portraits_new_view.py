from mapping.module_processor import ModuleProcessor
import pandas as pd
import numpy as np
from pandas.tseries import offsets

from product.date_time_util import before_n_month, before_n_year


class PcreditPortraitsNewView(ModuleProcessor):

    def process(self):
        self._get_portraits_new_msg()

    def _get_portraits_new_msg(self):
        pcredit_loan_df = self.cached_data.get("pcredit_loan")
        credit_base_info_df = self.cached_data.get("credit_base_info")
        pcredit_info_df = self.cached_data.get("pcredit_info")
        report_time = None
        if credit_base_info_df is not None and not credit_base_info_df.empty:
            report_time = credit_base_info_df.loc[0, 'report_time']
        if pcredit_loan_df is not None and not pcredit_loan_df.empty:
            pcredit_loan_df_acc_type = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01', '02', '03'])]
            self._get_loan_overdue_money(pcredit_loan_df_acc_type)
            self._get_loan_overdue_cnt(pcredit_loan_df_acc_type, report_time)
            pcredit_crdit_df_acc_type = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['04', '05'])]
            self._get_creidt_overdue_money(pcredit_crdit_df_acc_type)
            self._get_creidt_overdue_cnt(pcredit_crdit_df_acc_type, report_time)

        if pcredit_info_df is not None and not pcredit_info_df.empty:
            self._get_pcredit_info_money(pcredit_info_df)

    def _get_loan_overdue_money(self, df):
        if df.empty:
            return
        '''
            1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03的id;
            2.遍历记录，若latest_replay_amount还款金额>=overdue_amount，则跳过，否则返回值+1
        '''
        overdue_amount = 0
        for index, row in df.iterrows():
            if pd.notna(row['latest_replay_amount']) and row['latest_replay_amount'] >= row['overdue_amount']:
                continue
            overdue_amount += row['overdue_amount'] if pd.isna(row['overdue_amount']) == False and row[
                'overdue_amount'] > 0 else 0
        self.variables['loan_now_overdue_money'] = overdue_amount
        df1 = df[pd.notnull(df['loan_amount'])]
        df1 = df1[(((df1['loan_type'].isin(['01', '07', '99'])) |
                    (df1['loan_type'].str.contains('融资租赁'))) &
                   (df1['loan_amount'] >= 10000)) |
                  ((df1['loan_type'] == '04') & (df1['loan_amount'] >= 200000))]
        if not df1.empty:
            repayment_df = self.cached_data.get("pcredit_repayment")
            overdue_cnt_df = pd.merge(df1, repayment_df, left_on='id', right_on='record_id')
            overdue_cnt_df = overdue_cnt_df[overdue_cnt_df['repayment_amt'] > overdue_cnt_df['loan_amount'] / 3]
            self.variables["business_loan_overdue_money"] = round(overdue_cnt_df.loc[:, 'repayment_amt'].sum(), 2)
            overdue_cnt_df['jhi_year'] = overdue_cnt_df['jhi_year'].astype(str)
            overdue_cnt_df['month'] = overdue_cnt_df['month'].astype(str)
            overdue_cnt_df['overdue_dt'] = overdue_cnt_df['jhi_year'] + '-' + overdue_cnt_df['month']
            self.variables["loan_overdue_detail_dt"] = overdue_cnt_df['overdue_dt'].tolist()
            self.variables["loan_overdue_detail_amt"] = overdue_cnt_df['repayment_amt'].tolist()

    def _get_loan_overdue_cnt(self, df, report_time):
        if df.empty:
            return
        repayment_df = self.cached_data.get("pcredit_repayment")
        repayment_df = repayment_df.drop(['id'], axis=1)
        df_temp = df[pd.notnull(df['loan_amount'])]
        df_temp1 = df_temp[
            (df_temp['loan_type'].isin(['01', '07', '99'])) | (df_temp['loan_type'].str.contains('融资租赁')) |
            ((df_temp['loan_type'] == '04') & (df_temp['loan_amount'] > 200000))]
        if not df_temp1.empty:
            # overdue_cnt_df = pd.merge(df_temp1, repayment_df, left_on='id', right_on='record_id')
            # overdue_cnt_df = overdue_cnt_df[overdue_cnt_df['repayment_amt'] > overdue_cnt_df['loan_amount']/3]
            # if not overdue_cnt_df.empty:
            #     self.variables["rhzx_business_loan_overdue_cnt"] = overdue_cnt_df['id'].unique().size
            #loan_overdue_2times_cnt
            df_temp2 = df_temp1[(df_temp1['repay_period'] > 0) | ((pd.notnull(df_temp1['loan_date'])) & (pd.notnull(df_temp1['loan_end_date'])))]
            



        merge_df = pd.merge(df, repayment_df, left_on='id', right_on='record_id')
        #
        # times_cnt_df = merge_df[merge_df['status'] == '2']
        # if not times_cnt_df.empty:
        #     self.variables["loan_overdue_2times_cnt"] = times_cnt_df.shape[0]

        if report_time is not None:
            # 征信不良信息-逾期信息-贷款当前逾期次数
            '''
            1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03的id;
            2.遍历记录，若latest_replay_amount还款金额>=overdue_amount，则跳过，否则返回值+1
            '''
            count = 0
            for index, row in df.iterrows():
                if pd.notna(row['latest_replay_amount']) and row['latest_replay_amount'] >= row['overdue_amount']:
                    continue
                count += 1 if pd.isna(row['overdue_amount']) == False and row['overdue_amount'] > 0 else 0
            self.variables['loan_now_overdue_cnt'] = count

            # 征信不良信息-逾期信息-单笔房贷2年内逾期次数
            merge_df_overdue_cnt_2y = self._util_filter_and_merge_df(df, repayment_df, ['03', '05', '06'])
            overdue_cnt_2y_df = self._util_filter_n_year_df(merge_df_overdue_cnt_2y, report_time, 2)
            if not overdue_cnt_2y_df.empty:
                self.variables["single_house_loan_overdue_cnt_2y"] = str(overdue_cnt_2y_df.groupby('id').size().max())

            # 征信不良信息-逾期信息-单笔车贷2年内逾期次数
            car_loan_overdue_cnt_2y_merge = self._util_filter_and_merge_df(df, repayment_df, ['02'])
            car_loan_overdue_cnt_2y_df = self._util_filter_n_year_df(car_loan_overdue_cnt_2y_merge, report_time, 2)
            if not car_loan_overdue_cnt_2y_df.empty:
                self.variables["single_car_loan_overdue_cnt_2y"] = str(
                    car_loan_overdue_cnt_2y_df.groupby('id').size().max())

            # 征信不良信息-逾期信息-单笔消费贷2年内逾期次数
            consume_loan_overdue_cnt_2y_merge = self._util_filter_and_merge_df(df, repayment_df, ['04'])
            consume_loan_overdue_cnt_2y_df = self._util_filter_n_year_df(consume_loan_overdue_cnt_2y_merge, report_time,
                                                                         2)
            if not consume_loan_overdue_cnt_2y_df.empty:
                self.variables["single_consume_loan_overdue_cnt_2y"] = str(
                    consume_loan_overdue_cnt_2y_df.groupby('id').size().max())

            # 征信不良信息-逾期信息-消费贷5年内总逾期次数
            consume_loan_overdue_cnt_5y_merge = self._util_filter_and_merge_df(df, repayment_df,
                                                                               ['02', '03', '04', '05', '06'])
            consume_loan_overdue_cnt_5y_df = self._util_filter_n_year_df(consume_loan_overdue_cnt_5y_merge, report_time,
                                                                         5)
            if not consume_loan_overdue_cnt_5y_df.empty:
                # 不准确，参考决策入参变量
                self.variables["total_consume_loan_overdue_cnt_5y"] = consume_loan_overdue_cnt_5y_df.shape[0]

                # 征信不良信息-逾期信息-消费贷5年内总逾期金额
                self.variables[
                    "total_consume_loan_overdue_money_5y"] = self._util_get_total_consume_loan_overdue_money_5y(
                    consume_loan_overdue_cnt_5y_df)

        # 征信不良信息-逾期信息-贷款历史总逾期次数
        total_overdue_cnt_df = merge_df[(merge_df['repayment_amt'] > 0) | (merge_df['status'].str.isdigit() == True)]
        if not total_overdue_cnt_df.empty:
            self.variables["loan_total_overdue_cnt"] = total_overdue_cnt_df.shape[0]

        # 征信不良信息-逾期信息-贷款最大连续逾期
        max_overdue_month_df = merge_df[merge_df['status'].str.isdigit() == True]
        if not max_overdue_month_df.empty:
            self.variables["loan_max_overdue_month"] = str(max_overdue_month_df['status'].apply(int).max())

    def _get_creidt_overdue_money(self, df):
        if df.empty:
            return
        # 征信不良信息-逾期信息-贷记卡当前逾期金额
        # 取贷记卡当前逾期总额>1000的记录,若latest_replay_amount还款金额>=overdue_amount，则跳过
        # 取准贷记卡，从pcredit_loan中选取account_type=05的id,对每一个id,在pcredit_repayment中record_id=id记录中找到最近一笔还款记录中的repayment_amt，筛选repayment_amt>1000且status>2的记录,将所有repayment_amt加总
        # 贷记卡+准贷记卡当前逾期总额
        repayment_df = self.cached_data.get("pcredit_repayment")
        overdue_amt_df = df.query('account_type == "04"').fillna(0)
        overdue_amt_df = overdue_amt_df[overdue_amt_df['overdue_amount'] > 1000]
        overdue_amount = 0
        for index, row in overdue_amt_df.iterrows():
            if pd.notna(row['latest_replay_amount']) and row['latest_replay_amount'] >= row['overdue_amount']:
                continue
            overdue_amount += row['overdue_amount'] if pd.isna(row['overdue_amount']) == False and row[
                'overdue_amount'] > 1000 else 0
        overdue_amt = overdue_amount

        loan_ids = df.query('account_type == "05"')["id"]
        report_time = self.cached_data["report_time"]
        target_time = report_time - offsets.DateOffset(months=1)
        for loan_id in loan_ids:
            df = repayment_df.query('record_id == ' + str(loan_id))
            # df = df.dropna(subset=["repayment_amt"])
            df = df[(df['jhi_year'] == target_time.year) & (df['month'] == target_time.month)]
            if not df.empty:
                df = df.sort_values(by=['jhi_year', 'month'], ascending=False)
                repayment_amt = df.iloc[0].repayment_amt
                status = df.iloc[0].status
                if repayment_amt > 1000 and status.isdigit() and int(status) > 2:
                    overdue_amt = overdue_amt + repayment_amt
        self.variables["credit_now_overdue_money"] = round(overdue_amt, 2)

    def _get_creidt_overdue_cnt(self, df, report_time):
        if df.empty:
            return
        repayment_df = self.cached_data.get("pcredit_repayment")
        repayment_df = repayment_df.drop(['id'], axis=1)
        merge_df = pd.merge(df, repayment_df, left_on='id', right_on='record_id')
        # 征信不良信息-逾期信息-贷记卡当前逾期次数
        # 取当前逾期期数>0且当前逾期金额>1000的记录数,若latest_replay_amount还款金额>=overdue_amount，则跳过
        # 准贷记卡，从pcredit_loan中选取account_type=05的id,对每一个id,在pcredit_repayment中record_id=id记录中找到最近一笔还款记录中的repayment_amt，筛选repayment_amt>1000且status>2的记录,
        # 贷记卡+准贷记卡当前逾期次数
        overdue_count = 0
        if report_time is not None:
            overdue_amt_df = df.query('account_type == "04"').fillna(0)
            overdue_amt_df = overdue_amt_df[overdue_amt_df['overdue_amount'] > 1000]
            for index, row in overdue_amt_df.iterrows():
                if pd.notna(row['latest_replay_amount']) and row['latest_replay_amount'] >= row['overdue_amount']:
                    continue
                overdue_count += 1 if pd.isna(row['overdue_amount']) == False and row[
                    'overdue_amount'] > 1000 else 0

            loan_ids = df.query('account_type == "05"')["id"]
            report_time = self.cached_data["report_time"]
            target_time = report_time - offsets.DateOffset(months=1)
            for loan_id in loan_ids:
                df = repayment_df.query('record_id == ' + str(loan_id))
                df = df[(df['jhi_year'] == target_time.year) & (df['month'] == target_time.month)]
                if not df.empty:
                    df = df.sort_values(by=['jhi_year', 'month'], ascending=False)
                    repayment_amt = df.iloc[0].repayment_amt
                    status = df.iloc[0].status
                    if repayment_amt > 1000 and status.isdigit() and int(status) > 2:
                        overdue_count += 1
            self.variables["credit_now_overdue_cnt"] = overdue_count

        # 征信不良信息-逾期信息-贷记卡历史总逾期次数
        total_overdue_cnt_df = merge_df[(merge_df['repayment_amt'] > 0) | (merge_df['status'].str.isdigit() == True)]
        if not total_overdue_cnt_df.empty:
            self.variables["credit_total_overdue_cnt"] = total_overdue_cnt_df.shape[0]
        # 征信不良信息-逾期信息-贷记卡最大连续逾期
        max_overdue_month_df = merge_df[merge_df['status'].str.isdigit() == True]
        if not max_overdue_month_df.empty:
            self.variables["credit_overdue_max_month"] = str(max_overdue_month_df['status'].apply(int).max())
        # 征信不良信息-逾期信息-单张贷记卡2年内逾期次数
        if report_time is not None:
            overdue_cnt_2y_df = self._util_filter_n_year_df(merge_df, report_time, 2)
            if not overdue_cnt_2y_df.empty:
                self.variables["single_credit_overdue_cnt_2y"] = str(overdue_cnt_2y_df.groupby('id').size().max())

    def _get_pcredit_info_money(self, df):
        # 信贷交易信息-资金压力解析-银行授信总额
        self.variables["total_bank_credit_limit"] = round(df.loc[
                                                              0, ['non_revolloan_totalcredit', 'revolcredit_totalcredit',
                                                                  'revolloan_totalcredit', 'undestroy_limit',
                                                                  'undestory_semi_limit']].sum(), 2)

        # 信贷交易信息-资金压力解析-银行总余额
        self.variables["total_bank_loan_balance"] = round(df.loc[
            0, ['non_revolloan_balance', 'revolcredit_balance', 'revolloan_balance', 'undestory_used_limit',
                'undestory_semi_overdraft']].sum(), 2)

    def _util_filter_n_year_df(self, df, report_time, n):
        year_2, month_2 = before_n_year(report_time, n)
        resp_df = df[
            ((df['jhi_year'] > year_2) | ((df['jhi_year'] == year_2) & (df['month'] <= month_2))) & ((
                    df['status'].str.isdigit() == True) | (df['repayment_amt'] > 0))]
        return resp_df

    def _util_filter_and_merge_df(self, df, target_df, filter_param_list):
        if filter_param_list is not None:
            df_temp = df[df['loan_type'].isin(filter_param_list)]
            df_merge = pd.merge(df_temp, target_df, left_on='id', right_on='record_id')
        else:
            df_merge = pd.merge(df, target_df, left_on='id', right_on='record_id')
        return df_merge

    def _util_get_total_consume_loan_overdue_money_5y(self, raw_df):
        total_money_list = []
        df_group = raw_df.groupby('id')
        df_group = df_group if df_group else {}
        for key, df in df_group:
            if df is None:
                continue
            list_repayment_amt = []
            list_status = []
            df = df if df else {}
            for index, row in df.iterrows():
                if row is None:
                    continue
                status = row['status']
                repayment_amt = row['repayment_amt']
                if status == '1' and len(list_status) > 1:
                    total_money_list.append(list_repayment_amt[-1])
                if status == '1':
                    list_repayment_amt.clear()
                    list_status.clear()
                list_status.append(status)
                list_repayment_amt.append(repayment_amt)
        if len(total_money_list) > 0:
            return '%.2f' % sum(total_money_list)
        return 0
