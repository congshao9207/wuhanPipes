import pandas as pd
from mapping.module_processor import ModuleProcessor
from pandas.tseries import offsets
from util.mysql_reader import sql_to_df


class LimitModel(ModuleProcessor):

    def __init__(self):
        super().__init__()
        self.report_time = None
        self.spouse_name = None
        self.variables = {
            "main_age": 0,  # 年龄
            "main_sex": 0,  # 性别
            "marriage_status": 0,  # 婚姻状况
            "opera_year": 0,  # 经营年限
            "unsettled_business_loan_org_cnt": 0,  # 经营性贷款在贷机构家数
            "unsettled_business_loan_org_avg_amt": 0,  # 经营性贷款在贷机构授信均值
            "total_overdue_cnt_3y": 0,  # 近3年贷记卡及贷款累计逾期期数
            "query_cnt_3m": 0,  # 近三个月贷款审批和贷记卡审批查询次数
            "total_credit_used_rate": 0,  # 总计贷记卡使用率
            "credit_min_payed_number": 0,  # 贷记卡最低还款张数
            "unsettled_house_loan_monthly_repay_amt": 0,  # 在贷房贷月还款总额
            "unsettled_mortgage_loan_limit": 0,  # 在贷抵押贷款授信总额
            "unsettled_consume_loan_org_cnt": 0,  # 消费性贷款在贷机构家数
            "unsettled_bank_business_loan_max_limit": 0,  # 单笔银行经营贷款最大授信金额
            "unsettled_bank_business_loan_org_avg_amt": 0,  # 银行在贷经营性贷款机构授信均值
            "unsettled_bank_consume_loan_org_cnt": 0,  # 银行在贷消费性贷款机构数
            "unsettled_bank_consume_loan_org_avg_amt": 0,  # 银行在贷消费性贷款机构授信均值
            "unsettled_loan_org_cnt": 0,  # 在贷机构家数
            "unsettled_loan_org_avg_amt": 0,  # 在贷机构授信均值
            "unsettled_car_loan_total_limit": 0,  # 在贷车贷授信总额
            "activated_credit_card_org_cnt": 0,  # 贷记卡发卡机构数
            "activated_credit_card_org_avg_amt": 0,  # 贷记卡机构授信均值
            "total_loan_overdue_cnt_3y": 0,  # 近3年贷款逾期次数
            "used_credit_card_org_cnt": 0,  # 有使用额度的发卡机构数
            "used_credit_card_org_avg_amt": 0,  # 有使用额度的贷记卡机构授信均值
            "unsettled_small_loan_org_cnt": 0,  # 5万以下在贷机构家数
            "total_credit_limit": 0,  # 贷记卡授信总额
            "unsettled_loan_max_limit": 0,  # 在贷授信最大值
            "couple_signment": 1,  # 配偶签字
        }

    def model_param(self):
        df = self.cached_data.get('pcredit_loan')
        self.variables['couple_signment'] = 0 if self.spouse_name is None else 1
        if df is None or df.shape[0] == 0:
            return
        loan_df = df[(pd.notnull(df['loan_amount'])) & (df['account_type'].isin(['01', '02', '03']))]
        credit_df = df[(df['account_type'].isin(['04', '05'])) &
                       (df['currency'].isin(['人民币元', 'CNY', 'RMB']))]
        large_info = self.cached_data.get('pcredit_large_scale')
        credit_df = pd.merge(credit_df, large_info[['record_id', 'large_scale_quota']], how='left',
                             left_on='id', right_on='record_id', sort=False)

        cp_basic_df = self.cached_data.get('spouse_pcredit_loan')
        if cp_basic_df is None:
            cp_basic_df = pd.DataFrame(columns=['account_type', 'loan_type', 'loan_balance', 'repay_amount',
                                                'loan_guarantee_type', 'loan_amount'])
        cp_loan_df = cp_basic_df[cp_basic_df['account_type'].isin(['01', '02', '03'])]

        # 在贷房贷月还款总额
        self.variables['unsettled_house_loan_monthly_repay_amt'] = loan_df[
            (loan_df['loan_type'].isin(['03', '05', '06'])) &
            (loan_df['loan_balance'] > 0)]['repay_amount'].sum() + cp_loan_df[
            (cp_loan_df['loan_type'].isin(['03', '05', '06'])) &
            (cp_loan_df['loan_balance'] > 0)]['repay_amount'].sum()
        # 在贷抵押贷款授信总额
        self.variables['unsettled_mortgage_loan_limit'] = loan_df[
            (loan_df['loan_balance'] > 0) &
            (loan_df['loan_guarantee_type'].isin(['02', '2'])) &
            (~loan_df['loan_type'].isin(['02']))]['loan_amount'].sum() + cp_loan_df[
            (cp_loan_df['loan_balance'] > 0) &
            (cp_loan_df['loan_guarantee_type'].isin(['02', '2'])) &
            (~cp_loan_df['loan_type'].isin(['02']))]['loan_amount'].sum()

        small_loan_df = loan_df[
            loan_df['loan_balance'] > 0].groupby('account_org', as_index=False).agg({'loan_amount': 'sum'})
        # 5万以下在贷机构家数
        self.variables['unsettled_small_loan_org_cnt'] = small_loan_df[
            small_loan_df['loan_amount'] < 50000].shape[0]

        # 在贷银行经营性贷款数据
        temp_df = loan_df[
            (loan_df['loan_type'].isin(['01', '07', '99'])) &
            (loan_df['loan_balance'] > 0) &
            (loan_df['account_org'].astype(str).str.contains('银行'))]
        org_cnt = temp_df['account_org'].nunique()
        # 单笔银行经营贷款最大授信金额
        if org_cnt < 3:
            self.variables['unsettled_bank_business_loan_max_limit'] = temp_df['loan_amount'].max()
        else:
            self.variables['unsettled_bank_business_loan_max_limit'] = sorted(temp_df['loan_amount'].tolist())[-2]
        # 银行在贷经营性贷款机构授信均值
        self.variables['unsettled_bank_business_loan_org_avg_amt'] = \
            temp_df['loan_amount'].sum() / org_cnt if org_cnt > 0 else 0

        # 在贷银行消费性贷款数据
        temp_df = loan_df[
            (loan_df['loan_type'].isin(['04'])) &
            (loan_df['loan_balance'] > 0) &
            (loan_df['account_org'].astype(str).str.contains('银行'))]
        org_cnt = temp_df['account_org'].nunique()
        # 银行在贷消费性贷款机构数
        self.variables['unsettled_bank_consume_loan_org_cnt'] = org_cnt
        # 银行在贷消费性贷款机构授信均值
        self.variables['unsettled_bank_consume_loan_org_avg_amt'] = \
            temp_df['loan_amount'].sum() / org_cnt if org_cnt > 0 else 0

        # 消费性贷款在贷机构家数
        self.variables['unsettled_consume_loan_org_cnt'] = loan_df[
            (loan_df['loan_type'].isin(['04'])) &
            (loan_df['loan_amount'] < 200000) &
            (loan_df['loan_balance'] > 0)]['account_org'].nunique()
        # 在贷经营性贷款数据
        temp_df = loan_df[
            ((loan_df['loan_type'].isin(['01', '07', '99'])) |
             ((loan_df['loan_type'].isin(['04'])) &
              (loan_df['loan_amount'] >= 200000))) &
            (loan_df['loan_balance'] > 0)]
        # 经营性贷款在贷机构家数
        unsettled_loan_org_cnt = temp_df['account_org'].nunique()
        self.variables['unsettled_business_loan_org_cnt'] = unsettled_loan_org_cnt
        # 经营性贷款在贷机构授信均值
        self.variables['unsettled_business_loan_org_avg_amt'] = \
            temp_df['loan_amount'].sum() / unsettled_loan_org_cnt if unsettled_loan_org_cnt > 0 else 0

        # 在贷贷款数据
        temp_df = loan_df[(loan_df['loan_balance'] > 0) &
                          (~loan_df['loan_type'].isin(['02', '03', '05', '06']))]
        org_cnt = temp_df['account_org'].nunique()
        # 在贷机构家数
        self.variables['unsettled_loan_org_cnt'] = org_cnt
        # 在贷机构授信均值
        self.variables['unsettled_loan_org_avg_amt'] = temp_df['loan_amount'].sum() / org_cnt if org_cnt > 0 else 0
        # 在贷车贷授信总额
        self.variables['unsettled_car_loan_total_limit'] = loan_df[
            (loan_df['loan_balance'] > 0) &
            (loan_df['loan_type'].isin(['02', '22']))]['loan_amount'].sum()
        # 在贷贷款授信最大值
        self.variables['unsettled_loan_max_limit'] = temp_df['loan_amount'].max()

        # 贷记卡数据
        temp_df = credit_df[
            (~credit_df['loan_status'].isin(['07', '08']))]
        org_cnt = temp_df['account_org'].nunique()
        used_org_cnt = temp_df[temp_df['avg_overdraft_balance_6'] > 0]['account_org'].nunique()
        # 贷记卡发卡机构数
        self.variables['activated_credit_card_org_cnt'] = org_cnt
        # 有使用额度的发卡机构数
        self.variables['used_credit_card_org_cnt'] = used_org_cnt
        # 贷记卡机构授信均值
        self.variables['activated_credit_card_org_avg_amt'] = \
            temp_df['loan_amount'].sum() / org_cnt if org_cnt > 0 else 0
        # 有使用额度的贷记卡机构授信均值
        self.variables['used_credit_card_org_avg_amt'] = \
            temp_df[temp_df['avg_overdraft_balance_6'] > 0]['loan_amount'].sum() / used_org_cnt \
            if used_org_cnt > 0 else 0

        # 近3年贷记卡及贷款累计逾期期数
        repay_df = self.cached_data.get('pcredit_repayment')
        repay_df = repay_df[(repay_df['jhi_year'] >= self.report_time.year - 2) |
                            ((repay_df['jhi_year'] == self.report_time.year - 3) &
                             (repay_df['month'] >= self.report_time.month))]
        # 近3年贷款逾期次数
        self.variables['total_loan_overdue_cnt_3y'] = repay_df[
            (repay_df['record_id'].isin(loan_df['id'].tolist())) &
            (pd.notna(repay_df['repayment_amt'])) &
            (repay_df['repayment_amt'] > 0)].shape[0]
        self.variables['total_overdue_cnt_3y'] = repay_df[
            (repay_df['record_id'].isin(loan_df['id'].tolist())) &
            (pd.notna(repay_df['repayment_amt'])) &
            (repay_df['repayment_amt'] > 0)].shape[0] + repay_df[
            (repay_df['record_id'].isin(credit_df['id'].tolist())) &
            (pd.notna(repay_df['repayment_amt'])) &
            (repay_df['repayment_amt'] > 500)].shape[0]

        # 贷记卡最低还款张数
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
        self.variables['credit_min_payed_number'] = p2

    def other_param(self):
        # 年龄
        birth_date = pd.to_datetime(self.id_card_no[6:14])
        age = self.report_time.year - birth_date.year
        if birth_date + offsets.DateOffset(months=age * 12) > self.report_time:
            age -= 1
        self.variables['main_age'] = age

        # 总计贷记卡使用率
        credit_info = self.cached_data.get('pcredit_info')
        total_used = max(credit_info.loc[:, ['undestory_used_limit', 'undestory_semi_overdraft']].sum().sum(),
                         credit_info.loc[:, ['undestory_avg_use', 'undestory_semi_avg_overdraft']].sum().sum())
        total_limit = credit_info.loc[:, ['undestroy_limit', 'undestory_semi_limit']].sum().sum()
        self.variables['total_credit_used_rate'] = total_used / total_limit if total_limit > 0 else 0

        # 贷记卡授信总额
        self.variables['total_credit_limit'] = total_limit

        # 近三个月贷款审批和贷记卡审批查询次数
        query_info = self.cached_data.get('pcredit_query_record')
        self.variables['query_cnt_3m'] = query_info[
            (query_info['reason'].isin(['01', '02', '08', '12', '20'])) &
            (query_info['jhi_time'] >= (self.report_time - offsets.DateOffset(months=3)).date()) &
            (~query_info['operator'].str.contains('上海晋福'))]['operator'].nunique()

        if self.spouse_name is None:
            marriage_status = '1'
            if age > 40:
                marriage_status = '3'
        else:
            marriage_status = '2'
        self.variables['marriage_status'] = marriage_status

    def operation_year(self):
        self.variables['opera_year'] = 0
        ent_code = self.cached_data.get('ent_code')
        if ent_code is None or len(ent_code) == 0:
            return
        ent_str = '"' + '","'.join(ent_code) + '"'
        sql = """select basic_id, ent_type, es_date from info_com_bus_face where basic_id in 
        (select max(id) as id from info_com_bus_basic where credit_code in (%s) group by credit_code)""" % ent_str
        basic_df = sql_to_df(sql)
        if basic_df.shape[0] == 0:
            return
        # 个体户经营年限
        ind_ent = basic_df[(basic_df['ent_type'].isin(['个体', '个体户'])) &
                           (pd.notna(basic_df['es_date']))]
        ind_year = 0
        if ind_ent.shape[0] > 0:
            for row in ind_ent.itertuples():
                es_date = getattr(row, 'es_date')
                try:
                    temp_date = pd.to_datetime(es_date)
                except ValueError:
                    temp_date = self.report_time
                temp_year = self.report_time.year - temp_date.year + (self.report_time.month - temp_date.month) / 12
                ind_year = max(ind_year, temp_year)
        # 其他企业经营年限
        nor_ent = basic_df[~basic_df['ent_type'].isin(['个体', '个体户'])]
        alt_year = 0
        con_year = 0
        nor_year = 0
        if nor_ent.shape[0] > 0:
            # 查找出资日期
            nor_id = nor_ent['basic_id'].tolist()
            id_str = ','.join([str(i) for i in nor_id])
            name_str = self.user_name
            if self.spouse_name is not None:
                name_str += '|' + self.spouse_name
            con_sql = """select * from info_com_bus_shareholder where basic_id in (%s)""" % id_str
            con_df = sql_to_df(con_sql)
            con_df = con_df[(con_df['share_holder_name'].str.contains(name_str)) &
                            (pd.notna(con_df['con_date']))]
            con_id = con_df['basic_id'].tolist()
            for row in con_df.itertuples():
                con_date = getattr(row, 'con_date')
                try:
                    temp_date = pd.to_datetime(con_date)
                except ValueError:
                    temp_date = self.report_time
                if temp_date >= self.report_time:
                    con_id.remove(getattr(row, 'basic_id'))
                temp_year = self.report_time.year - temp_date.year + (self.report_time.month - temp_date.month) / 12
                con_year = max(con_year, temp_year)
            # 查找变更日期
            alt_sql = """select * from info_com_bus_alter where basic_id in (%s)""" % id_str
            alt_df = sql_to_df(alt_sql)
            alt_df = alt_df[(alt_df['alt_item'].str.contains('股东|股权|出资|投资人|法人|法定代表人')) &
                            (alt_df['alt_af'].str.contains(name_str)) &
                            (~alt_df['alt_be'].str.contains(name_str)) &
                            (pd.notna(alt_df['alt_date']))]
            for row in alt_df.itertuples():
                alt_date = getattr(row, 'alt_date')
                try:
                    temp_date = pd.to_datetime(alt_date)
                except ValueError:
                    temp_date = self.report_time
                temp_year = self.report_time.year - temp_date.year + (self.report_time.month - temp_date.month) / 12
                alt_year = max(alt_year, temp_year)
            # 剩余企业继续查看成立日期
            nor_df = nor_ent[(~nor_ent['basic_id'].isin(con_id)) &
                             (~nor_ent['basic_id'].isin(alt_df['basic_id'].tolist())) &
                             (pd.notna(nor_ent['es_date']))]
            for row in nor_df.itertuples():
                es_date = getattr(row, 'es_date')
                try:
                    temp_date = pd.to_datetime(es_date)
                except ValueError:
                    temp_date = self.report_time
                temp_year = self.report_time.year - temp_date.year + (self.report_time.month - temp_date.month) / 12
                nor_year = max(nor_year, temp_year)
        self.variables['opera_year'] = max(ind_year, nor_year, con_year, alt_year)

    def process(self):
        self.report_time = pd.to_datetime(self.cached_data.get('report_time'))
        self.spouse_name = self.cached_data.get('spouseName')
        self.model_param()
        self.other_param()
        self.operation_year()
        for k, v in self.variables.items():
            if isinstance(v, float):
                self.variables[k] = round(v, 2)
