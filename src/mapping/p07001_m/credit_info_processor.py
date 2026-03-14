# @Time : 2020/4/28 2:52 PM 
# @Author : lixiaobo
# @File : credit_info_processor.py.py 
# @Software: PyCharm
import pandas as pd
from pandas.tseries import offsets

from mapping.module_processor import ModuleProcessor
# credit开头的相关变量
from product.date_time_util import after_ref_date


class CreditInfoProcessor(ModuleProcessor):
    def process(self):
        self._credit_fiveLevel_a_level_cnt()
        self._credit_now_overdue_money()
        self._credit_overdue_max_month()
        self._credit_overdrawn_2card()
        self._credit_overdue_5year()
        self._credit_max_overdue_2year()
        self._credit_fiveLevel_b_level_cnt()
        self._credit_financial_tension()
        self._credit_activated_number()
        self._credit_min_payed_number()
        self._credit_fiveLevel_c_level_cnt()
        self._credit_now_overdue_cnt()
        self._credit_total_overdue_cnt()
        # self._credit_status_bad_cnt_2y()  # 贷记卡账户近2年出现过"呆账"
        self._credit_status_bad_cnt()  # 贷记卡账户近2年出现过"呆账"
        self._credit_status_legal_cnt()  # 贷记卡账户状态存在"司法追偿"
        self._credit_status_b_level_cnt()  # 贷记卡账户状态存在"银行止付、冻结"
        self._credit_overdue_2year()

    # 贷记卡五级分类存在“可疑、损失”
    def _credit_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["04", "05"] and category in ["4", "5"]')
        self.variables["credit_fiveLevel_a_level_cnt"] = df.shape[0]

    # 贷记卡当前逾期金额
    def _credit_now_overdue_money(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,筛选当前逾期总额>1000的记录,若latest_replay_amount还款金额>=overdue_amount，则跳过，然后将overdue_amount相加
        # 2.从pcredit_loan中选取所有report_id=report_id且account_type=05的id,对每一个id,在pcredit_repayment中record_id=id记录中找到最近一笔还款记录中的repayment_amt,将所有repayment_amt加总
        # 3.变量值=1和2中结果相加
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")
        overdue_amt_df = credit_loan_df.query('account_type == "04"').fillna(0)
        overdue_amt_df = overdue_amt_df[overdue_amt_df['overdue_amount'] > 1000]
        overdue_amount = 0
        for index, row in overdue_amt_df.iterrows():
            if pd.notna(row['latest_replay_amount']) and row['latest_replay_amount'] >= row['overdue_amount']:
                continue
            overdue_amount += row['overdue_amount'] if pd.isna(row['overdue_amount']) == False and row[
                'overdue_amount'] > 1000 else 0
        overdue_amt = overdue_amount

        loan_ids = credit_loan_df.query('account_type == "05"')["id"]
        report_time = self.cached_data["report_time"]
        target_time=report_time - offsets.DateOffset(months=1)
        for loan_id in loan_ids:
            df = repayment_df.query('record_id == ' + str(loan_id))
            # df = df.dropna(subset=["repayment_amt"])
            df=df[(df['jhi_year']==target_time.year) & (df['month']==target_time.month)]
            if not df.empty:
                df = df.sort_values(by=['jhi_year', 'month'], ascending=False)
                repayment_amt = df.iloc[0].repayment_amt
                status=df.iloc[0].status
                if repayment_amt > 1000 and status.isdigit() and int(status)>2:
                    overdue_amt = overdue_amt + repayment_amt

        self.variables["credit_now_overdue_money"] = overdue_amt

    # 贷记卡最大连续逾期月份数
    def _credit_overdue_max_month(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id
        # 2.对每一个id,max(pcredit_payment中record_id=id且status是数字的status)
        # 3.从2中所有结果中选取最大值"
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"]')
        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        repayment_df = repayment_df[repayment_df['repayment_amt'] > 1000]
        if not repayment_df.empty:
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    status_list.append(int(row["status"]))
            self.variables["credit_overdue_max_month"] = 0 if len(status_list) == 0 else max(status_list)

    # 贷记卡总透支率达80%且最低额还款张数多
    def _credit_overdrawn_2card(self):
        # 1.从pcredit_info中选取所有report_id=report_id的undestroy_limit,undestory_used_limit,undestory_semi_overdraft,undestory_avg_use,undestory_semi_avg_overdraft,undestory_semi_limit,计算max(undestory_used_limit+undestory_semi_overdraft,undestory_avg_use+undestory_semi_avg_overdraft)/(undestroy_limit+undestory_semi_limit)
        # 2.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录,统计其中满足以下条件的记录按照机构数进行去重，作为最低还款张数：
        # repay_amount*2>amount_replay_amount
        # 3.若1中结果>=0.8返回贷记卡最低额还款张数，否则返回0。
        credit_info_df = self.cached_data["pcredit_info"]
        pcredit_large_scale_df = self.cached_data["pcredit_large_scale"]
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_info_df.fillna(0)
        v1_satisfy = False
        for row in df.itertuples():
            v1 = max(row.undestory_used_limit + row.undestory_semi_overdraft,
                     row.undestory_avg_use + row.undestory_semi_avg_overdraft)
            v2 = row.undestroy_limit + row.undestory_semi_limit
            if v2 > 0:
                v1_satisfy |= (v1 / v2) >= 0.8
                self.variables['total_credit_used_rate'] = "%d%%" % (v1 / v2 * 100)

        df = credit_loan_df.query('account_type in ["04", "05"]')
        df = df.merge(pcredit_large_scale_df[['record_id', 'large_scale_quota']], left_on='id', right_on='record_id',
                      how='left')
        df = df[(df['repay_amount'].isna() == False) & (df['amout_replay_amount'].isna() == False)]
        if df.empty:
            return
        org_list = []
        for index, row in df.iterrows():
            if row.repay_amount * 2 > row.amout_replay_amount:
                org_list.append(row.account_org)
        self.variables['total_credit_min_repay_cnt'] = len(set(i for i in org_list if i != ''))

        if v1_satisfy:
            self.variables["credit_overdrawn_2card"] = len(set(i for i in org_list if i != ''))
        else :
            self.variables["credit_overdrawn_2card"] = 0

    # 总计贷记卡5年内逾期次数
    def _credit_overdue_5year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04的id,对每一个id,count(pcredit_payment中record_id=id且repayment_amt>1000且还款时间在report_time五年内的记录)
        # 2.从pcredit_loan中选取所有report_id = report_id且account_type = 05的id，对每一个id, count(pcredit_payment中record_id=id且status > 2且还款时间在report_time五年内且repayment_amt > 1000的记录)
        # 3.将1和2中所有结果加总
        pcredit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if pcredit_loan_df.empty or repayment_df.empty:
            return
        report_time = self.cached_data["report_time"]
        count = 0

        credit_loan_df = pcredit_loan_df.query('account_type in ["04"]')
        credit_repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if not credit_repayment_df.empty:
            for index, row in credit_repayment_df.iterrows():
                if row['repayment_amt'] > 1000:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                        count = count + 1

        semi_credit_loan_df = pcredit_loan_df.query('account_type in ["05"]')
        semi_repayment_df = repayment_df.query('record_id in ' + str(list(semi_credit_loan_df.id)))
        semi_repayment_df = semi_repayment_df[
            (semi_repayment_df['repayment_amt'] > 1000)]
        if not semi_repayment_df.empty:
            for index, row in semi_repayment_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                    if row['status'].isdigit() and int(row['status'])>2:
                        count = count + 1

        self.variables["credit_overdue_5year"] = count

    # 单张贷记卡近2年内最大逾期次数
    def _credit_max_overdue_2year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]

        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"]')
        if credit_loan_df.empty or repayment_df.empty:
            return

        report_time = self.cached_data["report_time"]
        status_list = [0]
        for row in credit_loan_df.itertuples():
            df = repayment_df.query('record_id == ' + str(row.id))
            if df.empty:
                continue
            count = 0
            for index, row in df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 1000:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        count = count + 1
            status_list.append(count)
            self.variables["credit_max_overdue_2year"] = max(status_list)

    # 贷记卡五级分类存在“次级
    def _credit_fiveLevel_b_level_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=04,05且latest_category=3的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type == "04" and category == "3"')

        self.variables["credit_fiveLevel_b_level_cnt"] = credit_loan_df.shape[0]

    # 贷记卡资金紧张程度
    def _credit_financial_tension(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录
        # 2.计算max(sum(quota_used),sum(avg_overdraft_balance_6))/sum(loan_amount)
        # 3.筛选满足repay_amount*2>amount_replay_amount的记录，按照机构去重，机构家数作为最低还款张数：
        # 4.计算(3中结果+1)*min(2,2中结果)
        credit_info_df = self.cached_data["pcredit_info"]
        if credit_info_df.empty:
            return
        credit_loan_df = self.cached_data["pcredit_loan"]
        pcredit_large_scale_df = self.cached_data["pcredit_large_scale"]
        df = credit_loan_df.query('account_type in ["04", "05"]')
        df = df.merge(pcredit_large_scale_df[['record_id', 'large_scale_quota']], left_on='id', right_on='record_id',
                      how='left')
        if df.empty:
            return
        undestory_used_limit = self._check_is_null(credit_info_df.loc[0, 'undestory_used_limit'])
        undestory_semi_overdraft = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_overdraft'])
        undestory_avg_use = self._check_is_null(credit_info_df.loc[0, 'undestory_avg_use'])
        undestory_semi_avg_overdraft = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_avg_overdraft'])
        undestroy_limit = self._check_is_null(credit_info_df.loc[0, 'undestroy_limit'])
        undestory_semi_limit = self._check_is_null(credit_info_df.loc[0, 'undestory_semi_limit'])
        if undestroy_limit + undestroy_limit > 0:
            max_v = max(undestory_used_limit + undestory_semi_overdraft,
                        undestory_avg_use + undestory_semi_avg_overdraft) / (undestroy_limit + undestory_semi_limit)
        else:
            max_v = 0

        df = df[(df['repay_amount'].isna() == False) & (df['amout_replay_amount'].isna() == False)]
        if df.empty:
            return
        org_list = []
        for index, row in df.iterrows():
            if row.repay_amount * 2 > row.amout_replay_amount:
                org_list.append(row.account_org)
        count = len(set(i for i in org_list if i != ''))
        self.variables["credit_financial_tension"] = (count + 1) * min(2, max_v)

    @staticmethod
    def _check_is_null(value):
        return 0 if pd.isnull(value) else value

    # 已激活贷记卡张数
    def _credit_activated_number(self):
        # count(pcredit_loan中report_id=report_id且account_type=04,05且loan_status不等于07,08的记录)
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_loan_df.query('account_type in ["04", "05"] and loan_status not in ["07", "08"]')
        self.variables["credit_activated_number"] = df.shape[0]

    # 贷记卡最低还款张数
    def _credit_min_payed_number(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的记录
        # 2.2.筛选满足repay_amount*2>amount_replay_amount的记录：
        # 3. 对以上记录按照机构数量去重，返回机构数量
        df = self.cached_data["pcredit_loan"]
        pcredit_large_scale_df = self.cached_data["pcredit_large_scale"]
        df = df.query('account_type in ["04", "05"]')
        df = df.merge(pcredit_large_scale_df[['record_id', 'large_scale_quota']], left_on='id', right_on='record_id',
                      how='left')
        df = df[(df['repay_amount'].isna() == False) & (df['amout_replay_amount'].isna() == False)]
        if df.empty:
            return
        org_list = []
        for index,row in df.iterrows():
            if row.repay_amount * 2 > row.amout_replay_amount:
                org_list.append(row.account_org)
        self.variables["credit_min_payed_number"] = len(set(i for i in org_list if i !=''))

    # 贷记卡状态存在"关注"
    def _credit_fiveLevel_c_level_cnt(self):
        # count(pcredit_loan中所有report_id=report_id且account_type=04,05且latest_category=2的记录)
        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type == "04" and category == "2"')
        self.variables["credit_fiveLevel_c_level_cnt"] = df.shape[0]

    # 贷记卡当前逾期次数
    def _credit_now_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=04,05的id;
        # 2.对每一个id,从pcredit_repayment中选取record_id=id且还款时间=report_time前一个月的status;
        # 3.将2中所有status是数字的结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]

        loan_df = loan_df.query('account_type in ["04", "05"]')
        if loan_df.empty or repayment_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)))
        report_time = self.cached_data["report_time"]
        count = 0
        for row in repayment_df.itertuples():
            if pd.isna(row.status) or not row.status.isdigit() or pd.isna(
                    row.repayment_amt) or row.repayment_amt <= 1000:
                continue
            if after_ref_date(row.jhi_year, row.month, report_time.year, report_time.month - 1):
                count = count + 1

        self.variables["credit_now_overdue_cnt"] = count

    # 贷记卡历史总逾期次数
    def _credit_total_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=04,05的id;
        # 2.对每一个id,count(pcredit_repayment中record_id=id且repayment_amt>0的记录);
        # 3.将2中所有结果加总
        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]

        loan_df = loan_df.query('account_type in ["04", "05"]')
        if loan_df.empty or repayment_df.empty:
            return

        # repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)) + ' and (repayment_amt > 0 or status.str.isdigit())')
        repayment_df = repayment_df[(repayment_df.record_id.isin(list(loan_df.id))) &
                                    (repayment_df.repayment_amt > 1000)]
        count = repayment_df.shape[0]
        self.variables["credit_total_overdue_cnt"] = count

    #  贷记卡账户近2年出现过"呆账"
    def _credit_status_bad_cnt(self):
        # 1.count(从pcredit_loan中report_id=report_id且account_type=04,05且loan_status=03的记录)
        # 2.从pcredit_loan中report_id = report_id且account_type = 04, 05的记录id
        # 3.count（从pcredit_repayment中选取所有record_id = id且status = 'B'的记录）
        # 4.统计步骤1和步骤3中的记录数量和
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["04", "05"] and loan_status in ["03"]')
        repayment_df = self.cached_data["pcredit_repayment"]
        report_time = self.cached_data["report_time"]
        repayment_df = repayment_df[(repayment_df['status'] == 'B') & (repayment_df['record_id'].isin(list(loan_df['id'])))]
        bad_cnt = 0
        if not repayment_df.empty:
            for index, row in repayment_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                    bad_cnt += 1
        self.variables["credit_status_bad_cnt"] = loan_df.shape[0] + bad_cnt

    #  贷记卡账户状态存在"司法追偿"
    def _credit_status_legal_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=04,05且loan_status=8的记录)
        pcredit_loan_df = self.cached_data["pcredit_loan"]
        pcredit_special_df = self.cached_data["pcredit_special"]
        pcredit_loan_df_temp = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['04', '05'])]
        pcredit_special_df_temp = pcredit_special_df[pcredit_special_df['special_type'] == '8']
        df_temp = pd.merge(pcredit_loan_df_temp, pcredit_special_df_temp, left_on='id', right_on='record_id',
                           how='inner')
        self.variables['credit_status_legal_cnt'] = df_temp.shape[0]

    #  贷记卡账户状态存在"银行止付、冻结"
    def _credit_status_b_level_cnt(self):
        # count(从pcredit_loan中report_id=report_id且account_type=04,05且loan_status=05,06,09的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["04", "05"] '
                                'and (loan_status == "05" or loan_status == "06" or loan_status=="09")')
        self.variables["credit_status_b_level_cnt"] = loan_df.shape[0]

    # 总计贷记卡（信用卡）2年内逾期次数
    def _credit_overdue_2year(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04的id，对每一个id,count(pcredit_payment中record_id=id且repayment_amt>1000且还款时间在report_time两年内的记录)
        # 2.从pcredit_loan中选取所有report_id = report_id且account_type = 05的id，对每一个id, count(pcredit_payment中record_id=id且status > 2且还款时间在report_time两年内且repayment_amt > 1000
        # 的记录)
        # 3.将1和2中所有结果加总
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df.empty or repayment_df.empty:
            return
        report_time = pd.to_datetime(self.cached_data.get("credit_base_info")["report_time"].values[0])
        count = 0

        credit_loan_df = credit_loan_df.query('account_type in ["04"]')
        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        repayment_df=repayment_df[repayment_df['repayment_amt']>1000]
        if not repayment_df.empty:
            for index, row in repayment_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                    count = count + 1

        semi_credit_loan_df=credit_loan_df.query('account_type in ["05"]')
        semi_repayment_df=repayment_df.query('record_id in ' + str(list(semi_credit_loan_df.id)))
        semi_repayment_df = semi_repayment_df[(semi_repayment_df['repayment_amt'] > 1000)]
        if not semi_repayment_df.empty:
            for index, row in semi_repayment_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                    if row['status'].isdigit() and int(row['status'])>2:
                        count = count + 1

        self.variables["credit_overdue_2year"] = count
