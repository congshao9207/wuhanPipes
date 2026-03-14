# @Time : 2020/4/30 8:54 AM
# @Author : lixiaobo
# @File : total_info_processor.py.py
# @Software: PyCharm
import pandas as pd

from mapping.module_processor import ModuleProcessor

# total开头的相关信息
from mapping.p07001_m.calculator import split_by_duration_seq
from product.date_time_util import after_ref_date


class TotalInfoProcessor(ModuleProcessor):
    def process(self):
        self._total_consume_loan_overdue_cnt_5y()
        self._total_consume_loan_overdue_money_5y()
        self._total_bank_credit_limit()
        self._total_bank_loan_balance()
        self._per_credit_debt_amt() # 个人征信负债（除车贷、房贷）

    # 消费贷5年内总逾期次数
    def _total_consume_loan_overdue_cnt_5y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且(loan_type=02,03,05,06或者(loan_type=04且loan_amount<200000))的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且repayment_amt>0且还款时间在report_time五年内的记录)
        # 3.将2中所有结果加总
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")
        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] '
                                              'and (loan_type in ["02", "03", "05", "06"]'
                                              ' or (loan_type == "04" and loan_amount < 200000))')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None:
            count = 0
            report_time = self.cached_data["report_time"]
            for index, row in repayment_df.iterrows():
                if pd.notna(row["repayment_amt"]) and row["repayment_amt"] > 1000 \
                        and pd.notna(row["status"]) and row["status"].isdigit():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 5, report_time.month):
                        count = count + 1
            self.variables["total_consume_loan_overdue_cnt_5y"] = count

    # 消费贷5年内总逾期金额
    def _total_consume_loan_overdue_money_5y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=02,03,04,05,06的id;
        # 2.对每一个id,从pcredit_payment中选取所有record_id=id且status是数字的记录,将每段连续逾期的最后一笔repayment_amt加总;
        # 3.将2中所有结果加总
        credit_loan = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        # account_type = ["01", "02", "03"]
        # loan_type = ["02", "03", "04", "05", "06"]
        credit_loan = credit_loan.query('account_type in ["01", "02", "03"] '
                                        'and (loan_type in ["02", "03", "05", "06"]'
                                        ' or (loan_type == "04" and loan_amount < 200000))')

        if credit_loan.empty or repayment_df.empty:
            return

        val_lists = []
        for loan in credit_loan.itertuples():
            df = repayment_df.query('record_id == ' + str(loan.id))
            df = df.sort_values("id")
            split_by_duration_seq(df, val_lists)
        final_result = sum(map(lambda x: 0 if pd.isna(x[-1]) and x[-1] <= 1000 else x[-1], val_lists))
        self.variables["total_consume_loan_overdue_money_5y"] = final_result

    # 银行授信总额
    def _total_bank_credit_limit(self):
        # 1.从pcredit_info中选取所有report_id=report_id的所有nonRevolloan_totalcredit,revolcredit_totalcredit,revolloan_totalcredit,undestroy_limit,undestory_semi_limit;
        # 2.将1中所有字段值相加
        credit_info_df = self.cached_data["pcredit_info"]
        df = credit_info_df.loc[:,
             ["non_revolloan_totalcredit", "revolcredit_totalcredit", "revolloan_totalcredit", "undestroy_limit",
              "undestory_semi_limit"]]
        if df.empty:
            return
        value = df.sum().sum()
        value = value if pd.notna(value) else 0
        self.variables["total_bank_credit_limit"] = value

    # 银行总余额
    def _total_bank_loan_balance(self):
        # 1.从pcredit_info中选取所有report_id=report_id的所有nonRevolloan_balance,revolcredit_balance,revolloan_balance,undestroy_used_limit,undestory_semi_overdraft;
        # 2.将1中所有字段值相加
        credit_info_df = self.cached_data["pcredit_info"]
        if credit_info_df.empty:
            return
        df = credit_info_df.loc[:,
             ["non_revolloan_balance", "revolcredit_balance", "revolloan_balance", "undestory_used_limit",
              "undestory_semi_overdraft"]]
        if df.empty:
            return
        value = df.sum().sum()
        value = value if pd.notna(value) else 0
        self.variables["total_bank_loan_balance"] = value

    # # 个人征信负债（除车贷、房贷）
    # def _per_credit_debt_amt(self):
    #     # 1.从pcredit_loan中选择account_type为01、02、03且loan_type不等于02,、03、05、06的记录（贷款除去车贷、房贷），取loan_balance的和
    #     # 2.从pcredit_info中取undestory_used_limit（贷记卡账户信息 - 已用额度 ）和undestory_semi_overdraft（准贷记卡账户信息 - 透支余额）求和
    #     # 3.从pcredit_large_scale中取大额专项分期信息，若分期额度到期日期end_date大于等于报告查询日期report_time，则取“已用分期金额”usedsum。
    #     # 4.统计步骤1-3中的金额之和
    #     credit_loan = self.cached_data["pcredit_loan"]
    #     credit_loan = credit_loan.query(
    #         'account_type in ["01", "02", "03"] and loan_type not in ["02", "03", "05", "06"]')
    #     pcredit_info = self.cached_data["pcredit_info"]
    #     pcredit_large_scale_df = self.cached_data['pcredit_large_scale']
    #     report_time = self.cached_data["report_time"]
    #     pcredit_large_scale_df = pcredit_large_scale_df[pcredit_large_scale_df['end_date'] >= report_time]
    #     self.variables['per_credit_debt_amt'] = credit_loan['loan_balance'].sum() + pcredit_info[
    #         'undestory_used_limit'].sum() + pcredit_info['undestory_semi_overdraft'].sum() + pcredit_large_scale_df[
    #                                                 'usedsum'].sum()

    # 个人征信负债（除车贷、房贷）
    def _per_credit_debt_amt(self):
        # 1.从pcredit_loan中选择account_type为01、02、03且loan_type不等于02,、03、05、06
        # 的记录（贷款除去车贷、房贷），返回loan_balance, loan_date, joint_loan_mark(共同借款标志，主借款人：1，从借款人：2，无或空：0)
        # 2.从pcredit_loan中选择account_type为04、05的记录，返回已用额度quota_used、loan_date、共同借款标志设为0；
        # 3.从pcredit_large_scale中取大额专项分期信息，若分期额度到期日期end_date大于等于报告查询日期report_time，根据record_id = id, 在pcredit_loan中取“余额”loan_balance，loan_date，共同借款标志设为0。
        # 4.合并以上信息，返回对象
        result=pd.DataFrame()
        pcredit_loan = self.cached_data["pcredit_loan"]
        pcredit_loan_1_df = pcredit_loan.query(
            'account_type in ["01", "02", "03"] and loan_type not in ["02", "03", "05", "06"]')
        temp_1_df=pcredit_loan_1_df[['loan_balance','loan_date','joint_loan_mark']]
        temp_1_df['joint_loan_mark']=temp_1_df['joint_loan_mark'].apply(lambda x:1 if x=='主借款人' else 2 if x=='从借款人' else 0)
        pcredit_loan_2_df=pcredit_loan[(pcredit_loan['account_type'].isin(['04','05']))]
        temp_2_df=pcredit_loan_2_df[['quota_used','loan_date']]
        temp_2_df['joint_loan_mark']=0
        temp_2_df=temp_2_df.rename(columns={'quota_used':'loan_balance'})
        pcredit_large_scale_df = self.cached_data['pcredit_large_scale']
        report_time = self.cached_data["report_time"]
        pcredit_large_scale_df = pcredit_large_scale_df[pcredit_large_scale_df['end_date'] >= report_time]
        merge_df=pcredit_large_scale_df.merge(pcredit_loan,left_on=['record_id'],right_on=['id'],how='left')
        temp_3_df=merge_df[['loan_balance','loan_date']]
        temp_3_df['joint_loan_mark'] = 0
        result=pd.concat([temp_1_df,temp_2_df,temp_3_df],axis=0)
        if result.empty:
            return
        result=result[result['loan_balance']>0]
        result['loan_date']=result['loan_date'].astype(str)
        self.variables['per_credit_debt_amt'] = result.reset_index(drop=True).to_json(orient='records')

