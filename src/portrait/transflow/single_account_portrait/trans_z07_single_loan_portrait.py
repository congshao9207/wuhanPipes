
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str, months_ago_datetime
import datetime
import pandas as pd


class SingleLoanPortrait:
    """
    单账户画像表_贷款信息
    author:汪腾飞
    created_time:20200708
    updated_time_v1:20200709 trans_time-->trans_date
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df_2_years
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._loan_type_detail()

        transform_class_str(self.role_list, 'TransSingleLoanPortrait')
        # self.db.session.bulk_save_objects(self.role_list)
        # self.db.session.add_all(self.role_list)
        # self.db.session.commit()

    def _loan_type_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        max_date = flow_df['trans_date'].max()
        min_date = flow_df['trans_date'].min()
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)

        loan_type_list = flow_df[pd.notna(flow_df['loan_type'])]['loan_type'].unique().tolist()
        months_cnt = flow_df['calendar_month'].max()

        for t in loan_type_list:
            # 24个公历月
            loan_type_df = flow_df[flow_df['loan_type'] == t]
            for i in range(1, months_cnt+1):
                temp_df = loan_type_df[loan_type_df['calendar_month'] == i]
                if len(temp_df) == 0:
                    continue
                temp_dict = dict()
                temp_dict['account_id'] = self.account_id
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['loan_type'] = t
                temp_dict['month'] = str(i)
                temp_dict['loan_amt'] = temp_df[temp_df['trans_amt'] > 0]['trans_amt'].sum()
                temp_dict['loan_cnt'] = temp_df[temp_df['trans_amt'] > 0].shape[0]
                temp_loan_mean = temp_df[temp_df['trans_amt'] > 0]['trans_amt'].mean()
                temp_dict['loan_mean'] = temp_loan_mean if pd.notnull(temp_loan_mean) else 0
                temp_dict['repay_amt'] = temp_df[temp_df['trans_amt'] < 0]['trans_amt'].sum()
                temp_dict['repay_cnt'] = temp_df[temp_df['trans_amt'] < 0].shape[0]
                temp_repay_mean = temp_df[temp_df['trans_amt'] < 0]['trans_amt'].mean()
                temp_dict['repay_mean'] = temp_repay_mean if pd.notnull(temp_repay_mean) else 0
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time

                # role = transform_class_str(temp_dict, 'TransSingleLoanPortrait')
                self.role_list.append(temp_dict)
            # 近3/6/12个月及历史可查
            for j in [3, 6, 12, 24]:
                if j != 24:
                    temp_min_date = months_ago_datetime(max_date, j)
                    temp_df = loan_type_df[loan_type_df['trans_date'] >= temp_min_date]
                else:
                    temp_df = loan_type_df.copy()
                if len(temp_df) == 0:
                    continue
                temp_dict = dict()
                temp_dict['account_id'] = self.account_id
                temp_dict['report_req_no'] = self.report_req_no
                temp_income_df = temp_df[temp_df['trans_amt'] > 0]
                temp_expense_df = temp_df[temp_df['trans_amt'] < 0]
                temp_dict['loan_type'] = t
                temp_dict['month'] = '近' + str(j) + '个月' if j != 24 else '历史可查'
                temp_dict['loan_amt'] = temp_income_df['trans_amt'].sum()
                temp_dict['loan_cnt'] = temp_income_df.shape[0]
                loan_mean = temp_income_df['trans_amt'].mean()
                temp_dict['loan_mean'] = loan_mean if pd.notnull(loan_mean) else 0
                temp_dict['repay_amt'] = temp_expense_df['trans_amt'].sum()
                temp_dict['repay_cnt'] = temp_expense_df.shape[0]
                repay_mean = temp_expense_df['trans_amt'].mean()
                temp_dict['repay_mean'] = repay_mean if pd.notnull(repay_mean) else 0
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time

                # role = transform_class_str(temp_dict, 'TransSingleLoanPortrait')
                self.role_list.append(temp_dict)



