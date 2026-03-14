
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
import pandas as pd
import datetime
import re


class SingleRelatedPortrait:
    """
    单账户画像表_关联人和担保人流水信息
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow, account_no, bank_name, name):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.account_no = account_no
        self.bank_name = bank_name
        self.name = name
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._relationship_detail()

        transform_class_str(self.role_list, 'TransSingleRelatedPortrait')
        # self.db.session.bulk_save_objects(self.role_list)
        # self.db.session.add_all(self.role_list)
        # self.db.session.commit()

    def _relationship_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        total_income = flow_df[flow_df.trans_amt > 0]['trans_amt'].sum()
        total_expense = flow_df[flow_df.trans_amt < 0]['trans_amt'].sum()
        flow_df = flow_df[(pd.notnull(flow_df.relationship)) &
                          (pd.notnull(flow_df.opponent_name))]

        base_type = flow_df['relationship'].unique().tolist()
        for t in base_type:
            temp_t_df = flow_df[flow_df.relationship == t]
            name_list = temp_t_df['opponent_name'].unique().tolist()
            if len(name_list) == 0:
                continue
            for n in name_list:
                temp_df = temp_t_df[temp_t_df.opponent_name == n]
                temp_dict = dict()
                temp_dict['account_id'] = self.account_id
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['opponent_name'] = re.sub(r'[\d/]', "", str(n)) if pd.notna(n) else ''
                temp_dict['relationship'] = t
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time

                # 20220921调整，给每个关联人的每个账号落一条数据
                temp_df['opponent_account_no'] = temp_df.apply(
                    lambda x: x['opponent_name'] if x['opponent_account_no'] is None else x[
                        'opponent_account_no'], axis=1)
                temp_df['opponent_account_no'] = temp_df['opponent_account_no'].apply(
                    lambda x: re.sub(r'\D', "", str(x)) if len(re.sub(r'\D', "", str(x))) >= 4 else "")
                acc_no_list = temp_df[pd.notna(temp_df.opponent_account_no)].opponent_account_no.unique().tolist()
                for acc_no in acc_no_list:
                    acc_df = temp_df[temp_df['opponent_account_no'] == acc_no]
                    temp_dict['income_cnt'] = acc_df[acc_df['trans_amt'] > 0].shape[0]
                    income_amt = acc_df[acc_df['trans_amt'] > 0]['trans_amt'].sum()
                    temp_dict['income_amt'] = income_amt
                    temp_dict['income_amt_proportion'] = income_amt / total_income if total_income != 0 else 0

                    temp_dict['expense_cnt'] = acc_df[acc_df['trans_amt'] < 0].shape[0]
                    expense_amt = acc_df[acc_df['trans_amt'] < 0]['trans_amt'].sum()
                    temp_dict['expense_amt'] = expense_amt
                    temp_dict['expense_amt_proportion'] = expense_amt / total_expense if total_expense != 0 else 0
                    temp_dict['opponent_account_no'] = \
                        f"{self.name}—{self.bank_name}（{self.account_no}）;{acc_no[-4:]}" \
                        if acc_no != "" else f"{self.name}—{self.bank_name}（{self.account_no}）"
                    # role = transform_class_str(temp_dict, 'TransSingleRelatedPortrait')
                    self.role_list.append(temp_dict.copy())
