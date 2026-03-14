
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
import pandas as pd
import datetime


class SingleRemarkPortrait:
    """
    单账户画像表 按照备注分类后的交易信息汇总
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.db = trans_flow.db
        self.income_lst = list()
        self.expense_lst = list()

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._remark_detail()
        self.income_lst.extend(self.expense_lst)
        transform_class_str(self.income_lst, 'TransSingleRemarkPortrait')
        # self.db.session.bulk_save_objects(self.income_lst)
        # self.db.session.add_all(self.income_lst)
        # self.db.session.commit()

    def _remark_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        income_df = flow_df[(pd.notnull(flow_df.remark_type)) & (flow_df.trans_amt > 0)]
        expense_df = flow_df[(pd.notnull(flow_df.remark_type)) & (flow_df.trans_amt < 0)]
        income_group_df = income_df.groupby(by='remark_type').agg({'trans_amt': sum, 'remark_type': len})
        expense_group_df = expense_df.groupby(by='remark_type').agg({'trans_amt': sum, 'remark_type': len})
        income_group_df.sort_values(by='trans_amt', ascending=False, inplace=True)
        expense_group_df.sort_values(by='trans_amt', ascending=True, inplace=True)

        income_len = len(income_group_df)
        expense_len = len(expense_group_df)
        for i in range(10):
            if i+1 <= income_len:
                income_temp = dict()
                income_temp['account_id'] = self.account_id
                income_temp['report_req_no'] = self.report_req_no
                income_temp['remark_income_amt_order'] = i + 1
                income_temp['remark_type'] = income_group_df.index[i]
                income_temp['remark_trans_cnt'] = income_group_df.iloc[i, 1]
                income_temp['remark_trans_amt'] = income_group_df.iloc[i, 0]
                income_temp['create_time'] = create_time
                income_temp['update_time'] = create_time
                # role1 = transform_class_str(income_temp, 'TransSingleRemarkPortrait')
                self.income_lst.append(income_temp)

            if i+1 <= expense_len:
                expense_temp = dict()
                expense_temp['account_id'] = self.account_id
                expense_temp['report_req_no'] = self.report_req_no
                expense_temp['remark_expense_amt_order'] = i + 1
                expense_temp['remark_type'] = expense_group_df.index[i]
                expense_temp['remark_trans_cnt'] = expense_group_df.iloc[i, 1]
                expense_temp['remark_trans_amt'] = expense_group_df.iloc[i, 0]
                expense_temp['create_time'] = create_time
                expense_temp['update_time'] = create_time
                # role2 = transform_class_str(expense_temp, 'TransSingleRemarkPortrait')
                self.expense_lst.append(expense_temp)
