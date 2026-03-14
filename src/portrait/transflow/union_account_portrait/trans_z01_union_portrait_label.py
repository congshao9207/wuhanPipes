from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
import pandas as pd
import datetime
from fileparser.trans_flow.trans_config import *


class TransUnionLabel:
    """
    联合标签画像表落库
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow):
        self.db = trans_flow.db
        self.apply_no = trans_flow.app_no
        self.query_data_array = trans_flow.query_data_array
        self.df = trans_flow.trans_u_flow_df
        self.label_list = []

    def process(self):
        if self.df is None:
            return
        self._in_out_order()
        self._save_union_trans_label()

        transform_class_str(self.label_list, 'TransUFlowPortrait')
        # self.db.session.bulk_save_objects(self.label_list)
        # self.db.session.add_all(self.label_list)
        # self.db.session.commit()

    def _in_out_order(self):
        self.df.drop(['id', 'income_cnt_order', 'expense_cnt_order', 'income_amt_order',
                      'expense_amt_order', 'create_time', 'update_time'],
                     axis=1, inplace=True)
        income_per_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt > 0) &
                                (self.df.opponent_type == 1) & (pd.isna(self.df.loan_type)) &
                                (pd.isna(self.df.unusual_trans_type)) &
                                (~self.df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) &
                                (~self.df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
                                (self.df.trans_flow_src_type != 1)]
        expense_per_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt < 0) &
                                 (self.df.opponent_type == 1) & (pd.isna(self.df.loan_type)) &
                                 (pd.isna(self.df.unusual_trans_type)) &
                                 (~self.df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) &
                                 (~self.df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
                                 (self.df.trans_flow_src_type != 1)]
        income_com_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt > 0) &
                                (self.df.opponent_type == 2) & (pd.isna(self.df.loan_type)) &
                                (pd.isna(self.df.unusual_trans_type)) & (self.df.trans_flow_src_type != 1)]
        income_com_df = income_com_df[
            (~income_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
            (~income_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
        expense_com_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt < 0) &
                                 (self.df.opponent_type == 2) & (pd.isna(self.df.loan_type)) &
                                 (pd.isna(self.df.unusual_trans_type)) & (self.df.trans_flow_src_type != 1)]
        expense_com_df = expense_com_df[
            (~expense_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
            (~expense_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
        income_per_cnt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:20]
        income_per_amt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:20]
        expense_per_cnt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:20]
        expense_per_amt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=True).index.to_list()[:20]
        income_com_cnt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:20]
        income_com_amt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:20]
        expense_com_cnt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:20]
        expense_com_amt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=True).index.to_list()[:20]
        for i in range(len(income_per_cnt_list)):
            self.df.loc[self.df['opponent_name'] == income_per_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(income_com_cnt_list)):
            self.df.loc[self.df['opponent_name'] == income_com_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(expense_per_cnt_list)):
            self.df.loc[self.df['opponent_name'] == expense_per_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(expense_com_cnt_list)):
            self.df.loc[self.df['opponent_name'] == expense_com_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(income_per_amt_list)):
            self.df.loc[self.df['opponent_name'] == income_per_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(income_com_amt_list)):
            self.df.loc[self.df['opponent_name'] == income_com_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(expense_per_amt_list)):
            self.df.loc[self.df['opponent_name'] == expense_per_amt_list[i], 'expense_amt_order'] = i + 1
        for i in range(len(expense_com_amt_list)):
            self.df.loc[self.df['opponent_name'] == expense_com_amt_list[i], 'expense_amt_order'] = i + 1

    def _save_union_trans_label(self):
        col_list = self.df.columns.to_list()
        # if 'trans_flow_src_type' in col_list:
        #     col_list.remove('trans_flow_src_type')
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        for row in self.df.itertuples():
            temp_dict = dict()
            temp_dict['apply_no'] = self.apply_no
            for col in col_list:
                if pd.notnull(getattr(row, col)):
                    temp_dict[col] = getattr(row, col)
            temp_dict['trans_time'] = str(temp_dict['trans_time'])[-8:]
            temp_dict['create_time'] = create_time
            temp_dict['update_time'] = create_time
            # role = transform_class_str(temp_dict, 'TransUFlowPortrait')
            self.label_list.append(temp_dict)
