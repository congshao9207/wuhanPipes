from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
from fileparser.trans_flow.trans_config import UNUSUAL_OPPO_NAME
import pandas as pd
import datetime


class SingleCounterpartyPortrait:
    """
    单账户画像表_主要交易对手交易信息汇总
    author:汪腾飞
    created_time:20200708
    updated_time_v1:补充进账第4,第5的交易对手月贡献率
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._counterparty_detail()

        transform_class_str(self.role_list, 'TransSingleCounterpartyPortrait')
        # self.db.session.bulk_save_objects(self.role_list)
        # self.db.session.add_all(self.role_list)
        # self.db.session.commit()

    def _counterparty_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        flow_df = flow_df[(pd.isnull(flow_df['relationship'])) &
                          (flow_df['is_sensitive'] != 1) &
                          (pd.notnull(flow_df['opponent_name']))
                          & (flow_df['trans_flow_src_type'] != 1)
                          & (~flow_df['opponent_name'].astype(str).str.isnumeric())
                          & (~flow_df['opponent_name'].astype(str).str.contains(''.join(UNUSUAL_OPPO_NAME)))
                          ]
        if flow_df.shape[0] == 0:
            return
        min_date = min(flow_df['trans_date'])
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)
        flow_df['str_date'] = flow_df['trans_date'].apply(lambda x: x.date if type(x) == datetime.datetime else x)

        income_order_df = flow_df[flow_df['trans_amt'] > 0].groupby('opponent_name').agg({'trans_amt': sum})
        income_order_df.sort_values(by='trans_amt', ascending=False, inplace=True)
        expense_order_df = flow_df[flow_df['trans_amt'] < 0].groupby('opponent_name').agg({'trans_amt': sum})
        expense_order_df.sort_values(by='trans_amt', ascending=True, inplace=True)

        # 所有进账和出账交易对手列表,进账降序,出账升序
        income_order_list = income_order_df.index.to_list()
        expense_order_list = expense_order_df.index.to_list()

        # 所有进账和出账交易对手个数
        income_order_cnt = len(income_order_list)
        expense_order_cnt = len(expense_order_list)

        # 判断是否有二十个交易对手
        top_n_income = min(income_order_cnt, 20)
        top_n_expense = min(expense_order_cnt, 20)

        total_income_amt = flow_df[flow_df['trans_amt'] > 0]['trans_amt'].sum()
        total_expense_amt = flow_df[flow_df['trans_amt'] < 0]['trans_amt'].sum()

        # 遍历前n(n<=20)个进账交易对手
        for i in range(top_n_income):
            # 交易对手姓名
            opponent_name = income_order_list[i]
            # 交易对手所有进账记录
            temp_income_df = flow_df[(flow_df['opponent_name'] == opponent_name) &
                                     (flow_df['trans_amt'] > 0)]
            # 交易对手所有有进账记录的月份
            temp_months = sorted(list(set(temp_income_df['calendar_month'].to_list())))
            # 遍历所有月份
            for m in temp_months:
                # 生成一行数据
                temp_dict = dict()
                temp_dict['account_id'] = self.account_id
                temp_dict['report_req_no'] = self.report_req_no
                # 该月份对应的所有交易记录
                temp_income_month_df = temp_income_df[temp_income_df['calendar_month'] == m]
                temp_dict['month'] = m
                temp_dict['opponent_name'] = opponent_name
                temp_dict['income_amt_order'] = i + 1
                temp_dict['trans_amt'] = temp_income_month_df['trans_amt'].sum()
                temp_dict['trans_cnt'] = temp_income_month_df.shape[0]
                # 进账排名前五的需要计算贡献率
                if i <= 4:
                    total_amt_months_m = flow_df[(flow_df['calendar_month'] == m) &
                                                 (flow_df['trans_amt'] > 0)]['trans_amt'].sum()
                    temp_dict['income_amt_proportion'] = temp_income_month_df['trans_amt'].sum() / total_amt_months_m \
                        if total_amt_months_m != 0 else 0
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time
                # role = transform_class_str(temp_dict, 'TransSingleCounterpartyPortrait')
                self.role_list.append(temp_dict)
            # 交易对手所有交易月份汇总
            total_months_dict = dict()
            total_months_dict['account_id'] = self.account_id
            total_months_dict['report_req_no'] = self.report_req_no
            total_months_dict['month'] = '汇总'
            total_months_dict['opponent_name'] = opponent_name
            total_months_dict['income_amt_order'] = i + 1
            total_months_dict['trans_amt'] = temp_income_df['trans_amt'].sum()
            total_months_dict['trans_month_cnt'] = len(temp_months)
            total_months_dict['trans_cnt'] = temp_income_df.shape[0]
            total_months_dict['trans_mean'] = temp_income_df['trans_amt'].mean()
            total_months_dict['trans_amt_proportion'] = temp_income_df['trans_amt'].sum() / total_income_amt if \
                total_income_amt != 0 else 0
            # 平均账期
            all_unique_trans_date = sorted(list(set(temp_income_df['str_date'].to_list())))
            diff_days = [(all_unique_trans_date[i + 1] - all_unique_trans_date[i]).days - 1
                         for i in range(len(all_unique_trans_date) - 1)]
            diff_days = [x for x in diff_days if x != 0]
            total_months_dict['trans_gap_avg'] = sum(diff_days) / len(diff_days) if len(diff_days) != 0 else 0
            total_months_dict['create_time'] = create_time
            total_months_dict['update_time'] = create_time
            # role = transform_class_str(total_months_dict, 'TransSingleCounterpartyPortrait')
            self.role_list.append(total_months_dict)

        # 前5/10/10%/20%..../90%/100%交易对手贡献率
        income_top_n_list = [5, 10, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        for k in income_top_n_list:
            if k > 1:
                opponent_name_list = income_order_list[:k]
            else:
                opponent_name_list = income_order_list[:int(income_order_cnt * k)]
            if len(opponent_name_list) == 0:
                continue
            temp_income_df = flow_df[(flow_df['opponent_name'].isin(opponent_name_list)) &
                                     (flow_df['trans_amt'] > 0)]
            temp_months = sorted(list(set(temp_income_df['calendar_month'].to_list())))
            if k in [5, 10, 0.1, 0.2, 0.5, 1]:
                for m in temp_months:
                    temp_dict = dict()
                    temp_dict['account_id'] = self.account_id
                    temp_dict['report_req_no'] = self.report_req_no
                    temp_income_month_df = temp_income_df[temp_income_df['calendar_month'] == m]
                    temp_dict['month'] = m
                    temp_dict['income_amt_order'] = '前%d' % k if k > 1 else '前%d%%' % (k * 100)
                    total_amt_months_m = flow_df[(flow_df['calendar_month'] == m) &
                                                 (flow_df['trans_amt'] > 0)]['trans_amt'].sum()
                    temp_dict['income_amt_proportion'] = temp_income_month_df['trans_amt'].sum() / total_amt_months_m \
                        if total_amt_months_m != 0 else 0
                    temp_dict['create_time'] = create_time
                    temp_dict['update_time'] = create_time
                    # role = transform_class_str(temp_dict, 'TransSingleCounterpartyPortrait')
                    self.role_list.append(temp_dict)
            if k <= 1:
                total_months_dict = dict()
                total_months_dict['account_id'] = self.account_id
                total_months_dict['report_req_no'] = self.report_req_no
                total_months_dict['month'] = '汇总'
                total_months_dict['income_amt_order'] = '前%d%%' % (k * 100)
                total_months_dict['trans_cnt'] = temp_income_df.shape[0]
                total_months_dict['income_amt_proportion'] = temp_income_df['trans_amt'].sum() / total_income_amt if \
                    total_income_amt != 0 else 0
                total_months_dict['create_time'] = create_time
                total_months_dict['update_time'] = create_time
                # role = transform_class_str(total_months_dict, 'TransSingleCounterpartyPortrait')
                self.role_list.append(total_months_dict)

        # 遍历前n(n<=20)个出账交易对手
        for j in range(top_n_expense):
            opponent_name = expense_order_list[j]
            temp_expense_df = flow_df[(flow_df['opponent_name'] == opponent_name) &
                                      (flow_df['trans_amt'] < 0)]
            temp_months = sorted(list(set(temp_expense_df['calendar_month'].to_list())))
            for m in temp_months:
                temp_dict = dict()
                temp_dict['account_id'] = self.account_id
                temp_dict['report_req_no'] = self.report_req_no
                temp_expense_month_df = temp_expense_df[temp_expense_df['calendar_month'] == m]
                temp_dict['month'] = m
                temp_dict['opponent_name'] = opponent_name
                temp_dict['expense_amt_order'] = j + 1
                temp_dict['trans_amt'] = temp_expense_month_df['trans_amt'].sum()
                temp_dict['trans_cnt'] = temp_expense_month_df.shape[0]
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time
                # role = transform_class_str(temp_dict, 'TransSingleCounterpartyPortrait')
                self.role_list.append(temp_dict)
            total_months_dict = dict()
            total_months_dict['account_id'] = self.account_id
            total_months_dict['report_req_no'] = self.report_req_no
            total_months_dict['month'] = '汇总'
            total_months_dict['opponent_name'] = opponent_name
            total_months_dict['expense_amt_order'] = j + 1
            total_months_dict['trans_amt'] = temp_expense_df['trans_amt'].sum()
            total_months_dict['trans_month_cnt'] = len(temp_months)
            total_months_dict['trans_cnt'] = temp_expense_df.shape[0]
            total_months_dict['trans_mean'] = temp_expense_df['trans_amt'].mean()
            total_months_dict['trans_amt_proportion'] = temp_expense_df['trans_amt'].sum() / total_expense_amt if \
                total_income_amt != 0 else 0
            # 平均账期
            all_unique_trans_date = sorted(list(set(temp_expense_df['str_date'].to_list())))
            diff_days = [(all_unique_trans_date[i + 1] - all_unique_trans_date[i]).days - 1
                         for i in range(len(all_unique_trans_date) - 1)]
            diff_days = [x for x in diff_days if x != 0]
            total_months_dict['trans_gap_avg'] = sum(diff_days) / len(diff_days) if len(diff_days) != 0 else 0
            total_months_dict['create_time'] = create_time
            total_months_dict['update_time'] = create_time
            # role = transform_class_str(total_months_dict, 'TransSingleCounterpartyPortrait')
            self.role_list.append(total_months_dict)
