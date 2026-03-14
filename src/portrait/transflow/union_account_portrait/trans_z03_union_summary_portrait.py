from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
from util.mysql_reader import sql_to_df
from pandas.tseries import offsets
import pandas as pd
import datetime


class UnionSummaryPortrait:
    """
    联合账户画像表_按时间统计的汇总信息
    结息日均和余额日均新字段
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_u_flow_portrait_df
        self.report_req_no = trans_flow.report_req_no
        self.app_no = trans_flow.app_no
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._calendar_month_detail()

        transform_class_str(self.role_list, 'TransUSummaryPortrait')
        # self.db.session.bulk_save_objects(self.role_list)
        # self.db.session.add_all(self.role_list)
        # self.db.session.commit()

    def _single_portrait(self):
        sql = """select * from trans_single_summary_portrait where report_req_no = '%s'""" % self.report_req_no
        single_u_df = sql_to_df(sql)
        return single_u_df

    def _calendar_month_detail(self):
        flow_df = self.trans_flow_portrait_df
        single_u_df = self._single_portrait()
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        max_date = flow_df['trans_date'].max()
        min_date = flow_df['trans_date'].min()
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)
        flow_df['month'] = flow_df['trans_date'].apply(lambda x: x.month)

        not_sensitive_df = flow_df[(pd.isnull(flow_df.relationship)) & (flow_df.is_sensitive != 1)]
        # 2.0逻辑在成本项中剔除了"到期货款"
        cost_df = flow_df[pd.notnull(flow_df.cost_type)&(~flow_df['cost_type'].astype(str).str.contains('到期货款'))]

        # 十三个公历月
        for i in range(13):
            temp_dict = {}
            normal_income_amt = not_sensitive_df[(not_sensitive_df.calendar_month == i + 1) &
                                                 (not_sensitive_df.trans_amt > 0)]['trans_amt'].sum()
            normal_expense_amt = cost_df[cost_df.calendar_month == i + 1]['trans_amt'].sum()
            temp_df = flow_df[flow_df['calendar_month'] == i + 1]
            if temp_df.shape[0] == 0:
                continue
            temp_dict['apply_no'] = self.app_no
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['month'] = str(i+1)
            temp_dict['normal_income_amt'] = normal_income_amt
            temp_dict['normal_expense_amt'] = normal_expense_amt
            temp_dict['net_income_amt'] = normal_income_amt + normal_expense_amt
            temp_dict['salary_cost_amt'] = temp_df[temp_df.cost_type == '工资']['trans_amt'].sum()
            temp_dict['living_cost_amt'] = temp_df[temp_df.cost_type == '水电']['trans_amt'].sum()
            temp_dict['tax_cost_amt'] = temp_df[temp_df.cost_type == '税费']['trans_amt'].sum()
            temp_dict['rent_cost_amt'] = temp_df[temp_df.cost_type == '房租']['trans_amt'].sum()
            temp_dict['insurance_cost_amt'] = temp_df[temp_df.cost_type == '保险']['trans_amt'].sum()
            temp_dict['loan_cost_amt'] = temp_df[temp_df.cost_type == '到期贷款']['trans_amt'].sum()
            # 新增可变成本
            temp_dict['variable_cost_amt'] = temp_df[temp_df.cost_type == '可变成本']['trans_amt'].sum()

            temp_dict['create_time'] = create_time
            temp_dict['update_time'] = create_time

            # role = transform_class_str(temp_dict, 'TransUSummaryPortrait')
            self.role_list.append(temp_dict)

        # 结息日均和余额日均
        """
        更新时间:22/06/20
        更新内容:剔除老版本的兼容
        """
        year_ago = format(max_date - offsets.DateOffset(months=12), '%Y-%m')
        quanter_list = single_u_df[single_u_df['month'].str.isnumeric() == False]['month'].unique().tolist()
        star_list = set([x.replace('*', '') for x in quanter_list if '*' in x and x >= year_ago])
        non_star_list = set([x for x in quanter_list if '*' not in x and x >= year_ago])
        star_list -= non_star_list
        star_list = [x + '*' for x in star_list]
        star_list.extend(list(non_star_list))
        quanter_list = sorted(star_list)
        st_df = pd.DataFrame(index=quanter_list, columns=['mean_interest', 'mean_balance', 'interest_balance_proportion'])
        # 将联合账户的季度结息汇总值st_df中
        for quanter in quanter_list:
            df_temp = single_u_df[single_u_df['month'] == quanter]
            if df_temp[pd.notna(df_temp['interest_amt'])].shape[0] > 0:
                df_temp = df_temp[pd.notna(df_temp['interest_amt'])]
                st_df.loc[quanter, 'mean_interest'] = df_temp.interest_amt.sum()
                st_df.loc[quanter, 'mean_balance'] = df_temp.balance_amt.sum()
                st_df.loc[quanter, 'interest_balance_proportion'] = \
                    df_temp.interest_amt.sum() / df_temp.balance_amt.sum()
            else:
                st_df.loc[quanter, 'mean_interest'] = None
                st_df.loc[quanter, 'mean_balance'] = df_temp.balance_amt.sum()
                st_df.loc[quanter, 'interest_balance_proportion'] = None
        # 计算st_df中近半年以及近一年
        if st_df.shape[0] >= 4:
            # interest_amt1, balance_amt1 分别为近一年的结息,余额日均 2 表示近半年
            interest_list1 = [x for x in st_df.mean_interest.values[-4:].tolist() if pd.notna(x)]
            interest_amt1 = None if len(interest_list1) == 0 else sum(interest_list1) / len(interest_list1)
            interest_list2 = [x for x in st_df.mean_interest.values[-2:].tolist() if pd.notna(x)]
            interest_amt2 = None if len(interest_list2) == 0 else sum(interest_list2) / len(interest_list2)
            balance_list1 = [x for x in st_df.mean_balance.values[-4:].tolist() if pd.notna(x)]
            balance_list2 = [x for x in st_df.mean_balance.values[-2:].tolist() if pd.notna(x)]
            balance_amt1 = None if len(balance_list1) == 0 else sum(balance_list1) / len(balance_list1)
            balance_amt2 = None if len(balance_list2) == 0 else sum(balance_list2) / len(balance_list2)
        elif 2 <= st_df.shape[0] < 4:
            interest_amt1, balance_amt1 = None, None
            interest_list2 = [x for x in st_df.mean_interest.values[-2:].tolist() if pd.notna(x)]
            interest_amt2 = None if len(interest_list2) == 0 else sum(interest_list2) / len(interest_list2)
            balance_list2 = [x for x in st_df.mean_balance.values[-2:].tolist() if pd.notna(x)]
            balance_amt2 = None if len(balance_list2) == 0 else sum(balance_list2) / len(balance_list2)
        else:
            interest_amt1, balance_amt1, interest_amt2, balance_amt2 = None, None, None, None
        proportion1 = None if (interest_amt1 is None) or (balance_amt1 is None) or (balance_amt1 == 0) \
            else interest_amt1 / balance_amt1
        proportion2 = None if (interest_amt2 is None) or (balance_amt2 is None) or (balance_amt2 == 0) \
            else interest_amt2 / balance_amt2
        year_index = ['half_year', 'year']
        year_index[0] += '*' if len([x for x in st_df.index.tolist()[-2:] if '*' in x]) > 0 else ''
        year_index[1] += '*' if len([x for x in st_df.index.tolist()[-4:] if '*' in x]) > 0 else ''
        if len([_ for _ in [interest_amt2, balance_amt2, proportion2] if pd.notna(_)]) != 0:
            st_df.loc[year_index[0]] = [interest_amt2, balance_amt2, proportion2]
        if len([_ for _ in [interest_amt1, balance_amt1, proportion1] if pd.notna(_)]) != 0:
            st_df.loc[year_index[1]] = [interest_amt1, balance_amt1, proportion1]
        # 将st_df中nan转换成None
        st_df = st_df.where(st_df.notnull(), None)

        for quanter in st_df.index.tolist()[-6:]:
            interest_amt = st_df.loc[quanter, 'mean_interest']
            balance_amt = st_df.loc[quanter, 'mean_balance']
            interest_balance_proportion = st_df.loc[quanter, 'interest_balance_proportion']
            temp_dict1 = dict()
            temp_dict1['apply_no'] = self.app_no
            temp_dict1['report_req_no'] = self.report_req_no
            temp_dict1['month'] = quanter
            temp_dict1['interest_amt'] = interest_amt
            temp_dict1['balance_amt'] = balance_amt
            temp_dict1['interest_balance_proportion'] = interest_balance_proportion
            temp_dict1['create_time'] = create_time
            temp_dict1['update_time'] = create_time
            # role1 = transform_class_str(temp_dict1, 'TransUSummaryPortrait')
            self.role_list.append(temp_dict1)
