from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
from fileparser.trans_flow.trans_config import INTEREST_KEY_WORD, NON_INTEREST_KEY_WORD
import pandas as pd
from pandas.tseries import offsets
import datetime


def interest_executor(data):
    df = data.copy()
    quar_start = df.trans_date.min() - offsets.QuarterEnd(1)
    quar_end = df.trans_date.max() + offsets.QuarterEnd(0)
    rng_df = pd.DataFrame({'interest_ym': [format(x, '%Y-%m') for x in pd.date_range(quar_start, quar_end, freq='3M')]})
    concat_list = ['trans_channel', 'trans_type', 'trans_use', 'remark']
    df[concat_list] = df[concat_list].fillna('').astype(str)
    df['concat_str'] = df['trans_channel'] + ';' + df['trans_type'] + ';' + df['trans_use'] + ';' + df['remark']
    st_df = df[
        (df.trans_date.dt.month.isin([3, 6, 9, 12]))
        & (df.trans_date.dt.day.isin([20, 21, 22, 23]))
        & (df.trans_amt > 0)
        & (df.concat_str.str.contains(INTEREST_KEY_WORD))
        & 1 - (df.concat_str.str.contains(NON_INTEREST_KEY_WORD))
        ]
    # st_df = df[df['is_interest'] == 1]
    st_df.drop_duplicates(subset='flow_id', keep='first', inplace=True)
    if st_df.empty:
        rng_df['trans_date'] = rng_df['interest_ym'].apply(lambda x: pd.to_datetime(x + '-21'))
        rng_df['mean_interest'] = None
        rng_df['account_balance'] = None
        return rng_df
    st_df['interest_ym'] = [str(_)[:7] for _ in st_df['trans_date']]
    common_day = st_df['trans_date'].dt.day.value_counts().sort_index().index.tolist()[0]
    st_df = st_df.loc[st_df.groupby('interest_ym')['trans_amt'].agg('idxmin')].sort_values(
        by='trans_date', ascending=True)
    st_df['trans_amt'] = st_df['trans_amt'] * 2000
    st_df.rename(columns={'trans_amt': 'mean_interest'}, inplace=True)
    st_df = pd.merge(rng_df, st_df, how='left', on='interest_ym')
    st_df['trans_date'] = st_df.apply(lambda x: x['trans_date'] if pd.notna(x['trans_date']) else
                                      pd.to_datetime(x['interest_ym'] + '-' + str(common_day).rjust(2, '0')), axis=1)
    return st_df[['trans_date', 'mean_interest', 'account_balance', 'interest_ym']]


def balance_executor(trans_df, st_df):
    flow = trans_df.copy()
    st_df = st_df.set_index('trans_date')
    flow.drop_duplicates(subset=['trans_date'], keep='last', inplace=True)
    flow.index = flow.trans_date
    time_index = pd.date_range(start=flow.index.min(),
                               end=flow.index.max())
    temp_flow = pd.DataFrame(index=time_index)  # 建空表--索引为 第一笔结息-最后一笔结息 所有日期
    temp_flow = pd.merge(temp_flow, flow[['account_balance']], how='left', left_index=True, right_index=True)
    # for t in flow.index:
    #     temp_flow.loc[t, 'account_balance'] = flow.loc[t, 'account_balance']  # 存在的交易金额赋值
    temp_flow.fillna(method='ffill', inplace=True)  # 前向填充
    temp_list = list()
    # 首次余额日均必定为空
    temp_list.append(-1)
    for _ in range(st_df.shape[0] - 1):
        start, end = st_df.index[_] + offsets.DateOffset(days=1), st_df.index[_ + 1]
        if temp_flow[(temp_flow.index >= start) & (temp_flow.index <= end)].shape[0] >= 45:
            temp_list.append(temp_flow[(temp_flow.index >= start) & (temp_flow.index <= end)].values.mean())
        else:
            temp_list.append(-1)
        if start not in temp_flow.index or end not in temp_flow.index:
            st_df.loc[end, 'interest_ym'] += '*'
    st_df['mean_balance'] = temp_list
    st_df = st_df[st_df['mean_balance'] != -1]
    st_df.drop('account_balance', axis=1, inplace=True)
    return st_df


class SingleSummaryPortrait:
    """
    单账户画像表_按时间统计的汇总信息
    结息日均和余额日均
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.trans_flow_portrait_df_2_years = trans_flow.trans_flow_portrait_df_2_years
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._calendar_month_detail()

        transform_class_str(self.role_list, 'TransSingleSummaryPortrait')
        # self.db.session.bulk_save_objects(self.role_list)
        # self.db.session.add_all(self.role_list)
        # self.db.session.commit()

    def _calendar_month_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        min_date = flow_df['trans_date'].min()
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)
        flow_df['month'] = flow_df['trans_date'].apply(lambda x: x.month)

        not_sensitive_df = flow_df[(pd.isnull(flow_df.relationship)) & (flow_df.is_sensitive != 1)]
        # 2.0逻辑在成本项中剔除了"到期货款"
        cost_df = flow_df[pd.notnull(flow_df.cost_type) & (~flow_df['cost_type'].astype(str).str.contains('到期货款'))]

        # 十三个公历月
        for i in range(13):
            temp_dict = {}
            normal_income_amt = not_sensitive_df[(not_sensitive_df.calendar_month == i + 1) &
                                                 (not_sensitive_df.trans_amt > 0)]['trans_amt'].sum()
            normal_expense_amt = cost_df[cost_df.calendar_month == i + 1]['trans_amt'].sum()
            temp_df = flow_df[flow_df['calendar_month'] == i + 1]
            if temp_df.shape[0] == 0:
                continue
            temp_dict['account_id'] = self.account_id
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['month'] = str(i + 1)
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

            # role = transform_class_str(temp_dict, 'TransSingleSummaryPortrait')
            self.role_list.append(temp_dict)

        # 结息日均和余额日均新字段

        temp_df = self.trans_flow_portrait_df_2_years
        temp_df.sort_values(by=['trans_date', 'flow_id'], ascending=True, inplace=True)
        if 1 in temp_df['trans_flow_src_type'].tolist():
            return
        st_df = interest_executor(temp_df)
        if st_df.empty:
            return
        st_df = balance_executor(temp_df, st_df)
        st_df['interest_balance_proportion'] = st_df['mean_interest'] / st_df['mean_balance']
        st_df.index = st_df['interest_ym']
        for ym in st_df.index.tolist()[-4:]:
            temp_dict1 = dict()
            temp_dict1['account_id'] = self.account_id
            temp_dict1['report_req_no'] = self.report_req_no
            temp_dict1['month'] = ym
            temp_dict1['interest_amt'] = None if pd.isna(st_df.loc[ym, 'mean_interest']) \
                else st_df.loc[ym, 'mean_interest']
            temp_dict1['balance_amt'] = None if pd.isna(st_df.loc[ym, 'mean_balance']) \
                else st_df.loc[ym, 'mean_balance']
            temp_dict1['interest_balance_proportion'] = None if pd.isna(st_df.loc[ym, 'interest_balance_proportion']) \
                else st_df.loc[ym, 'interest_balance_proportion']
            temp_dict1['create_time'] = create_time
            temp_dict1['update_time'] = create_time
            # role1 = transform_class_str(temp_dict1, 'TransSingleSummaryPortrait')
            self.role_list.append(temp_dict1)
