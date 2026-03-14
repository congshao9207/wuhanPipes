import pandas as pd
from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
from pandas import DateOffset


class JsonUBalanceTrendChart(TransFlow):

    def process(self):
        self.variables['trans_balance_trend_chart'] = []
        # self.get_u_flow_portrait_detail()
        self.balance_trend_chart()

    def get_u_flow_portrait_detail(self):
        # 取联合版块的画像数据
        df = self.trans_u_flow_portrait.copy()
        if df.shape[0] == 0:
            return pd.DataFrame()
        # 获取一年前的日期
        year_ago = pd.to_datetime(df['trans_date']).max() - DateOffset(months=12)
        # 新增交易年-月列
        df['trans_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
        # 筛选近一年数据
        df = df.loc[pd.to_datetime(df.trans_date) >= year_ago]
        if df.shape[0] == 0:
            return pd.DataFrame()
        return df

    @staticmethod
    def get_trans_account_info(id_list):
        sql = """select id, account_name from trans_account where id in %(id_list)s"""
        df = sql_to_df(sql=sql, params={"id_list": id_list})
        return df

    def balance_trend_chart(self):
        """
        考虑要同时输出余额和理财数据，故也同步考虑微信和支付宝流水，余额数据输出为0
        :return:
        """
        df = self.get_u_flow_portrait_detail()
        if df.shape[0] == 0:
            return
        # 将df取每天最后一笔交易的account_balance,若某天为空，则取前一天的account_balance
        # start_date = pd.to_datetime(df['trans_date']).min()
        # end_date = pd.to_datetime(df['trans_date']).max()
        # 要满足分主体、分账户的展示，所以是输出每个账户的余额和理财数据，由前端或后端去计算
        # 数据中已包含账户信息，只用关联取主体信息即可
        account_id_list = df.account_id.unique().tolist()
        account_info = self.get_trans_account_info(account_id_list)
        for name in account_info['account_name'].unique().tolist():
            account_id_list_all = account_info.loc[account_info['account_name'] == name, 'id'].tolist()
            account_df = df.loc[df.account_id.isin(account_id_list_all)]
            if account_df.shape[0] > 0:
                for account_no in account_df['account_no'].unique().tolist():
                    account_no_df = account_df.loc[account_df['account_no'] == account_no]
                    if account_no_df.shape[0] > 0:
                        last_balance, mean_balance_d, mean_balance_m, financial_scale, account_detail_d, account_detail_m = \
                            self.get_account_balance_trend(account_no_df)
                        account_detail_dict = {'account_name': name,
                                               'account_no': account_no,
                                               "last_balance": last_balance,
                                               "mean_balance_d": mean_balance_d,
                                               "mean_balance_m": mean_balance_m,
                                               "financial_scale": financial_scale,
                                               "account_detail_d": account_detail_d,
                                               "account_detail_m": account_detail_m}
                        self.variables['trans_balance_trend_chart'].append(account_detail_dict)

    # 处理每个账户的日余额和理财数据
    @staticmethod
    def get_account_balance_trend(account_df):
        """
        获取账户的日余额和理财数据
        :param account_df:
        :return:
        """

        # 建空表--索引为 第一笔交易-最后一笔交易 所有日期
        # start_date = account_df['trans_date'].min()
        # end_date = account_df['trans_date'].max()
        # date_index = pd.date_range(start=start_date, end=end_date)
        # temp_flow = pd.DataFrame(index=date_index)
        flow = account_df.copy()
        # 微信和支付宝，没有余额，存在account_balance为空的情况
        flow['account_balance'].fillna(0, inplace=True)

        def func1(x):
            return x[x.index.max()].sum()

        def func2(x):
            return x[x.index.isin(flow.loc[flow.usual_trans_type.str.contains('理财')].index.tolist())].sum()

        flow_group_d = flow.groupby('trans_date').agg({
            'account_balance': func1,
            'trans_amt': func2
        })
        # account_detail = temp_flow.merge(flow_group_d, how='left', left_index=True, right_index=True)
        account_detail_d = flow_group_d.reset_index()
        account_detail_d['account_balance'].fillna(method='ffill', inplace=True)
        account_detail_d['trans_amt'].fillna(0, inplace=True)
        # account_detail_d = account_detail_d.reset_index().rename(columns={'index': 'trans_date'})
        account_detail_d['trans_date'] = account_detail_d['trans_date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        # 处理月度余额
        flow_group_m = account_detail_d.copy()
        # 将月份处理为YYYY-MM-01格式
        flow_group_m['trans_month'] = flow_group_m['trans_date'].apply(lambda x: x[:7] + '-01')
        account_detail_m = flow_group_m.groupby('trans_month').agg({'account_balance': 'last'})
        account_detail_m.reset_index(inplace=True)
        # 月末余额均值
        mean_balance_m = round(account_detail_m['account_balance'].mean() / 10000, 2)

        # 期末余额
        last_balance = round(account_detail_d['account_balance'].iloc[-1] / 10000, 2)
        # 日末余额均值
        mean_balance_d = round(account_detail_d['account_balance'].mean() / 10000, 2)
        # 理财规模变动
        financial_scale = round(account_detail_d['trans_amt'].sum() / 10000, 2)

        account_detail_d['account_balance'] = (account_detail_d['account_balance'] / 10000).round(2)
        account_detail_m['account_balance'] = (account_detail_m['account_balance'] / 10000).round(2)
        account_detail_d['trans_amt'] = (account_detail_d['trans_amt'] / 10000).round(2)
        return last_balance, mean_balance_d, mean_balance_m, financial_scale, account_detail_d.to_dict(
            orient='records'), account_detail_m.to_dict(orient='records')
