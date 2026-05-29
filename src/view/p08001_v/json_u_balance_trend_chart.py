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
        # 缓存日期转换结果（trans_date 已是 datetime-like 类型，但统一转一次供复用）
        trans_date_dt = pd.to_datetime(df['trans_date'])
        year_ago = trans_date_dt.max() - DateOffset(months=12)
        # 筛选近一年数据（向量化，利用缓存结果）
        df = df.loc[trans_date_dt >= year_ago].copy()
        if df.shape[0] == 0:
            return pd.DataFrame()
        # 新增交易年-月列
        df['trans_month'] = df['trans_date'].apply(lambda x: x.strftime('%Y-%m'))
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

        account_id_list = df.account_id.unique().tolist()
        account_info = self.get_trans_account_info(account_id_list)
        account_info.rename(columns = {'id':'account_id'}, inplace = True)
        # 将账户信息 merge 进 df，一次完成，避免循环内重复过滤
        df = df.merge(account_info, on='account_id', how='left')

        # 用 groupby 替代嵌套循环，pandas C 级分组比 Python 级循环快得多
        for (name, account_no), group_df in df.groupby(['account_name', 'account_no']):
            last_balance, mean_balance_d, mean_balance_m, financial_scale, account_detail_d, account_detail_m = \
                self.get_account_balance_trend(group_df)
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

        flow = account_df.copy()
        # 微信和支付宝，没有余额，存在account_balance为空的情况
        flow['account_balance'].fillna(0, inplace=True)

        # 日余额：取每天最后一笔的 account_balance（向量化 last，比自定义 func1 快）
        daily_balance = flow.groupby('trans_date')['account_balance'].last()

        # 日理财金额：逐日汇总 usual_trans_type 包含"理财"的交易金额
        # 关键优化：str.contains('理财') 只计算一次，而不是在每个 group 里重复全表扫描
        finance_mask = flow['usual_trans_type'].str.contains('理财', na=False)
        daily_finance = flow[finance_mask].groupby('trans_date')['trans_amt'].sum()

        flow_group_d = pd.DataFrame({'account_balance': daily_balance, 'trans_amt': daily_finance})
        flow_group_d['trans_amt'] = flow_group_d['trans_amt'].fillna(0)

        account_detail_d = flow_group_d.reset_index()
        account_detail_d['account_balance'].fillna(method='ffill', inplace=True)
        account_detail_d['trans_amt'].fillna(0, inplace=True)
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
