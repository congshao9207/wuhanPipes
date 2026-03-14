import pandas as pd
from view.TransFlow import TransFlow
from pandas import DateOffset


class JsonUTransFrequencyDetail(TransFlow):

    def process(self):
        self.variables['trans_frequency_detail'] = {
            "trans_frequency_d": {
                "income_detail": {},
                "expense_detail": {}
            },
            "trans_frequency_m": {
                "income_detail": {},
                "expense_detail": {}
            }
        }
        self.process_trans_frequency()

    def process_trans_frequency(self):
        """
        处理交易频率
        :return:
        """
        df = self.trans_u_flow_portrait.copy()
        # 获取一年前的日期
        year_ago = pd.to_datetime(df['trans_date']).max() - DateOffset(months=12)
        # 新增交易年-月列
        df['trans_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
        # 筛选近一年经营性数据
        df = df.loc[(pd.to_datetime(df.trans_date) >= year_ago)
                    & pd.isna(df.loan_type) & pd.isna(df.unusual_trans_type) & pd.isna(df.relationship)]
        if df.shape[0] == 0:
            return
        trans_frequency_d = self._get_trans_frequency_detail(df, 'trans_date')
        trans_frequency_m = self._get_trans_frequency_detail(df, 'trans_month')
        self.variables['trans_frequency_detail']['trans_frequency_d'] = trans_frequency_d
        self.variables['trans_frequency_detail']['trans_frequency_m'] = trans_frequency_m

    @staticmethod
    def _get_trans_frequency_detail(df, trans_frequency=None):
        """
        获取交易频率明细
        :param trans_frequency:
        :return:
        """
        trans_amt_type = ['income_detail', 'expense_detail']
        detail = {}
        for amt_type in trans_amt_type:
            if amt_type == 'income_detail':
                detail_df = df.loc[df.trans_amt > 0]
            else:
                detail_df = df.loc[df.trans_amt < 0]
            if detail_df.shape[0] > 0:
                df_group = detail_df.groupby(trans_frequency).agg({'trans_amt': 'count'})
                df_group.reset_index(inplace=True)
                df_group.rename(columns={'trans_amt': 'trans_cnt'}, inplace=True)
                # 将日期或月份填充
                if trans_frequency == 'trans_date':
                    frequency_range = pd.date_range(detail_df[trans_frequency].min(), detail_df[trans_frequency].max())
                    # frequency_range = frequency_range.strftime('%Y-%m-%d')
                else:
                    start_date = detail_df[trans_frequency].min()
                    end_date = detail_df[trans_frequency].max()
                    end_date_next_month = pd.to_datetime(end_date) + DateOffset(months=1)
                    frequency_range = pd.date_range(start_date, end_date_next_month, freq='M')
                    frequency_range = frequency_range.strftime('%Y-%m')
                temp_df = pd.DataFrame(index=frequency_range)
                df_group.index = df_group[trans_frequency]
                df_group = temp_df.merge(df_group[['trans_cnt']], how='left', left_index=True, right_index=True)
                # 笔数填充为空值
                df_group.fillna(0, inplace=True)
                df_group = df_group.reset_index().rename(columns={'index': trans_frequency})
                # 将trans_date转换为字符串
                df_group[trans_frequency] = df_group[trans_frequency].apply(lambda x: x.strftime('%Y-%m-%d')) if (
                        trans_frequency == 'trans_date') else df_group[trans_frequency]
                # 计算交易频率均值、最大、最小
                trans_cnt_mean = df_group['trans_cnt'].mean()
                trans_cnt_max = df_group['trans_cnt'].max()
                trans_cnt_min = df_group['trans_cnt'].min()
                detail[amt_type] = {
                    "trans_cnt_mean": int(round(trans_cnt_mean, 0)),
                    "trans_cnt_max": int(trans_cnt_max),
                    "trans_cnt_min": int(trans_cnt_min),
                    "trans_cnt_detail": df_group.to_dict(orient='records')
                }
            else:
                detail[amt_type] = {}
        return detail
