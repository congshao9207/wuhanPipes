import pandas as pd
from view.TransFlow import TransFlow
from pandas import DateOffset
import numpy as np
from pandas.tseries.offsets import MonthEnd


class JsonULabelTotal(TransFlow):

    def process(self):
        self.variables['label_flow_change'] = {
            "income": {},
            "expense": {}
        }
        self.label_total_trans()

    def label_total_trans(self):
        """
        流水变动分析
        :return:
        """
        df = self.trans_u_flow_portrait.copy()
        # 新增交易年-月列
        df['year_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
        # 如果为None替换成空字符串
        df['mutual_exclusion_label'] = df['mutual_exclusion_label'].fillna('')
        # 标签映射字典
        label_dict = df[['mutual_exclusion_label', 'label1']].drop_duplicates().set_index('mutual_exclusion_label')[
            'label1'].to_dict()
        # month_df = pd.DataFrame({'year_month': [format(x, '%Y-%m') for x in pd.date_range(
        #     df['trans_time'].min(), df['trans_time'].max(), freq='M')]})
        full_date_range = [format(x, '%Y-%m') for x in
                   pd.date_range(df['trans_time'].min(), df['trans_time'].max() + MonthEnd(2), freq='M')]

        temp_df = (df.reset_index()
                  .pivot_table(index='year_month', columns='mutual_exclusion_label', values='trans_amt', aggfunc='sum', fill_value=0)
                  .reindex(full_date_range, fill_value=0)
                  .sort_index(axis=1)).reset_index()

        for io_type in ['income', 'expense']:
            all_io_amt = df[df['trans_amt'] > 0]['trans_amt'].sum() \
                if io_type == 'income' else df[df['trans_amt'] < 0]['trans_amt'].sum()
            io_label_code = '01' if io_type == 'income' else '02'
            label_code_li = [i for i in list(label_dict.keys()) if i[2:4] == io_label_code]
            if len(label_code_li) == 0:
                continue
            for label_code in label_code_li:
                trans_label_dict = {}
                label_df = temp_df[['year_month', label_code]]
                label_df.columns = ['year_month', 'trans_amt']
                label_name = label_dict[label_code]
                trans_mean = label_df['trans_amt'].mean()
                trans_amt_proportion = label_df['trans_amt'].sum() / all_io_amt
                trans_detail = label_df[['year_month','trans_amt']].to_dict('records')

                trans_label_dict['label_name'] = label_name
                trans_label_dict['trans_mean'] = trans_mean
                trans_label_dict['trans_amt_proportion'] = np.round(trans_amt_proportion, 4)
                trans_label_dict['trans_detail'] = trans_detail

                self.variables['label_flow_change'][io_type][label_code] = trans_label_dict

