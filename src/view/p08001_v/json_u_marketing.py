import json

from view.TransFlow import TransFlow
import pandas as pd


class JsonUnionMarketing(TransFlow):

    def process(self):
        self.read_u_marketing_in_u_flow()

    @staticmethod
    def create_json(df, oppo_type, order):
        if order == 'income_cnt_order':
            df1 = df[(df.opponent_type == oppo_type)
                     & (pd.notnull(df[order]))
                     & (df.trans_amt > 0)
                     & (pd.isna(df.relationship))][[order, 'opponent_name', 'trans_amt', 'phone']]
        else:
            df1 = df[(df.opponent_type == oppo_type)
                     & (pd.notnull(df[order]))
                     & (df.trans_amt < 0)
                     & (pd.isna(df.relationship))][[order, 'opponent_name', 'trans_amt', 'phone']]

        df1 = df1.groupby([order, 'opponent_name'])['trans_amt'] \
            .agg(['count', 'sum']).reset_index() \
            .rename(columns={'count': 'trans_cnt', 'sum': 'trans_amt'})

        if not df1.empty:
            df1 = df1.sort_values(by=order, ascending=True)
            if 'expense' in order:
                df1.sort_values(by=['trans_cnt', 'trans_amt'], axis=0, ascending=[False, True], inplace=True)
            else:
                df1.sort_values(by=['trans_cnt', 'trans_amt'], axis=0, ascending=[False, False], inplace=True)
            df1 = df1[abs(df1.trans_amt) >= 100000]
            df1[order] = list(range(1, df1.shape[0] + 1))
            return df1
        else:
            return pd.DataFrame(columns=[order, 'opponent_name', 'trans_cnt', 'trans_amt'])

    @staticmethod
    def _remove_duplicate(df1, df2):
        union_name1 = set(df1.opponent_name.tolist()).intersection(set(df2.opponent_name.tolist()))
        for name in union_name1:
            amt1 = abs(df1[df1.opponent_name == name]['trans_amt'].sum())
            amt2 = abs(df2[df2.opponent_name == name]['trans_amt'].sum())
            if amt1 > amt2:
                df2.drop(df2[df2.opponent_name == name].index.tolist(), axis=0, inplace=True)
            else:
                df1.drop(df1[df1.opponent_name == name].index.tolist(), axis=0, inplace=True)
        df1.reset_index(drop=True, inplace=True)
        df1['income_cnt_order'] = df1.index + 1
        df1 = df1[df1['income_cnt_order'] <= 10]
        df2.reset_index(drop=True, inplace=True)
        df2['expense_cnt_order'] = df2.index + 1
        df2 = df2[df2['expense_cnt_order'] <= 10]
        return df1, df2

    def read_u_marketing_in_u_flow(self):
        df = self.trans_u_flow_portrait.copy()
        df['phone'] = None
        df1 = self.create_json(df, 1, 'income_cnt_order')
        df2 = self.create_json(df, 1, 'expense_cnt_order')
        df3 = self.create_json(df, 2, 'income_cnt_order')
        df4 = self.create_json(df, 2, 'expense_cnt_order')

        df1, df2 = self._remove_duplicate(df1, df2)
        df3, df4 = self._remove_duplicate(df3, df4)

        json1 = "\"对私进账\":" + df1.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ","
        json2 = "\"对私出账\":" + df2.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ","
        json3 = "\"对公进账\":" + df3.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ","
        json4 = "\"对公出账\":" + df4.to_json(orient='records').encode('utf-8').decode("unicode_escape")

        self.variables["营销反哺"] = json.loads("{" + json1 + json3 + json2 + json4 + "}")
        if df1.shape[0] + df2.shape[0] + df3.shape[0] + df4.shape[0] > 0:
            self.variables['trans_report_overview']['marketing_feedback']['risk_tips'] = \
                f"潜在营销客户{df1.shape[0] + df2.shape[0] + df3.shape[0] + df4.shape[0]}个"
