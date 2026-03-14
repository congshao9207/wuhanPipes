import json
from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df


class JsonSingleMarketing(TransFlow):

    def process(self):
        self.read_single_marketing_in_flow()

    def create_json(self, oppo_type, order):
        sql = """
            select *
            from trans_flow_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"account_id": self.account_id,
                               "report_req_no":self.reqno})
        df1 = None
        if order == 'income_cnt_order':
            df1 = df[(df.opponent_type == oppo_type)
                     & (pd.notnull(df[order]))
                     & (df.trans_amt > 0)
                     & (pd.isna(df.relationship))][[order,
                                                    'opponent_name',
                                                    'trans_amt',
                                                    'phone']]
        else:
            df1 = df[(df.opponent_type == oppo_type)
                     & (pd.notnull(df[order]))
                     & (df.trans_amt < 0)
                     & (pd.isna(df.relationship))][[order,
                                                    'opponent_name',
                                                    'trans_amt',
                                                    'phone']]
            
        df1_1 = df1.groupby([order, 'opponent_name'])['trans_amt'] \
            .agg(['count', 'sum']).reset_index() \
            .rename(columns={'count': 'trans_cnt', 'sum': 'trans_amt'})

        df1_2 = df1[[order, 'opponent_name', 'phone']].drop_duplicates() \
            .groupby([order, 'opponent_name'])['phone'] \
            .apply(lambda x: x.str.cat(sep=';')).reset_index()

        if not df1_1.empty and not df1_2.empty:
            df1 = pd.merge(df1_1, df1_2, how='left', on=[order, 'opponent_name']).sort_values( by=order,
                                                                                               ascending= True)
            # order列 直接是 varchar
            # df1[order] = df1[order].apply(lambda x: "No." + x  )
            # order列 是int
            # df1[order] = df1[order].apply(lambda x: "No." + str(x)[0])
            df1[order] = list(range(1, df1.shape[0] + 1))
            return df1.to_json(orient='records').encode('utf-8').decode("unicode_escape")
        return '[]'


    def read_single_marketing_in_flow(self):
        json1 = "\"对私进账\":" + self.create_json(oppo_type=1, order='income_cnt_order') + ","
        json3 = "\"对私出账\":" + self.create_json(oppo_type=1, order='expense_cnt_order') + ","
        json2 = "\"对公进账\":" + self.create_json(oppo_type=2, order='income_cnt_order') + ","
        json4 = "\"对公出账\":" + self.create_json(oppo_type=2, order='expense_cnt_order')

        json_str = "{" + json1 + json2 + json3 + json4 + "}"
        self.variables["营销反哺"] = json.loads(json_str)

