import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df

class JsonUnionRemarkTransDetail(TransFlow):

    def process(self):
        self.read_u_remark_trans_in_u_flow()

    def connect_json( self,json ):
        string = ''
        for text in json :
            string += text
        return string[:-1]

    def read_u_remark_trans_in_u_flow(self):
        sql1 = """
            select bank,account_no,concat(trans_date," ",trans_time) as trans_time,opponent_name,trans_amt,remark
            from trans_u_flow_portrait
            where report_req_no = %(report_req_no)s
        """
        flow_df = sql_to_df(sql=sql1,
                            params={"report_req_no": self.reqno})

        flow_df['account_no'] = flow_df['account_no'].fillna("").astype(str)
        flow_df['account_no'] = flow_df['account_no'].apply( lambda x: self.flow_account_clean(x))
        flow_df['trans_time'] = flow_df['trans_time'].astype(str)
        flow_df['trans_time'] = flow_df.trans_time.apply(lambda x: x.replace(" 00:00:00" , ""))


        sql2 = """
            select *
            from trans_u_remark_portrait
            where report_req_no = %(report_req_no)s
        """
        remark_portrait = sql_to_df(sql=sql2,
                                    params={"report_req_no": self.reqno})
        if flow_df.empty or remark_portrait.empty:
            return
        remark_portrait.drop(columns=['id', 'apply_no', 'report_req_no', 'create_time', 'update_time'],
                             inplace=True)
        remark_income_dict = remark_portrait[pd.notnull(remark_portrait.remark_income_amt_order)] \
            [['remark_income_amt_order', 'remark_type']]. \
            set_index('remark_income_amt_order')['remark_type'].to_dict()

        json1 = []
        for i in remark_income_dict:
            # order列应为int
            # i = int(i)
            temp_df = flow_df[(flow_df.remark == remark_income_dict[i]) & (flow_df.trans_amt > 0)]. \
                rename(columns={'opponent_name': 'oppo_name'})
            temp_df['trans_amt'] = temp_df.trans_amt.apply(lambda x: '%.2f' % x)
            key = str(i).replace(".", "_")
            json1.append((f"\"{key}\"" + ":" + temp_df.to_json(orient='records')) + ",")

        json_1 = self.connect_json(json1).encode('utf-8').decode("unicode_escape")

        remark_expense_dict = remark_portrait[pd.notnull(remark_portrait.remark_expense_amt_order)] \
            [['remark_expense_amt_order', 'remark_type']]. \
            set_index('remark_expense_amt_order')['remark_type'].to_dict()
        json2 = []
        for j in remark_expense_dict:
            # order列应为int
            # j = int(j)
            temp_df = flow_df[(flow_df.remark == remark_expense_dict[j]) & (flow_df.trans_amt < 0)]. \
                rename(columns={'opponent_name': 'oppo_name'})
            temp_df['trans_amt'] = temp_df.trans_amt.apply(lambda x: '%.2f' % x)
            key = str(j).replace(".", "_")
            json2.append((f"\"{key}\"" + ":" + temp_df.to_json(orient='records')) + ",")

        json_2 = self.connect_json(json2).encode('utf-8').decode("unicode_escape")

        self.variables["交易对手明细"] = json.loads(
            "{\"remark_income_amt_order\":{" + json_1 + "},\"remark_expense_amt_order\":{" + json_2 + "}}")