import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df


class JsonSingleRemarkPortrait(TransFlow):

    def process(self):
        self.read_single_remark_pt()

    def read_single_remark_pt(self):
        sql = """
            select *
            from trans_single_remark_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"account_id": self.account_id,
                               "report_req_no":self.reqno})
        if df.empty:
            return

        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],
                                    inplace = True)
        # income_order_max = df.remark_income_amt_order.max()
        # expense_order_max = df.remark_expense_amt_order.max()

        income_df = df[pd.notnull(df['remark_income_amt_order'])].drop(columns='remark_expense_amt_order')
        income_df.rename(columns={'remark_income_amt_order': 'order'}, inplace=True)
        json1 = income_df.to_json(orient = 'records').encode('utf-8').decode("unicode_escape")

        expense_df = df[pd.notnull(df['remark_expense_amt_order'])].drop(columns='remark_income_amt_order')
        expense_df.rename(columns={'remark_expense_amt_order': 'order'}, inplace=True)
        json2 = expense_df.to_json(orient = 'records').encode('utf-8').decode("unicode_escape")

        json_str = "{\"remark_income\":" + json1 + ",\"remark_expense\":" + json2 + "}"

        self.variables["trans_single_remark_portrait"] = json.loads(json_str)