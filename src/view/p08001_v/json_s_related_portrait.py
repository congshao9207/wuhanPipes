import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonSingleRelatedPortrait(TransFlow):

    def process(self):
        self.read_single_related_pt()

    def read_single_related_pt(self):
        sql = """
            select *
            from trans_single_related_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"account_id": self.account_id,
                               "report_req_no":self.reqno})

        if df.empty:
            return

        df.drop(columns = ['id','account_id','report_req_no',
                           'income_cnt_order','income_amt_order',
                           'expense_cnt_order','expense_amt_order',
                           'create_time','update_time'],
                inplace = True)

        self.variables["trans_single_related_portrait"] = json.loads(df.to_json(orient='records')
                                                                     .encode('utf-8')
                                                                     .decode("unicode_escape"))
