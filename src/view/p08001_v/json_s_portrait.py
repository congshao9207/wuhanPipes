import json

import pandas as pd

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonSinglePortrait(TransFlow):

    def process(self):
        self.read_single_pt()

    def read_single_pt(self):
        sql = """
            select * 
            from trans_single_portrait
            where account_id = %(account_id)s and report_req_no=%(report_req_no)s
            """

        df = sql_to_df(sql=sql,
                       params={"account_id": self.account_id, "report_req_no": self.reqno})
        if df.empty:
            return
        df.drop(columns=['id', 'account_id', 'create_time', 'update_time'], inplace=True)

        df['analyse_start_time'] = df['analyse_start_time'].astype(str)
        df['analyse_end_time'] = df['analyse_end_time'].astype(str)

        sql1 = '''
            select report_req_no,min(trans_date) as loan_anal_start_time
            from trans_flow_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        '''
        df1 = sql_to_df(sql=sql1,
                        params={"account_id": self.account_id, "report_req_no": self.reqno})

        df1['loan_anal_start_time'] = df1['loan_anal_start_time'].astype(str)

        df = pd.merge(df,df1,how='left', on='report_req_no' ).drop(columns = 'report_req_no')

        json_str = df.to_json(orient='records')
        data = json.loads(json_str)
        self.variables["trans_single_portrait"] = data[0]


