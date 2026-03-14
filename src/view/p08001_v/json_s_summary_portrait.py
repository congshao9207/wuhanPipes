import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonSingleSummaryPortrait(TransFlow):

    def process(self):
        self.read_single_summary_pt()

    def read_single_summary_pt(self):

        sql = """
        select *
        from trans_single_summary_portrait
        where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"account_id":self.account_id,
                               "report_req_no":self.reqno})
        if df.empty:
            return

        df.drop(columns=['id', 'account_id', 'report_req_no', 'q_1_year', 'q_2_year',
                         'q_3_year', 'q_4_year', 'create_time', 'update_time'],
                inplace=True)

        # col_list = df.columns.tolist()
        # month_list = list(map(str, list(range(1,14)))) + \
        #              ['quarter1','quarter2','quarter3','quarter4','half_year','year']
        #
        # for m in month_list:
        #     temp_df = df[df.month == m]
        #     if temp_df.empty:
        self.variables["trans_single_summary_portrait"] = json.loads(df.to_json(orient='records'))
