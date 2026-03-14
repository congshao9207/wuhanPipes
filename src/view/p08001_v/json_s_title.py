import json
# import locale
from util.mysql_reader import sql_to_df
from view.TransFlow import TransFlow


class JsonSingleTitle(TransFlow):

    def process(self):
        self.create_s_title()

    def create_s_title(self):
        # locale.setlocale(locale.LC_CTYPE, 'chinese')
        sql1 = '''
            SELECT  min(trans_date) as start_time , max(trans_date) as end_time
            from 
            (
            SELECT trans_date
            FROM  trans_flow_portrait  
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s ) aaa
        '''

        df1 = sql_to_df(sql=sql1,
                       params={"account_id":self.account_id,
                               "report_req_no":self.reqno})

        if not df1.empty:
            start_end_date = df1.at[0,'start_time'].strftime('%Y/%m/%d') \
                             + "—" + df1.at[0,'end_time'].strftime('%Y/%m/%d')
        else:
            start_end_date = "error"

        sql2 = '''
            select related_name as name , relationship as relation
            from trans_apply
            where report_req_no = %(report_req_no)s
            and relationship is not null
        '''
        df2 = sql_to_df(sql=sql2,
                        params={"report_req_no": self.reqno}).drop_duplicates()

        if not df2.empty:
            relation_json = df2.to_json(orient='records').encode('utf-8').decode("unicode_escape")
        else:
            relation_json = "[]"

        json_str = "{\"cusName\":\"" + self.cusName  \
                               + "\",\"流水信息\":[{\"bankName\":\"" + self.bankName \
                               + "\",\"bankAccount\":\"" + self.bankAccount \
                               + "\",\"startEndDate\":\"" + start_end_date + "\"}],"  \
                                + "\"关联人\":" + relation_json + "}"

        self.variables["表头"] = json.loads(json_str)
