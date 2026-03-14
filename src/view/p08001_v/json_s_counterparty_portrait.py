import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df


class JsonSingleCounterpartyPortrait(TransFlow):

    def process(self):
        self.read_single_counterparty_pt()

    def connect_json(self,json):
        string = ''
        for text in json:
            string += text
        return string[:-1]

    def read_single_counterparty_pt(self):
        sql = """
            select *
            from trans_single_counterparty_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
            """
        df = sql_to_df(sql = sql,
                       params= {"account_id":self.account_id,
                               "report_req_no":self.reqno})
        if df.empty:
            return

        sql = """
            SELECT max(cast(month as decimal)) as cm
            FROM `trans_single_counterparty_portrait` 
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s and month not regexp '汇总'
        """
        cm_max = int(sql_to_df(sql=sql,
                           params={"account_id": self.account_id,
                                   "report_req_no": self.reqno}).loc[0,'cm'])
        # cm_max = pd.to_numeric(cm_df['cm']).max()

        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],
                inplace = True)

        income_df = df[pd.notnull(df.income_amt_order)]

        income_order_list = list(map(str, list(range(1,11)))) + \
                     ['前5','前10',
                      '前10%','前20%','前30%','前40%','前50%',
                      '前60%','前70%','前80%','前90%','前100%']
        json1 = []

        for income_order in income_order_list:
            temp_df1 = income_df[income_df.income_amt_order == income_order]
            if income_order in  list(map(str, list(range(1,6)))) + ['前5','前10']:
                cm_temp = pd.DataFrame(data = list(str(i) for i in range(1,cm_max+1)),
                                       columns = ['month'])
                temp_df1 = pd.merge(cm_temp, temp_df1,
                                    how= 'outer',
                                    on = 'month')
                temp_df1['income_amt_order'] = income_order

            json1.append("\"" + income_order +  "\":"   +
                         temp_df1.to_json( orient = 'records').encode('utf-8').decode("unicode_escape") + ",")

        expense_df = df[pd.notnull(df.expense_amt_order)]
        expense_order_list = list(map(str, list(range(1,11))))
        json2 = []

        for expense_order in expense_order_list:
            temp_df2 = expense_df[expense_df.expense_amt_order == expense_order].drop(columns='expense_amt_order')

            json2.append("\"" + expense_order + "\":" +
                         temp_df2.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ",")

        json_str = "{\"income_amt_order\":{" + self.connect_json(json1)\
                                           +"},\"expense_amt_order\":{"  + self.connect_json(json2)  + "}}"

        self.variables["trans_single_counterparty_portrait"] = json.loads(json_str)