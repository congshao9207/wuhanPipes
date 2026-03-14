import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df

class JsonSingleLoanPortrait(TransFlow):

    def process(self):
        self.read_single_loan_pt()
        self.read_single_loan_trans_detail()

    def connect_json(self,json):
        string = ''
        for text in json:
            string += text
        return string[:-1]

    def read_single_loan_pt(self):
        sql = """
            select * 
            from trans_single_loan_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql = sql,
                       params= {"account_id": self.account_id,
                               "report_req_no":self.reqno})
        if df.empty:
            return

        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],
                    inplace = True)

        loan_type_list = ['消金',
                          '融资租赁',
                          '担保',
                          '保理',
                          '小贷',
                          '银行',
                          '第三方支付',
                          '其他金融',
                          '民间借贷']

        json_list =[]
        for loan in loan_type_list:
            temp_df = df[df.loan_type == loan]

            json_list.append("\"" + loan + "\":" +
                        temp_df.set_index('loan_type').to_json(orient='records').encode('utf-8').decode("unicode_escape")
                        + ",")

        json_str =  "{" + self.connect_json(json_list) + "}"
        self.variables["trans_single_loan_portrait"] =json.loads(json_str)

    def read_single_loan_trans_detail(self):

        sql = """
            select concat(trans_date," ",trans_time) as concat_dt,
            trans_date,cast(trans_time as char) as trans_time,opponent_name,trans_amt,trans_use,remark,loan_type
            from trans_flow_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s and loan_type is not null
        """
        flow_df = sql_to_df(sql=sql,
                            params={"account_id": self.account_id,
                                    "report_req_no": self.reqno}
                            )

        if flow_df.empty:
            return

        flow_df['concat_dt'] = flow_df['concat_dt'].astype(str)
        flow_df['trans_date'] = flow_df['trans_date'].astype(str)
        flow_df['trans_time'] = flow_df['trans_time'].astype(str)
        if not flow_df.empty:
            flow_df['remark'] = flow_df[['remark', 'trans_use']].fillna("").apply(
                lambda x: ",".join([x['remark'], x['trans_use']])
                if len(x['trans_use']) > 0 and len(x['remark']) > 0
                else "".join([x['remark'], x['trans_use']]),
                axis=1
            )

        if flow_df[flow_df.trans_time == '00:00:00'].shape[0] == flow_df.shape[0]:
            flow_df.drop(columns=['concat_dt', 'trans_time','trans_use'], inplace=True)
            flow_df = flow_df.rename(columns={'trans_date': 'trans_time'})
        else:
            flow_df.drop(columns=['trans_date', 'trans_time','trans_use'], inplace=True)
            flow_df = flow_df.rename(columns={'concat_dt': 'trans_time'})

        flow_df['trans_amt'] = flow_df.trans_amt.apply(lambda x: '%.2f' % x)

        json_str = []
        loan_type_list = ['消金',
                          '融资租赁',
                          '担保',
                          '保理',
                          '小贷',
                          '银行',
                          '第三方支付',
                          '其他金融',
                          '民间借贷']
        for loan in loan_type_list:
            temp_df = flow_df[flow_df.loan_type == loan].drop(columns=["loan_type"])
            if loan == "民间借贷":
                temp_df.sort_values(by=['opponent_name', 'trans_time'],
                                    ascending=[True, True],
                                    inplace=True)
            json_str.append("\"" + loan + "\":" +
                            temp_df.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ",")

        self.variables["多头明细"] = json.loads("{" + self.connect_json(json_str) + "}")