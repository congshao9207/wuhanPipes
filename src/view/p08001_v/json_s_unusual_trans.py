import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df

class JsonSingleUnusualTrans(TransFlow):

    def process(self):
        self.read_unusual_in_flow()

    def read_unusual_in_flow(self):
        sql = """
            select concat(trans_date," ",trans_time) as concat_dt,
            trans_date, cast(trans_time as char) as trans_time,
            opponent_name,trans_amt,trans_use,remark,unusual_trans_type
            from trans_flow_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
            and unusual_trans_type is not null
        """
        df = sql_to_df(sql=sql,
                       params={"account_id": self.account_id,
                               "report_req_no":self.reqno})
        if df.empty:
            return

        # df = df[pd.notnull(df.unusual_trans_type)]
        df['concat_dt'] = df['concat_dt'].astype(str)
        df['trans_date'] = df['trans_date'].astype(str)
        df['trans_time'] = df['trans_time'].astype(str)
        if not df.empty:
            df['remark'] = df[['remark','trans_use']].fillna("").apply(
                lambda x : ",".join([x['remark'],x['trans_use']])
                if len(x['trans_use'])>0 and len(x['remark'])>0
                else "".join([x['remark'],x['trans_use']]),
                axis = 1
            )


        if df[df.trans_time == '00:00:00'].shape[0] == df.shape[0]:
            df.drop(columns=['concat_dt','trans_time','trans_use'] , inplace=True)
            df = df.rename(columns={'trans_date': 'trans_time'})
        else:
            df.drop(columns=['trans_date','trans_time','trans_use'] , inplace=True)
            df = df.rename(columns={'concat_dt': 'trans_time'})

        df['trans_amt'] = df.trans_amt.apply(lambda x: '%.2f' % x)

        unusual_dict = {
            "博彩娱乐风险": "博彩娱乐",
            "案件纠纷风险": "案件纠纷",
            "身体健康风险": "医院",
            # "夜间交易风险": "夜间交易",
            "夜间交易风险-不良交易":"夜间不良交易",
            "夜间交易风险-夜间交易":"夜间交易",
            "民间借贷风险": "民间借贷",
            "贷款逾期风险": "逾期",
            "隐形股东风险": "收购",
            "投资风险": "对外投资",
            "经营性负债": "预收款",
            "经营性风险": "分红退股",
            "典当风险": "典当",
            "公安交易风险": "公安",
            "家庭稳定风险": "家庭不稳定",
            "异常金额-大额整进整出": "整进整出",
            "异常金额-偶发大额": "偶发大额",
            "异常金额-大额快进快出": "快进快出",
            "股票投机风险": "股票投机",
            "变现风险": "变现",
            "存在理财": "理财",
            "担保异常": "担保异常",
            "存在代偿": "代偿"
        }

        json_str = ""
        for risk in unusual_dict:
            temp_df = df[df['unusual_trans_type'].str.contains( unusual_dict[risk] )].drop(columns= 'unusual_trans_type')
            if unusual_dict[risk] in ['夜间交易', '逾期', '预收款', '变现']:
                temp_df = pd.DataFrame()
            if unusual_dict[risk] == "民间借贷" and not temp_df.empty:
                temp_df.sort_values(by=['opponent_name', 'trans_time'],
                                    ascending=[True, True],
                                    inplace=True)
            json_str +=  f"\"{risk}\":" + temp_df.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ","

        json_str = "{" + json_str[:-1] + "}"
        self.variables["异常交易风险"] = json.loads(json_str)
