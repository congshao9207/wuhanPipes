import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df


class JsonUnionGuarantor(TransFlow):
    """
        第三方担保交易信息
        author:岳帅
        created_time:20220520
        updated_time_v2:
    """

    def process(self):
        self.read_guarantor_in_u_flow()

    def create_guarantor_json(self, guarantor):
        sql = """
            select concat(trans_date," ",trans_time) as trans_time,
            opponent_name,trans_amt,remark,is_before_interest_repay,
            income_amt_order,expense_amt_order,income_cnt_order,expense_cnt_order
            from trans_u_flow_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})
        df = df[df.opponent_name == guarantor]
        df.rename(columns={'opponent_name': 'guarantor'}, inplace=True)

        json1 = "\"流水\":" + df[['guarantor', 'trans_amt',
                                'trans_time', 'remark',
                                'is_before_interest_repay']].to_json(orient='records').encode('utf-8').decode(
            "unicode_escape") + ","

        income_df = df[df.trans_amt > 0][['guarantor', 'trans_amt']]
        expense_df = df[df.trans_amt < 0][['guarantor', 'trans_amt']]

        income_df = income_df.groupby(['guarantor'])['trans_amt'].agg(['count', 'sum']) \
            .reset_index().rename(columns={'count': 'income_cnt', 'sum': 'income_amt'})
        expense_df = expense_df.groupby(['guarantor'])['trans_amt'].agg(['count', 'sum']) \
            .reset_index().rename(columns={'count': 'expense_cnt', 'sum': 'expense_amt'})

        df_ = pd.merge(income_df, expense_df, how='left', on='guarantor')

        df_ = pd.merge(df_, df[['guarantor', 'income_amt_order', 'expense_amt_order',
                                'income_cnt_order', 'expense_cnt_order']].drop_duplicates(),
                       how='left', on='guarantor')
        if df_.empty:
            json2 = "\"提示\":{}"
        else:
            json2 = "\"提示\":" + df_.to_json(orient='records').encode('utf-8').decode("unicode_escape")[1:-1]

        return "{" + json1 + json2 + "},"

    # 第三方担保交易信息
    def get_guarantor_trans_detail(self, guarantor_list):
        trans_detail = []
        sql1 = """
                   select concat(trans_date," ",trans_time) as trans_time,
                   opponent_name,trans_amt,remark,is_before_interest_repay,
                   income_amt_order,expense_amt_order,income_cnt_order,expense_cnt_order
                   from trans_u_flow_portrait
                   where report_req_no = %(report_req_no)s
               """
        df = sql_to_df(sql=sql1,
                       params={"report_req_no": self.reqno})

        sql2 = """
                   select report_req_no,normal_income_amt from trans_u_portrait
                   where report_req_no = %(report_req_no)s

               """
        df2 = sql_to_df(sql=sql2,
                        params={"report_req_no": self.reqno})
        normal_income_amt = None
        if not df2.empty:
            normal_income_amt = df2['normal_income_amt'].tolist()[0]

        for guarantor in guarantor_list:
            df_temp = df[df['opponent_name'] == guarantor]
            if not df_temp.empty:
                trans_amt = df_temp['trans_amt'].values[0]
                trans_time = df_temp['trans_time'].values[0]
                is_before_interest_repay = df_temp['is_before_interest_repay'].values[0]
                remark = df_temp['remark'].values[0]
                temp_dict = dict(zip(['guarantor', 'trans_amt', 'trans_time', 'is_before_interest_repay',
                                      'remark'],
                                     [guarantor, trans_amt, trans_time, is_before_interest_repay, remark]))
                trans_detail.append(temp_dict)
        return trans_detail, normal_income_amt

    # 专家经验清洗逻辑
    def get_guarantor_risk_tips_by_trans_detail(self, trans_detail, normal_income_amt):
        temp_list = list()
        income_cnt, expense_cnt = 0, 0
        for temp_dict in trans_detail:
            income_amt = -999
            expense_amt = -999

            trans_amt = temp_dict['trans_amt']
            if trans_amt >= 0:
                income_amt = trans_amt
                income_cnt += 1
            else:
                expense_amt = trans_amt
                expense_cnt += 1
            guarantor = temp_dict['guarantor']
            if income_amt >= normal_income_amt * 0.3:
                temp_list.append("申请人与担保人" + guarantor + "之间交易紧密，担保人" + guarantor + "可能是申请人的下游客户")
            elif expense_amt >= normal_income_amt * 0.3:
                temp_list.append("申请人与担保人" + guarantor + "之间交易紧密，担保人" + guarantor + "可能是申请人的上游客户")
            elif (income_cnt >= 5) and (income_amt >= 500000) and (expense_cnt >= 5) and (expense_amt >= 500000):
                temp_list.append("申请人与担保人" + guarantor + "之间交易紧密，两者存在高频的资金借调情况")
        temp_list = list(set(temp_list))
        risk_tips = ';'.join(temp_list)
        return risk_tips

    def read_guarantor_in_u_flow(self):
        # 获取第三方担保交易信息
        # 所有担保人
        guarantor_list = self.guarantor_list

        if guarantor_list is not None and len(guarantor_list) > 0:
            trans_detail, normal_income_amt = self.get_guarantor_trans_detail(guarantor_list)
            # 专家经验
            if normal_income_amt is not None:
                risk_tips = self.get_guarantor_risk_tips_by_trans_detail(trans_detail, normal_income_amt)
            else:
                risk_tips = ""
        else:
            trans_detail = []
            risk_tips = ""
        # 此处为第三方担保交易返回数据
        self.variables['third_party_guarantee_info'] = {
            "risk_tips": risk_tips,
            "trans_detail": trans_detail
        }
        self.variables["trans_report_overview"]["related_info"]["guarantor_info"][
            "risk_tips"] = risk_tips
