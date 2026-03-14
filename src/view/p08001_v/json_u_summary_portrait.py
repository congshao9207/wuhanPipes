import pandas as pd
from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df

COL_NAME = ["month", "mean_interest_last", "mean_balance_last", "interest_amt", "balance_amt",
            "mean_interest_rise_rate", "mean_balance_rise_rate", "interestBalProps"]
# 具体 年-月, half_year, year模式列表
CONTAINS_STR_LIST = ['-03|quarter1', '-06|quarter2', '-09|quarter3', '-12|quarter4', 'half', 'year']


def get_u_summary_risk_tips(trans_detail):
    """
    返回专家经验整体为1-2条, 逻辑上不返回空
    """
    temp_list = list()
    overview_tips = ""
    guide_tips = ""
    for temp_dict in trans_detail:
        if temp_dict['month'].isnumeric():
            continue
        if pd.notna(temp_dict['interestBalProps']) and ('*' not in temp_dict['month']) and \
                ((temp_dict['interestBalProps'] < 0.75) or (temp_dict['interestBalProps'] > 1.25)):
            temp_list.append('结息日均与余额日均差别较大，请核实客户是否存在理财账户')
        if 'half_year' in temp_dict['month']:
            mean_interest = temp_dict['interest_amt']
            if pd.notna(mean_interest):
                overview_tips = f"客户近半年结息日均{mean_interest / 10000:.2f}万元"
                if mean_interest >= 200000:
                    guide_tips = '营销建议：客户近半年结息日均较大，可对客户进行结算户转移或存款营销'
    temp_list = list(set(temp_list))
    risk_tips = ';'.join(temp_list)
    return risk_tips, overview_tips, guide_tips


class JsonUnionSummaryPortrait(TransFlow):
    """
        结息日均和余额日均模块
        author:雷宇航
        created_time:20220520
        updated_time_v2:
    """

    def process(self):
        self.read_u_summary_pt()

    def get_u_summary_detail(self):
        trans_detail = []
        sql = """
                    select *
                    from trans_u_summary_portrait
                    where report_req_no = %(report_req_no)s
                """
        df = sql_to_df(sql=sql, params={"report_req_no": self.reqno})  # 当前single_summary信息
        if df.empty:  # 未取到流水
            return trans_detail
        df.drop(['id', 'apply_no', 'report_req_no', 'q_1_year', 'q_2_year', 'q_3_year', 'q_4_year', 'create_time',
                 'update_time'], axis=1, inplace=True)
        # 添加月份信息
        for _ in df[df['month'].str.isnumeric()]['month']:
            value_df = df[df['month'] == str(_)]
            dict_temp = value_df.to_dict(orient='records')[0]
            dict_temp['interest_amt'] = None if pd.isna(dict_temp['interest_amt']) else dict_temp['interest_amt']
            dict_temp['balance_amt'] = None if pd.isna(dict_temp['balance_amt']) else dict_temp['balance_amt']
            dict_temp['interest_balance_proportion'] = None if pd.isna(dict_temp['interest_balance_proportion']) else \
                dict_temp['interest_balance_proportion']

            trans_detail.append(dict_temp)

        # 续贷
        df = df[~df['month'].astype(str).str.isnumeric()][-6:]
        self.previous_out_apply_no = None
        if pd.notna(self.previous_out_apply_no):
            t_apply_acc_id_sql = """
                        select account_id 
                        from trans_apply
                        where apply_no = %(previous_out_apply_no)s and account_id != ''
                        """
            single_df_last_sql = """
                        select * 
                        from trans_single_summary_portrait
                        where account_id = %(account_id)s
                        """
            acc_id_df = sql_to_df(t_apply_acc_id_sql, params={"previous_out_apply_no": self.previous_out_apply_no})
            if acc_id_df.empty:
                # 若在trans_apply表中未查询到apply_no对应的account_id, 即未查询到历史续贷记录, 返回空表
                last_single_df = pd.DataFrame(columns=['month'])
            else:
                acc_id = int(acc_id_df.values.max())  # 最后续贷的acc_id即为历史画像
                last_single_df = sql_to_df(single_df_last_sql, params={"account_id": acc_id})
                last_single_df.drop_duplicates(subset=['month', 'interest_amt', 'balance_amt'], keep='first',
                                               inplace=True)  # 存在除id以外，完全相同的数据
            for contains_str in CONTAINS_STR_LIST:
                # 遍历时间模式列表，匹配前版本、现版本 同一季度的流水
                if contains_str == 'year':
                    last_value_df = last_single_df[last_single_df['month'] == contains_str]
                    value_df = df[df['month'] == contains_str]
                else:
                    last_value_df = last_single_df[last_single_df['month'].str.contains(contains_str)]
                    value_df = df[df['month'].str.contains(contains_str)]
                if value_df.empty:  # 当前季度无结息信息, 跳入下一季度
                    continue
                else:
                    if last_value_df.empty:  # 仅当前结息存在
                        temp_month = value_df['month'].values[0]
                        interest_amt = None if pd.isna(value_df['interest_amt'].values[0]) else \
                            value_df['interest_amt'].values[0]
                        balance_amt = None if pd.isna(value_df['balance_amt'].values[0]) else \
                            value_df['balance_amt'].values[0]
                        interest_balance_proportion = None if pd.isna(
                            value_df['interest_balance_proportion'].values[0]) else \
                            value_df['interest_balance_proportion'].values[0]
                        temp_list = [temp_month, None, None, interest_amt, balance_amt, None, None,
                                     interest_balance_proportion]
                    else:  # 同时存在
                        temp_month = value_df['month'].values[0]
                        last_interest_amt = None if pd.isna(last_value_df['interest_amt'].values[0]) else \
                            last_value_df['interest_amt'].values[0]
                        last_balance_amt = None if pd.isna(last_value_df['balance_amt'].values[0]) else \
                            last_value_df['balance_amt'].values[0]
                        interest_amt = None if pd.isna(value_df['interest_amt'].values[0]) else \
                            value_df['interest_amt'].values[0]
                        balance_amt = None if pd.isna(value_df['balance_amt'].values[0]) else \
                            value_df['balance_amt'].values[0]
                        mean_interest_rise_rate = None \
                            if (last_interest_amt == 0) or (last_interest_amt is None) \
                            else (interest_amt - last_interest_amt) / last_interest_amt
                        mean_balance_rise_rate = None \
                            if (last_balance_amt == 0) or (last_balance_amt is None) \
                            else (balance_amt - last_balance_amt) / last_balance_amt
                        interest_balance_proportion = value_df['interest_balance_proportion'].values[0]
                        temp_list = [temp_month, last_interest_amt, last_balance_amt, interest_amt, balance_amt,
                                     mean_interest_rise_rate, mean_balance_rise_rate, interest_balance_proportion]
                    temp_dict = dict(zip(COL_NAME, temp_list))
                    temp_dict['living_cost_amt'] = None
                    temp_dict['insurance_cost_amt'] = None
                    temp_dict['loan_cost_amt'] = None
                    temp_dict['net_income_amt'] = None
                    temp_dict['normal_expense_amt'] = None
                    temp_dict['normal_income_amt'] = None
                    temp_dict['rent_cost_amt'] = None
                    temp_dict['salary_cost_amt'] = None
                    temp_dict['tax_cost_amt'] = None
                    trans_detail.append(temp_dict)
        else:  # 非续贷
            month_list = df[df['month'].str.contains('-|_|year')]['month'].tolist()
            for _ in month_list:
                interest_amt = None if pd.isna(df[df['month'] == _].interest_amt.values[0]) else \
                    df[df['month'] == _].interest_amt.values[0]
                balance_amt = None if pd.isna(df[df['month'] == _].balance_amt.values[0]) else \
                    df[df['month'] == _].balance_amt.values[0]
                interest_balance_proportion = None if pd.isna(
                    df[df['month'] == _].interest_balance_proportion.values[0]) else \
                    df[df['month'] == _].interest_balance_proportion.values[0]
                temp_list = [_, None, None, interest_amt, balance_amt, None, None, interest_balance_proportion]
                temp_dict = dict(zip(COL_NAME, temp_list))
                temp_dict['living_cost_amt'] = None
                temp_dict['insurance_cost_amt'] = None
                temp_dict['loan_cost_amt'] = None
                temp_dict['net_income_amt'] = None
                temp_dict['normal_expense_amt'] = None
                temp_dict['normal_income_amt'] = None
                temp_dict['rent_cost_amt'] = None
                temp_dict['salary_cost_amt'] = None
                temp_dict['tax_cost_amt'] = None
                trans_detail.append(temp_dict)
        return trans_detail

    def read_u_summary_pt(self):
        # 结息日均和余额日均表单信息
        trans_detail = self.get_u_summary_detail()
        if len(trans_detail) != 0:
            # 结息日均和余额日均专家经验
            risk_tips, overview_tips, guide_tips = get_u_summary_risk_tips(trans_detail)

            self.variables["trans_u_summary_portrait"] = {
                "risk_tips": risk_tips,
                "trans_detail": trans_detail
            }
            self.variables['trans_report_overview']['trans_general_info']['trans_scale'][
                'risk_tips'] = overview_tips
            self.variables['suggestion_and_guide']['business_info']['daily_mean_balance'][
                'risk_tips'] = guide_tips
