from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
from view.p08001_v.trans_report_util import convert_relationship


# 强关联关系和一般关联关系交易分析
class JsonUnionRelatedPortrait(TransFlow):
    """
        强关联关系和一般关联关系交易分析
        author:汪腾飞
        created_time:20220520
        updated_time_v2:
    """

    def process(self):
        self.read_u_related_pt()

    def get_strong_related_trans_detail(self, df):
        strong_related_trans_detail = []
        all_tips_list = []
        opponent_account_no_list = df['opponent_account_no'].unique().tolist()
        for opponent_account_no in opponent_account_no_list:
            tips_list = []
            acc_no_df = df[df['opponent_account_no'] == opponent_account_no]
            total_income_proportion = acc_no_df['income_amt_proportion'].sum()
            if total_income_proportion >= 0.7:
                tips_list.append(
                    f"{opponent_account_no}与强关联关系交易进账金额占比达{total_income_proportion:.1%}，"
                    f"为非主要进账经营流水")
            temp_list = acc_no_df[['opponent_name', 'opponent_account_no', 'income_amt', 'income_amt_proportion',
                                   'income_cnt', 'expense_amt', 'expense_amt_proportion', 'expense_cnt',
                                   'relationship', 'diff_balance']].to_dict('records')
            strong_related_trans_detail.extend(temp_list)
            self_list = acc_no_df[(acc_no_df['temp_name'].str.contains(self.user_name)) &
                                  (acc_no_df['is_in_acc_str'] == 1) &
                                  (((acc_no_df['income_amt_proportion'] >= 0.25) &
                                    (acc_no_df['income_cnt'] >= 10)) |
                                   ((acc_no_df['expense_amt_proportion'] >= 0.25) &
                                    (acc_no_df['expense_cnt'] >= 10)))]['acc_no'].tolist()
            if self_list:
                tips_list.append(f"{opponent_account_no}与借款人尾号为{'、'.join(self_list)}的银行同名账户交易较多，"
                                 f"建议收集相关流水")
            if tips_list:
                all_tips_list.append("，".join(tips_list))
        return strong_related_trans_detail, ";".join(all_tips_list)

    @staticmethod
    def get_generally_related_trans_detail(df):
        generally_related_trans_detail = []
        all_tips_list = []
        opponent_account_no_list = df['opponent_account_no'].unique().tolist()
        for opponent_account_no in opponent_account_no_list:
            acc_no_df = df[df['opponent_account_no'] == opponent_account_no]
            total_income_proportion = acc_no_df['income_amt_proportion'].sum()
            if total_income_proportion >= 0.5:
                all_tips_list.append(f"{opponent_account_no}与一般关联关系交易进账金额占比达{total_income_proportion:.1%}，"
                                     f"可能存在虚增流水现象，请线下核实")
            temp_list = acc_no_df[['opponent_name', 'opponent_account_no', 'income_amt', 'income_amt_proportion',
                                   'income_cnt', 'expense_amt', 'expense_amt_proportion', 'expense_cnt',
                                   'relationship', 'diff_balance']].to_dict('records')
            generally_related_trans_detail.extend(temp_list)
        return generally_related_trans_detail, ";".join(all_tips_list)

    @staticmethod
    def get_guarantor_trans_detail(df):
        guarantor_trans_detail = []
        all_tips_list = []
        opponent_account_no_list = df['opponent_account_no'].unique().tolist()
        for opponent_account_no in opponent_account_no_list:
            acc_no_df = df[df['opponent_account_no'] == opponent_account_no]
            opponent_name_list = acc_no_df['opponent_name'].unique().tolist()
            for name in opponent_name_list:
                df_temp = acc_no_df[acc_no_df['opponent_name'] == name]
                if not df_temp.empty:
                    opponent_account_no = df_temp['opponent_account_no'].values[0].split(";")[0]
                    income_amt = df_temp['income_amt'].sum()
                    income_amt_proportion = df_temp['income_amt_proportion'].sum()
                    income_cnt = int(df_temp['income_cnt'].sum())
                    expense_amt = df_temp['expense_amt'].sum()
                    expense_amt_proportion = df_temp['expense_amt_proportion'].sum()
                    expense_cnt = int(df_temp['expense_cnt'].sum())
                    if income_amt_proportion >= 0.5 and income_amt_proportion >= expense_amt_proportion:
                        all_tips_list.append(
                            f"{opponent_account_no}与担保对象{name}交易进账金额占比达{income_amt_proportion:.1%}，"
                            f"{name}可能是借款人的下游客户")
                    elif expense_amt_proportion >= 0.5 and income_amt_proportion < expense_amt_proportion:
                        all_tips_list.append(
                            f"{opponent_account_no}与担保对象{name}交易出账金额占比达{expense_amt_proportion:.1%}，"
                            f"{name}可能是借款人的上游客户")

                    relationship = df_temp['relationship'].values[0]
                    temp_dict = {"opponent_name": name,
                                 "opponent_account_no": opponent_account_no,
                                 "income_amt": income_amt,
                                 "income_amt_proportion": income_amt_proportion,
                                 "income_cnt": income_cnt,
                                 "expense_amt": expense_amt,
                                 "expense_amt_proportion": expense_amt_proportion,
                                 "expense_cnt": expense_cnt,
                                 "relationship": relationship,
                                 "diff_balance": income_amt + expense_amt
                                 }
                    guarantor_trans_detail.append(temp_dict)
        return guarantor_trans_detail, ";".join(all_tips_list)

    def read_u_related_pt(self):
        # 根据report_req_no 查出本次流水报告请求中所有关联关系的流水宽表信息
        sql1 = """
            select *
            from trans_single_related_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql1,
                       params={"report_req_no": self.reqno})

        # 从df中筛选出强关联关系数据
        # 根据产品编号判断使用哪一种关联关系列表
        product_code = self.origin_data['strategyInputVariables']['product_code']
        strong_list = [
            "U_PERSONAL",
            "U_PER_LG_COMPANY",
            "U_PER_SH_H_COMPANY",
            "U_PER_SH_M_COMPANY",
            "U_PER_SP_PERSONAL",
            "U_PER_SP_LG_COMPANY",
            "U_PER_SP_SH_H_COMPANY",
            "U_PER_SP_SH_M_COMPANY",
            "U_PER_CT_COMPANY",
            "U_PER_SP_CT_COMPANY",
            "U_PER_PARENTS_PERSONAL",
            "U_PER_CHILDREN_PERSONAL",
            "U_PER_COMPANY_SH_PERSONAL",
            "U_PER_COMPANY_FIN_PERSONAL",
            "U_COMPANY",
            "U_COM_LEGAL_PERSONAL",
            "U_COMPANY_SH_H_COMPANY",
            "U_COMPANY_SH_M_COMPANY",
            "U_COM_CT_PERSONAL",
            "U_COM_CT_CT_COMPANY",
            "U_COM_CT_LG_COMPANY",
            "U_COM_CT_SH_H_COMPANY",
            "U_COM_CT_SH_M_COMPANY",
            "U_COM_CT_SP_PERSONAL",
            "U_COM_CT_SP_CT_COMPANY",
            "U_COM_CT_SP_LG_COMPANY",
            "U_COM_CT_SP_SH_H_COMPANY",
            "U_COM_CT_SP_SH_M_COMPANY",
            "U_COM_FIN_PERSONAL",
            'U_PER_REL_PERSONAL',
            'U_PER_REL_NA_PERSONAL',
            'U_PER_REL_COMPANY',
            'U_COM_REL_PERSONAL',
            'U_COM_REL_NA_PERSONAL',
            'U_COM_REL_COMPANY'
        ] if product_code == '08001' else [
            'U_PERSONAL',
            'U_PER_REL_PERSONAL',
            'U_PER_REL_NA_PERSONAL',
            'U_PER_REL_COMPANY',
            'U_COMPANY',
            'U_COM_REL_PERSONAL',
            'U_COM_REL_NA_PERSONAL',
            'U_COM_REL_COMPANY'
        ]
        if df.shape[0] == 0:
            return
        df['acc_no'] = df['opponent_account_no'].apply(lambda x: x.split(";")[-1] if ";" in x else "空账号")
        df['opponent_account_no'] = df['opponent_account_no'].apply(lambda x: x.split(";")[0])
        df['temp_name'] = df['opponent_name']
        df.sort_values(by='opponent_name', inplace=True)
        df['opponent_name'] = df.apply(
            lambda x: f"{x['temp_name']}（尾号{x['acc_no']}）" if x['acc_no'] != '空账号' else
            f"{x['temp_name']}（{x['acc_no']}）", axis=1)
        df['diff_balance'] = df['income_amt'] + df['expense_amt']
        all_account_str = ';'.join(df['opponent_account_no'].unique().tolist())
        df['is_in_acc_str'] = df['acc_no'].apply(
            lambda x: 1 if x not in all_account_str and x != '空账号' else 0)
        strong_related_df = df[df['relationship'].isin(strong_list)]

        # 强关联关系交易分析表单信息+强关联关系交易分析专家经验
        strong_related_trans_detail, strong_related_risk_tips = \
            self.get_strong_related_trans_detail(strong_related_df)
        all_account_list = set(df['opponent_account_no'].unique().tolist())
        is_strong_list = set(df[df['relationship'].isin(strong_list)]['opponent_account_no'].unique().tolist())
        non_strong_list = list(all_account_list - is_strong_list)
        if non_strong_list:
            strong_related_risk_tips += f";{'、'.join(non_strong_list)}与强关联关系之间无交易，请线下核实"

        # 从df中筛选出一般关联关系数据
        generally_related_df = df[df['relationship'].isin(["U_PER_PARTNER_PERSONAL",
                                                           "U_PER_CHAIRMAN_COMPANY",
                                                           "U_PER_SUPERVISOR_COMPANY",
                                                           "U_PER_OTHER",
                                                           "U_COMPANY_SH_PERSONAL",
                                                           "U_COM_OTHER"])]
        # 一般关联关系交易分析表单信息+一般关联关系交易分析专家经验
        generally_related_trans_detail, generally_related_risk_tips = \
            self.get_generally_related_trans_detail(generally_related_df)

        # 从df中筛选出担保关系数据
        guarantor_df = df[df['relationship'].isin(['U_PER_GUARANTOR_PERSONAL', 'U_PER_GUARANTOR_COMPANY',
                                                   'U_COM_GUARANTOR_PERSONAL', 'U_COM_GUARANTOR_COMPANY'])]
        # 第三方担保交易分析表单信息+第三方担保交易分析专家经验
        guarantor_trans_detail, guarantor_risk_tips = self.get_guarantor_trans_detail(guarantor_df)

        # 强关联关系交易分析和一般关联交易分析输出指标
        self.variables["trans_u_related_portrait"] = {
            "strong_related_trans_info": {
                "risk_tips": strong_related_risk_tips,
                "trans_detail": strong_related_trans_detail
            },
            "generally_related_trans_info": {
                "risk_tips": generally_related_risk_tips,
                "trans_detail": generally_related_trans_detail
            }
        }
        self.variables['third_party_guarantee_info'] = {
            "risk_tips": guarantor_risk_tips,
            "trans_detail": guarantor_trans_detail
        }

        self.variables["trans_report_overview"]["related_info"]["strong_relation_info"][
            "risk_tips"] = strong_related_risk_tips
        self.variables["trans_report_overview"]["related_info"]["normal_relation_info"][
            "risk_tips"] = generally_related_risk_tips
        self.variables["trans_report_overview"]["related_info"]["guarantor_info"][
            "risk_tips"] = guarantor_risk_tips
