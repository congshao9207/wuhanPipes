import pandas as pd
from datetime import datetime

from mapping.utils.df_utils import df_value
from mapping.utils.df_utils import df_zero
from util.mysql_reader import sql_to_df
from mapping.trans_module_processor import TransModuleProcessor


class GetVariableInDB(TransModuleProcessor):

    def process(self):
        self._from_u_portrait()
        self._from_u_summary_portrait()
        self._from_u_counterparty()
        # self._from_u_loan_portrait()
        self._from_u_related_portrait()

    def _from_u_portrait(self):
        flow_cnt = len(self.trans_u_flow_portrait)
        sql = """
            select *
            from trans_u_portrait
            where report_req_no = %(report_req_no)s
        """
        portrait = sql_to_df(sql=sql,
                             params={"report_req_no": self.reqno})
        if flow_cnt > 0:
            self.variables['balance_0_5_prop'] = round(df_zero(portrait['balance_0_to_5_day']) / flow_cnt,4)
            self.variables['income_0_5_prop'] = round(df_zero(portrait['income_0_to_5_cnt']) / flow_cnt,4)
        self.variables['balance_min_weight'] = df_zero(portrait['balance_weight_min'])
        self.variables['balance_max_weight'] = df_zero(portrait['balance_weight_max'])
        self.variables['income_max_weight'] = df_zero(portrait['income_weight_max'])
        self.variables['normal_income_mean'] = df_value(portrait['normal_income_mean'])
        self.variables['normal_income_amt_d_mean'] = df_value(portrait['normal_income_d_mean'])
        self.variables['normal_income_amt_m_mean'] = df_value(portrait['normal_income_m_mean'])
        self.variables['relationship_risk'] = df_value(portrait['relationship_risk'])

    def _from_u_summary_portrait(self):
        sql = """
            select *
            from trans_u_summary_portrait
            where report_req_no = %(report_req_no)s
        """
        summary_portrait = sql_to_df(sql=sql,
                                     params={"report_req_no": self.reqno})
        self.variables['half_year_interest_amt'] = df_value(summary_portrait[summary_portrait['month'] == 'half_year'][
            'interest_amt'])
        self.variables['half_year_balance_amt'] = df_value(summary_portrait[summary_portrait['month'] == 'half_year'][
            'balance_amt'])
        self.variables['year_interest_amt'] = df_value(summary_portrait[summary_portrait['month'] == 'year'][
            'interest_amt'])
        self.variables['q_2_balance_amt'] = df_zero(summary_portrait[summary_portrait['month'] == 'quarter2'][
            'balance_amt'])
        self.variables['q_3_balance_amt'] = df_zero(summary_portrait[summary_portrait['month'] == 'quarter3'][
            'balance_amt'])
        self.variables['year_interest_balance_prop'] = df_value(summary_portrait[summary_portrait['month'] == 'year'][
            'interest_balance_proportion'])
        self.variables['q_4_interest_balance_prop'] = df_value(summary_portrait[summary_portrait['month'] == 'quarter4'][
            'interest_balance_proportion'])

        val = summary_portrait[summary_portrait.month.isin(['2', '3', '4'])]['net_income_amt'].sum()
        if val != 0:
            self.variables['income_net_rate_compare_2'] = \
                round(summary_portrait[summary_portrait.month.isin(['5', '6', '7'])]['net_income_amt'].sum() / val,4)
        else:
            self.variables['income_net_rate_compare_2'] = ""

    def _from_u_counterparty(self):
        sql = """
            select *
            from trans_u_counterparty_portrait
            where report_req_no = %(report_req_no)s
        """
        counterparty = sql_to_df(sql=sql,
                                 params={"report_req_no": self.reqno})
        self.variables['income_rank1_amt'] = df_value(counterparty[(counterparty['income_amt_order'] == '1')
                                                           & (counterparty['month'] == '汇总')]['trans_amt'])
        self.variables['income_rank2_amt'] = df_value(counterparty[(counterparty['income_amt_order'] == '2')
                                                           & (counterparty['month'] == '汇总')]['trans_amt'])
        self.variables['income_rank3_amt'] = df_value(counterparty[(counterparty['income_amt_order'] == '3')
                                                           & (counterparty['month'] == '汇总')]['trans_amt'])
        self.variables['income_rank4_amt'] = df_value(counterparty[(counterparty['income_amt_order'] == '4')
                                                           & (counterparty['month'] == '汇总')]['trans_amt'])

        val = df_zero(counterparty[(counterparty['income_amt_order'] == '前100%') & (counterparty['month'] == '汇总')]['trans_cnt'])
        if val != 0:
            self.variables['income_rank_2_cnt_prop'] = df_zero(counterparty[(counterparty['income_amt_order'] == '2')
                                                                    & (counterparty['month'] == '汇总')]['trans_cnt']) / val
        else:
            self.variables['income_rank_2_cnt_prop'] = ""

        self.variables['expense_rank_6_avg_gap'] = df_value(counterparty[(counterparty['expense_amt_order'] == '6')
                                                                & (counterparty['month'] == '汇总')]['trans_gap_avg'])

        self.variables['income_rank_9_avg_gap'] = df_value(counterparty[(counterparty['income_amt_order'] == '9')
                                                               & (counterparty['month'] == '汇总')]['trans_gap_avg'])

        self.variables['expense_rank_10_avg_gap'] = df_value(counterparty[(counterparty['expense_amt_order'] == '10')
                                                                 & (counterparty['month'] == '汇总')]['trans_gap_avg'])

    def _from_u_loan_portrait(self):
        sql = """
            select *
            from trans_u_loan_portrait
            where report_req_no = %(report_req_no)s
        """
        loan_portrait = sql_to_df(sql=sql,
                                  params={"report_req_no": self.reqno})
        cm_3 = list(map(str, list(range(1, 4))))
        cm_6 = list(map(str, list(range(1, 7))))
        cm_12 = list(map(str, list(range(1, 13))))

        self.variables['hitloantype_cnt_6cm'] = loan_portrait[loan_portrait.month.isin(cm_6)]['loan_type'].nunique()

        self.variables['private_income_amt_12cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                   & (loan_portrait['loan_type'] == '民间借贷')][
            'loan_amt'].sum()
        val = loan_portrait[(loan_portrait.month.isin(cm_12)) & (loan_portrait['loan_type'] == '民间借贷')]['loan_cnt'].sum()
        if val > 0:
            self.variables['private_income_mean_12cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                        & (loan_portrait['loan_type'] == '民间借贷')][
                                                              'loan_amt'].sum() / val
        else:
            self.variables['private_income_mean_12cm'] = ""
        self.variables['pettyloan_income_amt_12cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                     & (loan_portrait['loan_type'] == '小贷')][
            'loan_amt'].sum()

        val = loan_portrait[(loan_portrait.month.isin(cm_12))& (loan_portrait['loan_type'] == '小贷')]['loan_cnt'].sum()
        if val > 0:
            self.variables['pettyloan_income_mean_12cm'] = loan_portrait[(loan_portrait.month.isin(cm_12))
                                                                          & (loan_portrait['loan_type'] == '小贷')][
                                                                'loan_amt'].sum() / val
        else:
            self.variables['pettyloan_income_mean_12cm'] = ""
        self.variables['finlease_expense_cnt_6cm'] = loan_portrait[(loan_portrait.month.isin(cm_6))
                                                                    & (loan_portrait['loan_type'] == '融资租赁')][
            'repay_cnt'].sum()

        val = loan_portrait[(loan_portrait.month.isin(cm_3)) & (loan_portrait['loan_type'] == '其他金融')]['loan_cnt'].sum()
        if val > 0:
            self.variables['otherfin_income_mean_3cm'] = loan_portrait[(loan_portrait.month.isin(cm_3))
                                                                        & (loan_portrait['loan_type'] == '其他金融')][
                                                              'loan_amt'].sum() / val
        else:
            self.variables['otherfin_income_mean_3cm'] = ""
        self.variables['allloantype_expense_cnt_3cm'] = loan_portrait[loan_portrait.month.isin(cm_3)]['repay_cnt'].sum()

    def _from_u_related_portrait(self):
        sql = """
            select *
            from trans_u_related_portrait
            where report_req_no = %(report_req_no)s
        """
        related_portrait = sql_to_df(sql=sql,
                                     params={"report_req_no": self.reqno})
        self.variables['enterprise_3_income_amt'] = related_portrait[related_portrait['relationship'].isin(["U_PER_SH_H_COMPANY","U_PER_SH_M_COMPANY","U_PER_SH_L_COMPANY"])]['income_amt'].sum()
