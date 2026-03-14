import datetime

from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_month_date, after_n_month_date, before_n_year_date
import pandas as pd


#     XB_MONTH("先息后本(按月)"),
#     XB_QUARTER("先息后本(按季)"),
#     XB_FIXED("先息后本(每x月定期还本y)"),
#     D_INTEREST("等额本息"),
#     D_PRINCIPAL("等额本金"),
#     D_INTEREST_PRINCIPAL("等本等息"),
class PcreditAccSpeculateView(ModuleProcessor):

    def process(self):
        self._get_pcredit_acc_speculate()

    def _get_pcredit_acc_speculate(self):
        pcredit_loan_df = self.cached_data.get("pcredit_loan")
        pcredit_acc_speculate_df = self.cached_data.get("pcredit_acc_speculate")
        if pcredit_acc_speculate_df is not None and not pcredit_acc_speculate_df.empty:
            pcredit_acc_speculate_df = self.acc_speculate_df_time_format(pcredit_acc_speculate_df)
            pcredit_acc_speculate_df_temp = pcredit_acc_speculate_df[pcredit_acc_speculate_df['account_status'] == '1']
        else:
            pcredit_acc_speculate_df_temp = pd.DataFrame(columns=['record_id','account_status','month','year'])
        credit_base_info_df = self.cached_data.get("credit_base_info")
        pcredit_info_df = self.cached_data.get("pcredit_info")
        if credit_base_info_df is None or credit_base_info_df.empty:
            return 
        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_6_month = before_n_month_date(report_time, 6)
        report_time_before_5_month = before_n_month_date(report_time, 5)
        report_time_before_4_month = before_n_month_date(report_time, 4)
        report_time_before_3_month = before_n_month_date(report_time, 3)
        report_time_before_2_month = before_n_month_date(report_time, 2)
        report_time_before_1_month = before_n_month_date(report_time, 1)
        report_time_after_1_month = after_n_month_date(report_time, 1)
        report_time_after_2_month = after_n_month_date(report_time, 2)
        report_time_after_3_month = after_n_month_date(report_time, 3)
        report_time_after_4_month = after_n_month_date(report_time, 4)
        report_time_after_5_month = after_n_month_date(report_time, 5)
        report_time_after_6_month = after_n_month_date(report_time, 6)
        report_time_after_7_month = after_n_month_date(report_time, 7)
        report_time_after_8_month = after_n_month_date(report_time, 8)
        report_time_after_9_month = after_n_month_date(report_time, 9)
        report_time_after_10_month = after_n_month_date(report_time, 10)
        report_time_after_11_month = after_n_month_date(report_time, 11)
        report_time_before_4_year = before_n_year_date(report_time, 4)
        report_time_before_3_year = before_n_year_date(report_time, 3)
        report_time_before_2_year = before_n_year_date(report_time, 2)
        report_time_before_1_year = before_n_year_date(report_time, 1)
        report_time_before_0_year = report_time


        if pcredit_loan_df is not None and not pcredit_loan_df.empty:
            loan_df_account_type_01_05 = pcredit_loan_df[
                pcredit_loan_df['account_type'].isin(['01', '02', '03', '04', '05'])]
            loan_df_account_type_01_03 = pcredit_loan_df[pcredit_loan_df['account_type'].isin(['01', '02', '03'])]
        else:
            loan_df_account_type_01_05 = pd.DataFrame()
            loan_df_account_type_01_03 = pd.DataFrame()

        if pcredit_info_df is not None and not pcredit_info_df.empty:
            undestory_avg_use = pcredit_info_df.loc[:, 'undestory_avg_use'].sum()
            undestory_semi_avg_overdraft = pcredit_info_df.loc[:, 'undestory_semi_avg_overdraft'].sum()
            repay_credit_n_month = undestory_avg_use + undestory_semi_avg_overdraft
        else:
            repay_credit_n_month = 0

        if not loan_df_account_type_01_05.empty:
            loan_df_account_type_01_05 = loan_df_account_type_01_05.drop(["repay_amount", "loan_repay_type", "loan_balance","account_status"], axis=1)
            loan_df_account_type_01_05 = pd.merge(loan_df_account_type_01_05, pcredit_acc_speculate_df_temp,
                                                  left_on='id', right_on='record_id')
            # 信贷交易信息-资金压力解析-应还总额6个月前
            total_repay_6m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_before_6_month)
            self.variables["total_repay_6m_before"] = total_repay_6m_before+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额5个月前
            total_repay_5m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_before_5_month)
            self.variables["total_repay_5m_before"] = total_repay_5m_before+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额4个月前
            total_repay_4m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_before_4_month)
            self.variables["total_repay_4m_before"] = total_repay_4m_before+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额3个月前
            total_repay_3m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_before_3_month)
            self.variables["total_repay_3m_before"] = total_repay_3m_before+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额2个月前
            total_repay_2m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_before_2_month)
            self.variables["total_repay_2m_before"] = total_repay_2m_before+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额1个月前
            total_repay_1m_before = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_before_1_month)
            self.variables["total_repay_1m_before"] = total_repay_1m_before+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额1个月后
            total_repay_1m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time)
            self.variables["total_repay_1m_after"] = total_repay_1m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额2个月后
            total_repay_2m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_1_month)
            self.variables["total_repay_2m_after"] = total_repay_2m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额3个月后
            total_repay_3m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_2_month)
            self.variables["total_repay_3m_after"] = total_repay_3m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额4个月后
            total_repay_4m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_3_month)
            self.variables["total_repay_4m_after"] = total_repay_4m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额5个月后
            total_repay_5m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_4_month)
            self.variables["total_repay_5m_after"] = total_repay_5m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额6个月后
            total_repay_6m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_5_month)
            self.variables["total_repay_6m_after"] = total_repay_6m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额7个月后
            total_repay_7m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_6_month)
            self.variables["total_repay_7m_after"] = total_repay_7m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额8个月后
            total_repay_8m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_7_month)
            self.variables["total_repay_8m_after"] = total_repay_8m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额9个月后
            total_repay_9m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                      report_time_after_8_month)
            self.variables["total_repay_9m_after"] = total_repay_9m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额10个月后
            total_repay_10m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_after_9_month)
            self.variables["total_repay_10m_after"] = total_repay_10m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额11个月后
            total_repay_11m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_after_10_month)
            self.variables["total_repay_11m_after"] = total_repay_11m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-应还总额12个月后
            total_repay_12m_after = self.util_get_repay_n_month_before(loan_df_account_type_01_05,
                                                                       report_time_after_11_month)
            self.variables["total_repay_12m_after"] = total_repay_12m_after+repay_credit_n_month
            # 信贷交易信息-资金压力解析-过去6个月平均应还款
            self.variables["average_repay_6m_before"] = '%.2f' % ((
                        total_repay_6m_before + total_repay_5m_before + total_repay_4m_before +
                        total_repay_3m_before + total_repay_2m_before + total_repay_1m_before) / 6+repay_credit_n_month)
            # 信贷交易信息-资金压力解析-未来12个月平均应还款
            self.variables["average_repay_12m_after"] = '%.2f' % ((
                        total_repay_1m_after + total_repay_2m_after + total_repay_3m_after + total_repay_4m_after + total_repay_5m_after + total_repay_6m_after + total_repay_7m_after +
                        total_repay_8m_after + total_repay_9m_after + total_repay_10m_after + total_repay_11m_after + total_repay_12m_after) / 12+repay_credit_n_month)

        if not loan_df_account_type_01_03.empty:
            loan_df_account_type_01_03 = loan_df_account_type_01_03.drop(["repay_amount", "loan_repay_type", "loan_balance","account_status"], axis=1)
            loan_df_account_type_01_03 = pd.merge(loan_df_account_type_01_03, pcredit_acc_speculate_df_temp,
                                                  left_on='id', right_on='record_id')
            # 信贷交易信息-资金压力解析-应还贷款总额6个月前
            self.variables["repay_loan_6m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_6_month)
            # 信贷交易信息-资金压力解析-应还本金6个月前
            self.variables["repay_principal_6m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_6_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额6个月前
            self.variables["repay_installment_6m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_6_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额5个月前
            self.variables["repay_loan_5m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_5_month)
            # 信贷交易信息-资金压力解析-应还本金5个月前
            self.variables["repay_principal_5m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_5_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额5个月前
            self.variables["repay_installment_5m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_5_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额4个月前
            self.variables["repay_loan_4m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_4_month)
            # 信贷交易信息-资金压力解析-应还本金4个月前
            self.variables["repay_principal_4m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_4_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额4个月前
            self.variables["repay_installment_4m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_4_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额3个月前
            self.variables["repay_loan_3m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_3_month)
            # 信贷交易信息-资金压力解析-应还本金3个月前
            self.variables["repay_principal_3m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_3_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额3个月前
            self.variables["repay_installment_3m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_3_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额2个月前
            self.variables["repay_loan_2m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_2_month)
            # 信贷交易信息-资金压力解析-应还本金2个月前
            self.variables["repay_principal_2m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_2_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额2个月前
            self.variables["repay_installment_2m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_2_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额1个月前
            self.variables["repay_loan_1m_before"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_before_1_month)
            # 信贷交易信息-资金压力解析-应还本金1个月前
            self.variables["repay_principal_1m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_1_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额1个月前
            self.variables["repay_installment_1m_before"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_before_1_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额1个月后
            self.variables["repay_loan_1m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time)
            # 信贷交易信息-资金压力解析-应还本金1个月后
            self.variables["repay_principal_1m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额1个月后
            self.variables["repay_installment_1m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额2个月后
            self.variables["repay_loan_2m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_1_month)
            # 信贷交易信息-资金压力解析-应还本金2个月后
            self.variables["repay_principal_2m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_1_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额2个月后
            self.variables["repay_installment_2m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_1_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额3个月后
            self.variables["repay_loan_3m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_2_month)
            # 信贷交易信息-资金压力解析-应还本金3个月后
            self.variables["repay_principal_3m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_2_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额3个月后
            self.variables["repay_installment_3m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_2_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额4个月后
            self.variables["repay_loan_4m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_3_month)
            # 信贷交易信息-资金压力解析-应还本金4个月后
            self.variables["repay_principal_4m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_3_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额4个月后
            self.variables["repay_installment_4m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_3_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额5个月后
            self.variables["repay_loan_5m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_4_month)
            # 信贷交易信息-资金压力解析-应还本金5个月后
            self.variables["repay_principal_5m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_4_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额5个月后
            self.variables["repay_installment_5m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_4_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额6个月后
            self.variables["repay_loan_6m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_5_month)
            # 信贷交易信息-资金压力解析-应还本金6个月后
            self.variables["repay_principal_6m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_5_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额6个月后
            self.variables["repay_installment_6m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_5_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额7个月后
            self.variables["repay_loan_7m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_6_month)
            # 信贷交易信息-资金压力解析-应还本金7个月后
            self.variables["repay_principal_7m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_6_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额7个月后
            self.variables["repay_installment_7m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_6_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额8个月后
            self.variables["repay_loan_8m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_7_month)
            # 信贷交易信息-资金压力解析-应还本金8个月后
            self.variables["repay_principal_8m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_7_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额8个月后
            self.variables["repay_installment_8m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_7_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额9个月后
            self.variables["repay_loan_9m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                       report_time_after_8_month)
            # 信贷交易信息-资金压力解析-应还本金9个月后
            self.variables["repay_principal_9m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_8_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额9个月后
            self.variables["repay_installment_9m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_8_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额10个月后
            self.variables["repay_loan_10m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_after_9_month)
            # 信贷交易信息-资金压力解析-应还本金10个月后
            self.variables["repay_principal_10m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_9_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额10个月后
            self.variables["repay_installment_10m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_9_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额11个月后
            self.variables["repay_loan_11m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_after_10_month)
            # 信贷交易信息-资金压力解析-应还本金10个月后
            self.variables["repay_principal_11m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_10_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额10个月后
            self.variables["repay_installment_11m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_10_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])

            # 信贷交易信息-资金压力解析-应还贷款总额12个月后
            self.variables["repay_loan_12m_after"] = self.util_get_repay_n_month_before(loan_df_account_type_01_03,
                                                                                        report_time_after_11_month)
            # 信贷交易信息-资金压力解析-应还本金12个月后
            self.variables["repay_principal_12m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_11_month, 'loan_repay_type', ['XB_QUARTER', 'XB_MONTH', 'XB_FIXED'])
            # 信贷交易信息-资金压力解析-应还分期额12个月后
            self.variables["repay_installment_12m_after"] = self.util_get_repay_n_month_before(
                loan_df_account_type_01_03, report_time_after_11_month, 'loan_repay_type', ['D_INTEREST', 'D_PRINCIPAL', 'D_INTEREST_PRINCIPAL'])


        # 信贷交易信息-资金压力解析-应还贷记卡6个月前
        self.variables["repay_credit_6m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡5个月前
        self.variables["repay_credit_5m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡4个月前
        self.variables["repay_credit_4m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡3个月前
        self.variables["repay_credit_3m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡2个月前
        self.variables["repay_credit_2m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡1个月前
        self.variables["repay_credit_1m_before"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡1个月后
        self.variables["repay_credit_1m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡2个月后
        self.variables["repay_credit_2m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡3个月后
        self.variables["repay_credit_3m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡4个月后
        self.variables["repay_credit_4m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡5个月后
        self.variables["repay_credit_5m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡6个月后
        self.variables["repay_credit_6m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡7个月后
        self.variables["repay_credit_7m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡8个月后
        self.variables["repay_credit_8m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡9个月后
        self.variables["repay_credit_9m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡10个月后
        self.variables["repay_credit_10m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡11个月后
        self.variables["repay_credit_11m_after"] = repay_credit_n_month
        # 信贷交易信息-资金压力解析-应还贷记卡12个月后
        self.variables["repay_credit_12m_after"] = repay_credit_n_month

        # 信贷交易信息-贷款信息-近五年经营性贷款余额变化-时间节点
        busi_loan_date_list = [report_time_before_4_year, report_time_before_3_year, report_time_before_2_year,
                               report_time_before_1_year, report_time_before_0_year]
        self.variables["busi_loan_date"] = list(map(lambda x: datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S"), busi_loan_date_list))

        if pcredit_loan_df is not None and not pcredit_loan_df.empty and \
                pcredit_acc_speculate_df is not None and not pcredit_acc_speculate_df.empty:
            pcredit_loan_type_df = pcredit_loan_df[(pcredit_loan_df['account_type'].isin(['01', '02', '03'])) & (
                    (pcredit_loan_df['loan_type'].isin(['01', '07', '99'])) |
                    (pcredit_loan_df['loan_type'].str.contains('融资租赁')) |
                    ((pcredit_loan_df['loan_type'] == '04') & (pcredit_loan_df['loan_amount'] >= 200000))
                 )]

            pcredit_acc_speculate_df_temp1 = pcredit_acc_speculate_df[(pcredit_acc_speculate_df['account_status'] == '1') |
                                                                      ((pcredit_acc_speculate_df[
                                                                            'account_status'] != '1') & (
                                                                           pcredit_acc_speculate_df[
                                                                               'loan_repay_type'].str.contains('XB_')))]
            pcredit_acc_speculate_df_temp2 = pcredit_acc_speculate_df[(pcredit_acc_speculate_df['account_status'] == '1') |
                                                                      ((pcredit_acc_speculate_df[
                                                                            'account_status'] != '1') & (
                                                                           pcredit_acc_speculate_df[
                                                                               'loan_repay_type'].str.contains('D_INTEREST')))]

            pcredit_loan_type_df_temp = pcredit_loan_type_df.drop(["repay_amount", "loan_repay_type", "loan_balance","account_status"], axis=1)
            max_temp_df = pd.merge(pcredit_loan_type_df_temp, pcredit_acc_speculate_df_temp1, left_on='id',
                                   right_on='record_id')
            min_temp_df = pd.merge(pcredit_loan_type_df_temp, pcredit_acc_speculate_df_temp2, left_on='id',
                                   right_on='record_id')
            busi_loan_balance_max_before_4_year = self.util_get_repay_n_month_before_loan_balance(max_temp_df,
                                                                                                  report_time_before_4_year)
            busi_loan_balance_max_before_3_year = self.util_get_repay_n_month_before_loan_balance(max_temp_df,
                                                                                                  report_time_before_3_year)
            busi_loan_balance_max_before_2_year = self.util_get_repay_n_month_before_loan_balance(max_temp_df,
                                                                                                  report_time_before_2_year)
            busi_loan_balance_max_before_1_year = self.util_get_repay_n_month_before_loan_balance(max_temp_df,
                                                                                                  report_time_before_1_year)
            busi_loan_balance_max_before_0_year = self.util_get_repay_n_month_before_loan_balance(max_temp_df,
                                                                                                  report_time_before_0_year)
            busi_loan_balance_min_before_4_year = self.util_get_repay_n_month_before_loan_balance(min_temp_df,
                                                                                                  report_time_before_4_year)
            busi_loan_balance_min_before_3_year = self.util_get_repay_n_month_before_loan_balance(min_temp_df,
                                                                                                  report_time_before_3_year)
            busi_loan_balance_min_before_2_year = self.util_get_repay_n_month_before_loan_balance(min_temp_df,
                                                                                                  report_time_before_2_year)
            busi_loan_balance_min_before_1_year = self.util_get_repay_n_month_before_loan_balance(min_temp_df,
                                                                                                  report_time_before_1_year)
            busi_loan_balance_min_before_0_year = self.util_get_repay_n_month_before_loan_balance(min_temp_df,
                                                                                                  report_time_before_0_year)
            # 信贷交易信息-贷款信息-近五年经营性贷款余额变化-最大余额
            self.variables["busi_loan_balance_max"] = [busi_loan_balance_max_before_4_year,
                                                       busi_loan_balance_max_before_3_year,
                                                       busi_loan_balance_max_before_2_year,
                                                       busi_loan_balance_max_before_1_year,
                                                       busi_loan_balance_max_before_0_year]
            # 信贷交易信息-贷款信息-近五年经营性贷款余额变化-最小余额
            self.variables["busi_loan_balance_min"] = [busi_loan_balance_min_before_4_year,
                                                       busi_loan_balance_min_before_3_year,
                                                       busi_loan_balance_min_before_2_year,
                                                       busi_loan_balance_min_before_1_year,
                                                       busi_loan_balance_min_before_0_year]
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前个数
            self.variables["busi_org_cnt_3y_ago"] = max_temp_df[
                (max_temp_df['year'] == report_time_before_3_year.year) &
                (max_temp_df['month'] == report_time_before_3_year.month)]['account_org'].nunique()
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前最大余额
            self.variables["busi_org_balance_3y_ago_max"] = busi_loan_balance_max_before_3_year
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化3年前最小余额
            self.variables["busi_org_balance_3y_ago_min"] = busi_loan_balance_min_before_3_year
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前个数
            self.variables["busi_org_cnt_2y_ago"] = max_temp_df[
                (max_temp_df['year'] == report_time_before_2_year.year) &
                (max_temp_df['month'] == report_time_before_2_year.month)]['account_org'].nunique()
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前最大余额
            self.variables["busi_org_balance_2y_ago_max"] = busi_loan_balance_max_before_2_year
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化2年前最小余额
            self.variables["busi_org_balance_2y_ago_min"] = busi_loan_balance_min_before_2_year
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前个数
            self.variables["busi_org_cnt_1y_ago"] = max_temp_df[
                (max_temp_df['year'] == report_time_before_1_year.year) &
                (max_temp_df['month'] == report_time_before_1_year.month)]['account_org'].nunique()
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前最大余额
            self.variables["busi_org_balance_1y_ago_max"] = busi_loan_balance_max_before_1_year
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化1年前最小余额
            self.variables["busi_org_balance_1y_ago_min"] = busi_loan_balance_min_before_1_year
        if pcredit_loan_df is not None and not pcredit_loan_df.empty:
            pcredit_loan_type_df = pcredit_loan_df[(pcredit_loan_df['account_type'].isin(['01', '02', '03'])) &
                                                   ((pcredit_loan_df['loan_type'].isin(['01', '07', '99'])) | (
                                                           (pcredit_loan_df['loan_type'] == '04') & (
                                                            pcredit_loan_df['loan_amount'] >= 200000)))]
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前个数
            self.variables["busi_org_cnt_now"] = pcredit_loan_type_df[
                pcredit_loan_type_df['loan_balance'] > 0]['account_org'].nunique()
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前余额
            self.variables["busi_org_balance_now_max"] = pcredit_loan_type_df.loc[:, 'loan_balance'].sum()
            # 信贷交易信息-贷款信息-经营性贷款银行融资机构个数及余额变化当前余额
            self.variables["busi_org_balance_now_min"] = pcredit_loan_type_df.loc[:, 'loan_balance'].sum()

    def util_get_repay_n_month_before(self, df, date, param=None, param_value_list=None):
        if not df.empty:
            year = date.year
            month = date.month
            df = df[(df['year'] == year) & (df['month'] == month) & (df['account_status'] == '1')]
            if param is not None:
                df = df[df[param].isin(param_value_list)]
            return df.loc[:, 'repay_amount'].sum()
        else:
            return 0

    def util_get_repay_n_month_before_loan_balance(self, df, date, param=None, param_value_list=None):
        if not df.empty:
            year = date.year
            month = date.month
            df = df[(df['year'] == year) & (df['month'] == month)]
            if param is not None:
                df = df[df[param].isin(param_value_list)]
            return df.loc[:, 'loan_balance'].sum()
        else:
            return 0

    def acc_speculate_df_time_format(self, df):
        df['repay_month'] = pd.to_datetime(df['repay_month'], format="%Y-%m")
        df['year'] = df['repay_month'].apply(lambda x: x.year)
        df['month'] = df['repay_month'].apply(lambda x: x.month)
        return df
