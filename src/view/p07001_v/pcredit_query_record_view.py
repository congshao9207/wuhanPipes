from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_month_date, before_n_year_date, date_to_timestamp
from util.common_util import format_timestamp
import pandas as pd


class PcreditQueryRecordView(ModuleProcessor):

    def process(self):
        self._get_query_record_msg()

    def _get_query_record_msg(self):
        df = self.cached_data.get("pcredit_query_record")
        if df is None or df.empty:
            return
        df = df[pd.notnull(df['jhi_time'])]
        df['jhi_time'] = df['jhi_time'].apply(lambda x: date_to_timestamp(x))
        loan_df = self.cached_data.get("pcredit_loan")
        loan_df = loan_df[pd.notnull(loan_df['loan_date'])]
        loan_df['loan_date'] = loan_df['loan_date'].apply(lambda x: date_to_timestamp(x))
        credit_base_info_df = self.cached_data.get("credit_base_info")
        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_3_month = before_n_month_date(report_time, 3)
        df_3_month = df[df['jhi_time'] > report_time_before_3_month]
        if not df_3_month.empty:
            # 查询信息-近三个月查询记录-查询日期
            self.variables["jhi_time_3m"] = df_3_month.loc[:, 'jhi_time'].apply(lambda x: format_timestamp(x)).tolist()
            # 查询信息-近三个月查询记录-查询机构
            self.variables["operator_3m"] = df_3_month.loc[:, 'operator'].tolist()
            # 查询信息-近三个月查询记录-查询原因
            self.variables["reason_3m"] = df_3_month.loc[:, 'reason'].tolist()
            # 查询信息-近三个月查询记录-保前审查记录条数
            self.variables["guar_query_cnt"] = df_3_month[df_3_month['reason'] == '08'].shape[0]
            # 查询信息-近三个月查询记录-资信审查记录条数
            self.variables["loan_query_cnt"] = df_3_month[df_3_month['reason']=='13'].shape[0]
            # 查询信息-近三个月查询记录-本人查询记录条数
            self.variables["self_query_cnt"] = df_3_month[df_3_month['reason']=='18'].shape[0]
        report_time_before_1_year = before_n_year_date(report_time, 1)
        df_1_year = df[df['jhi_time'] > report_time_before_1_year]
        if not df_1_year.empty:
            df_1_year_reason = df_1_year[df_1_year['reason'].isin(['01', '02'])]
            if not df_1_year_reason.empty:
                # 查询信息-近一年贷款审批和贷记卡审批的查询记录查询日期
                self.variables["jhi_time_1y"] = df_1_year_reason.loc[:, 'jhi_time'].apply(
                    lambda x: format_timestamp(x)).tolist()
                # 查询信息-近一年贷款审批和贷记卡审批的查询记录查询机构
                self.variables["operator_1y"] = df_1_year_reason.loc[:, 'operator'].tolist()
                # 查询信息-近一年贷款审批和贷记卡审批的查询记录查询原因
                self.variables["reason_1y"] = df_1_year_reason.loc[:, 'reason'].tolist()
                # 查询信息-近一年贷款审批和贷记卡审批的查询记录是否放款
                for index, row in df_1_year_reason.iterrows():
                    jhi_time = row['jhi_time']
                    operator = row['operator']
                    df_temp = loan_df[(loan_df['account_type'].isin(['01', '02', '03', '04', '05'])) & (
                                loan_df['loan_date'] >= jhi_time) & (loan_df['account_org'] == operator)]
                    if not df_temp.empty:
                        df_1_year_reason.loc[index, 'if_loan'] = "是"
                    else:
                        df_1_year_reason.loc[index, 'if_loan'] = "否"
                self.variables["if_loan"] = df_1_year_reason.loc[:, 'if_loan'].tolist()
                # 查询信息-近一年贷款审批和贷记卡审批的查询记录银行查询未放款笔数
                self.variables["bank_query_loan_cnt"] = df_1_year_reason[(df_1_year_reason['reason'] == "01")
                                                                         & (df_1_year_reason['if_loan'] == "否")].shape[
                    0]
                # 查询信息-近一年贷款审批和贷记卡审批的查询记录贷记卡查询未放款笔数
                self.variables["credit_query_loan_cnt"] = df_1_year_reason[(df_1_year_reason['reason'] == "02")
                                                                           & (df_1_year_reason[
                                                                                  'if_loan'] == "否")].shape[0]

            # 查询信息-近一年贷款审批和贷记卡审批的查询记录银行查询笔数
            self.variables["bank_query_cnt"] = df_1_year[df_1_year['reason'] == '01'].shape[0]
            # 查询信息-近一年贷款审批和贷记卡审批的查询记录贷记卡查询笔数
            self.variables["credit_query_cnt"] = df_1_year[df_1_year['reason'] == '02'].shape[0]
