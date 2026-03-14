# @Time : 12/11/20 2:58 PM 
# @Author : lixiaobo
# @File : tp00001.py.py 
# @Software: PyCharm
import math

import pandas as pd
from jsonpath import jsonpath
from pandas.tseries import offsets

from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df

logger = LoggerUtil().logger(__name__)


class Tp0002(Transformer):
    """
    额度模型
    """
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'bus_total_accounts_payable_debt_amt': None,  # 企业应付账款总负债
            'operating_cycle': 0,  # 营业周期
            'main_business_running_years': 0,  # 主营企业经营年限
            'bus_total_assets_amt': None,  # 企业总资产
            'total_currency_assets_amt': 0,  # 个人_企业货币资金总资产
            'quick_debt_to_assets_ratio': 0,  # 速动比率
            'sale_to_loan_ratio': 0,  # 销贷比
            'bus_industry_2nd_cnt': 0,  # 二类行业数
            'per_guar_debt_balance': None,  # 个人对外担保总负债余额
            'loan_amount_min_amt': 0,  # 授信金额最小值
            'loan_amount_avg_6m': None,  # 6个月内新增贷款金额笔均
            'guar_type_1_4_amt_prop': None,  # 信用保证类贷款金额占比
            'single_business_loan_overdue_cnt_3m': None,  # 3个月内单笔经营性贷款逾期次数
            'credit_min_payed_number': None,  # 信用卡最低还款张数
            'guar_type_4_avg_amt': None,  # 信用类贷款平均授信金额
            'credit_card_max_overdue_amt_60m': None,  # 5年内信用卡最大逾期金额
            'single_business_loan_overdue_cnt_6m': None,  # 6个月内单笔经营性贷款逾期次数
            'query_cnt_3m': 0,  # 3个月内贷款+贷记卡审批查询次数
            'loan_cnt_inc_6m': 0,  # 6个月内新增贷款笔数
            'credit_card_max_overdue_month_60m': None,  # 5年内单张信用卡最大逾期期数
            'credit_guar_bal': 0,  # 个人对外担保总负债余额
            'operating_income_6m': 0,  # 6个月经营收入
            'balance_day_avg_6m': 0,  # 6个月余额日均
            # 'flow_limit_amt': 0,  # 流水指标
            'loan_amt_pred_avg': 0,  # 主体配偶预测额度平均值
            # 'model_pred': 0,  # 违约模型,
            'is_first_loan': "N",  # 是否为首次贷款
            'has_house_asset': 0,  # 是否有房产
            'last_total_sale_amt': 0  # 上年销售总额
        }
        self.per_asset_info = None
        self.per_debt_info = None
        self.com_busi_info = None
        self.com_sale_info = None
        self.com_asset_info = None
        self.com_debt_info = None
        self.report_id = None
        self.report_time = None
        self.pcredit_loan = None
        self.pcredit_info = None
        self.pcredit_repayment = None
        self.pcredit_query = None
        self.trans_u_flow = None

    def all_variables(self, final_call=0):
        strategy_param = self.full_msg.get('strategyParam')
        query_data = strategy_param.get('queryData')
        if query_data is None or len(query_data) == 0:
            return
        per_msg = None
        if final_call == 1:
            if len(query_data) > 0:
                per_msg = query_data[0].get('extraParam')
        else:
            for i in range(len(query_data)):
                if query_data[i].get('idno') == self.id_card_no:
                    per_msg = query_data[i].get('extraParam')
        com_msg = strategy_param.get('extraParam')
        if per_msg is None or com_msg is None:
            return
        per_passthrough_msg = per_msg.get('passthroughMsg')
        com_passthrough_msg = com_msg.get('passthroughMsg')
        if per_passthrough_msg is None or com_passthrough_msg is None:
            return
        self.per_asset_info = pd.DataFrame(per_passthrough_msg.get('iqp_indiv_ass'))
        self.per_debt_info = pd.DataFrame(per_passthrough_msg.get('iqp_indiv_fam_lby'))
        self.com_busi_info = pd.DataFrame(com_passthrough_msg.get('iqp_busi_info'))
        self.com_sale_info = pd.DataFrame(com_passthrough_msg.get('iqp_sale_info'))
        self.com_asset_info = pd.DataFrame(com_passthrough_msg.get('iqp_asset_info'))
        self.com_debt_info = pd.DataFrame(com_passthrough_msg.get('iqp_debt_info'))

        if final_call == 1:
            if self.per_asset_info is not None and self.per_asset_info.shape[0] > 0:
                if self.per_asset_info[self.per_asset_info['indiv_ass_type'] == '02'].shape[0] > 0:
                    self.variables['has_house_asset'] = 1
            if self.com_busi_info is not None and self.com_busi_info.shape[0] > 0:
                if self.com_sale_info is not None and self.com_sale_info.shape[0] > 0:
                    temp_sale_info = pd.merge(self.com_sale_info, self.com_busi_info[['serno', 'stock_perc']],
                                              how='left', left_on='bus_info_serno', right_on='serno')
                    temp_sale_info['stock_perc'] = temp_sale_info['stock_perc'].fillna(0)
                    self.variables['last_total_sale_amt'] = \
                        sum(temp_sale_info['last_sale_total'] * temp_sale_info['stock_perc'])
                if self.com_asset_info is not None and self.com_asset_info.shape[0] > 0:
                    if self.com_asset_info[self.com_asset_info['asset_typ'] == '8'].shape[0] > 0:
                        self.variables['has_house_asset'] = 1
            return

        per_total_debt_amt = 0
        per_total_bank_debt_amt = 0
        if self.per_debt_info is not None and self.per_debt_info.shape[0] > 0:
            self.per_debt_info['indiv_debt_amt'] = self.per_debt_info.apply(
                lambda x: x['indiv_debt_amt'] if x['indiv_debt_typ'] != '4' else x['indiv_debt_amt'] / 2, axis=1)
            self.per_debt_info['indiv_debt_amt_bal'] = self.per_debt_info.apply(
                lambda x: x['indiv_debt_amt_bal'] if x['indiv_debt_typ'] != '4' else x['indiv_debt_amt_bal'] / 2,
                axis=1)
            per_guar_debt_balance = self.per_debt_info[
                self.per_debt_info['indiv_debt_typ'] == '4']['indiv_debt_amt_bal'].sum()
            self.variables['per_guar_debt_balance'] = per_guar_debt_balance

            per_total_debt_amt = self.per_debt_info['indiv_debt_amt_bal'].sum()
            per_total_bank_debt_amt = self.per_debt_info[
                self.per_debt_info['indiv_debt_typ'].isin(['2', '3', '5'])]['indiv_debt_amt_bal'].sum()

        per_currency_assets_amt = 0
        per_total_flow_assets_amt = 0
        if self.per_asset_info is not None and self.per_asset_info.shape[0] > 0:
            self.per_asset_info['indiv_ass_val'] = self.per_asset_info.apply(
                lambda x: x['s_avg_daily'] if x['indiv_ass_type'] == '05' and pd.notna(x['s_avg_daily'])
                else x['indiv_ass_val'], axis=1)
            per_currency_assets_amt = self.per_asset_info[
                self.per_asset_info['indiv_ass_type'] == "05"]['indiv_ass_val'].sum()
            per_total_flow_assets_amt = self.per_asset_info[
                self.per_asset_info['indiv_ass_type'].isin(['05', '06', '07'])]['indiv_ass_val'].sum()

        bus_total_busi_currency_assets_amt = 0
        if self.com_busi_info is not None and self.com_busi_info.shape[0] > 0:
            self.variables['bus_industry_2nd_cnt'] = \
                self.com_busi_info[(pd.notna(self.com_busi_info['cll_typ'])) &
                                   (self.com_busi_info['cll_typ'] != '')]['cll_typ'].apply(lambda x: x[:3]).nunique()

            self.com_busi_info['opera_year'].replace('', 0, inplace=True)
            self.com_busi_info['opera_year'] = self.com_busi_info.apply(
                lambda x: 0 if pd.isna(x['opera_year']) else int(x['opera_year']), axis=1)

            last_total_sale_amt = 0
            last_total_profit_amt = 0
            if self.com_sale_info is not None and self.com_sale_info.shape[0] > 0:
                temp_sale_info = pd.merge(self.com_sale_info, self.com_busi_info[['serno', 'stock_perc']],
                                          how='left', left_on='bus_info_serno', right_on='serno')
                temp_sale_info['stock_perc'] = temp_sale_info['stock_perc'].fillna(0)
                temp_sale_info['interest_fee'] = temp_sale_info['interest_fee'].fillna(0)
                try:
                    opera_year = self.com_busi_info[self.com_busi_info['serno'].isin(
                        self.com_sale_info['bus_info_serno'].to_list())]['opera_year'].max()
                except TypeError:
                    opera_year = -999
                self.variables['main_business_running_years'] = opera_year

                last_total_sale_amt = sum(temp_sale_info['last_sale_total'] * temp_sale_info['stock_perc'])
                last_total_profit_amt = sum(temp_sale_info['last_gross_total'] * temp_sale_info['stock_perc'])

            bus_total_flow_assets_amt = 0
            bus_total_stock_assets_amt = 0
            bus_total_receivables_assets_amt = 0
            if self.com_asset_info is not None and self.com_asset_info.shape[0] > 0:
                temp_asset_info = pd.merge(self.com_asset_info, self.com_busi_info[['serno', 'stock_perc']],
                                           how='left', left_on='bus_info_serno', right_on='serno')
                temp_asset_info['stock_perc'] = temp_asset_info['stock_perc'].fillna(0)
                temp_asset_info['cost_perc'] = temp_asset_info['cost'] * temp_asset_info['stock_perc']
                self.variables['bus_total_assets_amt'] = temp_asset_info['cost_perc'].sum()
                bus_total_busi_currency_assets_amt = \
                    temp_asset_info[temp_asset_info['asset_typ'] == '1']['cost_perc'].sum()

                bus_total_stock_assets_amt = temp_asset_info[temp_asset_info['asset_typ'] == '4']['cost_perc'].sum()
                bus_total_receivables_assets_amt = \
                    temp_asset_info[temp_asset_info['asset_typ'] == '2']['cost_perc'].sum()
                bus_total_flow_assets_amt = temp_asset_info[
                    temp_asset_info['asset_typ'].isin(['1', '2', '3', '4', '5', '6'])]['cost_perc'].sum()

            bus_total_debt_amt = 0
            bus_total_bank_debt_amt = 0
            if self.com_debt_info is not None and self.com_debt_info.shape[0] > 0:
                temp_debt_info = pd.merge(self.com_debt_info, self.com_busi_info[['serno', 'stock_perc']],
                                          how='left', left_on='bus_info_serno', right_on='serno')
                temp_debt_info['stock_perc'] = temp_debt_info['stock_perc'].fillna(0)
                temp_debt_info['advan_amt'] = temp_debt_info.apply(
                    lambda x: x['advan_amt'] if x['debt_typ'] != '5' else x['advan_amt'] / 2, axis=1)
                temp_debt_info['cost_perc'] = temp_debt_info['advan_amt'] * temp_debt_info['stock_perc']

                self.variables['bus_total_accounts_payable_debt_amt'] = temp_debt_info[
                    temp_debt_info['debt_typ'] == '2']['cost_perc'].sum()

                bus_total_debt_amt = temp_debt_info['cost_perc'].sum()
                bus_total_bank_debt_amt = temp_debt_info[temp_debt_info['debt_typ'] == '1']['cost_perc'].sum()

            inventory_turnover_ratio = (last_total_sale_amt - last_total_profit_amt) / bus_total_stock_assets_amt \
                if bus_total_stock_assets_amt > 0 else -999
            receivables_turnover_ratio = last_total_sale_amt / bus_total_receivables_assets_amt \
                if bus_total_receivables_assets_amt > 0 else -999
            cash_conversion_cycle = 0
            cash_conversion_cycle += 365 / inventory_turnover_ratio if inventory_turnover_ratio > 0 else 0
            cash_conversion_cycle += 365 / receivables_turnover_ratio if receivables_turnover_ratio > 0 else 0
            self.variables['operating_cycle'] = cash_conversion_cycle

            self.variables['quick_debt_to_assets_ratio'] = \
                (per_total_debt_amt + bus_total_debt_amt) / \
                (bus_total_flow_assets_amt + per_total_flow_assets_amt - bus_total_stock_assets_amt) \
                if bus_total_flow_assets_amt + per_total_flow_assets_amt - bus_total_stock_assets_amt != 0 else -999
            self.variables['sale_to_loan_ratio'] = \
                (per_total_bank_debt_amt + bus_total_bank_debt_amt) / last_total_sale_amt \
                if last_total_sale_amt != 0 else -999
        self.variables['total_currency_assets_amt'] = \
            per_currency_assets_amt + bus_total_busi_currency_assets_amt

    def fetch_info(self, table_name):
        sql = """select * from %(table_name)s where report_id = %(report_id)s"""
        df = sql_to_df(sql=sql, params={'table_name': table_name,
                                        'report_id': self.report_id})
        return df

    def base_info(self, req_no):
        sql = """select report_id, report_time from credit_base_info where report_id = 
            (select report_id from credit_parse_request where biz_req_no = %(biz_req_no)s)"""
        base_df = sql_to_df(sql=sql, params={'biz_req_no': req_no})
        if base_df.shape[0] == 0:
            return
        self.report_id = base_df['report_id'].tolist()[0]
        self.report_time = pd.to_datetime(base_df['report_time'].tolist()[0])
        self.pcredit_loan = self.fetch_info('pcredit_loan')
        self.pcredit_info = self.fetch_info('pcredit_info')
        self.pcredit_repayment = self.fetch_info('pcredit_repayment')
        self.pcredit_query = self.fetch_info('pcredit_query_record')

    def credit_variables(self):
        if self.pcredit_loan is not None:
            loan_df = self.pcredit_loan[self.pcredit_loan['account_type'].isin(['01', '02', '03'])]
            credit_df = self.pcredit_loan[self.pcredit_loan['account_type'].isin(['04', '05'])]
            if loan_df.shape[0] > 0:
                self.variables['loan_amount_min_amt'] = \
                    0 if pd.isna(loan_df['loan_amount'].min()) else round(loan_df['loan_amount'].min(), 2)
                loan_amount_df = loan_df[
                    pd.to_datetime(loan_df['loan_date']) >= self.report_time.date() + offsets.DateOffset(months=-6)]
                self.variables['loan_cnt_inc_6m'] = loan_amount_df.shape[0]
                loan_amount_avg_6m = loan_amount_df['loan_amount'].mean()
                self.variables['loan_amount_avg_6m'] = loan_amount_avg_6m \
                    if pd.isna(loan_amount_avg_6m) else round(loan_amount_avg_6m, 2)
                total_bank_credit_limit = 0
                if self.pcredit_info is not None and self.pcredit_info.shape[0] > 0:
                    total_bank_credit_limit = round(self.pcredit_info.loc[0, ['non_revolloan_totalcredit',
                                                                              'revolcredit_totalcredit',
                                                                              'revolloan_totalcredit',
                                                                              'undestroy_limit',
                                                                              'undestory_semi_limit']].sum(), 2)
                    self.variables['credit_guar_bal'] = \
                        round(self.pcredit_info.loc[0, ['ind_repay_balance', 'ent_repay_balance']].sum(), 2)
                guar_type_1_4_amt = loan_df[loan_df['loan_guarantee_type'].isin(['01', '04'])]['loan_amount'].sum()
                self.variables['guar_type_1_4_amt_prop'] = round(guar_type_1_4_amt / total_bank_credit_limit, 2) \
                    if total_bank_credit_limit > 0 else 0
                guar_type_4_avg_amt = loan_df[loan_df['loan_guarantee_type'] == '04']['loan_amount'].mean()
                self.variables['guar_type_4_avg_amt'] = 0 if pd.isna(guar_type_4_avg_amt) else guar_type_4_avg_amt

                business_loan_df = loan_df[(loan_df['loan_type'].isin(['01', '07', '99'])) |
                                           ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] > 200000))]
                if business_loan_df.shape[0] > 0:
                    before_3m = self.report_time + offsets.DateOffset(months=-3)
                    before_6m = self.report_time + offsets.DateOffset(months=-6)
                    business_overdue_df = self.pcredit_repayment[
                        (self.pcredit_repayment['record_id'].isin(business_loan_df['id'].tolist())) &
                        ((self.pcredit_repayment['status'].str.isdigit()) |
                         (self.pcredit_repayment['repayment_amt'] > 0))]
                    self.variables['single_business_loan_overdue_cnt_3m'] = business_overdue_df[
                        ((self.pcredit_repayment['jhi_year'] > before_3m.year) |
                         ((self.pcredit_repayment['jhi_year'] == before_3m.year) &
                          (self.pcredit_repayment['month'] > before_3m.month)))]['record_id'].value_counts().max()
                    self.variables['single_business_loan_overdue_cnt_6m'] = business_overdue_df[
                        ((self.pcredit_repayment['jhi_year'] > before_6m.year) |
                         ((self.pcredit_repayment['jhi_year'] == before_6m.year) &
                          (self.pcredit_repayment['month'] > before_6m.month)))]['record_id'].value_counts().max()

            if credit_df.shape[0] > 0:
                before_5y = self.report_time + offsets.DateOffset(years=-5)
                credit_overdue_df = self.pcredit_repayment[
                    (self.pcredit_repayment['record_id'].isin(credit_df['id'].tolist())) &
                    (self.pcredit_repayment['repayment_amt'] > 1000) &
                    ((self.pcredit_repayment['jhi_year'] > before_5y.year) |
                     ((self.pcredit_repayment['jhi_year'] == before_5y.year) &
                      (self.pcredit_repayment['month'] > before_5y.month)))]
                self.variables['credit_min_payed_number'] = \
                    credit_df[credit_df['repay_amount'] * 2 > credit_df['amout_replay_amount']].shape[0]
                if credit_overdue_df.shape[0] > 0:
                    self.variables['credit_card_max_overdue_amt_60m'] = credit_overdue_df['repayment_amt'].max()
                    self.variables['credit_card_max_overdue_month_60m'] = \
                        credit_overdue_df['record_id'].value_counts().max()

        if self.pcredit_query is not None:
            before_3m = self.report_time + offsets.DateOffset(months=-3)
            self.variables['query_cnt_3m'] = self.pcredit_query[
                ((self.pcredit_query['reason'].isin(['01', '02', '08'])) |
                 (self.pcredit_query['reason'].str.contains('融资审批|贷款审批|信用卡审批|保前审查'))) &
                (self.pcredit_query['jhi_time'] >= before_3m.date)
            ].groupby(['operator', 'reason']).agg({'report_id': len}).shape[0]

    def credit_transform(self):
        strategy_param = self.full_msg.get('strategyParam')
        query_data = strategy_param.get('queryData')
        if query_data is None or len(query_data) == 0:
            return
        per_msg = None
        for i in range(len(query_data)):
            if query_data[i].get('idno') == self.id_card_no:
                per_msg = query_data[i].get('extraParam')
        if per_msg is None:
            return
        req_no = per_msg.get('creditParseReqNo')
        self.base_info(req_no)
        self.credit_variables()

    def basic_data(self):
        strategy_param = self.full_msg.get('strategyParam')
        extra_param = strategy_param.get('extraParam')
        if extra_param is None:
            return
        app_no = extra_param.get('outApplyNo')
        if app_no is not None:
            sql = """select * from trans_u_flow_portrait where report_req_no = 
            (select report_req_no from trans_apply where apply_no = %(app_no)s order by id desc limit 1)"""
            self.trans_u_flow = sql_to_df(sql=sql, params={'app_no': app_no})

    def flow_variables(self):
        if self.trans_u_flow is not None and self.trans_u_flow.shape[0] > 0:
            # self.trans_u_flow['trans_date'] = self.trans_u_flow['trans_time'].apply(lambda x: x.date)
            trans_max = pd.to_datetime(self.trans_u_flow['trans_date'].max())
            # 剔除强关联关系及异常交易类型
            temp_trans_detail = self.trans_u_flow[
                (pd.isnull(self.trans_u_flow['relationship'])) &
                (pd.isnull(self.trans_u_flow['unusual_trans_type'])) &
                (pd.to_datetime(self.trans_u_flow['trans_date']) >= trans_max + offsets.DateOffset(months=-6))]
            if temp_trans_detail.shape[0] > 0:
                operating_income_6m = temp_trans_detail[
                    temp_trans_detail['trans_amt'] > 0]['trans_amt'].sum()
                self.variables['operating_income_6m'] = operating_income_6m / 10000
                temp_trans_detail.drop_duplicates(subset='trans_date', keep='last', inplace=True)
                date_list = temp_trans_detail['trans_date'].tolist()
                amt_list = temp_trans_detail['account_balance'].tolist()
                diff_days = [(date_list[i + 1] - date_list[i]).days for i in range(len(date_list) - 1)]
                diff_days.append(1)
                total_days = sum(diff_days)
                total_amt_list = [diff_days[j] * amt_list[j] for j in range(len(date_list))]
                balance_day_avg_6m = sum(total_amt_list) / total_days if total_days != 0 else 0
                self.variables['balance_day_avg_6m'] = balance_day_avg_6m / 10000
                self.variables['flow_limit_amt'] = round(max(operating_income_6m / 150000, balance_day_avg_6m * 50000))

    def trans_transform(self):
        self.basic_data()
        self.flow_variables()

    def transform(self):
        if self.id_card_no is not None and self.phone is not None:
            self.all_variables()
            self.credit_transform()
            self.trans_transform()
        else:
            if self.origin_data is not None:
                if len(self.origin_data) == 2:
                    main_amt_pred = self.origin_data[0].get('loan_amt_pred')
                    spouse_amt_pred = self.origin_data[1].get('loan_amt_pred')
                    main_model_pred = self.origin_data[0].get('model_pred')
                    spouse_model_pred = self.origin_data[1].get('model_pred')
                    flow_limit_amt = self.origin_data[0].get('flow_limit_amt')
                    if main_amt_pred is not None and spouse_amt_pred is not None:
                        self.variables['loan_amt_pred_avg'] = math.floor((main_amt_pred + spouse_amt_pred) / 2)
                        self.variables['model_pred'] = max(main_model_pred, spouse_model_pred)
                    else:
                        self.variables['model_pred'] = 0
                    if flow_limit_amt is not None:
                        self.variables['flow_limit_amt'] = flow_limit_amt
                    else:
                        self.variables['flow_limit_amt'] = 0
                    self.all_variables(final_call=1)


            is_first_loan = jsonpath(self.full_msg, "$.strategyParam.extraParam.isFirstLoan")
            apply_amt= jsonpath(self.full_msg, '$..strategyParam.queryData[*].applyAmo')
            if apply_amt and len(apply_amt)>0:
                self.variables['apply_amt'] = apply_amt[0]
            else:
                self.variables['apply_amt'] = 0
            if is_first_loan and len(is_first_loan) > 0:
                self.variables["is_first_loan"] = is_first_loan[0]
