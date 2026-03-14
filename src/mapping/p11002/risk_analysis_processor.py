from datetime import datetime

import pandas as pd
import sys
from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df

'''
风险分析
财报与流水交叉校验
'''


class RiskAnalysisProcessor(ModuleProcessor):

    def process(self):
        self.get_month()
        # 数字纠偏
        for i in ['_T', '_T1', '_T2']:
            self.digital_correction(i)

        # 红黄预警
        self.risk_warning()

        # 可视化指标
        self.visualized_analysis()

        # 财报与流水交叉验证
        # self._cross_check_financial_statements_with_flow()

    # def _cross_check_financial_statements_with_flow(self):
    #     # 财报时间范围，取最近的一份年报对应时间
    #     self.variables['financial_report_time_range']=self.variables['asset_dept_year']+"/12/01-"+self.variables['asset_dept_year']+"/12/01"
    #     target_year='_T1'
    #     # 流水数据获取,取数据库表数据进行赋值
    #
    #     # 指标赋值
    #     # 货币资金
    #     cash_funds_financial=self.empty_to_zero_enhanced('cash_funds'+target_year)
    #     cash_funds_bank=''
    #     cash_funds_diff=cash_funds_bank-cash_funds_financial
    #     cash_funds_ratio=cash_funds_diff/cash_funds_financial if cash_funds_financial>0 else ''
    #     cash_funds_tip='收集流水的货币资金与财报数据差距较大，建议收集其他经营流水。' if cash_funds_ratio<-0.3 else '收集流水的流水期末余额较大，建议关注流水真实性。' if cash_funds_ratio>0.3 else ''
    #
    #     result_list=[]
    #     result_list.extend(
    #         {
    #             "belonging_statement": "资产负债表",
    #             "indicator": "货币资金",
    #             "financial_statement_value": cash_funds_financial,  # 财报数值
    #             "transaction_credit_value": cash_funds_bank,  # 流水数值
    #             "difference": cash_funds_diff,  # 差额
    #             "deviation_ratio": cash_funds_ratio,  # 偏差比例
    #             "risk_warning": cash_funds_tip,  # 风险提示
    #         })
    #     self.variables['cross_check_financial_statements_with_flow']=result_list

    def get_month(self):
        if len(self.cached_data.get('ASSET_DEBT')) > 0 and self.cached_data.get('ASSET_DEBT').get(
                'end_date_t') is not None:
            self.asset_dept_month_T = int(self.cached_data.get('ASSET_DEBT').get('end_date_t')[5:7])
            if self.asset_dept_month_T >= 7:
                self.variables['asset_dept_year'] = self.variables['asset_dept_T']
            else:
                if self.cached_data.get('ASSET_DEBT').get('end_date_t1') is not None:
                    self.variables['asset_dept_year'] = self.variables['asset_dept_T1']
        else:
            self.asset_dept_month_T = ''

        if len(self.cached_data.get('PROFIT')) > 0 and self.cached_data.get('PROFIT').get('end_date_t') is not None:
            self.profit_month_T = int(self.cached_data.get('PROFIT').get('end_date_t')[5:7])
            if self.profit_month_T >= 7:
                self.variables['profit_year'] = self.variables['profit_T']
            else:
                if self.cached_data.get('PROFIT').get('end_date_t1') is not None:
                    self.variables['profit_year'] = self.variables['profit_T1']
        else:
            self.profit_month_T = ''

    # 数字纠偏
    def digital_correction(self, T):
        # 流动资产合计与明细项目不一致，请核实各项数据
        if isinstance(self.variables['total_current' + T], (float, int)) and self.sum_financial_metrics(
                ["cash_funds", "fair_value_assets", "derivative_assets", "notes_receivable", "accounts_receivable",
                 "prepaid", "other_receivables", "inventory", "assets_for_sale", "due_assets", "other_current"], T) != \
                self.variables['total_current' + T]:
            self.variables['check_current_assets' + T] = 1
        # 非流动资产合计与明细项目不一致，请核实各项数据
        if isinstance(self.variables['total_non_current_assets' + T], (float, int)) and self.sum_financial_metrics(
                ["available_for_sale_assets", "held_to_maturity_investments", "long_term_receivables",
                 "long_term_equity_investments", "investment_real_estate", "fixed_assets", "construction_in_progress",
                 "productive_biological_assets", "oil_and_gas_assets", "intangible_assets", "development_expenditures",
                 "goodwill", "long_term_prepaid_expenses", "deferred_tax_assets", "other_non_current_assets"], T) != \
                self.variables['total_non_current_assets' + T]:
            self.variables['check_noncurrent_assets' + T] = 1
        # 流动负债合计与明细项目不一致，请核实各项数据
        if isinstance(self.variables['total_current_liabilities' + T], (float, int)) and self.sum_financial_metrics(
                ["short_term_loans", "fair_value_liabilities", "derivative_liabilities", "notes_payable",
                 "accounts_payable", "advances_from_customers", "salaries_payable", "taxes_payable", "other_payables",
                 "liabilities_for_sale", "due_liabilities", "other_current_liabilities"], T) != self.variables[
            'total_current_liabilities' + T]:
            self.variables['check_current_liabilities' + T] = 1
        # 非流动负债合计与明细项目不一致，请核实各项数据
        if isinstance(self.variables['total_non_current_liabilities' + T], (float, int)) and self.sum_financial_metrics(
                ["long_term_loans", "bonds_payable", "long_term_payables", "provisions", "deferred_income",
                 "deferred_tax_liabilities", "other_non_current_liabilities"], T) != self.variables[
            'total_non_current_liabilities' + T]:
            self.variables['check_noncurrent_liabilities' + T] = 1
        # 所得者权益（或股东权益）总计与明细项目不一致，请核实各项数据
        if isinstance(self.variables['total_equity' + T], (float, int)) and self.sum_financial_metrics(
                ["paid_in_capital", "other_equity_instruments", "capital_reserve", "other_comprehensive_income",
                 "specific_reserves", "surplus_reserves", "undistributed_profits"], T) != self.variables[
            'total_equity' + T]:
            self.variables['check_equity' + T] = 1
        # 资产总计!=负债合计+所有者权益，请核实各项数据
        if isinstance(self.variables['total_assets' + T], (float, int)) and self.sum_financial_metrics(
                ["total_equity", "total_liabilities"], T) != self.variables['total_assets' + T]:
            self.variables['check_assets_eq_liabilities' + T] = 1
        # 期间费用与明细项目不一致，请核实各项数据
        if isinstance(self.variables['period_cost' + T], (float, int)) and self.sum_financial_metrics(
                ["selling_expense", "admin_expense", "r_and_d_expense", "finance_expense", "other_expense"], T) != \
                self.variables['period_cost' + T]:
            self.variables['check_period_expenses' + T] = 1
        # 毛利润！=营业收入-营业成本，请核实各项数据
        if isinstance(self.variables['gross_profit' + T], (float, int)) and round(
                self.sum_financial_metrics(["revenue"], T) - self.sum_financial_metrics(["operating_cost"], T), 4) != \
                self.variables['gross_profit' + T]:
            self.variables['check_gross_profit' + T] = 1
        # 营业利润！=毛利润-税金及附加-期间费用+其他收益-资产减值损失，请核实各项数据
        if isinstance(self.variables['operating_profit' + T], (float, int)) and round(
                self.sum_financial_metrics(["gross_profit"], T) - self.sum_financial_metrics(["taxes_surcharges"],
                                                                                             T) - self.sum_financial_metrics(
                        ["selling_expense", "admin_expense", "r_and_d_expense", "finance_expense", "other_expense"],
                        T) + self.sum_financial_metrics(["other_income"], T) - self.sum_financial_metrics(
                        ["asset_impairment"], T), 4) != self.variables['operating_profit' + T]:
            self.variables['check_operating_profit' + T] = 1
        # 利润总额！=营业利润+营业外收入-营业外支出，请核实各项数据
        if isinstance(self.variables['total_profit' + T], (float, int)) and round(
                self.sum_financial_metrics(["operating_profit"], T) + self.sum_financial_metrics(
                        ["non_operating_income"], T) - self.sum_financial_metrics(["non_operating_expense"], T), 4) != \
                self.variables['total_profit' + T]:
            self.variables['check_total_profit' + T] = 1
        # 净利润！=利润总额-所得税费用，请核实各项数据
        if isinstance(self.variables['net_profit' + T], (float, int)) and round(
                self.sum_financial_metrics(["total_profit"], T) - self.sum_financial_metrics(["income_tax"], T), 4) != \
                self.variables['net_profit' + T]:
            self.variables['check_net_profit' + T] = 1
        # 现金流量表各项主要指标存在不平衡，请核实各项数据。
        if isinstance(self.variables['net_increase_in_cash' + T], (float, int)) and self.sum_financial_metrics(
                ["net_operating_cash_flow", "net_investment_cash_flow", "net_financing_cash_flow"], T) != \
                self.variables['net_increase_in_cash' + T]:
            self.variables['check_cashflow_balance' + T] = 1
        # 货币资金！=期末现金及现金等价物余额-期初现金及现金等价物余额，存在勾稽问题。
        # cash_funds=self.empty_to_zero_enhanced('cash_funds' + T)
        # change_cash_balance=round(self.empty_to_zero_enhanced('ending_cash_balance' + T)-self.empty_to_zero_enhanced('beginning_cash_balance' + T),2)
        # print("货币资金！=期末现金及现金等价物余额-期初现金及现金等价物余额，存在勾稽问题,",cash_funds,change_cash_balance)
        # if cash_funds!=change_cash_balance:
        #     self.variables['check_cash_funds' + T] = 1

        # 净利润！=期末未分配利润 - 期初未分配利润，存在勾稽问题
        T1=""
        if T=='_T':
            T1="_T1"
        elif T=="_T1":
            T1="_T2"
        if T in ('_T','_T1'):
            nert_profit=self.empty_to_zero_enhanced('net_profit' + T)
            change_undistributed_profits = round(
                self.empty_to_zero_enhanced('undistributed_profits' + T) - self.empty_to_zero_enhanced(
                    'undistributed_profits' + T1), 2)
            # print("净利润！=期末未分配利润 - 期初未分配利润，存在勾稽问题,",nert_profit,change_undistributed_profits)
            if nert_profit != change_undistributed_profits:
                self.variables['check_undistributed_profits' + T] = 1

    def sum_financial_metrics(self, list_key, T):
        value = 0
        for i in list_key:
            if isinstance(self.variables[i + T], (float, int)):
                value += self.variables[i + T]
        return round(value, 4)

    # 红黄预警
    def risk_warning(self):
        # T年损益表与现金流量表利润不匹配，请核实各项数据。
        if isinstance(self.variables['net_increase_in_cash_T'], (float, int)) and isinstance(
                self.variables['net_increase_in_cash_T1'], (float, int)) and isinstance(self.variables['net_profit_T'],
                                                                                        (float, int)) and \
                self.variables['net_profit_T'] > self.variables['net_increase_in_cash_T'] - self.variables[
            'net_increase_in_cash_T1']:
            self.variables['check_pnl_cashflow_T'] = 1
        # T-1年损益表与现金流量表利润不匹配，请核实各项数据。
        if isinstance(self.variables['net_increase_in_cash_T1'], (float, int)) and isinstance(
                self.variables['net_increase_in_cash_T2'], (float, int)) and isinstance(self.variables['net_profit_T1'],
                                                                                        (float, int)) and \
                self.variables['net_profit_T1'] > self.variables['net_increase_in_cash_T1'] - self.variables[
            'net_increase_in_cash_T2']:
            self.variables['check_pnl_cashflow_T1'] = 1

        # 资产负债率
        if isinstance(self.variables['total_liabilities_T'], (float, int)) and isinstance(
                self.variables['total_assets_T'], (float, int)) and self.variables['total_assets_T'] != 0:
            self.variables['debt_asset_ratio_T'] = self.variables['total_liabilities_T'] / self.variables[
                'total_assets_T']

        if isinstance(self.variables['total_liabilities_T1'], (float, int)) and isinstance(
                self.variables['total_assets_T1'], (float, int)) and self.variables['total_assets_T1'] != 0:
            self.variables['debt_asset_ratio_T1'] = self.variables['total_liabilities_T1'] / self.variables[
                'total_assets_T1']

        # 净利润增长率
        self.growth_rate('net_profit', 'net_profit_growth_rate', 'ASSET_DEBT')

        # 营业成本增长率
        self.growth_rate('operating_cost', 'cost_growth_rate', 'PROFIT')

        # 存货增长率
        self.growth_rate('inventory', 'inventory_growth_rate', 'ASSET_DEBT')

        # 营业收入增长率
        self.growth_rate('revenue', 'revenue_growth_rate', 'PROFIT')

        # 水电增长率
        self.growth_rate('utilities', 'utilities_growth_rate', 'PROFIT')

        # 应收账款增长率
        self.growth_rate('accounts_receivable', 'accounts_receivable_growth_rate', 'ASSET_DEBT')

        # 应收账款周转率
        if self.variables['receivables_turnover_T'] != 0 and self.variables['receivables_turnover_T'] != "":
            self.variables['accounts_receivable_turnover_rate'] = 360 / self.variables['receivables_turnover_T']

        # 应收占流动资产的比例
        if isinstance(self.variables['accounts_receivable_T'], (float, int)) and isinstance(
                self.variables['total_current_T'], (float, int)) and self.variables['total_current_T'] != 0:
            self.variables['receivables_current_assets_ratio'] = self.variables['accounts_receivable_T'] / \
                                                                 self.variables['total_current_T']

        # 应付账款增长率
        self.growth_rate('accounts_payable', 'accounts_payable_growth_rate', 'ASSET_DEBT')

        # 资产
        if isinstance(self.variables['fixed_assets_T'], (float, int)) and isinstance(self.variables['fixed_assets_T1'],
                                                                                     (float, int)) and isinstance(
                self.variables['intangible_assets_T'], (float, int)) and isinstance(
                self.variables['intangible_assets_T1'], (float, int)) and self.variables['intangible_assets_T1'] + \
                self.variables['fixed_assets_T1'] != 0:
            self.variables['fixed_and_intangible_assets'] = (self.variables['fixed_assets_T'] + self.variables[
                'intangible_assets_T'] - self.variables['fixed_assets_T1'] - self.variables['intangible_assets_T1']) / (
                                                                        self.variables['intangible_assets_T1'] +
                                                                        self.variables['fixed_assets_T1'])

        # 主营成本增长率
        if isinstance(self.variables['main_cost_T'], (float, int)) and isinstance(self.variables['main_cost_T1'],
                                                                                  (float, int)) and self.variables[
            'main_cost_T1'] != 0:
            self.variables['main_business_cost'] = (self.variables['main_cost_T'] - self.variables['main_cost_T1']) / \
                                                   self.variables['main_cost_T1']

        # 管理费用增长率
        self.growth_rate('admin_expense', 'admin_expense_growth_rate', 'PROFIT')

        # 主营收入增长率
        self.growth_rate('main_revenue', 'main_revenue_growth_rate', 'PROFIT')

        # 增加红黄预警相关指标
        # 经营活动产生的现金流量净额增长率,当分母为负数时的特殊处理,分母用绝对值
        self.growth_rate("net_operating_cash_flow", "operating_cash_flow_growth_rate", "CASH_FLOW")

    def growth_rate(self, key1, key2, table_type):
        if table_type == 'ASSET_DEBT':
            month_T = self.asset_dept_month_T
        else:
            month_T = self.profit_month_T

        if month_T != '':
            if month_T >= 7:
                if isinstance(self.variables[key1 + '_T'], (float, int)) and isinstance(self.variables[key1 + '_T1'],
                                                                                        (float, int)) and \
                        self.variables[key1 + '_T1'] != 0:
                    self.variables[key2] = (self.variables[key1 + '_T'] / month_T * 12 - self.variables[key1 + '_T1']) / \
                                           abs(self.variables[key1 + '_T1'])
            else:
                if isinstance(self.variables[key1 + '_T1'], (float, int)) and isinstance(self.variables[key1 + '_T2'],
                                                                                         (float, int)) and \
                        self.variables[key1 + '_T2'] != 0:
                    self.variables[key2] = (self.variables[key1 + '_T1'] - self.variables[key1 + '_T2']) / \
                                           abs(self.variables[key1 + '_T2'])

    # 可视化指标
    def visualized_analysis(self):
        # 折线图-近三年经营利润情况 和 ”资产负债率指标“部分指标
        # 可视化指标初始值需为0，不能复用表格中的指标
        line_chart = ['revenue_T', 'revenue_T1', 'revenue_T2', 'operating_cost_T', 'operating_cost_T1',
                      'operating_cost_T2', 'gross_margin_T', 'gross_margin_T1', 'gross_margin_T2', 'net_margin_T',
                      'net_margin_T1', 'net_margin_T2', 'debt_asset_ratio_T', 'current_ratio_T', 'quick_ratio_T',
                      'cash_ratio_T']
        for var in line_chart:
            if isinstance(self.variables[var], (int, float)):
                # 根据后端命名需求做更改
                if var[-2] == '_':
                    self.variables[var[:-2] + '_view' + var[-2:]] = self.variables[var]
                else:
                    self.variables[var[:-3] + '_view' + var[-3:]] = self.variables[var]

        # 营业及成本分布
        sum_value = 0
        for process in ['sum', 'ratio']:
            for i in ['operating_cost_T', 'taxes_surcharges_T', 'selling_expense_T', 'admin_expense_T',
                      'finance_expense_T', 'other_expense_T', 'asset_impairment_T', 'operating_profit_T']:
                if isinstance(self.variables[i], (int, float)):
                    if process == 'sum':
                        sum_value += self.variables[i]
                    else:
                        if sum_value != 0:
                            self.variables[i.replace('_T', '') + '_ratio'] = self.variables[i] / sum_value

        # 较去年资产负债率增长了
        if isinstance(self.variables['debt_asset_ratio_T'], (int, float)) and isinstance(
                self.variables['debt_asset_ratio_T1'], (int, float)):
            self.variables['debt_asset_ratio_T_growth'] = self.variables['debt_asset_ratio_T'] - self.variables[
                'debt_asset_ratio_T1']

        # 存货周转率
        if isinstance(self.variables['operating_cost_T'], (int, float)) and isinstance(self.variables['inventory_T'],
                                                                                       (int, float)) and isinstance(
                self.variables['inventory_T1'], (int, float)) and self.variables['inventory_T'] + self.variables[
            'inventory_T1'] != 0:
            self.variables['inventory_turnover_rate'] = self.variables['operating_cost_T'] / (
                        (self.variables['inventory_T'] + self.variables['inventory_T1']) / 2)

        # 总资产周转率
        if isinstance(self.variables['revenue_T'], (int, float)) and isinstance(self.variables['total_assets_T'],
                                                                                (int, float)) and isinstance(
                self.variables['total_assets_T1'], (int, float)) and (
                self.variables['total_assets_T'] + self.variables['total_assets_T1']) != 0:
            self.variables['total_asset_turnover'] = self.variables['revenue_T'] / (
                        self.variables['total_assets_T'] + self.variables['total_assets_T1']) / 2

        # 应收账款周转率
        if self.variables['accounts_receivable_turnover_rate'] != 9999:
            self.variables['receivable_turnover_rate'] = self.variables['accounts_receivable_turnover_rate']
        # 资产负债分布-资产分布
        if self.empty_to_zero_enhanced('total_assets_T') > 0:
            self.asset_pie()
        # 资产负债分布-负债分布
        if self.empty_to_zero_enhanced('total_liabilities_T') > 0:
            self.debt_pie()

        '''
        筛选有12月份财报的年份
        '''
        sql='''
        SELECT * FROM finance_table_info WHERE  task_no=%(task_no)s
        '''
        year_df=sql_to_df(sql,params={"task_no":self.cached_data["task_no"]})
        '''
        需要做如下判断：
        1.同一年是否包含财务三表：资产负债表，损益表，现金流量表
        2.每个年份对应的是T、T1、T2
        '''
        year_dict = {
        }
        t_year=str(self.variables['profit_T'])[:4]
        if t_year!='':
            t1_year = str(int(t_year) - 1)
            t2_year = str(int(t_year) - 2)
            year_dict={
                t_year:"_T",
                t1_year:"_T1",
                t2_year:"_T2"
            }
            year_df['end_date']=pd.to_datetime(year_df['end_date'])
            december_years = year_df[year_df['end_date'].dt.month == 12]['end_date'].dt.year.unique()
            year_list=[str(year) for year in december_years]
            if len(year_list)>0:
                # 杜邦分析
                for year in year_list :
                    print("year:",year,str(int(year) - 1),year_dict.keys())
                    if str(year) in year_dict.keys() and str(int(year) - 1) in year_dict.keys():
                        temp_dict = self.du_pond_analysis(year,year_dict[year],year_dict[str(int(year) - 1)])
                        self.variables['du_pond_analysis'].append(temp_dict)
                # 因素分析
                if len(self.variables['du_pond_analysis']) > 1:
                    # 按年份排序，确保正确的前后顺序
                    sorted_dicts = sorted(self.variables['du_pond_analysis'], key=lambda x: x['year'])
                    last_year = sorted_dicts[0]
                    current_year = sorted_dicts[1]
                    change_in_return_on_equity = current_year['return_on_equity'] - last_year['return_on_equity']
                    # 上年净资产收益率
                    return_on_equity = "{:.2f}%*{:.4f}*{:.4f}={:.2f}%  (1)".format(last_year['operating_profit_margin'] * 100,
                                                                           last_year['total_asset_turnover'],
                                                                           last_year['equity_multiplier'],
                                                                           last_year['return_on_equity'] * 100)
                    # 替代营业净利润
                    roe_2 = current_year['operating_profit_margin'] * 100 * last_year['total_asset_turnover'] * last_year[
                        'equity_multiplier']
                    adjusted_net_operating_profit = "{:.2f}%*{:.4f}*{:.4f}={:.2f}%  (2)".format(
                        current_year['operating_profit_margin'] * 100, last_year['total_asset_turnover'],
                        last_year['equity_multiplier'], roe_2)
                    # 替代总资产周转率
                    roe_3 = current_year['operating_profit_margin'] * 100 * current_year['total_asset_turnover'] * last_year[
                        'equity_multiplier']
                    adjusted_total_asset_turnover = "{:.2f}%*{:.4f}*{:.4f}={:.2f}%  (3)".format(
                        current_year['operating_profit_margin'] * 100, current_year['total_asset_turnover'],
                        last_year['equity_multiplier'], roe_3)
                    # 替代权益系数
                    roe_4 = current_year['operating_profit_margin'] * 100 * current_year['total_asset_turnover'] * current_year[
                        'equity_multiplier']
                    adjusted_equity_multiplier = "{:.2f}%*{:.4f}*{:.4f}={:.2f}%  (4)".format(
                        current_year['operating_profit_margin'] * 100, current_year['total_asset_turnover'],
                        current_year['equity_multiplier'], roe_4)
                    # 营业净利率变动的影响
                    impact_of_operating_profit_margin_change = "(2)-(1)={:.2f}%-{:.2f}%={:.2f}%".format(roe_2, last_year[
                        'return_on_equity'] * 100, roe_2 - last_year['return_on_equity'] * 100)
                    impact_of_total_asset_turnover_change = "(3)-(2)={:.2f}%-{:.2f}%={:.2f}%".format(roe_3, roe_2,
                                                                                                     roe_3 - roe_2)
                    impact_of_equity_multiplier_change = "(4)-(3)={:.2f}%-{:.2f}%={:.2f}%".format(roe_4, roe_3, roe_4 - roe_3)
                    self.variables['factor_analysis'] = {
                        "change_in_return_on_equity": change_in_return_on_equity,  # 净资产收益率变动
                        "return_on_equity": return_on_equity,  # 上年净资产收益率
                        "adjusted_net_operating_profit": adjusted_net_operating_profit,  # 替代营业净利润
                        "adjusted_total_asset_turnover": adjusted_total_asset_turnover,  # 替代总资产周转率
                        "adjusted_equity_multiplier": adjusted_equity_multiplier,  # 替代权益系数
                        "impact_of_operating_profit_margin_change": impact_of_operating_profit_margin_change,  # 营业净利率变动的影响
                        "impact_of_total_asset_turnover_change": impact_of_total_asset_turnover_change,  # 总资产周转率变动的影响
                        "impact_of_equity_multiplier_change": impact_of_equity_multiplier_change,  # 权益系数变动的影响
                    }

        # 利润画像，取最新完整年报的数据
        year_df['end_date']=pd.to_datetime(year_df['end_date'])
        profit_df=year_df[(year_df['end_date'].dt.month == 12) & (year_df['table_type']=='PROFIT')]
        if profit_df.shape[0]>0:
            profit_year_list=profit_df['end_date'].dt.year.unique()
            if len(profit_year_list)>0:
                if str(max(profit_year_list)) in year_dict.keys():
                    profit_year=year_dict[str(max(profit_year_list))]
                    operating_profit = self.empty_to_zero_enhanced('operating_profit'+profit_year)  # 营业利润
                    investment_income = self.empty_to_zero_enhanced('investment_income'+profit_year)  # 投资收益
                    non_operating_income_and_expense = self.empty_to_zero_enhanced('non_operating_income'+profit_year) - self.empty_to_zero_enhanced(
                        'non_operating_expense'+profit_year)  # 营业外收支
                    net_profit = self.empty_to_zero_enhanced('net_profit'+profit_year)  # 净利润
                    operating_profit_flag = '+' if operating_profit > 0 else '-' if operating_profit<0 else '/'
                    investment_income_flag = '+' if investment_income > 0 else '-' if investment_income<0 else '/'
                    non_operating_income_and_expense_flag = '+' if non_operating_income_and_expense > 0 else '-' if non_operating_income_and_expense<0 else '/'
                    net_profit_flag = '+' if net_profit > 0 else '-' if net_profit<0 else '/'
                    flag = operating_profit_flag + investment_income_flag + non_operating_income_and_expense_flag + net_profit_flag
                    summary_dict = {
                        "++++": "盈利能力比较稳定，状况比较好。",
                        "++--": "虽然企业的利润为负，但是是由于企业的营业外收支导致。不构成企业的经常性利润，所以并不影响企业的盈利能力状况，这种亏损状况是暂时的。​​建议​​核实非经常性亏损是否持续影响，关注扣除非经营性损益后的净利润。",
                        "+---": "企业的盈利情况比较差，投资业务失利导致企业的经营性利润比较差，企业的盈利能力不够稳定。",
                        "-+++": "企业的利润水平依赖于企业的投资业务和营业外业务，其投资项目的好坏直接关系到企业的盈利能力，B135应该关注其项目收益的稳定性。",
                        "--++": "企业的盈利状况很差，虽然当年盈利，但是其经营依赖于企业的营业外收支（如政府补贴、资产处置），持续下去会导致企业破产。",
                        "----": "企业的盈利状况非常差，企业的财务状况存在危机。"
                    }
                    summary = summary_dict[flag] if flag in summary_dict.keys() else ""
                    self.variables['profit_portrait'] = {
                        "operating_profit": operating_profit,
                        "investment_income": investment_income,
                        "non_operating_income_and_expense": non_operating_income_and_expense,
                        "net_profit": net_profit,
                        "summary": summary
                    }

        # 现金流画像,覆盖3年 没有传现金流报表，不展示
        cash_flow_df = year_df[(year_df['table_type'] == 'CASH_FLOW')]
        if cash_flow_df.shape[0]>0:
            task_no = self.cached_data["task_no"]
            sql = '''
            SELECT * FROM finance_report_parse_record 
            where task_no = %(task_no)s  order by created_date DESC LIMIT 1
            '''
            df_finance_report_parse_record = sql_to_df(sql=sql, params={"task_no": task_no})
            company_name = ''
            if df_finance_report_parse_record.shape[0] > 0:
                company_name = df_finance_report_parse_record.at[0, 'subject_name']  # 企业名称
            current_date = datetime.now()
            if self.variables['basic_es_date']!="":
                es_date = pd.to_datetime(self.variables['basic_es_date'])
                age = current_date.year - es_date.year
                # 如果今年生日未过，则减1年
                if (current_date.month, current_date.day) < (es_date.month, es_date.day):
                    age -= 1
                years_since_establishment = age  # 成立年限
            else:
                years_since_establishment=0
            industry = self.variables['basic_industry_phyname']  # 行业
            net_cash_flow_from_investing_activities = [self.variables['net_investment_cash_flow_T2'],
                                                       self.variables['net_investment_cash_flow_T1'],
                                                       self.variables['net_investment_cash_flow_T']],  # 投资活动产生的现金流量净额
            # （固定资产 + 无形资产）增长率
            a = self.empty_to_zero_enhanced('fixed_assets_T') + self.empty_to_zero_enhanced('intangible_assets_T')
            b = self.empty_to_zero_enhanced('fixed_assets_T1') + self.empty_to_zero_enhanced('intangible_assets_T1')
            c = self.empty_to_zero_enhanced('fixed_assets_T2') + self.empty_to_zero_enhanced('intangible_assets_T2')
            growth_rate_of_fixed_and_intangible_assets = [(a - b) / b if b > 0 else 0, (b - c) / c if c > 0 else 0]
            # 有形资产占比
            total_assets = self.empty_to_zero_enhanced('total_assets_T')
            tangible_assets_ratio = (self.empty_to_zero_enhanced('fixed_assets_T') + self.empty_to_zero_enhanced(
                'inventory_T')) / total_assets if total_assets > 0 else 0
            net_cash_flow_from_operating_activities = [self.variables['net_operating_cash_flow_T2'],
                                                       self.variables['net_operating_cash_flow_T1'],
                                                       self.variables['net_operating_cash_flow_T']],  # 经营活动产生的现金流量净额
            net_cash_flow_from_financing_activities = [self.variables['net_financing_cash_flow_T2'],
                                                       self.variables['net_financing_cash_flow_T1'],
                                                       self.variables['net_financing_cash_flow_T']],  # 筹资活动产生的现金流量净额
            # 画像结论
            net_operating_cash_flow_flag = "+" if self.empty_to_zero_enhanced('net_operating_cash_flow_T') > 0 else '-' if self.empty_to_zero_enhanced('net_operating_cash_flow_T') <0 else '/'
            net_investment_cash_flow_flag = "+" if self.empty_to_zero_enhanced('net_investment_cash_flow_T') > 0 else '-' if self.empty_to_zero_enhanced('net_investment_cash_flow_T') < 0 else '/'
            net_financing_cash_flow_flag = "+" if self.empty_to_zero_enhanced('net_financing_cash_flow_T') > 0 else "-" if self.empty_to_zero_enhanced('net_financing_cash_flow_T') < 0 else '/'
            flag = net_operating_cash_flow_flag + net_investment_cash_flow_flag + net_financing_cash_flow_flag
            cash_flow_portrait_class_dict = {
                "+++": "公司主营业务在现金流方面能自给自足，投资方面收益状况良好，这时仍然进行融资，如果没有新的投资机会，会造成资金浪费。",
                "++-": "公司进入成熟期，经营现金流净额足以支持公司，若投资现金流净额为正不是因为变卖资产，说明公司情况良好，但缺少进一步发展的动力。",
                "+-+": "公司经营状况良好，通过筹集资金进行投资，企业往往处于扩张时期，应该着重分析投资项目的盈利能力。",
                "+--": "若经营净现金流大于其他两项，则企业运营良好，自身经营产生的现金流足以支持企业进一步扩张和偿还借款或分红。",
                "-++": "公司靠借钱维持生产经营的需要，财务状况可能恶化，应着重分析投资活动现金净流入是来自投资收益还是收回投资，如果是后者，企业的形式将非常严峻。",
                "-+-": "经营活动已经发出危险信号，如果投资活动现金流入主要来自收回投资，则企业将处于破产的边缘，需要高度警惕。",
                "--+": "企业靠借债维持日常经营和生产规模的扩大，财务状况很不稳定，如果是处于投入期的企业，一旦渡过难关，还可能有发展，如果是成长期或稳定器的企业，则非常危险。",
                "---": "企业财务状况危急，必须及时扭转，这样的情况往往发生在扩张时，由于市场变化导致经营状况恶化，加上扩张时投入了大量资金，会使企业陷入进退两难的境地。"
            }
            if flag == '+++':
                portrait_label = '妖精型'
            elif flag == "+-+":
                portrait_label = "蛮牛型"
            elif flag == "+--":
                portrait_label = "奶牛型"
            elif flag == "++-":
                portrait_label = "母鸡型"
            else:
                portrait_label = "常规型"

            operating_conclusion = "连续3年经营活动产生的现金流量净额：{:.2f}万、{:.2f}万、{:.2f}万".format(
                round(self.empty_to_zero_enhanced('net_operating_cash_flow_T')/10000,2),
                round(self.empty_to_zero_enhanced('net_operating_cash_flow_T1')/10000,2),
                round(self.empty_to_zero_enhanced('net_operating_cash_flow_T2')/10000,2))
            if tangible_assets_ratio > 0.6:
                operating_conclusion += "，重资产运作，有形资产占比{:.2f}%".format(tangible_assets_ratio * 100)
            elif tangible_assets_ratio < 0.4:
                operating_conclusion += "，轻资产运作，有形资产占比{:.2f}%".format(tangible_assets_ratio * 100)
            investment_conclusion = "投资活动产生的现金流量净额:{:.2f}万、{:.2f}万、{:.2f}万。".format(
                round(self.empty_to_zero_enhanced('net_investment_cash_flow_T')/10000,2),
                round(self.empty_to_zero_enhanced('net_investment_cash_flow_T1')/10000,2),
                round(self.empty_to_zero_enhanced('net_investment_cash_flow_T2')/10000,2))
            if growth_rate_of_fixed_and_intangible_assets[0] > 0.2:
                investment_conclusion += "（固定资产+无形资产）增长率:{:.2f}%、{:.2f}%,企业可能在积极扩展其生产能力或提升竞争优势。".format(
                    growth_rate_of_fixed_and_intangible_assets[0] * 100,
                    growth_rate_of_fixed_and_intangible_assets[1] * 100)
            financing_conclusion = "筹资活动产生的现金流量净额：{:.2f}万、{:.2f}万、{:.2f}万。".format(
                round(self.empty_to_zero_enhanced('net_financing_cash_flow_T')/10000,2),
                round(self.empty_to_zero_enhanced('net_financing_cash_flow_T1')/10000,2),
                round(self.empty_to_zero_enhanced('net_financing_cash_flow_T2')/10000,2))
            self.variables["cash_flow_portrait"] = {
                                                       "company_name": company_name,
                                                       "years_since_establishment": years_since_establishment,
                                                       "industry": industry,
                                                       # "net_cash_flow_from_investing_activities": net_cash_flow_from_investing_activities,
                                                       # "growth_rate_of_fixed_and_intangible_assets": growth_rate_of_fixed_and_intangible_assets,
                                                       # "tangible_assets_ratio": tangible_assets_ratio,
                                                       # "net_cash_flow_from_operating_activities": net_cash_flow_from_operating_activities,
                                                       # "net_cash_flow_from_financing_activities": net_cash_flow_from_financing_activities,
                                                       "operating": operating_conclusion,  # 经营
                                                       "investment": investment_conclusion,  # 投资
                                                       "financing": financing_conclusion,  # 筹资
                                                       "portrait_label": portrait_label,  # 画像类型
                                                       "cash_flow_portrait_class": cash_flow_portrait_class_dict[
                                                           flag] if flag in cash_flow_portrait_class_dict.keys() else ''
                                                   }

    # 杜邦分析,以最近2次的年报进行分析展示
    def du_pond_analysis(self,year, T,T1):
        du_pond_dict = {}
        du_pond_dict['net_profit'] = self.empty_to_zero_enhanced('net_profit' + T)
        du_pond_dict['total_assets'] = self.empty_to_zero_enhanced('total_assets' + T)
        du_pond_dict['revenue'] = self.empty_to_zero_enhanced('revenue' + T)
        # 全部成本
        du_pond_dict['total_costs'] = self.empty_to_zero_enhanced('operating_cost' + T)+self.empty_to_zero_enhanced('taxes_surcharges'+T)+self.empty_to_zero_enhanced('period_cost'+T)
        du_pond_dict['other_business_profit'] = self.empty_to_zero_enhanced('non_operating_income' + T) - self.empty_to_zero_enhanced(
            'non_operating_expense' + T)
        du_pond_dict['income_tax'] = self.empty_to_zero_enhanced('income_tax' + T)
        du_pond_dict['current_assets'] = self.empty_to_zero_enhanced('total_current' + T) # 流动资产合计
        du_pond_dict['long_term_assets'] = self.empty_to_zero_enhanced('total_non_current_assets' + T)
        du_pond_dict['operating_costs'] = self.empty_to_zero_enhanced('operating_cost' + T)
        du_pond_dict['sales_expenses'] = self.empty_to_zero_enhanced('selling_expense' + T)
        du_pond_dict['management_expenses'] = self.empty_to_zero_enhanced('admin_expense' + T)
        du_pond_dict['financial_expenses'] = self.empty_to_zero_enhanced('finance_expense' + T)
        du_pond_dict['cash_and_cash_equivalents'] = self.empty_to_zero_enhanced('cash_funds' + T) # 货币资金
        du_pond_dict['accounts_receivable'] = self.empty_to_zero_enhanced('accounts_receivable' + T)
        du_pond_dict['inventory'] = self.empty_to_zero_enhanced('inventory' + T)
        du_pond_dict['other_current_assets'] = self.empty_to_zero_enhanced('other_current' + T)
        du_pond_dict['equity_multiplier'] = self.empty_to_zero_enhanced('total_assets' + T) / self.empty_to_zero_enhanced(
            'total_equity' + T) if self.empty_to_zero_enhanced('total_equity' + T)>0 else 0 # 权益系数=总资产/股东权益总额
        du_pond_dict['operating_profit_margin'] = self.empty_to_zero_enhanced('net_profit'+T) / self.empty_to_zero_enhanced(
            'revenue'+T) if self.empty_to_zero_enhanced('revenue'+T)>0 else 0  # 主营业务利润率=净利润/营业收入
        total_assets_avg=(self.empty_to_zero_enhanced('total_assets' + T) + self.empty_to_zero_enhanced('total_assets' + T1)) / 2
        du_pond_dict['total_asset_turnover'] = self.empty_to_zero_enhanced('revenue' + T) / total_assets_avg if total_assets_avg>0 else 0  # 总资金周转率=营业收入/平均总资产=营业收入/((期初总资产+期末总资产)/2)
        du_pond_dict['return_on_assets'] = du_pond_dict['operating_profit_margin'] * du_pond_dict['total_asset_turnover']  # 总资产收益率=营业利润率*总资金周转率
        du_pond_dict['return_on_equity'] = du_pond_dict['return_on_assets'] * du_pond_dict['equity_multiplier']  # 净资产收益率=总资产收益率*权益系数
        du_pond_dict['year'] = year  # 年份
        return du_pond_dict


    def empty_to_zero_enhanced(self,var):
        '''
            将空值转换为0，非空则返回原数据
            参数:
                data: 输入数据
            返回:
                如果是空字符串""、None或空序列则返回0，否则返回原数据
        '''
        if self.variables[var] == "":
            return 0
        try:
            if isinstance(self.variables[var],(int,float)):
                return self.variables[var]
            else:
                cleaned_str=self.variables[var].replace(":","").replace("：","").replace(",",'')
                number=float(cleaned_str)
                if number.is_integer():
                    return int(number)
                else:
                    return number
        except:
            return f"无法将字符串'{self.variables[var]}'转换为数值"

    # 资产负债分布-资产分布
    def asset_pie(self):
        asset_class = ['货币资金', '存货', '应收账款', '固定资产', '无形资产', '长期待摊费用', '商誉', '其他']
        temp_asset = 0
        for i in ['cash_funds_T', 'inventory_T', 'accounts_receivable_T', 'fixed_assets_T', 'intangible_assets_T',
                  'long_term_prepaid_expenses_T', 'goodwill_T']:
            temp_asset += self.empty_to_zero_enhanced(i)

        value = [self.empty_to_zero_enhanced('cash_funds_T'),
                 self.empty_to_zero_enhanced('inventory_T'),
                 self.empty_to_zero_enhanced('accounts_receivable_T'),
                 self.empty_to_zero_enhanced('fixed_assets_T'),
                 self.empty_to_zero_enhanced('intangible_assets_T'),
                 self.empty_to_zero_enhanced('long_term_prepaid_expenses_T'),
                 self.empty_to_zero_enhanced('goodwill_T'),
                 self.empty_to_zero_enhanced('total_assets_T') - temp_asset]
        total_assets_T=self.empty_to_zero_enhanced('total_assets_T')
        ratio = [self.empty_to_zero_enhanced('cash_funds_T') / total_assets_T,
                 self.empty_to_zero_enhanced('inventory_T') / total_assets_T,
                 self.empty_to_zero_enhanced('accounts_receivable_T') / total_assets_T,
                 self.empty_to_zero_enhanced('fixed_assets_T') / total_assets_T,
                 self.empty_to_zero_enhanced('intangible_assets_T') / total_assets_T,
                 self.empty_to_zero_enhanced('long_term_prepaid_expenses_T') / total_assets_T,
                 self.empty_to_zero_enhanced('goodwill_T') / total_assets_T,
                 (self.empty_to_zero_enhanced('total_assets_T') - temp_asset) / total_assets_T]
        keys = ['asset_class', 'value', 'ratio']
        self.variables['asset_pie'] = [{keys[0]: item1, keys[1]: item2, keys[2]: item3} for item1, item2, item3 in
                                       zip(asset_class, value, ratio)]

    # 资产负债分布-负债分布
    def debt_pie(self):
        debt_class = ['长期借款', '长期应付款',  '短期借款', '应付票据', '应付账款','其他']
        temp_debt=0
        for i in ['long_term_loans_T','long_term_payables_T','short_term_loans_T','notes_payable_T','accounts_payable_T']:
            temp_debt+=self.empty_to_zero_enhanced(i)
        value = [self.empty_to_zero_enhanced('long_term_loans_T'),
                 self.empty_to_zero_enhanced('long_term_payables_T'),
                 self.empty_to_zero_enhanced('short_term_loans_T'),
                 self.empty_to_zero_enhanced('notes_payable_T'),
                 self.empty_to_zero_enhanced('accounts_payable_T'),
                 self.empty_to_zero_enhanced('total_liabilities_T') - temp_debt]
        total_liabilities_T=self.empty_to_zero_enhanced('total_liabilities_T')
        ratio = [self.empty_to_zero_enhanced('long_term_loans_T') / total_liabilities_T,
                 self.empty_to_zero_enhanced('long_term_payables_T') / total_liabilities_T,
                 self.empty_to_zero_enhanced('short_term_loans_T') / total_liabilities_T,
                 self.empty_to_zero_enhanced('notes_payable_T') / total_liabilities_T,
                 self.empty_to_zero_enhanced('accounts_payable_T') / total_liabilities_T,
                 (self.empty_to_zero_enhanced('total_liabilities_T') - temp_debt) / total_liabilities_T]
        keys = ['debt_class', 'value', 'ratio']
        self.variables['debt_pie'] = [{keys[0]: item1, keys[1]: item2, keys[2]: item3} for item1, item2, item3 in
                                      zip(debt_class, value, ratio)]
