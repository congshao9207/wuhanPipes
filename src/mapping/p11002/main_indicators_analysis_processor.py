import pandas as pd

from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df

'''
主要财务指标分析
'''
class MainIndicatorsAnalysisProcessor(ModuleProcessor):
    def process(self):
        self.variables['main_T']=max(self.variables.get('asset_dept_T'),self.variables.get('cash_flow_T'),self.variables.get('profit_T'))
        self.variables['main_T1'] = max(self.variables.get('asset_dept_T1'), self.variables.get('cash_flow_T1'),
                                       self.variables.get('profit_T1'))
        self.variables['main_T2'] = max(self.variables.get('asset_dept_T2'), self.variables.get('cash_flow_T2'),
                                        self.variables.get('profit_T2'))

        for i in ['_T','_T1','_T2']:
            self.profitability(i)
            self.short_term_solvency(i)
            self.additional_ratios(i)
            self.cash_flow(i)

        for i,j in zip(['_T','_T1'],['_T1','_T2']):
            self.profitability_multi(i,j)
            self.operation_capacity(i,j)
            self.trend_analysis(i,j)

        # report_date = '20231231'  # 根据数据更新情况进行调整 todo 需要是上传的财报的时间维度的去年年份，例如，上传最新财报时间2024年6月，则对比20231231的数据
        # # basic_industry_code = self.variables['basic_industry_code']
        # basic_industry_code='A0513' # todo 上线调整
        # industry = self._get_industry(basic_industry_code)
        # # self.neeq_fiancial_report(report_date, industry) # todo
        # self.a_share_financial_report(report_date, industry)

    def _get_industry(self,basic_industry_code):
        industry_mapping = {
            "A": "农林牧渔",
            "B": "采矿业",
            "C": "制造业",
            "D": "水电煤气",
            "E": "建筑业",
            "F": "批发和零售业",
            "G": "交通运输、仓储和邮政业",
            "H": "住宿和餐饮业",
            "I": "信息传输、软件和信息技术服务业",
            "J": "金融业",
            "K": "房地产业",
            "L": "租赁和商务服务业",
            "M": "科学研究和技术服务业",
            "N": "水利、环境和公共设施管理业",
            "O": "居民服务、修理和其他服务业",
            "P": "教育",
            "Q": "卫生和社会工作",
            "R": "文化、体育和娱乐业",
            "S": "公共管理、社会保障和社会组织",
            "T": "国际组织",
            "U": "未列明行业"
        }
        industry_code = str(basic_industry_code)[:1]
        return industry_mapping[industry_code]


    # 去掉头尾10%的数据，剩余数据取均值
    def _cal_main_indicators(self,df):
        mean_dict={}
        for i in [
            'gross_margin',
            'net_profit_margin',
            'roa',
            'roe',
            'inventory_turnover_ratio',
            'ar_turnover_ratio',
            'ap_turnover_ratio',
            'total_asset_turnover_ratio',
            'asset_turnover_rate',
            'current_ratio',
            'quick_ratio',
            'cash_ratio',
            'asset_liability_ratio',
            'debt_to_equity_ratio',
            'equity_multiplier',
            'operating_revenue_growth_rate',
            'net_profit_growth_rate']:
            # 去除头尾10%的极值
            sorted_series = df[i].sort_values()
            n = len(sorted_series)
            keep_start = int(n * 0.1)  # 截断前10%索引
            keep_end = n - int(n * 0.1)  # 截断后10%索引
            trimmed_series = sorted_series.iloc[keep_start:keep_end]
            temp_mean=trimmed_series.mean()
            mean_dict[i]=temp_mean
        return mean_dict


    # 新三板数据，取最新一个年报的记录
    def neeq_fiancial_report(self,report_date,industry):
        sql='''
        SELECT 
            ent_name,
            industry,
            stock_code,
            gross_margin,
            net_profit_margin,
            roa,
            roe,
            inventory_turnover_ratio,
            ar_turnover_ratio,
            ap_turnover_ratio,
            total_asset_turnover_ratio,
            asset_turnover_rate,
            current_ratio,
            quick_ratio,
            cash_ratio,
            asset_liability_ratio,
            debt_to_equity_ratio,
            equity_multiplier,
            operating_revenue_growth_rate,
            net_profit_growth_rate
        FROM financial_summary_neeq
        WHERE report_date =%(report_date)s
        AND industry=%(industry)s
        ;
        '''
        neeq_df=sql_to_df(sql,params={"report_date":report_date,"industry":industry})
        if neeq_df.shape[0]>0:
            mean_dict=self._cal_main_indicators(neeq_df)
            # self.variables['neeq_data'] = mean_dict

    # A股数据，取最新一个年报的记录
    def a_share_financial_report(self,report_date,industry):
        sql = '''
                SELECT 
                    ent_name,
                    industry,
                    stock_code,
                    gross_margin,
                    net_profit_margin,
                    roa,
                    roe,
                    inventory_turnover_ratio,
                    ar_turnover_ratio,
                    ap_turnover_ratio,
                    total_asset_turnover_ratio,
                    asset_turnover_rate,
                    current_ratio,
                    quick_ratio,
                    cash_ratio,
                    asset_liability_ratio,
                    debt_to_equity_ratio,
                    equity_multiplier,
                    operating_revenue_growth_rate,
                    net_profit_growth_rate
                FROM financial_summary_a_share
                WHERE report_date =%(report_date)s
                AND industry=%(industry)s
                ;
                '''
        a_share_df = sql_to_df(sql, params={"report_date": report_date, "industry": industry})
        if a_share_df.shape[0] > 0:
            mean_dict=self._cal_main_indicators(a_share_df)
            self.variables['a_share_data'] = [{
                "a_share_gross_profit_margin_pct": mean_dict['gross_margin'],  # 毛利率(%)
                "a_share_operating_profit_to_total_profit_pct": None,  # 营业利润对利润总额占比（%）
                "a_share_net_cash_flow_from_investing_to_total_profit_pct": None,  # 投资活动产生的现金流量净额对利润总额占比（%）
                "a_share_net_profit_margin_pct": mean_dict['net_profit_margin'],  # 净利润率（%）
                "a_share_return_on_total_assets": mean_dict['roa'],  # 总资产净利率
                "a_share_roe": mean_dict['roe'],  # 净资产收益率
                "a_share_inventory_turnover": mean_dict['inventory_turnover_ratio'],  # 存货周转率（次/年）
                "a_share_accounts_receivable_turnover": mean_dict['ar_turnover_ratio'],  # 应收账款周转率（次/年）
                "a_share_accounts_payable_turnover": mean_dict['ap_turnover_ratio'],  # 应付账款周转率（次/年）
                "a_share_advance_from_customers_turnover": None,  # 预收账款周转率（次/年）
                "a_share_prepaid_expenses_turnover": None,  # 预付账款周转率（次/年）
                "a_share_total_asset_turnover": mean_dict['total_asset_turnover_ratio'],  # 总资产周转率（次/年）
                "a_share_current_asset_turnover": mean_dict['asset_turnover_rate'],  # 流动资产周转率（次/年）
                "a_share_non_current_asset_turnover": None,  # 非流动资产周转率（次/年）
                "a_share_current_ratio": mean_dict['current_ratio'],  # 流动比率
                "a_share_quick_ratio": mean_dict['quick_ratio'],  # 速动比率
                "a_share_cash_ratio": mean_dict['cash_ratio'],  # 现金比率
                "a_share_asset_liability_ratio_pct": mean_dict['asset_liability_ratio'],  # 资产负债率（％）
                "a_share_equity_ratio": mean_dict['debt_to_equity_ratio'],  # 产权比率
                "a_share_equity_multiplier": mean_dict['equity_multiplier'],  # 权益乘数
                "a_share_interest_coverage_ratio": None,  # 利息保障倍数
                "a_share_cash_flow_ratio": None,  # 现金流量比率
                "a_share_cash_flow_interest_coverage_ratio": None,  # 现金流量利息保障倍数
                "a_share_cash_flow_to_debt_ratio": None,  # 现金流量与负债比率
                "a_share_operating_cash_flow_to_net_profit_ratio": None,  # 经营现金流与净利比率
                "a_share_revenue_growth_rate": mean_dict['operating_revenue_growth_rate'],  # 营业收入增长率
                "a_share_net_profit_growth_rate": mean_dict['net_profit_growth_rate'],  # 净利润增长率
                "a_share_total_asset_growth_rate": None,  # 总资产增长率
            },
                {
                    "delta_a_share_gross_profit_margin_pct": self._cal_delta('gross_margin_T1',
                                                                             mean_dict['gross_margin']),
                    # 差额_毛利率(%)
                    "delta_a_share_operating_profit_to_total_profit_pct": None,  # 差额_营业利润对利润总额占比（%）
                    "delta_a_share_net_cash_flow_from_investing_to_total_profit_pct": None,
                    # 差额_投资活动产生的现金流量净额对利润总额占比（%）
                    "delta_a_share_net_profit_margin_pct": self._cal_delta('net_margin_T1',
                                                                           mean_dict['net_profit_margin']),
                    # 差额_净利润率（%）
                    "delta_a_share_return_on_total_assets": self._cal_delta('return_on_total_assets_T1',
                                                                            mean_dict['roa']),
                    # 差额_总资产净利率
                    "delta_a_share_roe": self._cal_delta('ROE_T1', mean_dict['roe']),
                    "delta_a_share_inventory_turnover": self._cal_delta('inventory_turnover_ratio_T1',
                                                                        mean_dict['inventory_turnover_ratio']),
                    # 差额_存货周转率（次/年）
                    "delta_a_share_accounts_receivable_turnover": self._cal_delta(
                        'receivables_turnover_ratio_T1', mean_dict['ar_turnover_ratio']),
                    # 差额_应收账款周转率（次/年）
                    "delta_a_share_accounts_payable_turnover": None,  # 差额_应付账款周转率（次/年）
                    "delta_a_share_advance_from_customers_turnover": None,  # 差额_预收账款周转率（次/年）
                    "delta_a_share_prepaid_expenses_turnover": None,  # 差额_预付账款周转率（次/年）
                    "delta_a_share_total_asset_turnover": self._cal_delta(
                        'total_asset_turnover_ratio_T1', mean_dict['total_asset_turnover_ratio']),
                    # 差额_总资产周转率（次/年）
                    "delta_a_share_current_asset_turnover": self._cal_delta(
                        'total_asset_turnover_ratio_T1', mean_dict['asset_turnover_rate']),
                    # 差额_流动资产周转率（次/年）
                    "delta_a_share_non_current_asset_turnover": None,  # 差额_非流动资产周转率（次/年）
                    "delta_a_share_current_ratio": self._cal_delta('current_ratio_T1',
                                                                   mean_dict['current_ratio']),
                    # 差额_流动比率
                    "delta_a_share_quick_ratio": self._cal_delta('quick_ratio_T1',
                                                                 mean_dict['quick_ratio']),
                    # 差额_速动比率
                    "delta_a_share_cash_ratio": self._cal_delta('cash_ratio_T1',
                                                                mean_dict['cash_ratio']),
                    # 差额_现金比率
                    "delta_a_share_asset_liability_ratio_pct": self._cal_delta(
                    'debt_to_assets_ratio_T1', mean_dict['asset_liability_ratio']),
                    # 差额_资产负债率（％）
                    "delta_a_share_equity_ratio": self._cal_delta('equity_ratio_T1',
                                                                  mean_dict['debt_to_equity_ratio']),
                    # 差额_产权比率
                    "delta_a_share_equity_multiplier": self._cal_delta('equity_multiplier_T1',
                                                                       mean_dict['equity_multiplier']),
                    # 差额_权益乘数
                    "delta_a_share_interest_coverage_ratio": None,  # 差额_利息保障倍数
                    "delta_a_share_cash_flow_ratio": None,  # 差额_现金流量比率
                    "delta_a_share_cash_flow_interest_coverage_ratio": None,  # 差额_现金流量利息保障倍数
                    "delta_a_share_cash_flow_to_debt_ratio": None,  # 差额_现金流量与负债比率
                    "delta_a_share_operating_cash_flow_to_net_profit_ratio": None,  # 差额_经营现金流与净利比率
                    "delta_a_share_revenue_growth_rate": self._cal_delta('revenue_growth_T1',
                                                                         mean_dict['operating_revenue_growth_rate']),
                    # 差额_营业收入增长率
                    "delta_a_share_net_profit_growth_rate": self._cal_delta('net_profit_growth_T1',
                                                                            mean_dict['net_profit_growth_rate']),
                    # 差额_净利润增长率
                    "delta_a_share_total_asset_growth_rate": None,  # 差额_总资产增长率
                }]

    def _cal_delta(self, a, b):
        if a in self.variables and isinstance(self.variables[a], (float, int)) and pd.isna(b)==False:
            print("self.variables[a]:",a,self.variables[a])
            return self.empty_to_zero_enhanced(a) - b
        else:
            return None
    #盈利能力
    def profitability(self,T):
        #营业利润对利润总额占比（%）
        self.profitability_variable('operating_profit','total_profit','operating_profit_ratio',T)

        #投资活动产生的现金流量净额对利润总额占比（%）
        self.profitability_variable('net_investment_cash_flow', 'total_profit', 'net_investment_ratio', T)

    def profitability_multi(self,T,T1):
        # 盈利能力：
        # 总资产净利率 = 净利润 / 平均总资产
        self.profitability_multi_variable('net_profit', 'total_assets', 'return_on_total_assets', T, T1)

        # 净资产收益率=净利润/平均所有者权益=总资产净利率*权益乘数=营业净利率*总资产周转率*权益乘数
        self.profitability_multi_variable('net_profit', 'total_equity', 'ROE', T, T1)

    def profitability_variable(self,key1,key2,key3,T):
        if key1+T in self.variables and key2+T in self.variables and isinstance(self.variables[key1+T],(float,int)) and isinstance(self.variables[key2+T],(float,int)) and self.variables[key2+T]!=0:
            self.variables[key3+T]=self.variables[key1+T]/self.variables[key2+T]

    def profitability_multi_variable(self,key1, key2, key3, T, T1):
        if key1+T in self.variables and key2+T in self.variables and isinstance(self.variables[key1 + T], (float, int)) and isinstance(self.variables[key2 + T],(float, int)) and isinstance(self.variables[key2 + T1], (float, int)) and (self.variables[key2 + T] + self.variables[key2 + T1]) != 0:
            self.variables[key3 + T] = self.variables[key1 + T] / ((self.variables[key2+ T] + self.variables[key2 + T1]) / 2)

    #营运能力
    def operation_capacity(self,T,T1):
        df_dict = self.cached_data.get('PROFIT')
        if len(df_dict)>0 and df_dict.get('end_date'+T.lower()) is not None and int(df_dict.get('end_date'+T.lower())[5:7])>=7:
            # 存货周转天数（天）
            self.operation_capacity_variables('operating_cost','inventory','inventory_turnover',T,T1)
            # 存货周转率
            self.variables['inventory_turnover_ratio'+T]=360/self.empty_to_zero_enhanced('inventory_turnover'+T) if self.empty_to_zero_enhanced('inventory_turnover'+T)>0 else None
            #应收账款周转天数（天）
            self.operation_capacity_variables('revenue', 'accounts_receivable', 'receivables_turnover', T, T1)
            # 应收账款周转率
            self.variables['receivables_turnover_ratio'+T]=360/self.empty_to_zero_enhanced('receivables_turnover'+T) if self.empty_to_zero_enhanced('receivables_turnover'+T)>0 else None
            # 应付账款周转天数（天）
            self.operation_capacity_variables('operating_cost', 'accounts_payable', 'payables_turnover', T, T1)
            # 应付账款周转率
            self.variables['payables_turnover_ratio'+T]=360/self.empty_to_zero_enhanced('payables_turnover'+T) if self.empty_to_zero_enhanced('payables_turnover'+T)>0 else None
            # 预付账款周转天数（天）
            self.operation_capacity_variables('operating_cost', 'prepaid', 'advances_turnover', T, T1)

            # 预收账款周转天数（天）
            self.operation_capacity_variables('revenue', 'advances_from_customers', 'deposits_turnover', T, T1)

            # 总资产周转天数
            self.operation_capacity_variables('revenue','total_assets','total_asset_turnover',T,T1)
            # 总资产周转率= 营业收入 / 平均资产合计
            self.variables['total_asset_turnover_ratio'+T]=360/self.empty_to_zero_enhanced('total_asset_turnover'+T) if  self.empty_to_zero_enhanced('total_asset_turnover'+T)>0 else None

            # 流动资产周转天数
            self.operation_capacity_variables('revenue', 'total_current', 'current_asset_turnover', T, T1)
            # 流动资产周转率=营业收入 / 平均流动资产合计
            if 'current_asset_turnover'+T in self.variables:
                self.variables['current_asset_turnover_ratio'+T]=360/self.empty_to_zero_enhanced('current_asset_turnover'+T) if self.empty_to_zero_enhanced('current_asset_turnover'+T)>0 else None

            # 长期偿债能力：
            # 利息保障倍数=平均（净利润 + 所得税费用 + 利息费用）/平均利息费用
            a1 = self.empty_to_zero_enhanced('net_profit' + T) + self.empty_to_zero_enhanced(
                'income_tax' + T) + self.empty_to_zero_enhanced('interest_expense' + T)
            a2 = self.empty_to_zero_enhanced('net_profit' + T1) + self.empty_to_zero_enhanced(
                'income_tax' + T1) + self.empty_to_zero_enhanced('interest_expense' + T1)
            b1 = self.empty_to_zero_enhanced('interest_expense' + T)
            b2 = self.empty_to_zero_enhanced('interest_expense' + T)
            self.variables['interest_coverage_ratio' + T] = ((a1 + a2) / 2) / ((b1 + b2) / 2) if (b1 + b2) > 0 else ''


    def operation_capacity_variables(self,key1, key2, key3, T, T1):
        if key1 + T in self.variables and key2 + T in self.variables and key2 + T1 in self.variables \
                and  isinstance(self.variables[key1 + T], (float, int)) \
                and isinstance(self.variables[key2 + T],(float, int)) \
                and isinstance(self.variables[key2 + T1], (float, int)) \
                and self.variables[key1 + T] !=0 and (self.variables[key2 + T] + self.variables[key2 + T1]) != 0:
            self.variables[key3 + T] = 360 / (self.variables[key1 + T] / ((self.variables[key2+ T] + self.variables[key2 + T1]) / 2))


    #短期偿债能力
    def short_term_solvency(self,T):
        #流动比率
        self.profitability_variable('total_current', 'total_current_liabilities', 'current_ratio', T)

        #速动比率
        if (isinstance(self.variables['total_current' + T], (float, int)) or isinstance(self.variables['inventory' + T],(float, int)) or isinstance(self.variables['prepaid' + T],(float, int))) and isinstance(self.variables['total_current_liabilities' + T],(float, int)) and self.variables['total_current_liabilities' + T]!=0:
            if isinstance(self.variables['total_current'+T],str):
                total_current=0
            else:
                total_current=self.variables['total_current'+T]

            if isinstance(self.variables['inventory'+T],str):
                inventory=0
            else:
                inventory=self.variables['inventory'+T]

            if isinstance(self.variables['prepaid'+T],str):
                prepaid=0
            else:
                prepaid=self.variables['prepaid'+T]

            self.variables['quick_ratio' + T]=(total_current-inventory-prepaid)/self.variables['total_current_liabilities' + T]

        #现金比率
        self.profitability_variable('cash_funds', 'total_current_liabilities', 'cash_ratio', T)

        #长期偿债能力
        self.profitability_variable('total_liabilities', 'total_assets', 'debt_to_assets_ratio', T)
        # 产权比率=负债合计/所有者权益
        self.profitability_variable('total_liabilities', 'total_equity', 'equity_ratio', T)
        # 权益乘数=资产合计/所有者权益
        self.profitability_variable('total_assets', 'total_equity', 'equity_multiplier', T)




    #趋势分析
    def trend_analysis(self,T,T1):
        #总资产同比（%）
        if isinstance(self.variables['total_assets' + T], (float, int)) and isinstance(self.variables['total_assets' + T1],(float, int)) and self.variables['total_assets' + T1]!=0:
            self.variables['total_assets_growth' + T]=(self.variables['total_assets' + T]-self.variables['total_assets' + T1])/self.variables['total_assets' + T1]
        #短期借款同比（%）
        if isinstance(self.variables['short_term_loans' + T], (float, int)) and isinstance(self.variables['short_term_loans' + T1],(float, int)) and self.variables['short_term_loans' + T1]!=0:
            self.variables['short_term_loans_growth' + T]=(self.variables['short_term_loans' + T]-self.variables['short_term_loans' + T1])/self.variables['short_term_loans' + T1]
        #长期借款同比（%）
        if isinstance(self.variables['long_term_loans' + T], (float, int)) and isinstance(self.variables['long_term_loans' + T1],(float, int)) and self.variables['long_term_loans' + T1]!=0:
            self.variables['long_term_loans_growth' + T]=(self.variables['long_term_loans' + T]-self.variables['long_term_loans' + T1])/self.variables['long_term_loans' + T1]
        #营业额同比（%）
        self.trend_analysis_variables('revenue','revenue_growth',T,T1,'PROFIT')
        #净利润同比（%）
        self.trend_analysis_variables('net_profit', 'net_profit_growth', T, T1, 'PROFIT')
        #应收账款同比（%）
        self.trend_analysis_variables('accounts_receivable', 'receivables_growth', T, T1, 'ASSET_DEBT')
        #存货同比（%）
        self.trend_analysis_variables('inventory', 'inventory_growth', T, T1, 'ASSET_DEBT')
        #其他应收同比（%）
        self.trend_analysis_variables('other_receivables', 'other_receivables_growth', T, T1, 'ASSET_DEBT')
        #应付账款同比（%）
        self.trend_analysis_variables('accounts_payable', 'payables_growth', T, T1, 'ASSET_DEBT')
        #其他应付同比（%）
        self.trend_analysis_variables('other_payables', 'other_payables_growth', T, T1, 'ASSET_DEBT')
        # 固定资产增长率
        self.trend_analysis_variables('fixed_assets', 'fixed_assets_growth', T, T1, 'ASSET_DEBT')

    def trend_analysis_variables(self,key1,key2,T,T1,table_type):
        df_dict = self.cached_data.get(table_type)
        if len(df_dict) > 0 and df_dict.get('end_date' + T.lower())!=None and int(df_dict.get('end_date' + T.lower())[5:7]) >= 7:
            if isinstance(self.variables[key1 + T], (float, int)) and isinstance(self.variables[key1 + T1],(float, int)) and self.variables[key1 + T1]!=0:
                self.variables[key2 + T]=(self.variables[key1 + T]/int(df_dict.get('end_date' + T.lower())[5:7])*12-self.variables[key1 + T1])/abs(self.variables[key1 + T1])


    #其他比率
    def additional_ratios(self,T):
        #销贷比（%）
        if (isinstance(self.variables['short_term_loans' + T], (float, int)) or isinstance(self.variables['long_term_loans' + T],(float, int))) and isinstance(self.variables['revenue' + T],(float, int)) and self.variables['revenue' + T]!=0:
            if isinstance(self.variables['short_term_loans' + T], str):
                short_term_loans=0
            else:
                short_term_loans=self.variables['short_term_loans' + T]

            if isinstance(self.variables['long_term_loans' + T], str):
                long_term_loans=0
            else:
                long_term_loans=self.variables['long_term_loans' + T]


            self.variables['sales_to_loans_ratio' + T]=(short_term_loans+long_term_loans)/self.variables['revenue' + T]

        #流动资产对短期负债占比（%）
        self.profitability_variable('total_current','short_term_loans','current_assets_to_short_debt',T)

        #存货对流动资产占比（%）
        self.profitability_variable('inventory', 'total_current', 'inventory_to_current_assets', T)

        # 对外投资对总资产占比（%）
        self.profitability_variable('long_term_equity_investments', 'total_assets', 'external_investment_to_assets', T)

    # 现金流量
    def cash_flow(self, T):
        # 现金流量比率=经营活动现金流量净额/流动负债
        if isinstance(self.variables['net_operating_cash_flow' + T], (float, int)) and isinstance(
                self.variables['total_current_liabilities' + T], (float, int)):
            self.variables['cash_flow_ratio' + T] = self.variables['net_operating_cash_flow' + T] / self.variables[
                'total_current_liabilities' + T] if self.variables['total_current_liabilities' + T] > 0 else 0

        # 现金流量比率=经营活动现金流量净额/利息费用
        if isinstance(self.variables['net_operating_cash_flow' + T], (float, int)) and isinstance(
                self.variables['interest_expense' + T], (float, int)):
            self.variables['cash_flow_interest_coverage_ratio' + T] = self.variables['net_operating_cash_flow' + T] / \
                                                                      self.variables['interest_expense' + T] if \
                self.variables['interest_expense' + T] > 0 else 0

        # 现金流量比率=经营活动现金流量净额/负债合计
        if isinstance(self.variables['net_operating_cash_flow' + T], (float, int)) and isinstance(
                self.variables['total_liabilities' + T], (float, int)):
            self.variables['cash_flow_to_debt_ratio' + T] = self.variables['net_operating_cash_flow' + T] / \
                                                            self.variables['total_liabilities' + T] if self.variables[
                                                                                                           'total_liabilities' + T] > 0 else 0

        # 现金流量比率=经营活动现金流量净额/净利润
        if isinstance(self.variables['net_operating_cash_flow' + T], (float, int)) and isinstance(
                self.variables['net_profit' + T], (float, int)):
            self.variables['operating_cash_flow_to_net_profit_ratio' + T] = self.variables[
                                                                                'net_operating_cash_flow' + T] / \
                                                                            self.variables['net_profit' + T] if \
            self.variables['net_profit' + T] > 0 else 0

    def empty_to_zero_enhanced(self,var):
        '''
            将空值转换为0，非空则返回原数据
            参数:
                data: 输入数据
            返回:
                如果是空字符串""、None或空序列则返回0，否则返回原数据
        '''
        if var not in self.variables or self.variables[var] == "":
            return 0
        return float(self.variables[var])