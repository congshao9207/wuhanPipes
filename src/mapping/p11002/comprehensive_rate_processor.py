import sys

from mapping.module_processor import ModuleProcessor

'''
财报解析报告：综合评级
'''


class ComprehensiveRateProcessor(ModuleProcessor):
    def process(self):
        if self.cached_data.get('PROFIT')!={} and self.cached_data.get('ASSET_DEBT')!={} and self.cached_data.get('CASH_FLOW')!={}:
            self.comprehensive_rate()

    def comprehensive_rate(self):
        '''
        统计信息:
        dimension:分析维度
        var:指标
        value:值
        score:得分
        dimension_tip:分析维度tip
        var_tip:指标tip
        score_tip:得分tip
        conclusion:结论
        '''
        finance_score = 0
        basic_industry_code = self.variables['basic_industry_code'] # 国标行业代码F5179
        net_profit_level = self.rate_profit_margin(basic_industry_code)
        current_ratio_level = self.rate_liquidity(basic_industry_code)
        # 资产负债率
        debt_to_assets_ratio_T = self.empty_to_zero_enhanced('total_liabilities_T') / self.empty_to_zero_enhanced('total_assets_T') if \
            self.empty_to_zero_enhanced('total_assets_T') > 0 else 1
        # 流动比率
        current_ratio_T = self.empty_to_zero_enhanced('total_current_T') / self.empty_to_zero_enhanced('total_current_liabilities_T') if \
            self.empty_to_zero_enhanced('total_current_liabilities_T') > 0 else 999
        # 净利润率
        net_margin_T_score, net_margin_T_conclusion, net_margin_T_score_tip="","",""
        interest_coverage_ratio_T,interest_coverage_ratio_T_score,interest_coverage_ratio_T_conclusion="","",""
        if self.cached_data.get('PROFIT')!={}:
            self.profit_month_T = int(self.cached_data.get('PROFIT').get('end_date_t')[5:7])
            net_margin_T = self.empty_to_zero_enhanced('net_profit_T') / self.empty_to_zero_enhanced('revenue_T') if self.empty_to_zero_enhanced(
                                                                                               'revenue_T') > 0 and self.profit_month_T >= 9 else \
                self.empty_to_zero_enhanced('net_profit_T1') / self.empty_to_zero_enhanced('revenue_T1') if self.empty_to_zero_enhanced('revenue_T1') > 0 else 0
            # 利息保障倍数 =(净利润+所得税费用+利息费用)/利息费用
            interest_coverage_ratio_T = (self.empty_to_zero_enhanced('net_profit_T') + self.empty_to_zero_enhanced('income_tax_T') + self.empty_to_zero_enhanced(
                'interest_expense_T')) / self.empty_to_zero_enhanced('interest_expense_T') if self.empty_to_zero_enhanced(
                                                                                     'interest_expense_T') > 0 and self.profit_month_T >= 9 \
                else (self.empty_to_zero_enhanced('net_profit_T1') + self.empty_to_zero_enhanced('income_tax_T1') + self.empty_to_zero_enhanced(
                'interest_expense_T1')) / self.empty_to_zero_enhanced('interest_expense_T1') if self.empty_to_zero_enhanced(
                                                                                       'interest_expense_T1') > 0 else 999
            # 盈利能力
            net_margin_T_score, net_margin_T_conclusion, net_margin_T_score_tip = self.net_margin_bin(net_profit_level,
                                                                                                      net_margin_T)
            # 融资成本
            interest_coverage_ratio_T_score, interest_coverage_ratio_T_conclusion = self.interest_coverage_ratio_bin(
                interest_coverage_ratio_T)

        # 经营活动现金流净额
        net_operating_cash_flow_T = self.empty_to_zero_enhanced('net_operating_cash_flow_T')

        # 根据行业区分利润等级、流动率等级
        industry = self.variables['basic_industry_phyname'] # 国标行业，例如'其他机械设备及电子产品批发'

        # 指标值对应分箱得分
        debt_to_assets_ratio_T_score,debt_to_assets_ratio_T_conclusion = self.debt_to_assets_ratio_bin(debt_to_assets_ratio_T)

        # 偿债能力
        current_ratio_T_score,current_ratio_T_conclusion,current_ratio_T_score_tip=self.current_ratio_bin(current_ratio_level,current_ratio_T)

        # 经营活动现金流净额

        net_operating_cash_flow_T_score = 20 if net_operating_cash_flow_T > 0 else 0
        net_operating_cash_flow_T_conclusion = "企业现金流为正" if net_operating_cash_flow_T > 0 else "企业自身造血能力弱"
        finance_score=debt_to_assets_ratio_T_score+current_ratio_T_score+net_margin_T_score+interest_coverage_ratio_T_score+net_operating_cash_flow_T_score
        self.variables['comprehensive_rate'] = {
            "finance_score": finance_score,
            "rate_detail": [
                {
                    "dimension": "资本结构",
                    "var": "资产负债率",
                    "value": debt_to_assets_ratio_T,
                    "score": debt_to_assets_ratio_T_score,
                    "dimension_tip": "",
                    "var_tip": "资产负债率=总负债 / 总资产",
                    "score_tip": "≤50%：20分；50%-70%：15分；>70%：0分",
                    "conclusion": debt_to_assets_ratio_T_conclusion,
                },
                {
                    "dimension": "偿债能力",
                    "var": "流动比率",
                    "value": current_ratio_T,
                    "score": current_ratio_T_score,
                    "dimension_tip": "",
                    "var_tip": "（1）流动比率=流动资产 / 流动负债;（2）根据行业差异化配置评分标准",
                    "score_tip": current_ratio_T_score_tip,
                    "conclusion": current_ratio_T_conclusion,
                },
                {
                    "dimension": "盈利能力",
                    "var": "净利润率",
                    "value": net_margin_T,
                    "score": net_margin_T_score,
                    "dimension_tip": "若最新财报月份>=9使用本年度数据统计，否则按照上年度数据统计",
                    "var_tip": "（1）净利润率=净利润 / 营业收入;（2）根据行业差异化配置评分标准",
                    "score_tip": net_margin_T_score_tip,
                    "conclusion": net_margin_T_conclusion,
                },
                {
                    "dimension": "融资成本",
                    "var": "利息保障倍数",
                    "value": interest_coverage_ratio_T,
                    "score": interest_coverage_ratio_T_score,
                    "dimension_tip": "若最新财报月份>=9使用本年度数据统计，否则按照上年度数据统计;若利息费用为0，值展示为999",
                    "var_tip": "利息保障倍数=息税前利润 / 利息费用",
                    "score_tip": ">=5：20分；3-5：15分；1-3：10分；<1：0分",
                    "conclusion": interest_coverage_ratio_T_conclusion,
                },
                {
                    "dimension": "现金流状况",
                    "var": "经营活动现金流净额",
                    "value": net_operating_cash_flow_T,
                    "score": net_operating_cash_flow_T_score,
                    "dimension_tip": "",
                    "var_tip": "",
                    "score_tip": ">0：20分；<=0：0分",
                    "conclusion": net_operating_cash_flow_T_conclusion,
                }

            ]
        }

    def debt_to_assets_ratio_bin(self,debt_to_assets_ratio_T):
        if debt_to_assets_ratio_T <= 0.5:
            return 20, "企业依赖股权融资较多"
        elif debt_to_assets_ratio_T <= 0.7:
            return 15, "企业债务和股权融资相对均衡"
        else:
            return 0, "企业依赖债务融资过多，可能存在风险"

    def current_ratio_bin(self,current_ratio_level, current_ratio_T):
        if current_ratio_level == '高':
            if current_ratio_T >= 999:
                return 20, "流动负债为0", "≥1.5：优秀（20分）;1.2-1.5：良好（15分）;1-1.2：一般（10分）;<1：高风险（0分）"
            elif current_ratio_T >= 1.5:
                return 20, "短期偿债能力强", "≥1.5：优秀（20分）;1.2-1.5：良好（15分）;1-1.2：一般（10分）;<1：高风险（0分）"
            elif current_ratio_T >= 1.2:
                return 15, "短期偿债能力较强", "≥1.5：优秀（20分）;1.2-1.5：良好（15分）;1-1.2：一般（10分）;<1：高风险（0分）"
            elif current_ratio_T >= 1:
                return 10, "短期偿债能力一般", "≥1.5：优秀（20分）;1.2-1.5：良好（15分）;1-1.2：一般（10分）;<1：高风险（0分）"
            else:
                return 0, "短期偿债能力弱", "≥1.5：优秀（20分）;1.2-1.5：良好（15分）;1-1.2：一般（10分）;<1：高风险（0分）"
        elif current_ratio_level == "中":
            if current_ratio_T >= 999:
                return 20, "流动负债为0", "≥2：优秀（20分）;1.5-2：良好（15分）;1-1.5：一般（10分）;<1：高风险（0分）"
            elif current_ratio_T >= 2:
                return 20, "短期偿债能力强", "≥2：优秀（20分）;1.5-2：良好（15分）;1-1.5：一般（10分）;<1：高风险（0分）"
            elif current_ratio_T >= 1.5:
                return 15, "短期偿债能力较强", "≥2：优秀（20分）;1.5-2：良好（15分）;1-1.5：一般（10分）;<1：高风险（0分）"
            elif current_ratio_T >= 1:
                return 10, "短期偿债能力一般", "≥2：优秀（20分）;1.5-2：良好（15分）;1-1.5：一般（10分）;<1：高风险（0分）"
            else:
                return 0, "短期偿债能力弱", "≥2：优秀（20分）;1.5-2：良好（15分）;1-1.5：一般（10分）;<1：高风险（0分）"
        else:
            if current_ratio_T >= 999:
                return 20, "流动负债为0", "≥2.5：优秀（20分）;2-2.5：良好（15分）;1.5-2：一般（10分）;<1.5：高风险（0分）"
            elif current_ratio_T >= 2.5:
                return 20, "短期偿债能力强", "≥2.5：优秀（20分）;2-2.5：良好（15分）;1.5-2：一般（10分）;<1.5：高风险（0分）"
            elif current_ratio_T >= 2:
                return 15, "短期偿债能力较强", "≥2.5：优秀（20分）;2-2.5：良好（15分）;1.5-2：一般（10分）;<1.5：高风险（0分）"
            elif current_ratio_T >= 1.5:
                return 10, "短期偿债能力一般", "≥2.5：优秀（20分）;2-2.5：良好（15分）;1.5-2：一般（10分）;<1.5：高风险（0分）"
            else:
                return 0, "短期偿债能力弱", "≥2.5：优秀（20分）;2-2.5：良好（15分）;1.5-2：一般（10分）;<1.5：高风险（0分）"

    def net_margin_bin(self, net_profit_level, net_margin_T):
        if net_profit_level == "高":
            tip = "≥20%：20分；10%-20%：15分；0%-10%：10分；<0%：0分"
            if net_margin_T >= 0.2:
                return 20, "盈利能力强，利润水平高", tip
            elif net_margin_T >= 0.1:
                return 15, "盈利能力较强，利润水平适中", tip
            elif net_margin_T >= 0:
                return 10, "盈利能力一般，需提升利润水平", tip
            else:
                return 0, "盈利能力弱，存在亏损风险", tip
        elif net_profit_level == "中":
            tip = "≥10%：20分；5%-10%：15分；0%-5%：10分；<0%：0分"
            if net_margin_T >= 0.1:
                return 20, "盈利能力强，利润水平高", tip
            elif net_margin_T >= 0.05:
                return 15, "盈利能力较强，利润水平适中", tip
            elif net_margin_T >= 0:
                return 10, "盈利能力一般，需提升利润水平", tip
            else:
                return 0, "盈利能力弱，存在亏损风险", tip
        else:
            tip = "≥5%：20分；3%-5%：15分；0%-3%：10分；<0%：0分"
            if net_margin_T >= 0.05:
                return 20, "盈利能力强，利润水平高", tip
            elif net_margin_T >= 0.03:
                return 15, "盈利能力较强，利润水平适中", tip
            elif net_margin_T >= 0:
                return 10, "盈利能力一般，需提升利润水平", tip
            else:
                return 0, "盈利能力弱，存在亏损风险", tip

    def interest_coverage_ratio_bin(self,interest_coverage_ratio_T):
        if interest_coverage_ratio_T >= 999:
            return 15, "利息费用为0"
        elif interest_coverage_ratio_T >= 5:
            return 20, "融资成本低，偿债能力强"
        elif interest_coverage_ratio_T >= 3:
            return 15, "融资成本适中，偿债能力较强"
        elif interest_coverage_ratio_T >= 1:
            return 10, "融资成本较高，需关注偿债能力"
        else:
            return 0, "融资成本过高，存在偿债风险"

    def rate_profit_margin(self, industry):
        if str(industry)[:1] in ['J','k'] or industry in ['C3962']:
        # if industry in ['金融业', '房地产业', '人工智能', '半导体', '新能源']:
            return '高'
        elif str(industry)[:1] in ['C','G','P','A']:
        # elif industry in ['制造业', '交通运输、仓储和邮政', '教育', '纺织服装', '机械制造', '农、林、牧、渔业']:
            return '低'
        else:
            return '中'

    def rate_liquidity(self, industry):
        if str(industry)[:3] in ['F52','H62']:
        # if industry in ['零售业', '餐饮业']:
            return '中'
        elif str(industry)[:1] in ['C', 'E']:
        # elif industry in ['制造业', '建筑业']:
            return '高'
        elif str(industry)[:1] in ['O'] :# 居民服务、修理和其他服务业
        # elif industry in ['服务业']:
            return '低'
        else:
            return '中'

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
        except Exception as e:
            print(f"无法将字符串'{self.variables[var]}'转换为数值")
            sys.exit(1)
