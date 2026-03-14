from mapping.p11002.basic_unique import BasicUnique
from mapping.p11002.comprehensive_rate_processor import ComprehensiveRateProcessor
from mapping.p11002.data_prepared_processor import DataPreparedProcessor
from mapping.p11002.index_mapping_process import IndexMappingProcess
from mapping.p11002.main_indicators_analysis_processor import MainIndicatorsAnalysisProcessor
from mapping.p11002.risk_analysis_processor import RiskAnalysisProcessor
from mapping.tranformer import Transformer


class T11002(Transformer):
    """
    财报决策变量清洗入口及调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            # 主体名称
            "name": "",

            # 综合评级
            "comprehensive_rate": {},

            # 资产负债表
            "asset_dept_T": "",  # T年
            "cash_funds_T": "",  # T年货币资金
            "fair_value_assets_T": "",  # T年交易性金融资产
            "derivative_assets_T": "",  # T年衍生金融资产
            "notes_receivable_T": "",  # T年应收票据
            "accounts_receivable_T": "",  # T年应收账款
            "prepaid_T": "",  # T年预付账款
            "other_receivables_T": "",  # T年其他应收款
            "inventory_T": "",  # T年存货
            "raw_materials_T": "",  # T年其中：原材料
            "work_in_progress_T": "",  # T年在产品
            "finished_goods_T": "",  # T年库存商品
            "revolving_materials_T": "",  # 周转材料
            "assets_for_sale_T": "",  # T年持有待售资产
            "due_assets_T": "",  # T年一年内到期的非流动资产
            "other_current_T": "",  # T年其他流动资产
            "total_current_T": "",  # T年流动资产合计
            "available_for_sale_assets_T": "",  # T年可供出售金融资产
            "held_to_maturity_investments_T": "",  # T年持有至到期投资
            "long_term_receivables_T": "",  # T年长期应收款
            "long_term_equity_investments_T": "",  # T年长期股权投资
            "investment_real_estate_T": "",  # T年投资性房地产
            "gross_fixed_assets_T": "",  # T年固定资产原价
            "accumulated_depreciation_T": "",  # T年减：累计折旧
            "fixed_assets_T": "",  # T年固定资产
            "construction_in_progress_T": "",  # T年在建工程
            "productive_biological_assets_T": "",  # T年生产性生物资产
            "oil_and_gas_assets_T": "",  # T年油气资产
            "intangible_assets_T": "",  # T年无形资产
            "development_expenditures_T": "",  # T年开发支出
            "goodwill_T": "",  # T年商誉
            "long_term_prepaid_expenses_T": "",  # T年长期待摊费用
            "deferred_tax_assets_T": "",  # T年递延所得税资产
            "other_non_current_assets_T": "",  # T年其他非流动资产
            "total_non_current_assets_T": "",  # T年非流动资产合计
            "short_term_loans_T": "",  # T年短期借款
            "fair_value_liabilities_T": "",  # T年交易性金融负债
            "derivative_liabilities_T": "",  # T年衍生金融负债
            "notes_payable_T": "",  # T年应付票据
            "accounts_payable_T": "",  # T年应付账款
            "advances_from_customers_T": "",  # T年预收款项
            "salaries_payable_T": "",  # T年应付职工薪酬
            "taxes_payable_T": "",  # T年应交税费
            "other_payables_T": "",  # T年其他应付款
            "liabilities_for_sale_T": "",  # T年持有待售负债
            "due_liabilities_T": "",  # T年一年内到期的非流动负债
            "other_current_liabilities_T": "",  # T年其他流动负债
            "total_current_liabilities_T": "",  # T年流动负债合计
            "long_term_loans_T": "",  # T年长期借款
            "bonds_payable_T": "",  # T年应付债券
            "long_term_payables_T": "",  # T年长期应付款
            "provisions_T": "",  # T年预计负债
            "deferred_income_T": "",  # T年递延收益
            "deferred_tax_liabilities_T": "",  # T年递延所得税负债
            "other_non_current_liabilities_T": "",  # T年其他非流动负债
            "total_non_current_liabilities_T": "",  # T年非流动负债合计
            "total_liabilities_T": "",  # T年负债合计
            "paid_in_capital_T": "",  # T年实收资本（或股本）
            "other_equity_instruments_T": "",  # T年其他权益工具
            "capital_reserve_T": "",  # T年资本公积
            "other_comprehensive_income_T": "",  # T年其他综合收益
            "specific_reserves_T": "",  # T年专项储备
            "surplus_reserves_T": "",  # T年盈余公积
            "undistributed_profits_T": "",  # T年未分配利润
            "total_equity_T": "",  # T年所有者权益（或股东权益）总计
            "total_liabilities_and_equity_T": "",  # T年负债和所有者权益（或股东权益）合计
            "total_assets_T": "",  # T年资产合计
            "asset_dept_T1": "",  # T-1年
            "cash_funds_T1": "",  # T-1年货币资金
            "fair_value_assets_T1": "",  # T-1年交易性金融资产
            "derivative_assets_T1": "",  # T-1年衍生金融资产
            "notes_receivable_T1": "",  # T-1年应收票据
            "accounts_receivable_T1": "",  # T-1年应收账款
            "prepaid_T1": "",  # T-1年预付账款
            "other_receivables_T1": "",  # T-1年其他应收款
            "inventory_T1": "",  # T-1年存货
            "raw_materials_T1": "",  # T-1其中：原材料
            "work_in_progress_T1": "",  # T-1在产品
            "finished_goods_T1": "",  # T-1库存商品
            "revolving_materials_T1": "",  # T-1周转材料
            "assets_for_sale_T1": "",  # T-1年持有待售资产
            "due_assets_T1": "",  # T-1年一年内到期的非流动资产
            "other_current_T1": "",  # T-1年其他流动资产
            "total_current_T1": "",  # T-1年流动资产合计
            "available_for_sale_assets_T1": "",  # T-1年可供出售金融资产
            "held_to_maturity_investments_T1": "",  # T-1年持有至到期投资
            "long_term_receivables_T1": "",  # T-1年长期应收款
            "long_term_equity_investments_T1": "",  # T-1年长期股权投资
            "investment_real_estate_T1": "",  # T-1年投资性房地产
            "gross_fixed_assets_T1": "",  # T-1年固定资产原价
            "accumulated_depreciation_T1": "",  # T-1年减：累计折旧
            "fixed_assets_T1": "",  # T-1年固定资产
            "construction_in_progress_T1": "",  # T-1年在建工程
            "productive_biological_assets_T1": "",  # T-1年生产性生物资产
            "oil_and_gas_assets_T1": "",  # T-1年油气资产
            "intangible_assets_T1": "",  # T-1年无形资产
            "development_expenditures_T1": "",  # T-1年开发支出
            "goodwill_T1": "",  # T-1年商誉
            "long_term_prepaid_expenses_T1": "",  # T-1年长期待摊费用
            "deferred_tax_assets_T1": "",  # T-1年递延所得税资产
            "other_non_current_assets_T1": "",  # T-1年其他非流动资产
            "total_non_current_assets_T1": "",  # T-1年非流动资产合计
            "short_term_loans_T1": "",  # T-1年短期借款
            "fair_value_liabilities_T1": "",  # T-1年交易性金融负债
            "derivative_liabilities_T1": "",  # T-1年衍生金融负债
            "notes_payable_T1": "",  # T-1年应付票据
            "accounts_payable_T1": "",  # T-1年应付账款
            "advances_from_customers_T1": "",  # T-1年预收款项
            "salaries_payable_T1": "",  # T-1年应付职工薪酬
            "taxes_payable_T1": "",  # T-1年应交税费
            "other_payables_T1": "",  # T-1年其他应付款
            "liabilities_for_sale_T1": "",  # T-1年持有待售负债
            "due_liabilities_T1": "",  # T-1年一年内到期的非流动负债
            "other_current_liabilities_T1": "",  # T-1年其他流动负债
            "total_current_liabilities_T1": "",  # T-1年流动负债合计
            "long_term_loans_T1": "",  # T-1年长期借款
            "bonds_payable_T1": "",  # T-1年应付债券
            "long_term_payables_T1": "",  # T-1年长期应付款
            "provisions_T1": "",  # T-1年预计负债
            "deferred_income_T1": "",  # T-1年递延收益
            "deferred_tax_liabilities_T1": "",  # T-1年递延所得税负债
            "other_non_current_liabilities_T1": "",  # T-1年其他非流动负债
            "total_non_current_liabilities_T1": "",  # T-1年非流动负债合计
            "total_liabilities_T1": "",  # T-1年负债合计
            "paid_in_capital_T1": "",  # T-1年实收资本（或股本）
            "other_equity_instruments_T1": "",  # T-1年其他权益工具
            "capital_reserve_T1": "",  # T-1年资本公积
            "other_comprehensive_income_T1": "",  # T-1年其他综合收益
            "specific_reserves_T1": "",  # T-1年专项储备
            "surplus_reserves_T1": "",  # T-1年盈余公积
            "undistributed_profits_T1": "",  # T-1年未分配利润
            "total_equity_T1": "",  # T-1年所有者权益（或股东权益）总计
            "total_liabilities_and_equity_T1": "",  # T-1年负债和所有者权益（或股东权益）合计
            "total_assets_T1": "",  # T-1年资产合计
            "asset_dept_T2": "",  # T-2年
            "cash_funds_T2": "",  # T-2年货币资金
            "fair_value_assets_T2": "",  # T-2年交易性金融资产
            "derivative_assets_T2": "",  # T-2年衍生金融资产
            "notes_receivable_T2": "",  # T-2年应收票据
            "accounts_receivable_T2": "",  # T-2年应收账款
            "prepaid_T2": "",  # T-2年预付账款
            "other_receivables_T2": "",  # T-2年其他应收款
            "inventory_T2": "",  # T-2年存货
            "raw_materials_T2": "",  # T-2其中：原材料
            "work_in_progress_T2": "",  # T-2在产品
            "finished_goods_T2": "",  # T-2库存商品
            "revolving_materials_T2": "",  # T-2周转材料
            "assets_for_sale_T2": "",  # T-2年持有待售资产
            "due_assets_T2": "",  # T-2年一年内到期的非流动资产
            "other_current_T2": "",  # T-2年其他流动资产
            "total_current_T2": "",  # T-2年流动资产合计
            "available_for_sale_assets_T2": "",  # T-2年可供出售金融资产
            "held_to_maturity_investments_T2": "",  # T-2年持有至到期投资
            "long_term_receivables_T2": "",  # T-2年长期应收款
            "long_term_equity_investments_T2": "",  # T-2年长期股权投资
            "investment_real_estate_T2": "",  # T-2年投资性房地产
            "gross_fixed_assets_T2": "",  # T-2年固定资产原价
            "accumulated_depreciation_T2": "",  # T-2年减：累计折旧
            "fixed_assets_T2": "",  # T-2年固定资产
            "construction_in_progress_T2": "",  # T-2年在建工程
            "productive_biological_assets_T2": "",  # T-2年生产性生物资产
            "oil_and_gas_assets_T2": "",  # T-2年油气资产
            "intangible_assets_T2": "",  # T-2年无形资产
            "development_expenditures_T2": "",  # T-2年开发支出
            "goodwill_T2": "",  # T-2年商誉
            "long_term_prepaid_expenses_T2": "",  # T-2年长期待摊费用
            "deferred_tax_assets_T2": "",  # T-2年递延所得税资产
            "other_non_current_assets_T2": "",  # T-2年其他非流动资产
            "total_non_current_assets_T2": "",  # T-2年非流动资产合计
            "short_term_loans_T2": "",  # T-2年短期借款
            "fair_value_liabilities_T2": "",  # T-2年交易性金融负债
            "derivative_liabilities_T2": "",  # T-2年衍生金融负债
            "notes_payable_T2": "",  # T-2年应付票据
            "accounts_payable_T2": "",  # T-2年应付账款
            "advances_from_customers_T2": "",  # T-2年预收款项
            "salaries_payable_T2": "",  # T-2年应付职工薪酬
            "taxes_payable_T2": "",  # T-2年应交税费
            "other_payables_T2": "",  # T-2年其他应付款
            "liabilities_for_sale_T2": "",  # T-2年持有待售负债
            "due_liabilities_T2": "",  # T-2年一年内到期的非流动负债
            "other_current_liabilities_T2": "",  # T-2年其他流动负债
            "total_current_liabilities_T2": "",  # T-2年流动负债合计
            "long_term_loans_T2": "",  # T-2年长期借款
            "bonds_payable_T2": "",  # T-2年应付债券
            "long_term_payables_T2": "",  # T-2年长期应付款
            "provisions_T2": "",  # T-2年预计负债
            "deferred_income_T2": "",  # T-2年递延收益
            "deferred_tax_liabilities_T2": "",  # T-2年递延所得税负债
            "other_non_current_liabilities_T2": "",  # T-2年其他非流动负债
            "total_non_current_liabilities_T2": "",  # T-2年非流动负债合计
            "total_liabilities_T2": "",  # T-2年负债合计
            "paid_in_capital_T2": "",  # T-2年实收资本（或股本）
            "other_equity_instruments_T2": "",  # T-2年其他权益工具
            "capital_reserve_T2": "",  # T-2年资本公积
            "other_comprehensive_income_T2": "",  # T-2年其他综合收益
            "specific_reserves_T2": "",  # T-2年专项储备
            "surplus_reserves_T2": "",  # T-2年盈余公积
            "undistributed_profits_T2": "",  # T-2年未分配利润
            "total_equity_T2": "",  # T-2年所有者权益（或股东权益）总计
            "total_liabilities_and_equity_T2": "",  # T-2年负债和所有者权益（或股东权益）合计
            "total_assets_T2": "",  # T-2年资产合计

            # 现金流量表
            "cash_flow_date_range": "",  # 编制时间
            "cash_flow_T": "",  # T年
            "cash_from_sales_T": "",  # T年销售商品、提供劳务收到的现金
            "tax_refunds_received_T": "",  # T年收到的税费返还
            "other_operating_cash_in_T": "",  # T年收到其他与经营活动有关的现金
            "total_operating_cash_in_T": "",  # T年经营活动现金流入小计
            "cash_paid_for_goods_T": "",  # T年购买商品、接受劳务支付的现金
            "cash_paid_to_employees_T": "",  # T年支付给职工以及为职工支付的现金
            "taxes_paid_T": "",  # T年支付的各项税费
            "other_operating_cash_out_T": "",  # T年支付其他与经营活动有关的现金
            "total_operating_cash_out_T": "",  # T年经营活动现金流出小计
            "net_operating_cash_flow_T": "",  # T年经营活动产生的现金流量净额
            "cash_from_investments_T": "",  # T年收回投资所收到的现金
            "cash_from_investment_income_T": "",  # T年取得投资收益收到的现金
            "cash_from_asset_disposals_T": "",  # T年处置固定资产、无形资产和其他长期资产而收回的现金净额
            "cash_from_sub_disposals_T": "",  # T年处置子公司及其他营业单位收到的现金净额
            "other_investment_cash_in_T": "",  # T年收到其他与投资活动有关的现金
            "total_investment_cash_in_T": "",  # T年投资活动现金流入小计
            "cash_paid_for_assets_T": "",  # T年购建固定资产、无形资产和其他长期资产支付的现金
            "cash_paid_for_investments_T": "",  # T年投资支付的现金
            "cash_paid_for_subs_T": "",  # T年取得子公司及其他营业单位支付的现金净额
            "other_investment_cash_out_T": "",  # T年支付其他与投资活动有关的现金
            "total_investment_cash_out_T": "",  # T年投资活动现金流出小计
            "net_investment_cash_flow_T": "",  # T年投资活动产生的现金流量净额
            "cash_from_investment_received_T": "",  # T年吸收投资收到的现金
            "cash_from_borrowing_T": "",  # T年取得借款收到的现金
            "other_financing_cash_in_T": "",  # T年收到其他与筹资活动有关的现金
            "total_financing_cash_in_T": "",  # T年筹资活动现金流入小计
            "cash_paid_for_debt_T": "",  # T年偿还债务支付的现金
            "cash_paid_for_dividends_T": "",  # T年分配股利、利润或偿付利息支付的现金
            "other_financing_cash_out_T": "",  # T年支付其他与筹资活动有关的现金
            "total_financing_cash_out_T": "",  # T年筹资活动现金流出小计
            "net_financing_cash_flow_T": "",  # T年筹资活动产生的现金流量净额
            "fx_impact_on_cash_T": "",  # T年四、汇率变动对现金及现金等价物的影响
            "net_increase_in_cash_T": "",  # T年五、现金及现金等价物净增加额
            "beginning_cash_balance_T": "",  # T年加：期初现金及现金等价物余额
            "ending_cash_balance_T": "",  # T年六、期末现金及现金等价物余额
            "cash_flow_T1": "",  # T-1年
            "cash_from_sales_T1": "",  # T-1年销售商品、提供劳务收到的现金
            "tax_refunds_received_T1": "",  # T-1年收到的税费返还
            "other_operating_cash_in_T1": "",  # T-1年收到其他与经营活动有关的现金
            "total_operating_cash_in_T1": "",  # T-1年经营活动现金流入小计
            "cash_paid_for_goods_T1": "",  # T-1年购买商品、接受劳务支付的现金
            "cash_paid_to_employees_T1": "",  # T-1年支付给职工以及为职工支付的现金
            "taxes_paid_T1": "",  # T-1年支付的各项税费
            "other_operating_cash_out_T1": "",  # T-1年支付其他与经营活动有关的现金
            "total_operating_cash_out_T1": "",  # T-1年经营活动现金流出小计
            "net_operating_cash_flow_T1": "",  # T-1年经营活动产生的现金流量净额
            "cash_from_investments_T1": "",  # T-1年收回投资所收到的现金
            "cash_from_investment_income_T1": "",  # T-1年取得投资收益收到的现金
            "cash_from_asset_disposals_T1": "",  # T-1年处置固定资产、无形资产和其他长期资产而收回的现金净额
            "cash_from_sub_disposals_T1": "",  # T-1年处置子公司及其他营业单位收到的现金净额
            "other_investment_cash_in_T1": "",  # T-1年收到其他与投资活动有关的现金
            "total_investment_cash_in_T1": "",  # T-1年投资活动现金流入小计
            "cash_paid_for_assets_T1": "",  # T-1年购建固定资产、无形资产和其他长期资产支付的现金
            "cash_paid_for_investments_T1": "",  # T-1年投资支付的现金
            "cash_paid_for_subs_T1": "",  # T-1年取得子公司及其他营业单位支付的现金净额
            "other_investment_cash_out_T1": "",  # T-1年支付其他与投资活动有关的现金
            "total_investment_cash_out_T1": "",  # T-1年投资活动现金流出小计
            "net_investment_cash_flow_T1": "",  # T-1年投资活动产生的现金流量净额
            "cash_from_investment_received_T1": "",  # T-1年吸收投资收到的现金
            "cash_from_borrowing_T1": "",  # T-1年取得借款收到的现金
            "other_financing_cash_in_T1": "",  # T-1年收到其他与筹资活动有关的现金
            "total_financing_cash_in_T1": "",  # T-1年筹资活动现金流入小计
            "cash_paid_for_debt_T1": "",  # T-1年偿还债务支付的现金
            "cash_paid_for_dividends_T1": "",  # T-1年分配股利、利润或偿付利息支付的现金
            "other_financing_cash_out_T1": "",  # T-1年支付其他与筹资活动有关的现金
            "total_financing_cash_out_T1": "",  # T-1年筹资活动现金流出小计
            "net_financing_cash_flow_T1": "",  # T-1年筹资活动产生的现金流量净额
            "fx_impact_on_cash_T1": "",  # T-1年四、汇率变动对现金及现金等价物的影响
            "net_increase_in_cash_T1": "",  # T-1年五、现金及现金等价物净增加额
            "beginning_cash_balance_T1": "",  # T-1年加：期初现金及现金等价物余额
            "ending_cash_balance_T1": "",  # T-1年六、期末现金及现金等价物余额
            "cash_flow_T2": "",  # T-2年
            "cash_from_sales_T2": "",  # T-2年销售商品、提供劳务收到的现金
            "tax_refunds_received_T2": "",  # T-2年收到的税费返还
            "other_operating_cash_in_T2": "",  # T-2年收到其他与经营活动有关的现金
            "total_operating_cash_in_T2": "",  # T-2年经营活动现金流入小计
            "cash_paid_for_goods_T2": "",  # T-2年购买商品、接受劳务支付的现金
            "cash_paid_to_employees_T2": "",  # T-2年支付给职工以及为职工支付的现金
            "taxes_paid_T2": "",  # T-2年支付的各项税费
            "other_operating_cash_out_T2": "",  # T-2年支付其他与经营活动有关的现金
            "total_operating_cash_out_T2": "",  # T-2年经营活动现金流出小计
            "net_operating_cash_flow_T2": "",  # T-2年经营活动产生的现金流量净额
            "cash_from_investments_T2": "",  # T-2年收回投资所收到的现金
            "cash_from_investment_income_T2": "",  # T-2年取得投资收益收到的现金
            "cash_from_asset_disposals_T2": "",  # T-2年处置固定资产、无形资产和其他长期资产而收回的现金净额
            "cash_from_sub_disposals_T2": "",  # T-2年处置子公司及其他营业单位收到的现金净额
            "other_investment_cash_in_T2": "",  # T-2年收到其他与投资活动有关的现金
            "total_investment_cash_in_T2": "",  # T-2年投资活动现金流入小计
            "cash_paid_for_assets_T2": "",  # T-2年购建固定资产、无形资产和其他长期资产支付的现金
            "cash_paid_for_investments_T2": "",  # T-2年投资支付的现金
            "cash_paid_for_subs_T2": "",  # T-2年取得子公司及其他营业单位支付的现金净额
            "other_investment_cash_out_T2": "",  # T-2年支付其他与投资活动有关的现金
            "total_investment_cash_out_T2": "",  # T-2年投资活动现金流出小计
            "net_investment_cash_flow_T2": "",  # T-2年投资活动产生的现金流量净额
            "cash_from_investment_received_T2": "",  # T-2年吸收投资收到的现金
            "cash_from_borrowing_T2": "",  # T-2年取得借款收到的现金
            "other_financing_cash_in_T2": "",  # T-2年收到其他与筹资活动有关的现金
            "total_financing_cash_in_T2": "",  # T-2年筹资活动现金流入小计
            "cash_paid_for_debt_T2": "",  # T-2年偿还债务支付的现金
            "cash_paid_for_dividends_T2": "",  # T-2年分配股利、利润或偿付利息支付的现金
            "other_financing_cash_out_T2": "",  # T-2年支付其他与筹资活动有关的现金
            "total_financing_cash_out_T2": "",  # T-2年筹资活动现金流出小计
            "net_financing_cash_flow_T2": "",  # T-2年筹资活动产生的现金流量净额
            "fx_impact_on_cash_T2": "",  # T-2年四、汇率变动对现金及现金等价物的影响
            "net_increase_in_cash_T2": "",  # T-2年五、现金及现金等价物净增加额
            "beginning_cash_balance_T2": "",  # T-2年加：期初现金及现金等价物余额
            "ending_cash_balance_T2": "",  # T-2年六、期末现金及现金等价物余额

            # 利润表
            "profit_date_range": "",  # 编制时间
            "profit_T": "",  # T年
            "revenue_T": "",  # T年一、营业收入
            "main_revenue_T": "",  # T年主营业务收入
            "other_revenue_T": "",  # T年其他业务收入
            "operating_cost_T": "",  # T年减：营业成本
            "main_cost_T": "",  # T年主营业务成本
            "other_cost_T": "",  # T年其他业务成本
            "gross_profit_T": "",  # T年毛利润
            "gross_margin_T": "",  # T年毛利润率（%）
            "taxes_surcharges_T": "",  # T年减：税金及附加
            "period_cost_T": "",  # T年减：期间费用
            "selling_expense_T": "",  # T年销售费用
            "rent_T": "",  # T年其中：租金
            "admin_expense_T": "",  # T年管理费用
            "wages_T": "",  # T年其中：工资
            "insurance_fund_T": "",  # T年社保、公积金
            "utilities_T": "",  # T年水电气
            "r_and_d_expense_T": "",  # T年研发费用
            "finance_expense_T": "",  # T年财务费用
            "interest_expense_T": "",  # T年其中：利息费用
            "interest_income_T": "",  # T年利息收入
            "other_expense_T": "",  # T年其他费用
            "other_income_T": "",  # T年加：其他收益
            "asset_impairment_T": "",  # T年减：资产减值损失
            "operating_profit_T": "",  # T年二、营业利润
            "non_operating_income_T": "",  # T年加：营业外收入
            "non_operating_expense_T": "",  # T年减：营业外支出
            "total_profit_T": "",  # T年三、利润总额
            "income_tax_T": "",  # T年减：所得税费用
            "net_profit_T": "",  # T年四、净利润
            "net_margin_T": "",  # T年净利润率（%）
            "profit_T1": "",  # T-1年
            "revenue_T1": "",  # T-1年一、营业收入
            "main_revenue_T1": "",  # T-1年主营业务收入
            "other_revenue_T1": "",  # T-1年其他业务收入
            "operating_cost_T1": "",  # T-1年减：营业成本
            "main_cost_T1": "",  # T-1年主营业务成本
            "other_cost_T1": "",  # T-1年其他业务成本
            "gross_profit_T1": "",  # T-1年毛利润
            "gross_margin_T1": "",  # T-1年毛利润率（%）
            "taxes_surcharges_T1": "",  # T-1年减：税金及附加
            "period_cost_T1": "",  # T-1年减：期间费用
            "selling_expense_T1": "",  # T-1年销售费用
            "rent_T1": "",  # T-1年其中：租金
            "admin_expense_T1": "",  # T-1年管理费用
            "wages_T1": "",  # T-1年其中：工资
            "insurance_fund_T1": "",  # T-1年社保、公积金
            "utilities_T1": "",  # T-1年水电气
            "r_and_d_expense_T1": "",  # T-1年研发费用
            "finance_expense_T1": "",  # T-1年财务费用
            "interest_expense_T1": "",  # T-1年其中：利息费用
            "interest_income_T1": "",  # T-1年利息收入
            "other_expense_T1": "",  # T-1年其他费用
            "other_income_T1": "",  # T-1年加：其他收益
            "asset_impairment_T1": "",  # T-1年减：资产减值损失
            "operating_profit_T1": "",  # T-1年二、营业利润
            "non_operating_income_T1": "",  # T-1年加：营业外收入
            "non_operating_expense_T1": "",  # T-1年减：营业外支出
            "total_profit_T1": "",  # T-1年三、利润总额
            "income_tax_T1": "",  # T-1年减：所得税费用
            "net_profit_T1": "",  # T-1年四、净利润
            "net_margin_T1": "",  # T-1年净利润率（%）
            "profit_T2": "",  # T-2年
            "revenue_T2": "",  # T-2年一、营业收入
            "main_revenue_T2": "",  # T-2年主营业务收入
            "other_revenue_T2": "",  # T-2年其他业务收入
            "operating_cost_T2": "",  # T-2年减：营业成本
            "main_cost_T2": "",  # T-2年主营业务成本
            "other_cost_T2": "",  # T-2年其他业务成本
            "gross_profit_T2": "",  # T-2年毛利润
            "gross_margin_T2": "",  # T-2年毛利润率（%）
            "taxes_surcharges_T2": "",  # T-2年减：税金及附加
            "period_cost_T2": "",  # T-2年减：期间费用
            "selling_expense_T2": "",  # T-2年销售费用
            "rent_T2": "",  # T-2年其中：租金
            "admin_expense_T2": "",  # T-2年管理费用
            "wages_T2": "",  # T-2年其中：工资
            "insurance_fund_T2": "",  # T-2年社保、公积金
            "utilities_T2": "",  # T-2年水电气
            "r_and_d_expense_T2": "",  # T-2年研发费用
            "finance_expense_T2": "",  # T-2年财务费用
            "interest_expense_T2": "",  # T-2年其中：利息费用
            "interest_income_T2": "",  # T-2年利息收入
            "other_expense_T2": "",  # T-2年其他费用
            "other_income_T2": "",  # T-2年加：其他收益
            "asset_impairment_T2": "",  # T-2年减：资产减值损失
            "operating_profit_T2": "",  # T-2年二、营业利润
            "non_operating_income_T2": "",  # T-2年加：营业外收入
            "non_operating_expense_T2": "",  # T-2年减：营业外支出
            "total_profit_T2": "",  # T-2年三、利润总额
            "income_tax_T2": "",  # T-2年减：所得税费用
            "net_profit_T2": "",  # T-2年四、净利润
            "net_margin_T2": "",  # T-2年净利润率（%）

            "investment_income_T": "",  # T年其中：投资收益
            "investment_income_T1": "",  # T-1年其中：投资收益
            "investment_income_T2": "",  # T-2年其中：投资收益

            # 主要财务指标分析
            "main_T": "",  # T年
            "main_T1": "",  # T-1年
            "main_T2": "",  # T-2年
            "return_on_total_assets_T": "",  # T年总资产净利率（ROA）
            "return_on_total_assets_T1":"",# T-1年总资产净利率（ROA）
            "return_on_total_assets_T2": "",  # T-2年总资产净利率（ROA）
            "ROE_T":"",# T年净资产收益率（ROE）
            "ROE_T1": "",  # T-1年净资产收益率（ROE）
            "ROE_T2": "",  # T-2年净资产收益率（ROE）

            "operating_profit_ratio_T": "",  # T年营业利润对利润总额占比（%）
            "net_investment_ratio_T": "",  # T年投资活动产生的现金流量净额对利润总额占比（%）
            "inventory_turnover_T": "",  # T年存货周转天数（天）
            "receivables_turnover_T": "",  # T年应收账款周转天数（天）
            "payables_turnover_T": "",  # T年应付账款周转天数（天）
            "advances_turnover_T": "",  # T年预付账款周转天数（天）
            "deposits_turnover_T": "",  # T年预收账款周转天数（天）
            "current_ratio_T": "",  # T年流动比率
            "quick_ratio_T": "",  # T年速动比率
            "cash_ratio_T": "",  # T年现金比率
            "debt_to_assets_ratio_T": "",  # T年资产负债率（％）
            "total_assets_growth_T": "",  # T年总资产同比（%）
            "short_term_loans_growth_T": "",  # T年短期借款同比（%）
            "long_term_loans_growth_T": "",  # T年长期借款同比（%）
            "revenue_growth_T": "",  # T年营业额同比（%）
            "net_profit_growth_T": "",  # T年净利润同比（%）
            "receivables_growth_T": "",  # T年应收账款同比（%）
            "inventory_growth_T": "",  # T年存货同比（%）
            "other_receivables_growth_T": "",  # T年其他应收同比（%）
            "payables_growth_T": "",  # T年应付账款同比（%）
            "other_payables_growth_T": "",  # T年其他应付同比（%）
            "sales_to_loans_ratio_T": "",  # T年销贷比（%）
            "current_assets_to_short_debt_T": "",  # T年流动资产对短期负债占比（%）
            "inventory_to_current_assets_T": "",  # T年存货对流动资产占比（%）
            "external_investment_to_assets_T": "",  # T年对外投资对总资产占比（%）
            "operating_profit_ratio_T1": "",  # T-1年营业利润对利润总额占比（%）
            "net_investment_ratio_T1": "",  # T-1年投资活动产生的现金流量净额对利润总额占比（%）
            "inventory_turnover_T1": "",  # T-1年存货周转天数（天）
            "receivables_turnover_T1": "",  # T-1年应收账款周转天数（天）
            "payables_turnover_T1": "",  # T-1年应付账款周转天数（天）
            "advances_turnover_T1": "",  # T-1年预付账款周转天数（天）
            "deposits_turnover_T1": "",  # T-1年预收账款周转天数（天）
            "current_ratio_T1": "",  # T-1年流动比率
            "quick_ratio_T1": "",  # T-1年速动比率
            "cash_ratio_T1": "",  # T-1年现金比率
            "debt_to_assets_ratio_T1": "",  # T-1年资产负债率（％）
            "total_assets_growth_T1": "",  # T-1年总资产同比（%）
            "short_term_loans_growth_T1": "",  # T-1年短期借款同比（%）
            "long_term_loans_growth_T1": "",  # T-1年长期借款同比（%）
            "revenue_growth_T1": "",  # T-1年营业额同比（%）
            "net_profit_growth_T1": "",  # T-1年净利润同比（%）
            "receivables_growth_T1": "",  # T-1年应收账款同比（%）
            "inventory_growth_T1": "",  # T-1年存货同比（%）
            "other_receivables_growth_T1": "",  # T-1年其他应收同比（%）
            "payables_growth_T1": "",  # T-1年应付账款同比（%）
            "other_payables_growth_T1": "",  # T-1年其他应付同比（%）
            "sales_to_loans_ratio_T1": "",  # T-1年销贷比（%）
            "current_assets_to_short_debt_T1": "",  # T-1年流动资产对短期负债占比（%）
            "inventory_to_current_assets_T1": "",  # T-1年存货对流动资产占比（%）
            "external_investment_to_assets_T1": "",  # T-1年对外投资对总资产占比（%）
            "operating_profit_ratio_T2": "",  # T-2年营业利润对利润总额占比（%）
            "net_investment_ratio_T2": "",  # T-2年投资活动产生的现金流量净额对利润总额占比（%）
            "inventory_turnover_T2": "",  # T-2年存货周转天数（天）
            "receivables_turnover_T2": "",  # T-2年应收账款周转天数（天）
            "payables_turnover_T2": "",  # T-2年应付账款周转天数（天）
            "advances_turnover_T2": "",  # T-2年预付账款周转天数（天）
            "deposits_turnover_T2": "",  # T-2年预收账款周转天数（天）
            "current_ratio_T2": "",  # T-2年流动比率
            "quick_ratio_T2": "",  # T-2年速动比率
            "cash_ratio_T2": "",  # T-2年现金比率
            "debt_to_assets_ratio_T2": "",  # T-2年资产负债率（％）
            "total_assets_growth_T2": "",  # T-2年总资产同比（%）
            "short_term_loans_growth_T2": "",  # T-2年短期借款同比（%）
            "long_term_loans_growth_T2": "",  # T-2年长期借款同比（%）
            "revenue_growth_T2": "",  # T-2年营业额同比（%）
            "net_profit_growth_T2": "",  # T-2年净利润同比（%）
            "receivables_growth_T2": "",  # T-2年应收账款同比（%）
            "inventory_growth_T2": "",  # T-2年存货同比（%）
            "other_receivables_growth_T2": "",  # T-2年其他应收同比（%）
            "payables_growth_T2": "",  # T-2年应付账款同比（%）
            "other_payables_growth_T2": "",  # T-2年其他应付同比（%）
            "sales_to_loans_ratio_T2": "",  # T-2年销贷比（%）
            "current_assets_to_short_debt_T2": "",  # T-2年流动资产对短期负债占比（%）
            "inventory_to_current_assets_T2": "",  # T-2年存货对流动资产占比（%）
            "external_investment_to_assets_T2": "",  # T-2年对外投资对总资产占比（%）
            "cash_flow_ratio_T": "",  # T年现金流量比率
            "cash_flow_ratio_T1": "",  # T-1年现金流量比率
            "cash_flow_ratio_T2": "",  # T-2年现金流量比率
            "cash_flow_interest_coverage_ratio_T":"",# T年现金流量利息保障倍数
            "cash_flow_interest_coverage_ratio_T1": "",  # T-1年现金流量利息保障倍数
            "cash_flow_interest_coverage_ratio_T2": "",  # T-2年现金流量利息保障倍数
            "cash_flow_to_debt_ratio_T":"", # T年现金流量与负债比率
            "cash_flow_to_debt_ratio_T1": "",  # T-1年现金流量与负债比率
            "cash_flow_to_debt_ratio_T2": "",  # T-2年现金流量与负债比率
            "operating_cash_flow_to_net_profit_ratio_T":"",# T年经营现金流与净利比率
            "operating_cash_flow_to_net_profit_ratio_T1": "",  # T-1年经营现金流与净利比率
            "operating_cash_flow_to_net_profit_ratio_T2": "",  # T-2年经营现金流与净利比率
            "equity_ratio_T":"",# T年产权比率
            "equity_ratio_T1": "",  # T-1年产权比率
            "equity_ratio_T2": "",  # T-2年产权比率
            "total_asset_turnover_ratio_T":"",# T年总资产周转率
            "total_asset_turnover_ratio_T1": "",  # T1年总资产周转率
            "total_asset_turnover_ratio_T2": "",  # T2年总资产周转率
            "total_asset_turnover_T":"",# T年总资产周转天数
            "total_asset_turnover_T1": "",  # T1年总资产周转天数
            "total_asset_turnover_T2": "",  # T2年总资产周转天数
            "current_asset_turnover_T":"", # T年流动资产周转天数
            "current_asset_turnover_T1": "",  # T1年流动资产周转天数
            "current_asset_turnover_T2": "",  # T2年流动资产周转天数


            # 数字纠偏
            "check_current_assets_T": 0,  # T年流动资产合计与明细项目不一致，请核实各项数据。
            "check_noncurrent_assets_T": 0,  # T年非流动资产合计与明细项目不一致，请核实各项数据。
            "check_current_liabilities_T": 0,  # T年流动负债合计与明细项目不一致，请核实各项数据。
            "check_noncurrent_liabilities_T": 0,  # T年非流动负债合计与明细项目不一致，请核实各项数据。
            "check_equity_T": 0,  # T年所得者权益（或股东权益）总计与明细项目不一致，请核实各项数据。
            "check_assets_eq_liabilities_T": 0,  # T年资产总计!=负债合计+所有者权益，请核实各项数据。
            "check_period_expenses_T": 0,  # T年期间费用与明细项目不一致，请核实各项数据。
            "check_gross_profit_T": 0,  # T年毛利润！=营业收入-营业成本，请核实各项数据。
            "check_operating_profit_T": 0,  # T年营业利润！=毛利润-税金及附加-期间费用+其他收益-资产减值损失，请核实各项数据。
            "check_total_profit_T": 0,  # T年利润总额！=营业利润+营业外收入-营业外支出，请核实各项数据。
            "check_net_profit_T": 0,  # T年净利润！=利润总额-所得税费用，请核实各项数据。
            "check_cashflow_balance_T": 0,  # T年现金流量表各项主要指标存在不平衡，请核实各项数据。
            "check_pnl_cashflow_T": 0,  # T年损益表与现金流量表利润不匹配，请核实各项数据。
            "check_current_assets_T1": 0,  # T-1年流动资产合计与明细项目不一致，请核实各项数据。
            "check_noncurrent_assets_T1": 0,  # T-1年非流动资产合计与明细项目不一致，请核实各项数据。
            "check_current_liabilities_T1": 0,  # T-1年流动负债合计与明细项目不一致，请核实各项数据。
            "check_noncurrent_liabilities_T1": 0,  # T-1年非流动负债合计与明细项目不一致，请核实各项数据。
            "check_equity_T1": 0,  # T-1年所得者权益（或股东权益）总计与明细项目不一致，请核实各项数据。
            "check_assets_eq_liabilities_T1": 0,  # T-1年资产总计!=负债合计+所有者权益，请核实各项数据。
            "check_period_expenses_T1": 0,  # T-1年期间费用与明细项目不一致，请核实各项数据。
            "check_gross_profit_T1": 0,  # T-1年毛利润！=营业收入-营业成本，请核实各项数据。
            "check_operating_profit_T1": 0,  # T-1年营业利润！=毛利润-税金及附加-期间费用+其他收益-资产减值损失，请核实各项数据。
            "check_total_profit_T1": 0,  # T-1年利润总额！=营业利润+营业外收入-营业外支出，请核实各项数据。
            "check_net_profit_T1": 0,  # T-1年净利润！=利润总额-所得税费用，请核实各项数据。
            "check_cashflow_balance_T1": 0,  # T-1年现金流量表各项主要指标存在不平衡，请核实各项数据。
            "check_pnl_cashflow_T1": 0,  # T-1年损益表与现金流量表利润不匹配，请核实各项数据。
            "check_current_assets_T2": 0,  # T-2年流动资产合计与明细项目不一致，请核实各项数据。
            "check_noncurrent_assets_T2": 0,  # T-2年非流动资产合计与明细项目不一致，请核实各项数据。
            "check_current_liabilities_T2": 0,  # T-2年流动负债合计与明细项目不一致，请核实各项数据。
            "check_noncurrent_liabilities_T2": 0,  # T-2年非流动负债合计与明细项目不一致，请核实各项数据。
            "check_equity_T2": 0,  # T-2年所得者权益（或股东权益）总计与明细项目不一致，请核实各项数据。
            "check_assets_eq_liabilities_T2": 0,  # T-2年资产总计!=负债合计+所有者权益，请核实各项数据。
            "check_period_expenses_T2": 0,  # T-2年期间费用与明细项目不一致，请核实各项数据。
            "check_gross_profit_T2": 0,  # T-2年毛利润！=营业收入-营业成本，请核实各项数据。
            "check_operating_profit_T2": 0,  # T-2年营业利润！=毛利润-税金及附加-期间费用+其他收益-资产减值损失，请核实各项数据。
            "check_total_profit_T2": 0,  # T-2年利润总额！=营业利润+营业外收入-营业外支出，请核实各项数据。
            "check_net_profit_T2": 0,  # T-2年净利润！=利润总额-所得税费用，请核实各项数据。
            "check_cashflow_balance_T2": 0,  # T-2年现金流量表各项主要指标存在不平衡，请核实各项数据。
            "check_pnl_cashflow_T2": 0,  # T-2年损益表与现金流量表利润不匹配，请核实各项数据。

            "check_cash_funds_T": 0,  # T年货币资金！=期末现金及现金等价物余额-期初现金及现金等价物余额，存在勾稽问题
            "check_cash_funds_T1": 0,  # T1年货币资金！=期末现金及现金等价物余额-期初现金及现金等价物余额，存在勾稽问题
            "check_cash_funds_T2": 0,  # T2年货币资金！=期末现金及现金等价物余额-期初现金及现金等价物余额，存在勾稽问题
            "check_undistributed_profits_T": 0,  # T年净利润！=期末未分配利润-期初未分配利润，存在勾稽问题
            "check_undistributed_profits_T1": 0,  # T1年净利润！=期末未分配利润-期初未分配利润，存在勾稽问题
            "check_undistributed_profits_T2": 0,

            # 风险预警与可视化
            "debt_asset_ratio_T": 0,  # 资产负债率过大
            "debt_asset_ratio_T1": 0,  # 资产负债率过大
            "net_profit_growth_rate": 0,  # 净利润同比下滑严重
            "cost_growth_rate": 0,  # 营业成本增长率与存货增长率趋势相反
            "inventory_growth_rate": 0,  # 营业成本增长率与存货增长率趋势相反
            "revenue_growth_rate": 0,  # 营业收入同比下滑较大
            "utilities_growth_rate": 0,  # 水电费增长率与收入增长率趋势相反
            "accounts_receivable_growth_rate": 0,  # 应收账款增长率远高于营业收入增长率
            "accounts_receivable_turnover_rate": 9999,  # 应收账款周转率<1.5，资金周转较差
            "receivables_current_assets_ratio": 0,  # 应收账款占流动资产比例较大
            "accounts_payable_growth_rate": 0,  # 应付账款同比增长较快
            "fixed_and_intangible_assets": 0,  # 企业管理成本的同时实现资产的有效增长，运营能力较好
            "main_business_cost": 0,  # 企业管理成本的同时实现资产的有效增长，运营能力较好
            "admin_expense_growth_rate": 0,  # 管理费用增长率与收入增长率趋势相反
            "main_revenue_growth_rate": 0,  # 管理费用增长率与收入增长率趋势相反

            "operating_cash_flow_growth_rate": 0,  # 经营活动产生的现金流量净额增长率

            # 可视化
            "revenue_view_T": 0,  # T年营业收入
            "revenue_view_T1": 0,  # T-1年营业收入
            "revenue_view_T2": 0,  # T-2年营业收入
            "operating_cost_view_T": 0,  # T年营业成本
            "operating_cost_view_T1": 0,  # T-1年营业成本
            "operating_cost_view_T2": 0,  # T-2年营业成本
            "gross_margin_view_T": 0,  # T年毛利润率
            "gross_margin_view_T1": 0,  # T-1年毛利润率
            "gross_margin_view_T2": 0,  # T-2年毛利润率
            "net_margin_view_T": 0,  # T年净利润率
            "net_margin_view_T1": 0,  # T-1年净利润率
            "net_margin_view_T2": 0,  # T-2年净利润率
            "operating_cost_ratio": 0,  # 营业成本所占比例
            "taxes_surcharges_ratio": 0,  # 税金及附加所占比例
            "selling_expense_ratio": 0,  # 销售费用所占比例
            "admin_expense_ratio": 0,  # 管理费用所占比例
            "finance_expense_ratio": 0,  # 财务费用所占比例
            "other_expense_ratio": 0,  # 其他费用所占比例
            "asset_impairment_ratio": 0,  # 资产减值损失所占比例
            "operating_profit_ratio": 0,  # 营业利润所占比例
            "debt_asset_ratio_view_T": 0,  # 资产负债率
            "receivable_turnover_rate": 0,  # 应收账款周转率
            "current_ratio_view_T": 0,  # 流动比率
            "quick_ratio_view_T": 0,  # 速动比率
            "cash_ratio_view_T": 0,  # 现金比率
            "debt_asset_ratio_T_growth": 0,  # 较去年资产负债率增长了
            "inventory_turnover_rate": 0,  # 存货周转率
            "total_asset_turnover": 0,  # 总资产周转率

            "asset_pie": [],  # 资产分布饼图（以最新的数据进行展示）
            "debt_pie": [],  # 负债分布饼图（以最新的数据进行展示）
            "du_pond_analysis": [],  # 杜邦分析（以最近2次的年报进行分析展示，如果只有一年的数据)
            "factor_analysis": [],  # 因素分析（当有2年的年报数据才展示，否则不展示，无法对比变动和因素分析）
            "profit_portrait": {},  # 利润画像（当存在最近一年的财报数据则展示）
            "cash_flow_portrait": {},  # 现金流画像（有3年的财务数据才展示4大类型的结论，否则不展示结论，只展示数据信息）

            "basic_es_date": "",  # 成立日期
            "basic_industry_phyname": "",  # 国标行业编码
            "basic_industry_code": "",  # 国标行业中文

            "inventory_turnover_ratio_T": '',  # 存货周转率（次 / 年）
            "inventory_turnover_ratio_T1":'',#存货周转率（次 / 年）
            "inventory_turnover_ratio_T2": '',  # 存货周转率（次 / 年）
            "receivables_turnover_ratio_T": '',  # 应收账款周转率（次 / 年）
            "receivables_turnover_ratio_T1":'',#应收账款周转率（次 / 年）
            "receivables_turnover_ratio_T2": '',  # 应收账款周转率（次 / 年）
            "current_asset_turnover_ratio_T": '',  # 流动资产周转率（次 / 年）
            "current_asset_turnover_ratio_T1": '',  # 流动资产周转率（次 / 年）
            "current_asset_turnover_ratio_T2":'',#流动资产周转率（次 / 年）
            "equity_multiplier_T":'',#权益乘数
            "equity_multiplier_T1": '',  # 权益乘数
            "equity_multiplier_T2": '',  # 权益乘数
            "interest_coverage_ratio_T":"",# 利息保障倍数
            "interest_coverage_ratio_T1": "",  # 利息保障倍数
            "interest_coverage_ratio_T2": "",  # 利息保障倍数

            # 财报与流水交叉验证
            "financial_report_time_range": "",  # 财报时间范围
            "bank_flow_time_range": "",  # 流水时间范围
            "bank_flow_subject_tip": "",  # 流水主体提示
            "financial_statement_and_transaction_credit_verification": []
        }

    def transform(self):
        if self.cached_data['product_code'] == '11001':
            handle_list = [
                DataPreparedProcessor(),
                IndexMappingProcess()
            ]
        else:
            handle_list = [
                BasicUnique(),
                DataPreparedProcessor(),
                IndexMappingProcess(),
                MainIndicatorsAnalysisProcessor(),
                RiskAnalysisProcessor(),
                ComprehensiveRateProcessor()
            ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
