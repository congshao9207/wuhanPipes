import pandas as pd

from mapping.module_processor import ModuleProcessor
from logger.logger_util import LoggerUtil
logger = LoggerUtil().logger(__name__)

'''
通用报表映射
'''
class IndexMappingProcess(ModuleProcessor):
    def __init__(self):
        self.asset_dept_mapping_dict={
            "货币资金": "cash_funds",
            "交易性金融资产": "fair_value_assets",
            "衍生金融资产": "derivative_assets",
            "应收票据": "notes_receivable",
            "应收账款": "accounts_receivable",
            "预付账款": "prepaid",
            "其他应收款": "other_receivables",
            "存货": "inventory",
            "其中：原材料":"raw_materials",
            "在产品": "work_in_progress",
            "库存商品": "finished_goods",
            "周转材料": "revolving_materials",
            "持有待售资产": "assets_for_sale",
            "一年内到期的非流动资产": "due_assets",
            "其他流动资产": "other_current",
            "流动资产合计": "total_current",
            "可供出售金融资产": "available_for_sale_assets",
            "持有至到期投资": "held_to_maturity_investments",
            "长期应收款": "long_term_receivables",
            "长期股权投资": "long_term_equity_investments",
            "投资性房地产": "investment_real_estate",
            "固定资产原价": "gross_fixed_assets",
            "减：累计折旧": "accumulated_depreciation",
            "固定资产": "fixed_assets",
            "在建工程": "construction_in_progress",
            "生产性生物资产": "productive_biological_assets",
            "油气资产": "oil_and_gas_assets",
            "无形资产": "intangible_assets",
            "开发支出": "development_expenditures",
            "商誉": "goodwill",
            "长期待摊费用": "long_term_prepaid_expenses",
            "递延所得税资产": "deferred_tax_assets",
            "其他非流动资产": "other_non_current_assets",
            "非流动资产合计": "total_non_current_assets",
            "短期借款": "short_term_loans",
            "交易性金融负债": "fair_value_liabilities",
            "衍生金融负债": "derivative_liabilities",
            "应付票据": "notes_payable",
            "应付账款": "accounts_payable",
            "预收款项": "advances_from_customers",
            "应付职工薪酬": "salaries_payable",
            "应交税费": "taxes_payable",
            "其他应付款": "other_payables",
            "持有待售负债": "liabilities_for_sale",
            "一年内到期的非流动负债": "due_liabilities",
            "其他流动负债": "other_current_liabilities",
            "流动负债合计": "total_current_liabilities",
            "长期借款": "long_term_loans",
            "应付债券": "bonds_payable",
            "长期应付款": "long_term_payables",
            "预计负债": "provisions",
            "递延收益": "deferred_income",
            "递延所得税负债": "deferred_tax_liabilities",
            "其他非流动负债": "other_non_current_liabilities",
            "非流动负债合计": "total_non_current_liabilities",
            "负债合计": "total_liabilities",
            "实收资本（或股本）": "paid_in_capital",
            "其他权益工具": "other_equity_instruments",
            "资本公积": "capital_reserve",
            "其他综合收益": "other_comprehensive_income",
            "专项储备": "specific_reserves",
            "盈余公积": "surplus_reserves",
            "未分配利润": "undistributed_profits",
            "所有者权益（或股东权益）总计": "total_equity",
            "负债和所有者权益（或股东权益）合计": "total_liabilities_and_equity",
            "资产合计": "total_assets"
        }

        self.cash_flow_mapping_dict={
            "销售商品、提供劳务收到的现金": "cash_from_sales",
            "收到的税费返还": "tax_refunds_received",
            "收到其他与经营活动有关的现金": "other_operating_cash_in",
            "经营活动现金流入小计": "total_operating_cash_in",
            "购买商品、接受劳务支付的现金": "cash_paid_for_goods",
            "支付给职工以及为职工支付的现金": "cash_paid_to_employees",
            "支付的各项税费": "taxes_paid",
            "支付其他与经营活动有关的现金": "other_operating_cash_out",
            "经营活动现金流出小计": "total_operating_cash_out",
            "经营活动产生的现金流量净额": "net_operating_cash_flow",
            "收回投资收到的现金": "cash_from_investments",
            "取得投资收益收到的现金": "cash_from_investment_income",
            "处置固定资产、无形资产和其他长期资产收回的现金净额": "cash_from_asset_disposals",
            "处置子公司及其他营业单位收到的现金净额": "cash_from_sub_disposals",
            "收到其他与投资活动有关的现金": "other_investment_cash_in",
            "投资活动现金流入小计": "total_investment_cash_in",
            "购建固定资产、无形资产和其他长期资产支付的现金": "cash_paid_for_assets",
            "投资支付的现金": "cash_paid_for_investments",
            "取得子公司及其他营业单位支付的现金净额": "cash_paid_for_subs",
            "支付其他与投资活动有关的现金": "other_investment_cash_out",
            "投资活动现金流出小计": "total_investment_cash_out",
            "投资活动产生的现金流量净额": "net_investment_cash_flow",
            "吸收投资收到的现金": "cash_from_investment_received",
            "取得借款收到的现金": "cash_from_borrowing",
            "收到其他与筹资活动有关的现金": "other_financing_cash_in",
            "筹资活动现金流入小计": "total_financing_cash_in",
            "偿还债务支付的现金": "cash_paid_for_debt",
            "分配股利、利润或偿付利息支付的现金": "cash_paid_for_dividends",
            "支付其他与筹资活动有关的现金": "other_financing_cash_out",
            "筹资活动现金流出小计": "total_financing_cash_out",
            "筹资活动产生的现金流量净额": "net_financing_cash_flow",
            "四、汇率变动对现金及现金等价物的影响": "fx_impact_on_cash",
            "五、现金及现金等价物净增加额": "net_increase_in_cash",
            "加：期初现金及现金等价物余额": "beginning_cash_balance",
            "六、期末现金及现金等价物余额": "ending_cash_balance"
        }

        self.profit_mapping_dict={
            "一、营业收入": "revenue",
            "主营业务收入": "main_revenue",
            "其他业务收入": "other_revenue",
            "减：营业成本": "operating_cost",
            "主营业务成本": "main_cost",
            "其他业务成本": "other_cost",
            "减：税金及附加": "taxes_surcharges",
            "销售费用": "selling_expense",
            "其中：租金": "rent",
            "管理费用": "admin_expense",
            "其中：工资": "wages",
            "社保、公积金": "insurance_fund",
            "水电气": "utilities",
            "研发费用": "r_and_d_expense",
            "财务费用": "finance_expense",
            "其中：利息费用": "interest_expense",
            "利息收入": "interest_income",
            "其他费用": "other_expense",
            "加：其他收益": "other_income",
            "其中：投资收益":"investment_income",
            "减：资产减值损失": "asset_impairment",
            "二、营业利润": "operating_profit",
            "加：营业外收入": "non_operating_income",
            "减：营业外支出": "non_operating_expense",
            "三、利润总额": "total_profit",
            "减：所得税费用": "income_tax",
            "四、净利润": "net_profit"
        }


    def process(self):
        self.asset_dept_mapping()
        self.cash_flow_or_profit_mapping('CASH_FLOW',self.cash_flow_mapping_dict)
        self.cash_flow_or_profit_mapping('PROFIT', self.profit_mapping_dict)

    def asset_dept_mapping(self):
        df_dict=self.cached_data.get('ASSET_DEBT')

        if df_dict is None or len(df_dict)==0:
            return

        # self.cached_data['total_assets']=df_t.loc[df_t['资产']=='资产合计','期末余额']
        #T年数据映射
        df_t=df_dict.get("df_t")
        end_date_t = df_dict.get("end_date_t")
        if df_t is not None:
            end_date = end_date_t
            start_date=str(int(end_date_t[:4])-1)
            self.variables['asset_dept_T'] =str(end_date_t[:4])+'年'+end_date_t[5:7]+'月'
            self.variables['asset_dept_T1'] = str(int(end_date_t[:4])-1)+'年'
            if '期末余额' in list(df_t.columns):
                self.mapping(self.asset_dept_mapping_dict, df_t, '资产', '期末余额', 'T')
            if '期初余额' in list(df_t.columns):
                self.mapping(self.asset_dept_mapping_dict, df_t, '资产', '期初余额', 'T1')

        # T-1年数据映射
        df_t1=df_dict.get("df_t1")
        end_date_t1 = df_dict.get("end_date_t1")
        if df_t1 is not None:
            start_date = str(int(end_date_t1[:4]) - 1)
            self.variables['asset_dept_T1'] = end_date_t1[:4]+'年'
            self.variables['asset_dept_T2'] = str(int(end_date_t1[:4]) - 1)+'年'
            if '期初余额' in list(df_t1.columns):
                self.mapping(self.asset_dept_mapping_dict, df_t1, '资产', '期初余额', 'T2')

        else:
            # T-2年数据映射
            df_t2 = df_dict.get("df_t2")
            end_date_t2 = df_dict.get("end_date_t2")
            if df_t2 is not None:
                start_date = str(int(end_date_t2[:4]) - 1)
                self.variables['asset_dept_T2'] = end_date_t2[:4]+'年'
                if '期末余额' in list(df_t2.columns):
                    self.mapping(self.asset_dept_mapping_dict, df_t2, '资产', '期末余额', 'T2')

        self.variables['asset_dept_date_range'] = start_date + '年01月01日—' + end_date[:4]+'年'+end_date[5:7]+'月'+end_date[8:]+'日'


    def cash_flow_or_profit_mapping(self,table_type,table_type_dict):
        start_date=''
        end_date=''
        df_dict = self.cached_data.get(table_type)

        if df_dict is None or len(df_dict)==0:
            return
        df_t = df_dict.get("df_t")
        end_date_t= df_dict.get("end_date_t")
        if df_t is not None:
            if '本年累计金额' in list(df_t.columns):
                self.mapping(table_type_dict,df_t,'项目','本年累计金额','T')
                self.variables[table_type.lower()+'_T']=end_date_t[:4]+'年'+end_date_t[5:7]+'月'
                end_date=end_date_t

            if '上年金额' in list(df_t.columns):
                self.mapping(table_type_dict,df_t,'项目','上年金额','T1')
                self.variables[table_type.lower() + '_T1'] = str(int(end_date_t[:4])-1)+'年'
                start_date = str(int(end_date_t[:4])-1)+'-01-01'
            else:
                pass

        df_t1 = df_dict.get("df_t1")
        end_date_t1 = df_dict.get("end_date_t1")
        if df_t1 is not None:
            if '本年累计金额' in list(df_t1.columns) and end_date_t1[5:7]=='12':
                self.mapping(table_type_dict,df_t1,'项目','本年累计金额','T1')
                self.variables[table_type.lower() + '_T1'] = str(int(end_date_t1[:4]))+'年'
                if end_date=='':
                    end_date=end_date_t1
                else:
                    start_date=str(int(end_date_t1[:4])-1)+'-01-01'

            if '上年金额' in list(df_t1.columns):
                self.mapping(table_type_dict,df_t1,'项目','上年金额','T2')
                self.variables[table_type.lower() + '_T2'] = str(int(end_date_t1[:4])-1)+'年'
                if start_date == '':
                    start_date = str(int(end_date_t1[:4]) - 2) + '-01-01'
            else:
                pass

        df_t2 = df_dict.get("df_t2")
        end_date_t2 = df_dict.get("end_date_t2")
        if df_t2 is not None:
            if '本年累计金额' in list(df_t2.columns) and end_date_t2[5:7]=='12':
                self.mapping(table_type_dict, df_t2, '项目', '本年累计金额', 'T2')
                self.variables[table_type.lower() + '_T2'] = str(int(end_date_t2[:4]))+'年'
                if start_date == '':
                    start_date = str(int(end_date_t2[:4]) - 2) + '-01-01'

        #编制时间
        if end_date!='' and start_date=='':
            self.variables[table_type.lower()+'_date_range']=end_date[:4]+ '年01月01日—'+end_date[:4]+'年'+end_date[5:7]+'月'+end_date[8:]+'日'
        elif end_date=='' and start_date!='':
            self.variables[table_type.lower() + '_date_range'] = start_date[:4] + '年01月01日—' + start_date[:4] + '年12月31日'
        elif end_date != '' and start_date != '':
            self.variables[table_type.lower() + '_date_range'] = start_date[:4] + '年01月01日—' + end_date[:4]+'年'+end_date[5:7]+'月'+end_date[8:]+'日'
        else:
            self.variables[table_type.lower() + '_date_range'] = ''

        for i in ['_T','_T1','_T2']:
            if isinstance(self.variables['revenue'+i],(float,int)) and isinstance(self.variables['operating_cost'+i],(float,int)) and  self.variables['revenue'+i]!=0:
                #毛利润
                self.variables['gross_profit'+i]=self.variables['revenue'+i]-self.variables['operating_cost'+i]
                #毛利率
                self.variables['gross_margin' + i]=self.variables['gross_profit'+i]/self.variables['revenue'+i]

            #减：期间费用
            for j in ['selling_expense','admin_expense','r_and_d_expense','finance_expense','other_expense']:
                if isinstance(self.variables[j+i],(float,int)):
                    if self.variables['period_cost'+i]=='':
                        self.variables['period_cost' + i] = self.variables[j + i]
                    else:
                        self.variables['period_cost'+i]+=self.variables[j+i]

            #净利润率
            if isinstance(self.variables['revenue' + i], (float, int)) and isinstance(self.variables['net_profit' + i],(float, int)) and self.variables['revenue' + i]!=0:
                self.variables['net_margin'+i]=self.variables['net_profit' + i]/self.variables['revenue' + i]


    def mapping(self,finance_mapping_dict,df_t,table_column_name,table_column_value_name,year):
        '''
        参数:
            finance_mapping_dict (dict): 财务报表映射字典，包含以下枚举值：
                - self.asset_dept_mapping_dict: 资产负债表映射字典
                - self.cash_flow_mapping_dict: 现金流量表映射字典
                - self.profit_mapping_dict: 利润表映射字典

            df_t (pd.DataFrame): 相应年份的数据，该数据在数据准备阶段缓存至 self.cached_data 中。
                获取方式示例：self.cached_data.get('ASSET_DEBT').get('df_t')，其中
                - df_t: T年数据
                - df_t1: T-1年数据
                - df_t2: T-2年数据

            table_column_name (str): 科目列的名称，例如：
                - 资产负债表的科目列： "资产"、"负债和所有者权益（或股东权益）"
                - 其他报表的相应科目列名称

            table_column_value_name (str): 需要获取值的列名，例如：
                - 期末金额、期初金额等
                - 注意：这些列名在数据准备阶段已进行规范化处理

            year (str): 年份标识符，例如：
                - "T" 表示当年
                - "T1" 或 "T-1" 表示前一年
                - "T2" 或 "T-2" 表示两年之前
        '''

        #财务标签
        df_finance_label = self.cached_data['finance_label']
        for key, value in finance_mapping_dict.items():
            # 判断是否有多列table_column_value_name
            target_cols = [col for col in df_t.columns if table_column_value_name in str(col)]
            if len(target_cols)>1:
                logger.error(f"映射{year}年数据存在重复列: {table_column_value_name}")
                break
            ending_balance = df_t.loc[df_t[table_column_name] == key, table_column_value_name]
            if len(ending_balance) == 1 and pd.isna(list(ending_balance)[0]) == False:
                self.variables[value + '_' + year] = list(ending_balance)[0]
            elif len(ending_balance) > 1:
                # 判断是否都为字符串
                if all(isinstance(item, str) for item in list(ending_balance)):
                    continue
                else:
                    need_sum = df_finance_label.loc[df_finance_label['label_definition'] == key, 'need_sum'].to_list()[
                        0]
                    # 判断是映射标签是否需要求和
                    if need_sum == 1:
                        ending_balance_value = sum([float(k) if k != '' else 0 for k in list(ending_balance)])
                        self.variables[value + '_' + year] = 0 if ending_balance_value == 0 else ending_balance_value
                    else:
                        for val in list(ending_balance):
                            if isinstance(val, (float, int)):
                                self.variables[value + '_' + year] = val
                                break
            else:
                pass
