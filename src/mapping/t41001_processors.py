# @Time : 12/7/21 11:52 AM 
# @Author : lixiaobo
# @File : t41001_delegate.py 
# @Software: PyCharm
from mapping.p07001_m.basic_info_processor import BasicInfoProcessor
from mapping.p07001_m.credit_info_processor import CreditInfoProcessor
from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from mapping.p07001_m.if_info_processor import IfInfoProcessor
from mapping.p07001_m.loan_info_processor import LoanInfoProcessor
from mapping.p07001_m.single_info_processor import SingleInfoProcessor
from mapping.p07001_m.total_info_processor import TotalInfoProcessor
from mapping.p07001_m.unsettle_info_processor import UnSettleInfoProcessor
from mapping.p09003_m.pcredit_rule import PcreditRule


class T41001Processors(object):
    def __init__(self):
        self.variables = {
            # 规则变量
            "no_loan": 1,  # "征信白户不准入（不能作为借款人，配偶是白户可以通过）"
            "loan_principal_overdue_cnt": 0,  # "5年内贷款存在本金逾期（仅指先息后本）"
            "public_sum_count": 0,  # "存在呆账、资产处置、保证人代偿"
            "loan_fiveLevel_a_level_cnt": 0,  # "贷款五级分类存在“关注、次级、可疑、损失”"
            "business_loan_average_overdue_cnt": 0,  # "5年内还款方式为等额本息分期偿还的贷款连续逾期2期"
            "credit_now_overdue_money": 0,  # "贷记卡当前逾期（单笔当前逾期＞500或当前逾期发卡机构＞1家）"
            "loan_now_overdue_money": 0,  # "贷款有当前逾期"
            "single_credit_or_loan_5year_overdue_max_month": 0,  # "单张贷记卡（信用卡）、单笔贷款5年内出现连续逾期≥3期不准入（单笔逾期金额在＜500元的除外）"
            "single_credit_overdue_2year_cnt": 0,  # "单张贷记卡（信用卡）近2年内存在5次以上逾期（单笔逾期金额在＜500元的除外）"
            "single_loan_overdue_2year_cnt": 0,  # "单笔贷款近2年内存在5次以上逾期"
            "credit_overdue_2year": 0,  # "总计贷记卡（信用卡）2年内逾期超过10次"
            "loan_consume_overdue_2year": 0,  # "总计贷款2年内逾期超过8次"
            "loan_scured_five_a_level_abnormality_cnt": 0,  # "对外担保五级分类存在“关注、次级、可疑、损失”"
            "extension_number": 0,  # "存在展期"
            "credit_status_bad_cnt_2y": 0,  # "贷记卡账户2年内出现过“呆账”"
            "credit_status_legal_cnt": 0,  # "贷记卡账户状态存在“司法追偿”"
            "credit_status_b_level_cnt": 0,  # "贷记卡账户状态存在“止付、冻结”（止付需满足已使用金额＞0，则拦截），建议改成只要有止付就拦截"
            "loan_status_bad_cnt": 0,  # "贷款账户状态存在“呆账”"
            "loan_status_legal_cnt": 0,  # "贷款账户状态存在“司法追偿”"
            "loan_status_b_level_cnt": 0,  # "贷款账户状态存在“止付、冻结”"
            "loan_credit_query_3month_cnt": 0,  # "近三个月征信查询（贷款审批及贷记卡审批等）超过6次（按机构数计数，同一机构重复查询视同一次）"
            "credit_overdrawn_min_payed_cnt": 0,  # "贷记卡总透支率达80%且存在2张贷记卡最低额还款且已激活贷记卡张数＞3"
            "unsettled_busLoan_agency_number": 0,  # "经营性贷款在贷机构超过6家"
            "unsettled_consume_agency_cnt": 0,  # "消费性贷款在贷机构家数（20万以内，去除车贷、房贷）超过6家"
            "unsettled_small_loan_org_cnt": 0,  # "征信在贷5万以下小额贷款＞4家（除车贷房贷外所有贷款，不含5万，按照机构，按发放额）"
            "unsettled_loan_agency_number": 0,  # "征信在贷机构＞8家（除去房贷车贷外）"
            "uncancelled_credit_organization_number": 0,  # "信用卡发卡机构激活的机构数（仅人民币账户）＞15"
            "credit_fiveLevel_abnormal_cnt": 0,  # "贷记卡五级分类存在异常

            # 模型变量
            "main_age": 0,  # 年龄
            "main_sex": 0,  # 性别
            "marriage_status": 0,  # 婚姻状况
            "opera_year": 0,  # 经营年限
            "unsettled_business_loan_org_cnt": 0,  # 经营性贷款在贷机构家数
            "unsettled_business_loan_org_avg_amt": 0,  # 经营性贷款在贷机构授信均值
            "total_overdue_cnt_3y": 0,  # 近3年贷记卡及贷款累计逾期期数
            "query_cnt_3m": 0,  # 近三个月贷款审批和贷记卡审批查询次数
            "total_credit_used_rate": 0,  # 总计贷记卡使用率
            "credit_min_payed_number": 0,  # 贷记卡最低还款张数
            "unsettled_house_loan_monthly_repay_amt": 0,  # 在贷房贷月还款总额
            "unsettled_mortgage_loan_limit": 0,  # 在贷抵押贷款授信总额
            "unsettled_consume_loan_org_cnt": 0,  # 消费性贷款在贷机构家数
            "unsettled_bank_business_loan_max_limit": 0,  # 单笔银行经营贷款最大授信金额
            "unsettled_bank_business_loan_org_avg_amt": 0,  # 银行在贷经营性贷款机构授信均值
            "unsettled_bank_consume_loan_org_cnt": 0,  # 银行在贷消费性贷款机构数
            "unsettled_bank_consume_loan_org_avg_amt": 0,  # 银行在贷消费性贷款机构授信均值
            "unsettled_loan_org_cnt": 0,  # 在贷机构家数
            "unsettled_loan_org_avg_amt": 0,  # 在贷机构授信均值
            "unsettled_car_loan_total_limit": 0,  # 在贷车贷授信总额
            "activated_credit_card_org_cnt": 0,  # 贷记卡发卡机构数
            "activated_credit_card_org_avg_amt": 0,  # 贷记卡机构授信均值
            "total_loan_overdue_cnt_3y": 0,  # 近3年贷款逾期次数
            "used_credit_card_org_cnt": 0,  # 有使用额度的发卡机构数
            "used_credit_card_org_avg_amt": 0,  # 有使用额度的贷记卡机构授信均值
            # "unsettled_small_loan_org_cnt": 0,  # 5万以下在贷机构家数
            "total_credit_limit": 0,  # 贷记卡授信总额
            "unsettled_loan_max_limit": 0,  # 在贷授信最大值
            "couple_signment": 1,  # 配偶签字

            # 输出变量
            "cus_level": "C",  # 客户分级
            "credit_model_score": 0,  # 信用模型分
            "credit_model_level": "nan",  # 信用模型等级
            "credit_model_coefficient": 0,  # 信用模型系数
            "level_c_limit": 0,  # 客群最低额度
            "base_limit": 0,  # 基础额度
            "credit_limit": 0,  # 信用额度
            "rule_limit": 0,  # 加强规则调整额度
            "final_limit": 0,  # 最终授信额度
            "plan": None  # 还款方式
        }

        self.variables_credit = {
            "report_id": 0,  # 报告编号
            "rhzx_business_loan_overdue_cnt": 0,  # 经营性贷款逾期笔数
            "public_sum_count": 0,  # 呆账、资产处置、保证人代偿笔数
            "credit_fiveLevel_a_level_cnt": 0,  # 贷记卡五级分类存在“可疑、损失”
            "loan_fiveLevel_a_level_cnt": 0,  # 贷款五级分类存在“次级、可疑、损失”
            "business_loan_average_overdue_cnt": 0,  # 还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数
            "large_loan_2year_overdue_cnt": 0,  # 经营性贷款（经营性+个人消费大于20万+农户+其他）2年内最大连续逾期期数
            "credit_now_overdue_money": 0,  # 贷记卡当前逾期金额
            "loan_now_overdue_money": 0,  # 贷款当前逾期金额
            "credit_overdue_max_month": 0,  # 贷记卡最大连续逾期月份数
            "single_house_overdue_2year_cnt": 0,  # 单笔房贷近2年内最大逾期次数
            "single_car_overdue_2year_cnt": 0,  # 单笔车贷近2年内最大逾期次数
            "single_consume_overdue_2year_cnt": 0,  # 单笔消费性贷款近2年内最大逾期次数
            "loan_credit_query_3month_cnt": 0,  # 近三个月征信查询（贷款审批及贷记卡审批）次数
            # "credit_overdrawn_min_payed_cnt": 0,  # 贷记卡总透支率达80%且最低额还款张数多
            "credit_overdrawn_2card":0,  # 贷记卡总透支率达80%且最低额还款张数多
            "total_credit_used_rate": 0,  # 总计贷记卡使用率
            "total_credit_min_repay_cnt": 0,  # 贷记卡最低还款张数
            "credit_overdue_5year": 0,  # 总计贷记卡5年内逾期次数
            "loan_consume_overdue_5year": 0,  # 总计消费性贷款（含车贷、房贷、其他消费性贷款）5年内逾期次数
            "credit_max_overdue_2year": 0,  # 单张贷记卡近2年内最大逾期次数
            "unsettled_busLoan_agency_number": 0,  # 有经营性贷款在贷余额的合作机构数
            "loan_credit_small_loan_query_3month_cnt": 0,  # 近三个月小额贷款公司贷款审批查询次数
            "unsettled_consume_agency_cnt": 0,  # 未结清消费性贷款机构数
            "divorce_50_female": 0,  # 年龄>=50,离异或者丧偶，女
            "divorce_55_male": 0,  # 年龄>55,离异,男
            "credit_fiveLevel_b_level_cnt": 0,  # 贷记卡五级分类存在“次级"
            "loan_fiveLevel_b_level_cnt": 0,  # 贷款五级分类存在"关注"
            "loan_scured_five_a_level_abnormality_cnt": 0,  # 对外担保五级分类存在“次级、可疑、损失”
            "extension_number": 0,  # 展期笔数
            "enforce_record": 0,  # 强制执行记录条数
            "unsettled_consume_total_cnt": 0,  # 未结清消费性贷款笔数
            "credit_financial_tension": 0,  # 贷记卡资金紧张程度
            "credit_activated_number": 0,  # 已激活贷记卡张数
            "credit_min_payed_number": 0,  # 贷记卡最低还款张数
            "uncancelled_credit_organization_number": 0,  # 未销户贷记卡发卡机构数
            "credit_fiveLevel_c_level_cnt": 0,  # 贷记卡状态存在"关注"
            "loan_scured_five_b_level_abnormality_cnt": 0,  # 对外担保五级分类存在"关注"
            "unsettled_busLoan_total_cnt": 0,  # 未结清经营性贷款笔笔数
            "marriage_status": 0,  # 离婚
            "credit_marriage_status": 99,  # 征信报告中的婚姻状态
            "judgement_record": 0,  # 民事判决记录数
            "loan_doubtful": 0,  # 疑似压贷笔数
            "loan_doubtful_org": '',  # 疑似压贷机构
            "guarantee_amont": 0,  # 对外担保金额
            "unsettled_loan_agency_number": 0,  # 未结清贷款机构数
            "unsettled_consume_total_amount": 0,  # 未结清消费性贷款总额
            "tax_record": 0,  # 欠税记录数
            "ad_penalty_record": 0,  # 行政处罚记录数
            "business_loan_overdue_money": 0,  # 经营性贷款逾期金额
            "loan_overdue_2times_cnt": 0,  # 贷款连续逾期2期次数
            "loan_now_overdue_cnt": 0,  # 贷款当前逾期次数
            "loan_total_overdue_cnt": 0,  # 贷款历史总逾期次数
            "loan_max_overdue_month": 0,  # 贷款最大连续逾期
            "credit_now_overdue_cnt": 0,  # 贷记卡当前逾期次数
            "credit_total_overdue_cnt": 0,  # 贷记卡历史总逾期次数
            "single_credit_overdue_cnt_2y": 0,  # 单张贷记卡2年内逾期次数
            "single_house_loan_overdue_cnt_2y": 0,  # 单笔房贷2年内逾期次数
            "single_car_loan_overdue_cnt_2y": 0,  # 单笔车贷2年内逾期次数
            "single_consume_loan_overdue_cnt_2y": 0,  # 单笔消费贷2年内逾期次数
            "total_consume_loan_overdue_cnt_5y": 0,  # 消费贷5年内总逾期次数
            "total_consume_loan_overdue_money_5y": 0,  # 消费贷5年内总逾期金额
            "total_bank_credit_limit": 0,  # 银行授信总额
            "total_bank_loan_balance": 0,  # 银行总余额
            "if_name": 0,  # 与ccs姓名比对
            "phone_alt": 0,  # 与ccs手机号比对
            "if_cert_no": 0,  # 与ccs身份证号比对
            "if_marriage": 0,  # 与ccs婚姻状况比对
            "if_postal_addr": 0,  # 与ccs通讯地址比对
            "if_residence_addr": 0,  # 与ccs户籍地址比对
            "if_live_addr": 0,  # 与ccs居住地址比对
            "if_employee": 0,  # 是否是员工
            "if_official": 0,  # 是否是公检法人员
            "if_spouse_name": 0,  # 与ccs配偶姓名匹配
            "if_spouse_cert_no": 0,  # 与ccs配偶身份证匹配
            "no_loan": 0,  # 名下无贷款无贷记卡
            "house_loan_pre_settle": 0,  # 存在房贷提前结清
            "guar_2times_apply": 0,  # 担保金额是借款金额2倍
            "all_house_car_loan_reg_cnt": 0,  # 所有房屋汽车贷款机构数
            "unsettled_loan_number": 0,  # 未结清贷款笔数
            "unsettled_house_loan_number": 0,  # 未结清房贷笔数
            "loan_approval_year1": 0,  # 贷款审批最近一年内查询次数
            # "credit_status_bad_cnt_2y": 0,  # 贷记卡账户状态存在"呆账"
            "credit_status_bad_cnt": 0,  # 贷记卡账户状态存在"呆账"
            "credit_status_legal_cnt": 0,  # 贷记卡账户状态存在"司法追偿"
            "credit_status_b_level_cnt": 0,  # 贷记卡账户状态存在"银行止付、冻结"
            "loan_status_bad_cnt": 0,  # 贷款账户状态存在"呆账"
            "loan_status_legal_cnt": 0,  # 贷款账户状态存在"司法追偿"
            "loan_status_b_level_cnt": 0,  # 贷款账户状态存在"银行止付、冻结"
            "consume_loan_now_overdue_money":0,  # 消费性贷款有当前逾期
            "bus_loan_now_overdue_money":0,  # 经营性贷款当前逾期金额
            "single_bus_loan_overdue_2year_cnt":0,  # 单笔经营性贷款近2年内最大逾期次数
            "single_consume_loan_overdue_2year_cnt":0,  # 单笔消费性贷款近2年内最大逾期次数
            "per_credit_debt_amt":[],  # 个人征信负债（除车贷、房贷）
            "single_credit_or_loan_3year_overdue_max_month":0,#单张贷记卡（信用卡）、单笔贷款3年内出现连续90天以上逾期记录（年费及手续费等逾期金额在1000元下的除外）
            # "credit_overdue_2year_cnt": 0,  # 总计贷记卡2年内逾期次数
            "credit_overdue_2year": 0,  # 总计贷记卡2年内逾期次数
            "single_credit_overdue_2year_cnt":0, #  单张贷记卡（信用卡）近2年内存在5次以上逾期（年费及手续费等逾期金额在1000元下的除外）
            "rhzx_business_loan_3year_ago_overdue_cnt":0, # 3年前经营性贷款存在本金逾期
            "business_loan_average_3year_ago_overdue_cnt":0, # 3年前还款方式为等额本息分期偿还的经营性贷款连续逾期2期
            # "loan_overdue_2year_cnt":0, # 总计贷款2年内逾期超过10次
            "loan_consume_overdue_2year":0, # 总计贷款2年内逾期超过10次
            "rhzx_business_loan_3year_overdue_cnt":0,# 3年内经营性贷款存在本金逾期
            "single_semi_credit_card_3year_overdue_max_month": 0,  # 3年内准贷记卡逾期最大连续期数
            "single_semi_credit_overdue_2year_cnt": 0,  # 单张准贷记卡近2年内最大逾期次数
        }


    def obtain_processors(self, product_code):
        if product_code and product_code == "09003":
            return [
                DataPreparedProcessor(),
                PcreditRule(),
                   ], self.variables
        else:
            return [
                DataPreparedProcessor(),
                BasicInfoProcessor(),
                CreditInfoProcessor(),
                LoanInfoProcessor(),
                SingleInfoProcessor(),
                UnSettleInfoProcessor(),
                TotalInfoProcessor(),
                IfInfoProcessor()

            ], self.variables_credit
