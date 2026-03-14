# @Time : 2020/6/19 2:13 PM 
# @Author : lixiaobo
# @File : 51001.py.py 
# @Software: PyCharm
import time
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer
from view.p08001_v.json_s_counterparty_portrait import JsonSingleCounterpartyPortrait
from view.p08001_v.json_s_loan_portrait import JsonSingleLoanPortrait
from view.p08001_v.json_s_marketing import JsonSingleMarketing
from view.p08001_v.json_s_portrait import JsonSinglePortrait
from view.p08001_v.json_s_related_guarantor import JsonSingleGuarantor
from view.p08001_v.json_s_related_portrait import JsonSingleRelatedPortrait
from view.p08001_v.json_s_remark_portrait import JsonSingleRemarkPortrait
from view.p08001_v.json_s_remark_trans_detail import JsonSingleRemarkTransDetail
from view.p08001_v.json_s_summary_portrait import JsonSingleSummaryPortrait
from view.p08001_v.json_s_title import JsonSingleTitle
from view.p08001_v.json_s_unusual_trans import JsonSingleUnusualTrans
from view.p08001_v.json_u_counterparty_portrait import JsonUnionCounterpartyPortrait
from view.p08001_v.json_u_loan_portrait import JsonUnionLoanPortrait
from view.p08001_v.json_u_marketing import JsonUnionMarketing
from view.p08001_v.json_u_portrait import JsonUnionPortrait
# from view.p08001_v.json_u_related_guarantor import JsonUnionGuarantor
from view.p08001_v.json_u_related_portrait import JsonUnionRelatedPortrait
from view.p08001_v.json_u_remark_portrait import JsonUnionRemarkPortrait
from view.p08001_v.json_u_remark_trans_detail import JsonUnionRemarkTransDetail
from view.p08001_v.json_u_summary_portrait import JsonUnionSummaryPortrait
from view.p08001_v.json_u_title import JsonUnionTitle
from view.p08001_v.json_u_unusual_trans import JsonUnionUnusualTrans
from view.p08001_v.json_u_fund_procurement import JsonUFundProcurement
from view.p08001_v.json_u_normal_income_portrait import JsonUnionNormalIncomePortrait
from view.p08001_v.json_u_funds_summary_portrait import JsonUnionFundsSummaryPortrait
from view.p08001_v.json_u_confidence_analyse import JsonUnionConfidenceAnalyse
from view.p08001_v.json_u_balance_trend_chart import JsonUBalanceTrendChart
from view.p08001_v.json_u_trans_frequency_detail import JsonUTransFrequencyDetail
from view.p08001_v.json_u_suspected_affiliates import JsonUSuspectedAffiliates
from view.p08001_v.json_u_label_total import JsonULabelTotal
from view.p08001_v.json_u_industry_portrait import JsonUIndustryPortrait

logger = LoggerUtil().logger(__name__)


class V51001(Transformer):
    """
    流水报告变量清洗
    """

    def __init__(self) -> None:
        super().__init__()

    def transform(self):
        view_handle_list = [
        ]
        # 添加视图名称映射字典
        view_name_mapping = {
            # Single views
            JsonSingleTitle: "单客户标题",
            JsonSinglePortrait: "单客户画像",
            JsonSingleSummaryPortrait: "单客户概要画像",
            JsonSingleCounterpartyPortrait: "单客户交易对手画像",
            JsonSingleRelatedPortrait: "单客户关联画像",
            JsonSingleGuarantor: "单客户担保人信息",
            JsonSingleLoanPortrait: "单客户贷款画像",
            JsonSingleUnusualTrans: "单客户异常交易",
            JsonSingleMarketing: "单客户营销分析",

            # Union views
            JsonUnionTitle: "报告头画像",
            JsonUnionPortrait: "联合画像",
            JsonUFundProcurement: "联合资金筹措分析",
            JsonULabelTotal: "联合标签汇总",
            JsonUnionSummaryPortrait: "概要画像",
            JsonUnionCounterpartyPortrait: "交易对手画像",
            JsonUnionRelatedPortrait: "关联画像",
            JsonUnionLoanPortrait: "多头画像",
            JsonUnionUnusualTrans: "异常交易",
            JsonUnionMarketing: "营销分析",
            JsonUnionNormalIncomePortrait: "经营收入画像",
            JsonUnionFundsSummaryPortrait: "资金概要画像",
            JsonUnionConfidenceAnalyse: "可信度分析",
            JsonUBalanceTrendChart: "余额趋势图",
            JsonUTransFrequencyDetail: "交易频率明细",
            JsonUSuspectedAffiliates: "疑似关联方分析",
            JsonUIndustryPortrait: "行业画像"
        }
        if self.cached_data["single"]:
            view_handle_list.append(JsonSingleTitle())
            view_handle_list.append(JsonSinglePortrait())
            view_handle_list.append(JsonSingleSummaryPortrait())
            # view_handle_list.append(JsonSingleRemarkPortrait())
            # view_handle_list.append(JsonSingleRemarkTransDetail())
            view_handle_list.append(JsonSingleCounterpartyPortrait())
            view_handle_list.append(JsonSingleRelatedPortrait())
            view_handle_list.append(JsonSingleGuarantor())
            view_handle_list.append(JsonSingleLoanPortrait())
            view_handle_list.append(JsonSingleUnusualTrans())
            view_handle_list.append(JsonSingleMarketing())

        else:
            view_handle_list.append(JsonUnionTitle())
            view_handle_list.append(JsonUnionPortrait())
            view_handle_list.append(JsonUFundProcurement())
            view_handle_list.append(JsonULabelTotal())
            view_handle_list.append(JsonUnionSummaryPortrait())
            # view_handle_list.append(JsonUnionRemarkPortrait())
            # view_handle_list.append(JsonUnionRemarkTransDetail())
            view_handle_list.append(JsonUnionCounterpartyPortrait())
            view_handle_list.append(JsonUnionRelatedPortrait())
            # view_handle_list.append(JsonUnionGuarantor())
            view_handle_list.append(JsonUnionLoanPortrait())
            view_handle_list.append(JsonUnionUnusualTrans())
            view_handle_list.append(JsonUnionMarketing())
            view_handle_list.append(JsonUnionNormalIncomePortrait())
            view_handle_list.append(JsonUnionFundsSummaryPortrait())
            view_handle_list.append(JsonUnionConfidenceAnalyse())
            view_handle_list.append(JsonUBalanceTrendChart())
            view_handle_list.append(JsonUTransFrequencyDetail())
            view_handle_list.append(JsonUSuspectedAffiliates())
            view_handle_list.append(JsonUIndustryPortrait())

        for view in view_handle_list:
            start = time.time()
            view.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            view.process()
            cost = time.time() - start
            logger.info(f"{self.origin_data['preReportReqNo']}:{view_name_mapping[view.__class__]}处理完成，耗时{cost:.2f}秒")
