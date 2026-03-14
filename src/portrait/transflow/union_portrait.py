# @Time : 2020/6/19 2:50 PM 
# @Author : lixiaobo
# @File : union_portrait.py 
# @Software: PyCharm

from portrait.portrait_processor import PortraitProcessor
from logger.logger_util import LoggerUtil
from portrait.transflow.single_account_portrait.trans_flow import TransFlowBasic
from portrait.transflow.union_account_portrait.trans_z01_union_portrait_label import TransUnionLabel
from portrait.transflow.union_account_portrait.trans_z02_union_portrait import UnionTransProtrait
from portrait.transflow.union_account_portrait.trans_z03_union_summary_portrait import UnionSummaryPortrait
from portrait.transflow.union_account_portrait.trans_z04_union_remark_portrait import UnionRemarkPortrait
from portrait.transflow.union_account_portrait.trans_z05_union_counterparty_portrait import UnionCounterpartyPortrait
from portrait.transflow.union_account_portrait.trans_z06_union_related_portrait import UnionRelatedPortrait
from portrait.transflow.union_account_portrait.trans_z07_union_loan_portrait import UnionLoanPortrait

logger = LoggerUtil().logger(__name__)


class UnionPortrait(PortraitProcessor):
    """
    联合画像清洗
    update_time_v1:20200708,汪腾飞
    """
    def __init__(self):
        super().__init__()
        self.out_req_no_list = []

    def process(self):
        """
        参数query_data_array的结构如下：
        [
            {
                "applyAmo":66600,
                "authorStatus":"AUTHORIZED",
                "extraParam":{
                    "bankAccount":"银行账户",
                    "bankName":"银行名",
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10",
                    "seasonable":"1",
                    "totalSalesLastYear":23232
                },
                "fundratio":0,
                "id":11879,
                "idno":"31011519910503253X",
                "name":"韩骁頔",
                "parentId":0,
                "phone":"13611647802",
                "relation":"CONTROLLER",
                "userType":"PERSONAL",
                "baseType":"U_COM_CT_PERSONAL"
            },
            {
                "applyAmo":66600,
                "extraParam":{
                    "bankAccount":"银行账户",
                    "bankName":"银行名",
                    "industry":"E20",
                    "industryName":"xx行业",
                    "seasonOffMonth":"2,3",
                    "seasonOnMonth":"9,10",
                    "seasonable":"1",
                    "totalSalesLastYear":23232
                },
                "fundratio":0,
                "id":11880,
                "idno":"91440300MA5EEJUR92",
                "name":"磁石供应链商业保理（深圳）有限公司",
                "parentId":0,
                "phone":"021-1234567",
                "relation":"MAIN",
                "userType":"COMPANY",
                "baseType":"U_COMPANY"
            }
        ]

        self.public_param的参数包含：
            "reqNo"
            "reportReqNo"
            "productCode"
            "isUnion"
            "outApplyNo"
            "applyAmt"
            "renewLoans"
            "historicalBiz"
        """
        # 1.读取原始数据,并筛选时间范围,包括一年内和两年内两种
        trans_flow = TransFlowBasic(self)
        # 查找所有关联人的标签数据,若为空则直接返回
        trans_flow.u_process(self.out_req_no_list)
        if trans_flow.trans_u_flow_df is None:
            return
        
        # 2.将联合账户标签信息落库
        # logger.info("------------%s-----------" % '进行联合账户标签信息落库')
        # trans_label = TransUnionLabel(trans_flow)
        # trans_label.process()

        # 落库完成后将该业务的所有标签信息读取出来进行后续画像表清洗
        # trans_flow.trans_union_portrait()

        # 3.将联合账户汇总信息画像表落库
        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '进行联合账户汇总信息画像表落库'))
        trans_single = UnionTransProtrait(trans_flow)
        trans_single.process()

        # 4.将联合账户时间汇总信息画像表落库
        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '进行联合账户时间汇总信息画像表落库'))
        trans_summary = UnionSummaryPortrait(trans_flow)
        trans_summary.process()

        # 5.将联合账户备注分类信息画像表落库
        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '进行联合账户备注分类信息画像表落库'))
        trans_remark = UnionRemarkPortrait(trans_flow)
        trans_remark.process()

        # 6.将联合账户主要交易对手信息画像表落库
        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '进行联合账户主要交易对手信息画像表落库'))
        trans_counterparty = UnionCounterpartyPortrait(trans_flow)
        trans_counterparty.process()

        # 7.将联合账户关联人和担保人信息画像表落库
        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '进行联合账户关联人和担保人信息画像表落库'))
        trans_related = UnionRelatedPortrait(trans_flow)
        trans_related.process()

        # 8.将联合账户贷款信息画像表落库
        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '进行联合账户贷款信息画像表落库'))
        trans_loan = UnionLoanPortrait(trans_flow)
        trans_loan.process()

        logger.info("------------%s:%s-----------" % (trans_flow.app_no, '联合账户画像表落库完成'))
