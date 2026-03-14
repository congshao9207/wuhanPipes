# @Time : 2020/6/19 2:49 PM 
# @Author : lixiaobo
# @File : trans_z02_single_portrait.py.py
# @Software: PyCharm

from portrait.portrait_processor import PortraitProcessor
from logger.logger_util import LoggerUtil
from portrait.transflow.single_account_portrait.trans_flow import TransFlowBasic
from portrait.transflow.single_account_portrait.trans_z00_apply import TransApply
from portrait.transflow.single_account_portrait.trans_z01_single_portrait_label import TransSingleLabel
from portrait.transflow.single_account_portrait.trans_z02_single_portrait import SingleTransProtrait
from portrait.transflow.single_account_portrait.trans_z03_single_summary_portrait import SingleSummaryPortrait
from portrait.transflow.single_account_portrait.trans_z04_single_remark_portrait import SingleRemarkPortrait
from portrait.transflow.single_account_portrait.trans_z05_single_counterparty_portrait import SingleCounterpartyPortrait
from portrait.transflow.single_account_portrait.trans_z06_single_related_portrait import SingleRelatedPortrait
from portrait.transflow.single_account_portrait.trans_z07_single_loan_portrait import SingleLoanPortrait
import pandas as pd

logger = LoggerUtil().logger(__name__)


class SinglePortrait(PortraitProcessor):
    """
    单账户画像数据
    update_time_v1:20200708,汪腾飞
    """

    def __init__(self):
        super().__init__()
        self.out_req_no_list = []

    def process(self):
        logger.info("单账户画像数据表清洗及落库,公共参数:%s" % str(self.public_param))
        logger.info("单账户画像数据表清洗及落库,关联关系参数:%s" % str(self.query_data_array))

        # 默认账号全查，还是查有效期内的账号

        # 1.读取原始数据,并筛选时间范围,包括一年内和两年内两种
        trans_flow = TransFlowBasic(self)

        # 2.将业务关联信息落库
        logger.info("-----------业务号%s正在%s------------" % (trans_flow.app_no, '进行业务关联信息落库'))
        trans_apply = TransApply(trans_flow)
        trans_apply.save_trans_apply_data()
        id_list = []
        # 开始遍历所有关联关系,若该关联人3个月内上传过流水,则将其对应的单账户画像表清洗落库
        while trans_flow.object_k < trans_flow.object_nums:
            temp_object = trans_flow.query_data_array[trans_flow.object_k]
            extra_param = temp_object.get('extraParam')
            name = temp_object.get("name")
            if extra_param is None:
                trans_flow.object_k += 1
                continue
            accounts = extra_param.get('accounts')
            if accounts is None or len(accounts) == 0:
                trans_flow.object_k += 1
                continue

            if temp_object.get("idno") in id_list:
                trans_flow.object_k += 1
                continue

            acc_df = pd.DataFrame(accounts).drop_duplicates()
            acc_list = acc_df.index.tolist()
            for i in acc_list:
                account_no = acc_df.loc[i, 'bankAccount']
                bank_name = acc_df.loc[i, 'bankName']
                trans_flow.object_k_k = i
                # 首先查找第k个关联人的流水数据第k个账户的信息
                out_req_no_list = trans_flow.process()
                if trans_flow.trans_flow_df is None:
                    continue
                # 3.将单账户标签信息落库
                # 20221208，单账户画像落库在上传阶段已进行
                # 20220104，单账户画像重新落库
                # logger.info("第%d个关联人第%d个账户-----------------------%s" %
                #             (trans_flow.object_k + 1, i + 1, '进行单账户标签信息落库'))
                # trans_label = TransSingleLabel(trans_flow)
                # if trans_flow.portrait_status:
                #     trans_label.process1()
                # else:
                #     trans_label.process()

                # 落库完成后将该账户的标签信息读取出来进行后续画像表清洗,并且记录id_no
                id_list.append(temp_object.get("idno"))
                self.out_req_no_list.extend(out_req_no_list)
                # trans_flow.trans_single_portrait()

                # 4.将单账户汇总信息画像表落库
                logger.info("报告编号%s, 第%d个关联人第%d个账户-----------------------%s" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '进行单账户汇总信息画像表落库'))
                trans_single = SingleTransProtrait(trans_flow)
                trans_single.process()

                # 5.将单账户时间汇总信息画像表落库
                logger.info("报告编号%s, 第%d个关联人第%d个账户-----------------------%s" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '进行单账户时间汇总信息画像表落库'))
                trans_summary = SingleSummaryPortrait(trans_flow)
                trans_summary.process()

                # 6.将单账户备注分类信息画像表落库
                logger.info("报告编号%s, 第%d个关联人第%d个账户-----------------------%s" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '进行单账户备注分类信息画像表落库'))
                trans_remark = SingleRemarkPortrait(trans_flow)
                trans_remark.process()

                # 7.将单账户主要交易对手信息画像表落库
                logger.info("报告编号%s, 第%d个关联人第%d个账户-----------------------%s" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '进行单账户主要交易对手信息画像表落库'))
                trans_counterparty = SingleCounterpartyPortrait(trans_flow)
                trans_counterparty.process()

                # 8.将单账户关联人和担保人信息画像表落库
                logger.info("报告编号%s, 第%d个关联人第%d个账户-----------------------%s" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '进行单账户关联人和担保人信息画像表落库'))
                trans_related = SingleRelatedPortrait(trans_flow, account_no, bank_name, name)
                trans_related.process()

                # 9.将单账户贷款信息画像表落库
                logger.info("报告编号%s, 第%d个关联人第%d个账户-----------------------%s" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '进行单账户贷款信息画像表落库'))
                trans_loan = SingleLoanPortrait(trans_flow)
                trans_loan.process()

                logger.info("报告编号%s, ------------第%d个关联人第%d个账户%s-----------" %
                            (trans_flow.app_no, trans_flow.object_k + 1, i + 1, '单账户画像表落库完成'))
            trans_flow.object_k += 1
