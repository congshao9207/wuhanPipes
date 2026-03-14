# @Time : 2020/6/18 6:45 PM 
# @Author : lixiaobo
# @File : Parser001.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from fileparser.Parser import Parser
from fileparser.trans_flow.trans_z01_profile import TransProfile
from fileparser.trans_flow.trans_z02_data_standardization import TransDataStandardization
from fileparser.trans_flow.trans_z03_title_distribute import TransBasic
from fileparser.trans_flow.trans_z04_time_standardization import TransactionTime
from fileparser.trans_flow.trans_z05_amount_standardization import TransactionAmt
from fileparser.trans_flow.trans_z06_balance_standardization import TransactionBalance
from fileparser.trans_flow.trans_z07_opponent_info_standardization import OpponentInfo
from fileparser.trans_flow.trans_z08_other_info_standardization import TransactionOtherInfo
from fileparser.trans_flow.trans_z09_flow_raw_data import TransFlowRawData

# 流水报告解析及验真
from util.common_util import exception

logger = LoggerUtil().logger(__name__)


class Parser001(Parser):
    def __init__(self):
        super().__init__()
        self.sql_db = None

    # 解析，验真逻辑， 此成员变量
    # self.param  提交的入参
    # self.file   待解析的文件
    # 返回解析验真结果
    @exception('purpose=Parser001')
    def process(self):
        """
            入参： param：
                {
                    'appId': '商户端ID',
                    'cusType': "主体类型：PERSONAL, COMPANY",
                    'cusName': "客户名称",
                    'idNo': "证件号",
                    'idType': "证件号码ID_CARD_NO, CREDIT_CODE, REG_NO,",
                    'bankAccount': "银行账号",
                    'bankName': "银行名",
                    'outApplyNo': "申请业务号",
                    'outReqNo': "外部请求编号",
                    'bizReqNo': "回执编号",
                    'accountId': "账户编号"
                }
            返回值：resCode：
                  0 成功
                  1 失败
                  2 异常
                  3 处理中
                  10 参数错误
                  20 解析失败
                  21 校验失败
                  22 验真失败
        """
        logger.info("流水报告解析及验真参数:param:%s", self.param)
        logger.info("流水报告解析及验真参数:file:%s", self.file)

        # 1.传入文件,检验是否存在标题行,以及标题行以上是否存在银行,姓名,账号等信息,若存在则与客户所填的信息进行匹配
        logger.info("%d-----------------------%s" % (1, '进行标题行校验'))
        trans_profile = TransProfile(self.file, self.param)
        trans_profile.process()
        # 若匹配不上,则直接返回相应的回应报文
        if not trans_profile.basic_status:
            return trans_profile.resp

        # 2.将1中得到的流水数据传入流水数据标准化类,即去掉不需要的行(空行,总结行等)
        logger.info("%d-----------------------%s" % (2, '进行流水数据标准化'))
        trans_data_standardization = TransDataStandardization(trans_profile.trans_data)
        trans_data_standardization.standard()

        # 3.将2中得到的流水数据传入基准类,将流水数据的标题行粗略分类
        logger.info("%d-----------------------%s" % (3, '进行标题行分类'))
        trans_basic = TransBasic(trans_data_standardization.trans_data)
        trans_basic.match_title()

        # 4.将3中得到的流水数据传入交易时间处理类,并进行时间间隔校验,并将流水数据按照交易时间顺序排列
        logger.info("%d-----------------------%s" % (4, '进行时间列校验'))
        transaction_time = TransactionTime(trans_basic.df, trans_basic.col_mapping, trans_profile.title_params)
        # 将时间列都转化为标准时间,若出现转化错误则直接返回相应的回应报文
        transaction_time.time_match()
        if not transaction_time.basic_status:
            return transaction_time.resp
        # 计算查询间隔,交易间隔,导入间隔,查看三者是否满足要求,若不满足,则直接返回相应的回应报文
        # transaction_time.time_interval_check()
        # if not transaction_time.basic_status:
        #     return transaction_time.resp

        # 5.将4中得到的流水数据传入交易金额列处理类
        logger.info("%d-----------------------%s" % (5, '进行交易金额列校验'))
        transaction_amt = TransactionAmt(transaction_time.df, trans_basic.col_mapping)
        # 将交易金额列转化为标准金额,若出现转化错误则直接返回相应的回应报文
        transaction_amt.amt_match()
        if not transaction_amt.basic_status:
            return transaction_amt.resp

        # 7.将6中得到的流水数据传入交易对手相关信息列处理类
        logger.info("%d-----------------------%s" % (7, '进行交易对手信息列处理'))
        opponent_info = OpponentInfo(transaction_amt.df, trans_basic.col_mapping)
        opponent_info.opponent_info_match()

        # 8.将7中得到的流水数据传入其他交易信息列处理类
        logger.info("%d-----------------------%s" % (8, '进行其他交易信息列处理'))
        other_info = TransactionOtherInfo(opponent_info.df, trans_basic.col_mapping)
        other_info.trans_info_match()

        # 6.将5中得到的流水数据传入交易余额列处理类
        logger.info("%d-----------------------%s" % (6, '进行交易余额列校验'))
        transaction_bal = TransactionBalance(other_info.df, trans_basic.col_mapping, transaction_time.sort_list)
        logger.info("%d-----------------------%s" % (6, '进行交易余额列校验-1'))
        # 将交易余额列转化为标准金额,若出现转化错误则直接返回相应的回应报文
        transaction_bal.balance_match()
        logger.info("%d-----------------------%s" % (6, '进行交易余额列校验-2'))
        if not transaction_bal.basic_status:
            return transaction_bal.resp
        # 从头检查上一行交易余额+本行交易金额是否等于本行交易余额,若出现不匹配则直接返回相应报文
        transaction_bal.balance_sequence_check()

        # 再从头读取数据看季度结息情况是否会负
        transaction_bal.interest_check()

        logger.info("%d-----------------------%s" % (6, '进行交易余额列校验-3'))
        # if not transaction_bal.basic_status:
        #     return transaction_bal.resp

        # 9.将8中的到的最终数据传入流水账户表和原始数据表中落库
        raw_data = TransFlowRawData(self.sql_db, self.param, trans_profile.title_params, transaction_bal.resp,
                                    transaction_bal.basic_status)
        logger.info("%d-----------------------%s" % (9, '移除重复数据'))
        raw_data.df = raw_data.remove_duplicate_data(transaction_bal.df)
        # 若没有新增数据将不进行落库操作
        if raw_data.df is not None and raw_data.df.shape[0] > 0 and raw_data.new_data:
            logger.info("%d-----------------------%s" % (10, '进行数据落库'))
            raw_data.save_raw_data()
        logger.info("%d-----------------------%s" % (11, '解析结束'))
        return raw_data.resp
