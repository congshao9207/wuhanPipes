from logger.logger_util import LoggerUtil
from product.p07003 import P07003

logger = LoggerUtil().logger(__name__)


# 企业征信拦截处理流程
class P07004(P07003):

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        super().shake_hand_process()

    def strategy_process(self):
        super().strategy_process()
