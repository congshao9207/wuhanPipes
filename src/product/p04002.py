# @Time : 2020/10/14 9:42 AM 
# @Author : lixiaobo
# @File : p03002.py 
# @Software: PyCharm

from logger.logger_util import LoggerUtil
from product.p03002 import P03002

logger = LoggerUtil().logger(__name__)


class P04002(P03002):
    """
    风险拦截2.0
    """

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        super().shake_hand_process()

    def strategy_process(self):
        super().strategy_process()
