# @Time : 2020/6/18 8:02 PM 
# @Author : lixiaobo
# @File : p08002.py.py 
# @Software: PyCharm


from logger.logger_util import LoggerUtil
from product.p08001 import P08001

logger = LoggerUtil().logger(__name__)


# 流水拦截的处理
class P08002(P08001):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        super().shake_hand_process()

    def strategy_process(self):
        super().strategy_process()
