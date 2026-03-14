from abc import ABCMeta, abstractmethod

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


class Generate(object):
    __metaclass__ = ABCMeta

    def __init__(self) -> None:
        super().__init__()
        self.request = {}
        self.response = {}
        self.df_client = None

    def shake_hand(self, request=None):
        """
        第一次交互
        :param request:
        :return:
        """
        self.input(request)
        self.shake_hand_process()
        return self.response

    def call_strategy(self, request=None):
        """
        第二次交互
        :param request:
        :return:
        """
        self.input(request)
        self.strategy_process()
        return self.response

    def input(self, request):
        self.request = request

    @abstractmethod
    def shake_hand_process(self):
        """
        defensor第一次调用处理逻辑
        :return:
        """
        pass

    @abstractmethod
    def strategy_process(self):
        """
        defensor第二次调用处理逻辑
        :return:
        """
        pass

    @staticmethod
    def create_strategy_resp(product_code, req_no, step_req_no, version_no, subject):
        return {
            'reqNo': req_no,
            'product_code': product_code,
            'stepReqNo': step_req_no,
            'versionNo': version_no,
            'subject': subject
        }
