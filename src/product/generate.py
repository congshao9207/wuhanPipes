import json
from abc import ABCMeta, abstractmethod

from py_eureka_client import eureka_client

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


    def async_call_strategy(self, request=None):
        """
        第二次交互
        :param request:
        :return:
        """
        self.input(request)
        self.strategy_process()
        return self.response

    def async_strategy(self, json_data):
        # 从 json_data 中获取 strategyParam 字典
        strategy_param = json_data.get('strategyParam')

        # 提取具体参数
        req_no = strategy_param.get('reqNo')
        product_code = strategy_param.get('productCode')
        step_req_no = strategy_param.get('stepReqNo')
        version_no = strategy_param.get('versionNo')

        logger.info(f"开始异步生成流水报告{req_no}")

        # 初始化返回结构
        resp = {
            "product_code": product_code,
            "reqNo": req_no,
            "stepReqNo": step_req_no,
            "versionNo": version_no
        }

        try:
            # 保存数据并执行策略
            self.json_data = json_data
            resp = self.async_call_strategy()
            logger.info(f"{req_no}@async_call_strategy执行成功")
        except Exception as e:
            logger.error(str(e))

        # 序列化响应数据
        json_body = json.dumps(resp, ensure_ascii=False, separators=(',', ':'))

        logger.info(f"{req_no}开始回调taia")

        # 调用外部服务
        res = eureka_client.do_service(
            "TAIA",
            "/api/report/trans/xaas/resp",
            method="POST",
            data=json_body,
            headers={"Content-Type": "application/json"}
        )

        logger.info(f"{req_no}回调taia结果{res}")

        return resp
