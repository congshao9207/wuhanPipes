# -*- coding: utf-8 -*-
# @Time : 2020/2/20 1:53 PM
# @Author : lixiaobo
# @Site :
# @File : p06001.py
# @Software: PyCharm
import json
import traceback

from flask import request

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from product.common.hand_shake_service import HandShakeService
from product.common.strategy_service import StrategyService
from product.generate import Generate
from service.base_type_service import BaseTypeService
from service.base_type_service_v2 import BaseTypeServiceV2

logger = LoggerUtil().logger(__name__)


# 贷后处理
class P06001(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            logger.info("1. 贷后报告决策握手处理开始，入参为：%s", json.dumps(json_data))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')

            base_type_service = BaseTypeServiceV2(query_data_array)
            hand_shake_service = HandShakeService()

            response_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                resp = hand_shake_service.hand_shake(base_type_service, data, product_code, req_no)
                response_array.append(resp)

            final_resp = {
                'queryData': response_array
            }

            for key in json_data:
                val = json_data.get(key)
                if not isinstance(val, dict) and not isinstance(val, list):
                    final_resp[key] = val
            self.response = final_resp

            logger.info("2. 贷后报告决策握手流程结束 应答 Defensor报文为：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1. 贷后策略：获取策略引擎结果，流程开启, 入参为：%s", json.dumps(json_data))
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            step_req_no = strategy_param.get('stepReqNo')
            pre_report_req_no = strategy_param.get('preReportReqNo')
            pre_biz_date = strategy_param.get('preBizDate')
            version_no = strategy_param.get('versionNo')
            query_data_array = strategy_param.get('queryData')
            subject = []

            strategy_service = StrategyService()
            # 遍历query_data_array调用strategy
            index = 0
            total = len(query_data_array)
            for data in query_data_array:
                data["preReportReqNo"] = pre_report_req_no
                data["preBizDate"] = pre_biz_date
                index = index + 1
                logger.info("strategy_process------------" + str(index) + "/" + str(total))
                resp = strategy_service.strategy(self.df_client, data, product_code, req_no)
                subject.append(resp)

            self.response = self.create_strategy_resp(product_code, req_no, step_req_no, version_no, subject)

            logger.info("2. 贷后策略：贷后策略调用，应答：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))
