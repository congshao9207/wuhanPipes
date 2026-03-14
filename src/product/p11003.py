# @Time : 2020/4/21 11:28 AM 
# @Author : lixiaobo
# @File : p07001.py 
# @Software: PyCharm

import json
import traceback

import requests
from flask import request
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each
from mapping.mapper import translate_for_strategy
from mapping.utils.np_encoder import NpEncoder
from product.generate import Generate
from product.p_config import product_codes_dict
from product.p_utils import _relation_risk_subject, _append_rules, score_to_int, _get_biz_types, _build_request
from strategy_config import obtain_strategy_url
from util.type_converter import echo_var_type, format_var


logger = LoggerUtil().logger(__name__)


# 财务报表预解析
class P11003(Generate):
    def shake_hand_process(self):
        raise NotImplementedError()

    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1. 财务通用报告：获取映射结果，流程开启, 入参为：%s", json.dumps(json_data))
            strategy_param = json_data.get('strategyParam')
            product_code = strategy_param.get('productCode')
            table_no=strategy_param.get('financeReportNo')
            data_repository = {'table_no':table_no,'product_code':product_code}
            variables, out_decision_code = translate_for_strategy(product_code, ['11003'], origin_data=None,
                                                                  data_repository=data_repository, full_msg=json_data)

            self.response = {"response": variables}
            logger.info(self.response)
            logger.info("2. 财务报表分析报告，应答：%s", json.dumps(self.response, cls=NpEncoder))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))


