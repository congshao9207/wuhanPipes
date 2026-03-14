import json
import traceback

from flask import request
import pandas as pd

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from product.p003 import P003
from service.base_type_service import BaseTypeService

logger = LoggerUtil().logger(__name__)


# 风险拦截
class P004(P003):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            resp_data = self.df_client.query_grey_list(name="小明", id_no="61242938382828347", id_type="ID_CARD_NO")
            content_data = json.dumps(resp_data)
            logger.info("resp_data-" + content_data)
            df = pd.read_json(content_data)
            print(df)

            json_data = request.get_json()
            logger.info("风险拦截BizTypes请求开始...")
            logger.debug("request_data" + str(json.dumps(json_data)))
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            base_type_service = BaseTypeService(query_data_array)
            response_array = []
            # 遍历query_data_array调用strategy
            for data in query_data_array:
                response_array.append(self._shake_hand_response(base_type_service, data, product_code, req_no))
            resp = {
                'productCode': product_code,
                'reqNo': req_no,
                'queryData': response_array
            }
            self.response = resp
            logger.info("response data" + str(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def calc_base_type(self, base_type_service, subject):
        base_type = base_type_service.find_base_type(subject)
        if base_type is not None:
            return base_type
        else:
            user_type = subject.get('userType')
            if user_type == "PERSONAL":
                return "U_PER_OTHER"
            elif user_type == "COMPANY":
                return "U_COM_OTHER"
            else:
                return "U_IGNORE_TYPE_OTHER"
