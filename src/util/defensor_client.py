# @Time : 2019/10/16 2:25 PM
# @Author : lixiaobo
# @Site :
# @File : defensor_client.py
# @Software: PyCharm
import json
from abc import ABCMeta
from urllib.parse import urlencode

from py_eureka_client import eureka_client

from exceptions import ServerException
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)

API_GREY_LIST_QUERY = "/api/open/grey-list/hit"


class DefensorClient(object):
    __metaclass__ = ABCMeta

    def __init__(self, headers) -> None:
        if headers is not None:
            self.app_id = headers.get("appId")
            logger.info("appId:%s", self.app_id)
        else:
            self.app_id = None
        logger.debug("DefensorClient module name:%s", __name__)

    def query_grey_list(self, name, id_no, id_type, hit=None):
        logger.info("query_grey_list begin.")
        if self.app_id is None:
            logger.debug("query_grey_list nothing to do, param: appId is empty.")
            return
        param = {"appId": self.app_id, "name": name, "idType": id_type, "idno": id_no}
        if hit is not None:
            param[hit] = hit

        logger.info("query_grey_list begin")

        resp = self.invoke_api(API_GREY_LIST_QUERY, param)

        logger.info("query_grey_list end")

        response_json = json.loads(resp)
        res_code = response_json.get("resCode")
        if res_code != "0":
            error_info = "查询灰名单出错，resCode:" + res_code + "message:" + response_json.get("message")
            raise ServerException(description="查询灰名单状态码不正确", response=error_info, code="500")
        return response_json.get("data")

    @staticmethod
    def invoke_api(api_url, param):
        url_data = urlencode(param)
        logger.info("invoke api begin, param:%s", param)
        full_api_url = api_url + "?" + url_data
        logger.info("invoke api full api url:%s", full_api_url)

        res = eureka_client.do_service("DEFENSOR", full_api_url)
        logger.info("invoke_api resp:%s", res)

        return res
