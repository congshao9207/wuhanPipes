# -*- coding: utf-8 -*-
# @Time : 2019/12/4 11:55 AM
# @Author : lixiaobo
# @Site : 
# @File : eureka_client_test.py.py
# @Software: PyCharm
from py_eureka_client import eureka_client


def test_eureka_01():
    eureka_client.init(eureka_server="http://192.168.1.27:8030/eureka/",
                       app_name="PIPES",
                       instance_port=8010)

    res = eureka_client.do_service("DEFENSOR", "/api/open/grey-list/hit?appId=99999&name=111&idno=11111&idType=ID_CARD_NO")
    print("result of other service" + res)
