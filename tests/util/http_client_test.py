import json

import requests
from py_eureka_client import eureka_client

from util.defensor_client import DefensorClient


def test_query_grey_list():
    print("begin query grey list.")
    param = {}
    param["appId"] = "0000000000"
    param["name"] = "小明"
    param["idType"] = "ID_CARD_NO"
    param["idno"] = "61242938382828347"
    param["hit"] = "法院失信明单"

    resp = requests.get("http://localhost:9781/api/open/grey-list/hit", param)
    if resp.status_code == 200:
        print("resp", resp.json().get("resCode"))
        print("content:", str(resp.content))
        print("content-:", resp.json().get("data"))
        data_items = resp.json().get("data")
        data1 = json.dumps(data_items)
        print(type(data1))


def test_defensor_client():
    param = {}
    param["appId"] = "0000000000"
    param["name"] = "小明"
    param["id_type"] = "ID_CARD_NO"
    param["idno"] = "61242938382828347"
    param["hit"] = "法院失信明单"
    df_client = DefensorClient(None)
    resp = df_client.query_grey_list(None, None, None)
    print("resp:", resp)


def test_defensor_client_01():
    full_url = "http://192.168.1.36:3000/gateway/defensor/api/open/grey-list/hit"
    param = {
        "appId": "0000000000",
        "name": "黎小波",
        "idType": "ID_CARD_NO",
        "idno": "61242938382828347"
    }
    resp = requests.post(full_url, data=param)
    print("resp:", resp.content)


def test_defensor_client_02():
    eureka_client.init(eureka_server="http://192.168.1.27:8030/eureka/",
                       app_name="PIPES",
                       instance_port=8010)
    print("eureka_client init finished.")

    df_client = DefensorClient(None)
    df_client.app_id = "0000000000"
    data = df_client.query_grey_list("小明", "61242938382828347", "ID_CARD_NO")
    print("data:", data)
