# -*- coding: utf-8 -*-
# @Time : 2019/11/22 4:43 PM
# @Author : lixiaobo
# @Site : 
# @File : test_t00001.py.py
# @Software: PyCharm
from mapping.t00001 import T00001
from util.defensor_client import DefensorClient


def test_df_client():
    df_client = DefensorClient(None)
    df_client.app_id = "99999"
    df_client.grey_list_query_url = "http://192.168.1.15:100/gateway/defensor/api/open/grey-list/hit"

    t = T00001()
    t.df_client = df_client
    t.run(user_name='蒋育宣', id_card_no='321088196405036526', phone='11111111111')
    print("-----", t.variables)