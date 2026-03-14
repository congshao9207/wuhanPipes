# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:55 AM
# @Author : lixiaobo
# @Site : 
# @File : test_t16001.py
# @Software: PyCharm
from mapping.p06001.tf0001 import Tf0001


def test_t0001():
    ps = Tf0001()
    ps.run(user_name='蒋育宣', id_card_no='321088196405036526', phone='13277154945', origin_data={"preBizDate": "2019-09-17 10:54:20"})
    print(ps.variables)

