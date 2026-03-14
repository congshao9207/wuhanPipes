# -*- coding: utf-8 -*-
# @Time : 2020/2/25 2:22 PM
# @Author : lixiaobo
# @Site : 
# @File : test_v16001.py.py
# @Software: PyCharm
import json

from view.p06001.v16001 import V16001


def test_v16001():
    ps = V16001()
    ps.run(user_name='施网明', id_card_no='310108196610024859', phone='13277154945', origin_data={"preBizDate": "2019-11-04 10:24:33"})
    print(json.dumps(ps.variables))


def test_list():
    info = [1, 2, 3, 4]
    infoa = [2, 3, {"info": {"name": "aaaa"}}]

    info.extend(infoa)
    print(info)
