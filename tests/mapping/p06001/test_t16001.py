# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:55 AM
# @Author : lixiaobo
# @Site : 
# @File : test_t16001.py
# @Software: PyCharm
from mapping.p06001.t16001 import T16001


def test_16001():
    ps = T16001()
    ps.run(user_name='施网明', id_card_no='310108196610024859', phone='13277154945', origin_data={"preBizDate": "2019-11-04 10:24:33"})
    print(ps.variables)


def test_16002():
    ps = T16001()
    ps.run(user_name='叶冬梅', id_card_no='433126198704090605', origin_data={"preBizDate": "2019-01-01 13:01:01"})
    print(ps.variables)

def test_map():
    info = {"a": "b"}
    for key in info:
        print(key)
