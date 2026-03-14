# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:23 AM
# @Author : lixiaobo
# @Site : 
# @File : tf0003.py
# @Software: PyCharm
from mapping.tranformer import Transformer


class Tf0003(Transformer):
    def __init__(self) -> None:
        super().__init__()
        '''self.variables = {
            'per_com_shares_frost_laf': '0',  # 联企工商_现在是否有股权冻结信息_贷后新增
            'per_com_shares_impawn_laf': '0',  # 联企工商_现在是否有股权出质登记信息_贷后新增
            'per_com_mor_detail_laf': '0',  # 联企工商_现在是否有动产抵押登记信息_贷后新增
            'per_com_liquidation_laf': '0',  # 联企工商_是否有清算信息_贷后新增
            'per_com_illegal_list_laf': '0',  # 联企工商_现在是否有严重违法失信信息_贷后新增
        }
        '''

    def transform(self):
        # 在贷后报告中，不用清洗
        pass
