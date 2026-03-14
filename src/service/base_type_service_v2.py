# -*- coding: utf-8 -*-
# @Time : 2019/12/9 11:09 AM
# @Author : lixiaobo
# @Site : 
# @File : base_type_service.py.py
# @Software: PyCharm
import json
import threading

from resources.resource_util import get_config_content
from service.base_type_service import BaseTypeService


class BaseTypeServiceV2(BaseTypeService):
    BASE_TYPE_MAPPING_V2 = []

    def __init__(self, query_data):
        self.query_data = query_data
        self.main_type = 'PERSONAL'

        if len(BaseTypeServiceV2.BASE_TYPE_MAPPING_V2) > 0:
            return
        self.init_data(BaseTypeServiceV2.BASE_TYPE_MAPPING_V2, "base_type_mapping_v2.json")

    def data(self):
        return BaseTypeServiceV2.BASE_TYPE_MAPPING_V2

    def base_type_mapping(self, subject, parents):
        s_type = subject["userType"]
        s_relation = subject["relation"]

        for type_to_relations in self.data():
            if len(type_to_relations) != len(parents) + 2:
                continue
            if s_type != type_to_relations[1]["userType"] or s_relation != type_to_relations[1]["relation"]:
                continue

            if "ratioMin" in type_to_relations[1] and "ratioMax" in type_to_relations[1] and "fundratio" in subject:
                fund_ratio = float(subject["fundratio"])
                ratioMin = float(type_to_relations[1]["ratioMin"])
                ratioMax = float(type_to_relations[1]["ratioMax"])
                if not (ratioMin <= fund_ratio < ratioMax):
                    continue

            if "authorStatus" in type_to_relations[1]:
                if "authorStatus" not in subject:
                    continue
                elif subject["authorStatus"] != type_to_relations[1]["authorStatus"]:
                    continue

            all_match = True
            for index, type_to_relation in enumerate(type_to_relations):
                if index < 2:
                    continue
                c_type = type_to_relation["userType"]
                c_relation = type_to_relation["relation"]
                if c_type != parents[index - 2]["userType"] or c_relation != parents[index - 2]["relation"]:
                    all_match = False
            if all_match:
                return type_to_relations[0]["baseType"]

    def get_normal_base_type(self, fund_ratio, auth_status, phone, relation, user_type):

        if relation == 'GUARANTOR':
            if user_type == 'COMPANY':
                return 'G_COMPANY'
            elif auth_status == 'AUTHORIZED':
                return 'G_PERSONAL'
            else:
                return 'G_S_PERSONAL'
        # 未匹配到baseType.
        if user_type == "PERSONAL":
            return "U_PER_OTHER"
        elif user_type == "COMPANY":
            return "U_COM_OTHER"
        else:
            return "U_IGNORE_TYPE_OTHER"
