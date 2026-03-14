# -*- coding: utf-8 -*-
# @Time : 2019/12/9 11:09 AM
# @Author : lixiaobo
# @Site : 
# @File : base_type_service.py.py
# @Software: PyCharm
import json
import threading

from resources.resource_util import get_config_content

BASE_TYPE_MAPPING_INIT_LOCK = threading.Lock()


class BaseTypeService(object):
    BASE_TYPE_MAPPING = []

    def __init__(self, query_data):
        self.query_data = query_data
        self.main_type = 'PERSONAL'

        if len(BaseTypeService.BASE_TYPE_MAPPING) > 0:
            return
        self.init_data(BaseTypeService.BASE_TYPE_MAPPING, "base_type_mapping.json")

    def init_data(self, repository, mapping_file_path, user_type='PERSONAL'):
        self.main_type = user_type
        try:
            BASE_TYPE_MAPPING_INIT_LOCK.acquire()
            if len(repository) == 0:
                mapping_data = get_config_content(mapping_file_path)
                mapping_dicts = json.loads(mapping_data)
                result_list = BaseTypeService.arrow_dict_to_array(mapping_dicts, user_type)
                if result_list:
                    for v in result_list:
                        repository.append(v)
        finally:
            BASE_TYPE_MAPPING_INIT_LOCK.release()

    def data(self):
        return BaseTypeService.BASE_TYPE_MAPPING

    def find_base_type(self, subject):
        parents = []
        self.fetch_parents(subject, parents)
        return self.base_type_mapping(subject, parents)

    def parse_base_type(self, subject):
        base_type = self.find_base_type(subject)
        if base_type is not None:
            return base_type

        phone = subject.get('phone')
        # user_type = subject.get('userType')
        auth_status = subject.get('authorStatus')
        fund_ratio = subject.get('fundratio')
        relation = subject.get('relation')
        return self.get_normal_base_type(fund_ratio, auth_status, phone, relation, self.main_type)

    def fetch_parents(self, subject, parents):
        parent_id = subject.get("parentId")
        if parent_id is None or parent_id == 0:
            return

        for sub in self.query_data:
            if sub["id"] == parent_id:
                parents.append(sub)
                self.fetch_parents(sub, parents)

    @staticmethod
    def arrow_dict_to_array(origin_data_struct, user_type='PERSONAL'):
        base_type_relations = []
        start_str = f'U_{user_type}'
        start_sign = False
        for item in origin_data_struct:
            base_type_items = []
            item_sections = item.split(">>>")
            if not start_sign and item_sections[0].strip() == start_str:
                start_sign = True
            if not start_sign:
                continue
            for col_index, item_section in enumerate(item_sections):
                item_info_arr = item_section.split("&")
                base_type_item = {}
                for item_info in item_info_arr:
                    if col_index == 0:
                        base_type_item["baseType"] = item_info.strip()
                    else:
                        key_val = item_info.split(":")
                        base_type_item[key_val[0].strip()] = key_val[1].strip()
                    # print("item_info:", base_type_item)
                base_type_items.append(base_type_item)
            base_type_relations.append(base_type_items)
        # print("base_type_relations=", base_type_relations)
        return base_type_relations

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
        return None

    def get_normal_base_type(self, fund_ratio, auth_status, phone, relation, user_type):
        if relation == 'GUARANTOR':
            if user_type == 'COMPANY':
                return 'G_COMPANY'
            elif auth_status == 'AUTHORIZED':
                return 'G_PERSONAL'
            else:
                return 'G_S_PERSONAL'
        if user_type == 'PERSONAL':
            if auth_status == 'AUTHORIZED' and phone is not None and phone != '':
                return 'U_PERSONAL'
            else:
                return 'U_S_PERSONAL'
        if user_type == 'COMPANY':
            if relation == 'CONTROLLER':
                return 'U_COMPANY'
            elif relation == 'LEGAL':
                return 'U_COMPANY'
            elif relation == 'MAIN':
                return 'U_COMPANY'
            elif relation == 'SHAREHOLDER' and float(fund_ratio) >= 0.50:
                return 'U_COMPANY'
            else:
                return 'U_S_COMPANY'
