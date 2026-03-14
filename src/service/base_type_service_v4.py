# -*- coding: utf-8 -*-
# @Time : 2019/12/9 11:09 AM
# @Author : lixiaobo
# @Site :
# @File : base_type_service.py.py
# @Software: PyCharm
import json

from resources.resource_util import get_config_content


class BaseTypeServiceV4:

    def __init__(self, query_data):
        self.query_data = query_data
        main_items = list(filter(lambda x: x["relation"] == "MAIN", query_data))
        self.main_type = main_items[0]['userType'] if main_items else 'PERSONAL'
        self.base_type_mapping_v4 = self.arrow_dict_to_array("base_type_mapping_v4.json")
        self.main_id = main_items[0]['id'] if main_items else -1

    def parse_base_type(self, subject):
        base_type = self.find_base_type(subject)
        if base_type is not None:
            return base_type
        # 未匹配到baseType.
        if self.main_type == "PERSONAL":
            return "U_PER_OTHER"
        elif self.main_type == "COMPANY":
            return "U_COM_OTHER"
        else:
            return "U_IGNORE_TYPE_OTHER"

    def find_base_type(self, subject):
        parents = []
        self.fetch_parents(subject, parents)
        return self.base_type_mapping(subject, parents)

    def arrow_dict_to_array(self, mapping_file_path):
        mapping_data = get_config_content(mapping_file_path)
        origin_data_struct = json.loads(mapping_data)
        base_type_relations = []
        start_str = f'U_{self.main_type}'
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

        for type_to_relations in self.base_type_mapping_v4:
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

    def fetch_parents(self, subject, parents):
        parent_id = subject.get("parentId")
        if parent_id is None or parent_id == 0 or parent_id == self.main_id:
            return

        for sub in self.query_data:
            if sub["id"] == parent_id:
                parents.append(sub)
                self.fetch_parents(sub, parents)
