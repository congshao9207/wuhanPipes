import os
from abc import ABCMeta, abstractmethod
from functools import reduce

import numpy as np
import pandas as pd
from pandas import DataFrame

from data.test_mapper import translate_for_strategy
from data.test_mapper_detail import translate_for_report_detail
from util.mysql_reader import sql_insert


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


class Process(object):
    __metaclass__ = ABCMeta

    def __init__(self) -> None:
        super().__init__()
        self.read_path = None
        self.write_path = None
        self.method = None
        self.product_code = None
        self.origin_data = None
        self.table_to_ids = {}

    def run(self, read_file_name=None, method=None):
        write_file_name = read_file_name.split('.')[0] + '_result' + '.' + read_file_name.split('.')[1]
        read_path = os.path.join(os.path.abspath('.'), 'input', read_file_name)
        write_path = os.path.join(os.path.abspath('.'), 'output', write_file_name)
        self.input(read_path, write_path, method)
        result = self.do_process_case()
        self.tear_down()
        return result

    def input(self, read_path, write_path, method):
        self.read_path = read_path
        self.write_path = write_path
        self.method = method

    def read_excel_as_df(self, read_path):
        path = read_path
        df = pd.read_excel(path, dtype={'测试模块': np.str})
        return df

    def write_df_into_excel(self, write_path, df=None):
        path = write_path
        if df is not None and len(df) > 0:
            df.to_excel(path)
        return path

    def run_processor(self, path, method):
        df = pd.read_excel(path)
        # 跑代码的实际结果
        actual_reslut_array = []
        # 与预期结果对比是否通过
        is_pass_array = []
        for index, row in df.iterrows():
            code_array = []
            code = str(row['测试模块'])
            if (len(code)) < 5:
                code = '0' + code
            code_array.append(code)
            field = row['用例标题']
            expect_result = row['预期测试结果'].split('=')[1]
            params = eval(row['key_value_main'])
            user_name = ''
            id_card_no = ''
            phone = ''
            # 运行代码生成的用例字段结果
            case_value = ''
            for key, value in params.items():
                if key in ['user_name', 'unique_name', 'name','ent_name']:
                    user_name = value
                if key in ['id_card_no', 'unique_id_no', 'credit_code']:
                    id_card_no = value
                if key in ['phone']:
                    phone = value
            if 't' in method:
                res = translate_for_strategy(self.product_code, code_array, user_name=user_name, id_card_no=id_card_no, phone=phone, origin_data=self.origin_data)
            elif 'v' in method:
                res = translate_for_report_detail(code_array, user_name=user_name, id_card_no=id_card_no, phone=phone)
            for key in res:
                if field == key:
                    case_value = res[key]
                    actual_reslut_array.append(case_value)
            if is_number(expect_result):
                try:
                    if case_value is not None:
                        if float(case_value) == float(expect_result):
                            is_pass_array.append("true")
                        else:
                            is_pass_array.append("false")
                    else:
                        is_pass_array.append("false")
                except ValueError:
                    if str(case_value) == str(expect_result):
                        is_pass_array.append("true")
                    else:
                        is_pass_array.append("false")
            else:
                compare_value = str(case_value)
                if compare_value.find('00:00:00') >= 0:
                    compare_value = compare_value[0:10]
                if compare_value == str(expect_result):
                    is_pass_array.append("true")
                else:
                    is_pass_array.append("false")
        df['实际测试结果'] = actual_reslut_array
        df['是否通过'] = is_pass_array
        df.to_excel(path)
        return df

    def tear_down(self):
        print("table_ids", self.table_to_ids)
        for tab_name in self.table_to_ids:
            ids = self.table_to_ids[tab_name]
            if len(ids) > 1:
                str_ids = reduce(lambda e1, e2: str(e1) + "," + str(e2), ids)
            else:
                str_ids = str(ids[0])
            sql = "delete from " + tab_name + " where id in(" + str_ids + ");"
            print("tear down sql:", sql)
            sql_insert(sql)

    def record_tab(self, table_name, id):
        ids = self.table_to_ids.get(table_name)
        if not ids:
            self.table_to_ids[table_name] = [id]
        else:
            self.table_to_ids[table_name].append(id)

    @abstractmethod
    def do_process_case(self) -> DataFrame:
        """
        处理测试用例方法

        """
        pass

