# @Time : 2020/10/15 10:10 AM 
# @Author : lixiaobo
# @File : interception_stats.py.py 
# @Software: PyCharm
import json

import pandas as pd
from jsonpath import jsonpath

from util.mysql_reader import sql_to_df

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)


class InterceptionStats(object):
    def __init__(self):
        self.total_item = 0
        self.processed_item_count = 0

    def extract_final_result(self, data):
        self.processed_item_count = self.processed_item_count + 1
        print("progress:", self.processed_item_count, "%", self.total_item, end="\r")
        try:
            if not data:
                return "NA"

            json_obj = json.loads(data)
            result = jsonpath(json_obj, "$.finalResult")
            if result and len(result) > 0:
                return result[0]
            else:
                return "NO_RESULT"
        except Exception as e:
            print("error----", str(e))
            return "EXCEPTION"

    def stats(self):
        sql = '''
            select * from report_request where product_code="004" and request_status="DONE" order by id desc
        '''
        df = sql_to_df(sql=sql)
        self.total_item = df.shape[0]
        # df["year"] = df["create_time"].apply(lambda e:e.year)
        # df["month"] = df["create_time"].apply(lambda e: e.month)
        df["year-month"] = df["create_time"].apply(lambda e: str(e.year) + "-" + str(e.month))

        df["finalResult"] = df["resp_raw_data"].apply(self.extract_final_result)

        df.to_excel("interception_stats.xlsx")


def test_interception_stats():
    stats = InterceptionStats()
    stats.stats()
