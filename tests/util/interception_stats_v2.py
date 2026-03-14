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


class InterceptionStatsV2(object):
    def __init__(self):
        self.total_item = 0
        self.processed_item_count = 0

    def group_by_data(self, df):
        arr = list(df['business_time'])
        df.loc[0, 'delta'] = 0
        df.loc[0, 'range'] = 1
        k = 1
        for i in range(1, len(df)):
            delta = arr[i] - arr[i - 1]
            df.loc[i, 'delta'] = delta.days * 86400 + delta.seconds

        for i in range(1, len(df)):
            if abs(df.loc[i, 'delta']) <= 2:
                df.loc[i, 'delta'] = df.loc[i - 1, 'delta']
            elif df.loc[i, 'delta'] != df.loc[i - 1, 'delta']:
                k += 1
            df.loc[i, 'range'] = k
        df.fillna('', inplace=True)
        df.drop(['delta'], axis=1, inplace=True)
        return df

    def stats(self):
        sql = '''
            SELECT * from assets_business_decision_detail where business_time > "2019-01-01 00:00:00";
        '''
        df = sql_to_df(sql=sql)
        self.total_item = df.shape[0]
        # df = self.group_by_data(df)
        # df["year"] = df["create_time"].apply(lambda e:e.year)
        # df["month"] = df["create_time"].apply(lambda e: e.month)
        df["year_month"] = df["business_time"].apply(lambda e: str(e.year) + "-" + str(e.month))

        group = df.groupby(by="batch_no")
        result_df = pd.DataFrame(columns=["business_time", "year_month", "result"])
        for index, df in group:
            result_set = []
            index = -1
            item = {}
            for row in df.itertuples():
                index = index + 1
                if index == 0:
                    item["business_time"] = row.business_time
                    item["year_month"] = row.year_month
                if row.result:
                    result_set.append(row.result)

                print("-----------------------", row.result)

            final_result = "A"
            if "D" in result_set:
                final_result = "D"
            elif "M" in result_set:
                final_result = "M"

            item["result"] = final_result
            print("result_df", result_df)
            result_df = result_df.append(item, ignore_index=True)

        result_df.to_excel("interception_stats_v2.xlsx")


def test_interception_stats_v2():
    stats = InterceptionStatsV2()
    stats.stats()
