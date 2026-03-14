# @Time : 2020/9/27 9:33 AM 
# @Author : lixiaobo
# @File : df_flatter_test.py 
# @Software: PyCharm
import pandas as pd

from util.DataFrameFlatter import DataFrameFlatter
from util.mysql_reader import sql_to_df

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)


def test_df_flatter():
    sql = '''
            select * from info_stats ss left join info_stats_item ssi on ss.id = ssi.stats_id;
          '''

    df = sql_to_df(sql)

    dff = DataFrameFlatter(df, "stats_id", "field_name", "field_value")
    result = dff.flat_df()
    print("-------------------------FINISHED-------------------------")
    result.to_excel("flat_result.xlsx")
