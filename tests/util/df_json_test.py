import json

import pandas as pd

from util.defensor_client import DefensorClient
from util.mysql_reader import sql_to_df


def test_json_2_df():
    info = '''[{"id": 1, "appId": "0000000000", "riskDetail": "法院失信名单", "strategyCode": "222,333,4444", "dataService": "\u5931\u4fe1\u540d\u5355", "createTime": "2019-10-16T02:50:06Z", "modifyTime": "2019-10-16T02:50:11Z"}]'''
    df = pd.read_json(info)
    print("\n")
    print(df.query('riskDetail == "法院失信名单"').empty)

    risk_detail = "法院失信名单"
    print(not df.query('riskDetail=="' + risk_detail + '"').empty)


def test_json_2_df1():
    df_client = DefensorClient(None)
    df_client.app_id = "9999999999"
    df_client.grey_list_query_url = "http://192.168.1.15:100/gateway/defensor/api/open/grey-list/hit"

    data = df_client.query_grey_list("孙洋洋", "340321198704272796", "ID_CARD_NO")
    info = json.dumps(data)
    df = pd.read_json(info)
    print(df)


def test_df_sql():
    sql = '''
            select * from risk_subject where name like "%%魏炳%%";
            '''
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    df = sql_to_df(sql)
    print("df:", df)


def test_df1():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [1, 2, 1]})
    print("\n", df, "\n")
    df = df["B"].unique()
    print(df.size)


def test_df2():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [1, 2, 1]})
    print("\n", df, "\n")
    for row in df.itertuples():
        print(row.A)


def test_df3():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [1, 2, pd.np.NaN]})
    print("\n", df, "\n")
    for row in df.itertuples():
        if row.B and pd.notna(row.B):
            print("B=", row.B)


def test_df4():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [1, 2, pd.np.NaN], 'C': [pd.np.NaN, pd.np.NaN, pd.np.NaN]})
    print("\n", df, "\n")
    v = df.sum()
    print(type(v.A), "\n", v, "\n")


def test_df5():
    df = pd.DataFrame({'A': [2, 2, 5], 'B': [1, 2, pd.np.NaN], 'C': [pd.np.NaN, pd.np.NaN, pd.np.NaN]})
    df = df["A"]
    print(4 in list(df))


def test_df6():
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=("A", "B", "C"))
    print(df)
    for _, row in df.iterrows():
        print(type(row.A))
        print(_)


def test_df7():
    df = pd.DataFrame([["1", "2", "3"], ["4", "5", "6"], ["7", "8", pd.np.NaN]], columns=("A", "B", "C"))
    print("\n", df)

    v = ["1", "4"]
    df = df.query('A in ' + str(v))
    print("\n", df)


def test_df8():
    df = pd.DataFrame([[1, 2, 3], [41, 5, 6], [9, 8, ]], columns=("A", "B", "C"))
    df = df.sort_values("A")
    print(df)


def test_df9():
    df = pd.DataFrame([
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [3, 4, 5]
    ], columns=list("ABC"))
    df = df.query('A >= 2')
    print("df:\n ", df)
    df.drop('A', axis=1, inplace=True)
    print(df)




