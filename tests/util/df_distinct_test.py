from datetime import date
from functools import reduce

import pandas as pd


def test_df_distinct():
    print("test_df_distinct begin.")
    df = pd.DataFrame({"A": [1, 1, 2, 3], "B": [1, 1, 221, 33], "C": ["A", "B", "C", "D"]})
    df.drop_duplicates(subset=["A", "B"], inplace=True)

    print(df)


def test_df_distinct1():
    print("test_df_distinct begin.")
    df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [11, 22, 221, 33]})
    print(df)

    for index, row in df.iterrows():
        if index == 0:
            df.loc[index, "order"] = 100
    print(df)


def test_df_fun():
    df = pd.DataFrame({"A": [1, 2, 2, 3], "B": [11, 22, 221, 33]})
    print(df)
    print(df.info)
    print(df.describe())
    print("value_counts", df["A"].value_counts())
    print(df.apply(pd.Series.value_counts))

    print("-----------")

    print(df.groupby(by="A", axis=0).all)


def test_df_fun1():
    df = pd.DataFrame({"A": [1, 2, 4, 3], "B": [11, 22, 221, 33]})
    # df = df.set_index(["A"])
    print(df)

    s = df.iloc[0]
    print("s-----", s)

    for i, r in df.iterrows():
        print("i====", i, "r====", r)
        print("$$$$$", r.loc["A"])


def test_df_contains():
    df = pd.DataFrame([{"name": "tqt", "age": 21}, {"name": "lxb", "age": 30}])
    print("df\n", df[df["name"].str.contains("tqt|lxb")])


def test_df_query():
    df = pd.DataFrame([{"name": "tqt", "age": 21}, {"name": "lxb", "age": 30}])

    arr = ["tqt", "lxb", "ccc",]
    info = reduce(lambda x, y: '"' + x + '"' + "," + '"' + y + '"', arr)
    info = "[" + info + "]"
    print("info:", info)
    print("arr=:" + str(arr))
    print("query df:\n", df.query('name in ' + str(arr)))
