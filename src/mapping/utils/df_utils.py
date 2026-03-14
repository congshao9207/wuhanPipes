import pandas as pd


def df_value(series):
    # 若空 返回空串  否则返回series第一条记录
    if series.empty:
        return ""
    else:
        return series.iloc[0]


def df_zero(series):
    if series.empty:
        return 0
    else:
        val = series.iloc[0]
        return val if not pd.isna(val) else 0


def nan_to_zero(val):
    if pd.isna(val):
        return 0
    else:
        return val
