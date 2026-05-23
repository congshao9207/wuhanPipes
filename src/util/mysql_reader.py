# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from config import GEARS_DB

DB_URI = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB

# 扩大连接池，避免并发报告生成时连接耗尽
# __DB_ENGINE = create_engine(DB_URI, pool_size=10, max_overflow=10)
__DB_ENGINE = create_engine(DB_URI, pool_size=20, max_overflow=20)


def sql_to_df(sql, index_col=None, coerce_float=True, params=None,
              parse_dates=None, columns=None, chunksize=None):
    # 增加连接的显式关闭，防止连接过多
    with __DB_ENGINE.connect() as conn:
        df = pd.read_sql(sql, con=conn, index_col=index_col, coerce_float=coerce_float, params=params,
                         parse_dates=parse_dates, columns=columns, chunksize=chunksize)
    return df


def sql_insert(sql, index_col=None, coerce_float=True, params=None,
               parse_dates=None, columns=None, chunksize=None):
    return __DB_ENGINE.execute(sql)
