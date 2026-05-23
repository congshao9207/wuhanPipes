# -*- coding: utf-8 -*-
import re

import pandas as pd
from sqlalchemy import create_engine
import logging
from config import GEARS_DB

#DB_URI = 'dm+dmPython://%(user)s:%(pw)s@%(host)s%(db_extra_param)s' % GEARS_DB
DB_URI = 'dm+dmPython://%(user)s:%(pw)s@%(host)s:%(port)s?schema=%(db)s' % GEARS_DB
# DB_URI = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB

__DB_ENGINE = create_engine(DB_URI, pool_size=10, max_overflow=10)


def quote_aliases(sql):
    pattern = re.compile(r'\sAS\s+(\w+)', re.IGNORECASE)

    def replacer(match):
        return f' AS "{match.group(1)}"'

    # 执行替换
    return pattern.sub(replacer, sql)

def sql_to_df(sql, index_col=None, coerce_float=True, params=None,
              parse_dates=None, columns=None, chunksize=None,db='gears'):
    sql_dm = re.sub('"', "'", sql)
    params_dm = params
    if params_dm and type(params_dm) == dict:
        sql_dm = convert_to_sql(sql_dm, params_dm)

    db_engine = __DB_ENGINE
    sql_dm = quote_aliases(sql_dm)
    # print("sql_dm:", sql_dm)
    df = pd.read_sql(sql_dm, con=db_engine, index_col=index_col, coerce_float=coerce_float,
                     parse_dates=parse_dates, columns=columns, chunksize=chunksize)
    df.columns = df.columns.str.lower()
    return df


def sql_insert(sql, index_col=None, coerce_float=True, params=None,
               parse_dates=None, columns=None, chunksize=None):
    # return __DB_ENGINE.execute(sql)
    with __DB_ENGINE.connect() as conn:
        trans = conn.begin()  # 显式开启事务
        try:
            result = conn.execute(sql)
            trans.commit()  # 提交
            return result
        except Exception as e:
            print(e)
            trans.rollback()  # 出错必须回滚！
            raise
    return __DB_ENGINE.execute(sql)


def convert_to_sql(sql, params):
    sql_dm = re.sub('"', "'", sql)
    params_dm = params
    if params_dm and type(params_dm) == dict:
        scb = SqlMatchCallBack(params_dm)
        sql_dm = re.sub("\\%\\(([\\w]+)\\)s", scb.call_back, sql_dm)

    return sql_dm


def _invalid_param(info):
    a = map(lambda x: x[1], info.items())
    for i in list(a):
        if i is None:
            return True
    return False


class SqlMatchCallBack(object):
    def __init__(self, params):
        self.params = params
        self.index = 0

    def call_back(self, match):
        param_name = match.group(1)
        param_value = self.params.get(param_name)
        if not param_value:
            return 'null'

        if type(param_value) == int:
            return str(param_value)
        elif type(param_value) == str:
            return "'" + param_value + "'"
        elif type(param_value) == list:
            p = ",".join([str(x) if type(x) == int else "'" + str(x) + "'" for x in param_value])
            return '(' + p + ")"
        else:
            return str(param_value)
