import pandas as pd

from util.mysql_reader import sql_to_df


def df_compare(variables, old_df, new_df, variable_name):
    if old_df.empty and new_df.empty:
        return
    elif new_df.empty:
        variables[variable_name] = 0
    elif old_df.empty:
        variables[variable_name] = 1
    else:
        diff_set = set(new_df.iloc[:, 0]).difference(set(old_df.iloc[:, 0]))
        variables[variable_name] = 0 if len(diff_set) == 0 else 1


def _find_value(df, val):
    for index, row, in df.iterrows():
        if val == row[0]:
            return True
    return False


def sql_list_to_df(transformer, sql_arr, param):
    params = {"user_name": transformer.user_name,
              "id_card_no": transformer.id_card_no,
              "pre_biz_date": transformer.pre_biz_date}
    for key in param:
        params[key] = param[key]
    df = pd.DataFrame()
    for sql in sql_arr:
        v_df = sql_to_df(sql=sql,
                         params=params)
        if df.empty and not v_df.empty:
            df = v_df
        elif not v_df.empty:
            df.append(v_df)
    return df


def to_df(transformer, single_sql, param):
    params = {"user_name": transformer.user_name,
              "id_card_no": transformer.id_card_no,
              "pre_biz_date": transformer.pre_biz_date}
    for key in param:
        params[key] = param[key]
    return sql_to_df(sql=single_sql,
                     params=params)


def var_compare(transformer, new_sql, old_sql, variable_name, extra_param={}):
    params = {"user_name": transformer.user_name,
              "id_card_no": transformer.id_card_no,
              "pre_biz_date": transformer.pre_biz_date}

    for k in extra_param:
        params[k] = extra_param[k]
    old_df = sql_to_df(sql=old_sql,
                       params=params)
    new_df = sql_to_df(sql=new_sql,
                       params=params)
    df_compare(transformer.variables, old_df, new_df, variable_name)
