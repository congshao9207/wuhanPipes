# -*- coding: utf-8 -*-
import re

from jsonpath import jsonpath
import pandas as pd
from file_utils.files import file_content

from mapping.t05001 import T05001
from mapping.t05002 import T05002
from mapping.t06001 import T06001
import math
import time
import datetime
import json
import numpy as np
from pyecharts.charts import Line
import pyecharts.options as opts

from util.id_card_info import GetInformation


def test_ps_name_id():
    x = ['0.2', '0.4', '0.6', '0.8', '1.0']
    # y = [47, 3, 3, 4, 19]
    y_js_01 = [1,2,3,4,5]
    line = (
        Line()
            .add_xaxis(xaxis_data=x)
            .add_yaxis(series_name="业务1部", y_axis=y_js_01)
            .set_global_opts(title_opts=opts.TitleOpts(title="历史违约风险指数过高-占比"),
                             yaxis_opts=opts.AxisOpts(is_show=True,axislabel_opts=opts.LabelOpts(formatter="{value}%",is_show=True))
                             )
    )


def test_phone_check():
    ps = T05001()
    ps.run(user_name='伊春光', id_card_no='412827199102225781', phone='15317063103')
    print(ps.variables)
    ps.run(user_name='仝太宝', id_card_no='342225198604171519', phone='13671770773')
    print(ps.variables)


def test_ps_crime_type():
    ps = T06001()

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': [''],
        'case_period': ['[0,3)']
    }))
    print(ps.variables)

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['DRUG_RELATED, ILLEGAL_F']
    }))
    assert ps.variables['ps_involve_drug'] == 1
    assert ps.variables['ps_drug'] == 0

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['DRUG_RELATED, DRUG']
    }))
    assert ps.variables['ps_involve_drug'] == 1
    assert ps.variables['ps_drug'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[0, 3)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[3, 6)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[6, 12)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[12, 24)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 2

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[24, 50)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 3

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[0,0)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 4


def test_df():
    data = {'a': ['2020-09', '2020-05', '2020-06', '2020-07'], 'c': ['10', '2', '3', '4'], 'e': [2, 2, 3, 4]}
    data1 = {'a': ['2020-09', '2020-05', '2020-06', '2020-07'], 'c': ['10', '2', '3', '4'], 'e': [2, 2, 3, 4]}
    df = pd.DataFrame(data)
    df1 = pd.DataFrame(data1)
    # data1={"d":[1,3],"h":['aaa','bbb']}
    # df1=pd.DataFrame(data1)
    # df2=pd.merge(df,df1,left_on='a',right_on='d')
    # df['d']=df.apply(get_credit_min_repay,axis=1,args=('a','c'))
    diff_set = set(df1.iloc[:, 0]).difference(set(df.iloc[:, 0]))
    value = 0 if len(diff_set) == 0 else 1
    print(value)


def test_datetime():
    data = {'借据编号': ['1', '2'], '借据起期': [20190521, 20190521]}
    df = pd.DataFrame(data=data)
    df = df.astype(object)
    df['借据起期'] = pd.to_datetime(df['借据起期'])
    # df['借据起期']=df['借据起期'].apply(lambda x:datetime.datetime.strptime(str(x), '%Y%m%d'))
    print(df)


def get_credit_min_repay(df, repay_amount, amount_replay_amount):
    return ['否', '是'][df[repay_amount] * 2 > df[amount_replay_amount]]


def test_get_age_by_id_card_no():
    information = GetInformation("430181197102010345")
    print(information.get_age())


def test_get_data():
    msg = file_content("./resource", "unin_level1_001.json")
    data = json.loads(msg)
    query_data_list = jsonpath(data, '$..queryData[*]')
    resp = []
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        print(user_type)
        strategy = query_data.get("strategy")
        print(strategy)
        if user_type == 'COMPANY' and strategy == '01':
            resp_dict = {"name": name, "idno": idno}
            resp.append(resp_dict)
    print(resp)


def test_002():
    data = {'借据编号': ['yyy777', '233333'], '借据起期': [20190521, 20190521]}
    df = pd.DataFrame(data=data)
    df['priority'] = 1
    print(df)


def test_003():
    data = "(0.24,0.333]"
    print(type(re.findall(r"\((.+?)\,", data)[0]))
    print(float('-999') == -999)


def test_004():
    print(np.round(float('0.43087915501708607'), 4))


def test_score_to_int():
    strategy_resp = json.loads(
        '{"StrategyOneResponse": {"Header": {"InquiryCode": "Q523137740502630400", "ProcessCode": "CREDIT_REP", "OrganizationCode": "", "ProcessVersion": 12, "LayoutVersion": 8}, "Body": {"Application": {"Variables": {"out_result": "T", "out_strategyBranch": "fffff", "y_pred": 0.085, "score": 510, "risk_conclusion": "高风险，需特批", "score_level": "C-"}, "Categories": [{"Reason": {"Variables": {"out_decisionBranchCode": "ZX029"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX030"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX031"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX035"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX041"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX001"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX007"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX013"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX019"}}}, {"Reason": {"Variables": {"out_decisionBranchCode": "ZX014"}}}]}}}}')
    resp_variables = jsonpath(strategy_resp, '$..Application.Variables')
    if resp_variables is not None:
        variables_ = resp_variables[0]
        for key, value in variables_.items():
            if key.startswith('score') & ~key.endswith('level'):
                variables_[key] = int(round(value))


def test_compare_yf_result():
    df1 = pd.read_excel(r'C:\Users\杨也晰\Desktop\金融产品部\一级改版\数据分析\二要素测试客户名单0824.xlsx')
    df2 = pd.read_excel(r'C:\Users\杨也晰\Desktop\金融产品部\一级改版\数据分析\EMW001 身份证实名认证.xlsx')
    df_write = None
    for index, row in df1.iterrows():
        name = row['姓名']
        id_card_no = row['身份证号']
        result = row['备注']
        for index, row in df2.iterrows():
            name2 = row['姓名']
            id_card_no2 = row['身份证号']
            result2 = row['结果']
            if name == name2 and id_card_no == id_card_no2:
                df_temp = pd.DataFrame({'姓名': name,
                                        '身份证号': id_card_no,
                                        '优分结果': result,
                                        '新数据源结果': result2,
                                        '是否一致': True if result == result2 else False}, index=[0])
                if df_write is None:
                    df_write = df_temp
                else:
                    df_write = df_write.append(df_temp, ignore_index=True)
                break
    writer = pd.ExcelWriter(r'C:\Users\杨也晰\Desktop\金融产品部\一级改版\数据分析\数据商对比结果.xlsx')
    df_write.to_excel(writer, index=False)
    writer.save()


def connect_json(self, json):
    string = ''
    for text in json:
        string += text
    return string[:-1]

def test_gene_josn_str():
    risk_tips = "(1)aaaaa;(2)bbbb"
    trans_detail = []
    dict1 = {"guarantor": "余绍波",
             "is_before_interest_repay": 1.0,
             "remark": "加急汇D往来款",
             "trans_amt": -300000.0,
             "trans_time": "2021-06-16 10:56:01"
             }
    dict2 = {"guarantor": "余绍波",
             "is_before_interest_repay": 1.0,
             "remark": "加急汇D往来款",
             "trans_amt": -300000.0,
             "trans_time": "2021-06-16 10:56:01"
             }
    trans_detail.append(dict1)
    trans_detail.append(dict2)


    # for trans in trans_detail:
    #     trans_detail_str += "\""+trans + "\","
    resp = {
        "risk_tips":risk_tips,
        "trans_detail":trans_detail
    }
    print(resp)
