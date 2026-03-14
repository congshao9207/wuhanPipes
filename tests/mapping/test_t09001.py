import re

import pandas as pd
import json
from mapping.t09001 import T09001
from mapping.tranformer import extract_money
from product.p003 import P003
from util.common_util import exception
from jsonpath import jsonpath


def test_ps_loan_other():
    ps = T09001()
    ps.run(user_name='', id_card_no='410305197903240547', phone='')
    print(ps.variables)
    # assert ps.variables['oth_loan_hit_org_cnt'] == 1
    # assert ps.variables['oth_loan_hit_bank_cnt'] == 2
    # assert ps.variables['oth_loan_hit_finance_cnt'] == 5
    # assert ps.variables['oth_loan_hit_p2p_cnt'] == 3
    # assert ps.variables['oth_loan_query_mac_cnt_6m'] == 6
    # assert ps.variables['oth_loan_query_mac_cnt_3m'] == 5
    # assert ps.variables['oth_loan_apro_cnt_6m'] == 2
    # assert ps.variables['oth_loan_hit_org_cnt_3m'] == 1


def test_get_oney_from_string():
    value = "一、撤销湖北省武汉市中级人民法院(2015)鄂武汉中行终字第00569号行政判决；二、维持武汉市黄陂区人民法院(2015)鄂黄陂行初字第00030号行政判决。一、二审案件受理费各50元，均由武汉市黄陂区农业机械有限责任公司负担。本判决为终审判决。"
    value = re.sub(r"(第?[0-9]\d*号)", "", value)
    value = re.sub(r"\([0-9]{4}\)", "", value)
    value = re.sub(r"〔[0-9]{4}〕", "", value)
    value = re.sub(r"(http://)((\w)|(\W)|(\.))*", "", value)
    moneyArray = re.findall(r"\d+\.?\d*", value)
    moneyMax = 0
    for money in moneyArray:
        if ('万元') in value:
            moneyRe = float(money) * 10000
        else:
            moneyRe = float(money)
        if moneyRe > moneyMax:
            moneyMax = moneyRe
    print("%.2f" % moneyMax)


def test_match_string():
    money = 0
    money_str = "0.00"
    value = "上海市宝山区卫生和计划生育委员会作出的宝第xxxxxxxxxx号行政处罚决定合法，准予强制执行。被执行人上海越飞酒店用品有限公司必须在收到本裁定书之日起五日内缴纳罚款人民币2,000元，逾期本院将依法强制执行。本案申请执行费人民币50元，由被执行人上海越飞酒店用品有限公司负担。本裁定书送达后立即生效。"
    # value = "[金额:176.45]"
    # value = "荆州市德泰工贸有限公司（以下简称“公司”）采取规避监管的方式排放水污染物一案，经荆州市环境监察支队现场调查，现已审查终结。 一、环境违法事实和证据 我局依法查明，你公司采取规避监管的方式排放水污染物，利用软管将含泥废水排放至附近池塘。 以上事实有荆州市环境监察支队2015年9月2日制作的《荆州市环保局调查询问笔录》为证。 你公司上述行为违反了《中华人民共和国水污染防治法》第二十二条第二款的规定。 2015年9月18日，我局依照法律程序告知你公司违法事实、处罚依据和拟作出的处罚决定，并告知你公司有权进行陈述、申辩。你公司在规定期限内放弃了陈述、申辩的权利。 以上事实，有我局《行政处罚事先告知书》（荆环法〔2015〕174号）及送达回执、2015年9月29日荆州市环境监察支队制作的《荆州市环境保护局调查询问笔录》为证。 二、行政处罚的依据、种类 《中华人民共和国水污染防治法》第七十五条第二款“除前款规定外，违反法律、行政法规和国务院环境保护主管部门的规定设置排污口或者私设暗管的，由县级以上地方人民政府环境保护主管部门责令限期拆除，处二万元以上十万元以下的罚款；逾期不拆除的，强制拆除，所需费用由违法者承担，处十万元以上五十万元以下的罚款；私设暗管或者有其他严重情节的，县级以上地方人民政府环境保护主管部门可以提请县级以上地方人民政府责令停产整顿”。 依据上述规定，我局决定对你公司作出罚款2万元的行政处罚。 三、行政处罚决定的履行方式和期限 根据《中华人民共和国行政处罚法》和《罚款决定与罚款收缴分离实施办法》的规定，你公司应于收到本处罚决定书之日起15日内，持荆州市环境监察支队出具的《湖北省非税收入一般缴款书》将罚款缴至荆州市非税收入管理局。 你公司缴纳罚款后，应将一般缴款书第一联报送荆州市环境监察支队备案。逾期不缴纳罚款的，依据《行政处罚法》第五十一条第（一）项规定，每日按罚款数额的3%加处罚款。 我局委托荆州市环境监察支队对你公司履行处罚决定的情况实施监督检查。 四、申请复议或者提起诉讼的途径和期限 你公司如不服本处罚决定，可在接到处罚决定书之日起六十日内向湖北省环境保护厅或荆州市人民政府申请复议，也可在接到处罚决定书之日起六个月内提起行政诉讼。逾期不申请行政复议，也未向人民法院起诉，又不履行本处罚决定的，我局将依法申请人民法院强制执行。"
    value = re.sub(r'\,', "", value)
    print(value)
    pattern1 = re.compile(r'(?<=罚款金额\(单位：万元\)\:)\d+\.?\d*')
    pattern2 = re.compile(r'(?<=金额\:)\d+\.?\d*')
    pattern3 = re.compile(r'(?<=罚款)\d+\.?\d*')
    pattern4 = re.compile(r'(?<=罚款人民币)\d+\.?\d*')
    if pattern1.search(value) != None:
        money_str = pattern1.search(value).group(0)
        print("pattern1")
    elif pattern2.search(value) != None:
        money_str = pattern2.search(value).group(0)
        print("pattern2")
    elif pattern3.search(value) != None:
        money_str = pattern3.search(value).group(0)
        print("pattern3")
    elif pattern4.search(value) != None:
        money_str = pattern4.search(value).group(0)
        print("pattern4")
    print(money_str)
    money = float(money_str)
    if ("万元" in value):
        money = money * 10000
    money = "%.2f" % money
    print(money)


def test_extract_money_court_excute_public():
    """
    执行公开信息模块从执行内容中提取金额
    :param value:
    :return:
    """
    value = "执行标的金额（元）:22000"
    value = re.sub(r'\,', "", value)
    money_array = re.findall(r"\d+\.?\d*", value)
    money_max = 0
    if money_array is not None and len(money_array) > 0:
        for money in money_array:
            if ('万元') in value:
                money_re = float(money) * 10000
            else:
                money_re = float(money)
            if money_re > money_max:
                money_max = money_re
        money_max = float("%.2f" % money_max)
    print(money_max)


def test_dropna():
    a = ['AA', 'CC', 'BB']
    b = [11, 22, 11]
    c = [11]
    df = pd.DataFrame({'execution_result': a, 'specific_date': b})
    df = df.drop_duplicates(subset=['specific_date'])
    print(df)
    # df2 = pd.DataFrame({'specific_date': c})
    # df_merge = pd.merge(df2,df,on=['specific_date'],how='left')
    # print(df_merge)
    # df1 = df.dropna(subset=['execution_result'], how='any')
    # print(df1)
    # array = []
    # for index, row in df.iterrows():
    #     value={
    #         'name':row['execution_result'],
    #         'id_card_no':row['specific_date']
    #     }
    #     array.append(value)
    # print(array)

def test_round():
    print(round(0.007,2))



@exception('purpose=企业工商&author=刘金昊')
def fun1():
    a = ['AA', 'CC', 'BB']
    b = [11, 22, 11]
    c = [11]
    df = pd.DataFrame({'execution_result': a, 'specific_date': b})
    row_str = '{frequency_detail_list=[{"detail":"7天内身份证申请次数：6"}], type=frequency_detail}'
    row_dict = json.loads(row_str)
    return df

def test_robust():
    a = fun1();
    print(a)

def test_number():
    rate = '0.5000'
    is_auth = True
    if is_auth:
        print("true")
    else:
        print("false")
    if float(rate) >= 0.50:
        print("true")
    else:
        print("false")

def test_json_append():
    resp = {}
    resp['name'] = 'test'
    resp['bizTypes'] = ['001','002','003']
    print(resp)

def test_str_in():
    array = ['U_PERSONAL','G_PERSONAL']
    str1 = 'U_S_PERSONAL'
    if str1 in array:
        print("true")
    else:
        print('false')

def test_json_path():
    resp_json = {
    "StrategyOneResponse": {
        "Header": {
            "InquiryCode": "Q356548494120615936",
            "ProcessCode": "Level1_m",
            "OrganizationCode": "",
            "ProcessVersion": 33,
            "LayoutVersion": 11
        },
        "Body": {
            "Application": {
                "Variables": {
                    "out_strategyBranch": "fffff",
                    "out_isQuery": "N",
                    "score_fraud": 39,
                    "score_debit": 85,
                    "score_credit": 52.5,
                    "score_business": 40.6,
                    "score_black": 100,
                    "score": 100,
                    "SCORE_GE_RAW": 59,
                    "out_result": "A",
                    "level": "高",
                    "level_black": "高",
                    "level_business": "中",
                    "level_credit": "中",
                    "level_debit": "高",
                    "level_fraud": "中",
                    "l_m_critical_score": 30,
                    "m_h_critical_score": 70
                },
                "Categories": [
                    {
                        "Reason": {
                            "Variables": {
                                "out_decisionBranchCode": "",
                                "out_ReasonCode": "RR205"
                            }
                        }
                    }
                ]
            }
        }
    }
}
    res = jsonpath(resp_json, '$..score_aa')
    if isinstance(res, list) and len(res) > 0:
        print(res[0])
    else:
        print(None)


def test_get_json_value():
    json = {
    "name": "施网明",
    "userType": "PERSONAL",
    "fundratio": "0.50",
    "ralation": "借款主体",
    "per_face_relent_indusCode1": None,
    "com_bus_face_outwardindusCode1": "",
    "com_bus_industrycode": "",
    "score_black": 0,
    "score_credit": 16,
    "score_debit": 31,
    "score_fraud": 30,
    "score_business": 10,
    "score": 56
}
    print(jsonpath(json, '$..per_face_relent_indusCode1'))


def test_json_df():
    array = [{
        'name': '施网明',
        'userType': 'PERSONAL',
        'fundratio': 0.00,
        'ralation': 'MAIN',
        'per_face_relent_indusCode1': "N",
        'com_bus_face_outwardindusCode1': "",
        'com_bus_industrycode': "L",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order':None
    },{
        'name': '001',
        'userType': 'PERSONAL',
        'fundratio':0.00,
        'ralation': 'SPOUSE',
        'per_face_relent_indusCode1': "",
        'com_bus_face_outwardindusCode1': "p",
        'com_bus_industrycode': "N",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order': None
    },{
        'name': '002',
        'userType': 'PERSONAL',
        'fundratio':0.65,
        'ralation': 'PARTNER',
        'per_face_relent_indusCode1': "",
        'com_bus_face_outwardindusCode1': "p",
        'com_bus_industrycode': "N",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order': None
    },{
        'name': '003',
        'userType': 'COMPANY',
        'fundratio':0.48,
        'ralation': 'SHAREHOLDER',
        'per_face_relent_indusCode1': "",
        'com_bus_face_outwardindusCode1': "p",
        'com_bus_industrycode': "N",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order': None
    },{
        'name': '004',
        'userType': 'COMPANY',
        'fundratio':0.58,
        'ralation': 'SHAREHOLDER',
        'per_face_relent_indusCode1': "",
        'com_bus_face_outwardindusCode1': "p",
        'com_bus_industrycode': "N",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order': None
    },{
        'name': '006',
        'userType': 'COMPANY',
        'fundratio':0.68,
        'ralation': 'SHAREHOLDER',
        'per_face_relent_indusCode1': "",
        'com_bus_face_outwardindusCode1': "p",
        'com_bus_industrycode': "N",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order': None
    },{
        'name': '005',
        'userType': 'COMPANY',
        'fundratio':0.58,
        'ralation': 'OTHER',
        'per_face_relent_indusCode1': "",
        'com_bus_face_outwardindusCode1': "p",
        'com_bus_industrycode': "N",
        'score_black': 0,
        'score_credit': 16,
        'score_debit': 31,
        'score_fraud': 30,
        'score_business': 10,
        'score': 56,
        'order': None
    }]
    print(type(array))
    df = pd.DataFrame(array)
    if df.query('ralation == "MAIN" and userType == "PERSONAL"').shape[0] > 0:
        sort_union_person_df(df)
    elif df.query('ralation == "MAIN" and userType == "COMPANY"').shape[0] > 0:
        sort_union_company_df(df)
    else:
        pass
    df_person = df.query('userType=="PERSONAL"').sort_values(by=["fundratio"],ascending=False).sort_values(by=["order"],ascending=True)[0:10]
    df_compay = df.query('userType=="COMPANY" and order != 999').sort_values(by=["fundratio"],ascending=False).sort_values(by=["order"],ascending=True)[0:10]

    person_index = 0
    company_index = 0
    variables = {}
    phycode_array = []
    for index, row in df_person.iterrows():
        person_index = person_index + 1
        variables['score_black_a'+ str(person_index)] = row['score_black']
        variables['score_credit_a' + str(person_index)] = row['score_credit']
        variables['score_debit_a' + str(person_index)] = row['score_debit']
        variables['score_fraud_a' + str(person_index)] = row['score_fraud']
        variables['score_a' + str(person_index)] = row['score']
        create_phycode_array(
            [row['per_face_relent_indusCode1'],row['com_bus_face_outwardindusCode1'],row['com_bus_industrycode']],phycode_array)

    for index, row in df_compay.iterrows():
        company_index = company_index + 1
        variables['score_black_c'+str(company_index)] = row['score_black']
        variables['score_business_c' + str(company_index)] = row['score_business']
        variables['score_c' + str(company_index)] = row['score']
        create_phycode_array(
            [row['per_face_relent_indusCode1'], row['com_bus_face_outwardindusCode1'], row['com_bus_industrycode']],
            phycode_array)



    print(len(df_person))
    print(len(df_compay))
    print(variables)
    print(phycode_array)

def create_phycode_array(values,array):
    for value in values:
        if value is not None and value != '' and value not in array:
            array.append(value)


def sort_union_company_df(df):
    for index, row in df.iterrows():
        if row['ralation'] == 'CONTROLLER' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 0
        elif row['ralation'] == 'CONTROLLER_SPOUSE' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 1
        elif row['ralation'] == 'SHAREHOLDER' and row['userType'] == 'PERSONAL' and row['fundratio'] >= 0.50:
            df.loc[index, 'order'] = 2
        elif row['ralation'] == 'OTHER' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 3
        elif row['ralation'] == 'CONTROLLER' and row['userType'] == 'COMPANY':
            df.loc[index, 'order'] = 0
        elif row['ralation'] == 'SHAREHOLDER' and row['userType'] == 'COMPANY':
            df.loc[index, 'order'] = 1
        else:
            df.loc[index, 'order'] = 999


def sort_union_person_df(df):
    for index, row in df.iterrows():
        if row['ralation'] == 'MAIN' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 0
        elif row['ralation'] == 'SPOUSE' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 1
        elif row['ralation'] == 'CHILDREN' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 2
        elif row['ralation'] == 'PARENT' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 3
        elif row['ralation'] == 'PARTNER' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 4
        elif row['ralation'] == 'OTHER' and row['userType'] == 'PERSONAL':
            df.loc[index, 'order'] = 5
        elif row['ralation'] == 'CONTROLLER' and row['userType'] == 'COMPANY':
            df.loc[index, 'order'] = 0
        elif row['ralation'] == 'SHAREHOLDER' and row['userType'] == 'COMPANY' and row['fundratio'] >= 0.50:
            df.loc[index, 'order'] = 1
        elif row['ralation'] == 'LEGAL' and row['userType'] == 'COMPANY':
            df.loc[index, 'order'] = 2
        elif row['ralation'] == 'SHAREHOLDER' and row['userType'] == 'COMPANY' and row['fundratio'] < 0.50:
            df.loc[index, 'order'] = 4
        else:
            df.loc[index, 'order'] = 999


def test_read_file():
    f = open('../resource/shake_hand_p003.txt','r', encoding='UTF-8')
    json = f.read()
    f.close()
    print(json)

def test_number_format():
    fundretaio = 1.0000
    print(float(fundretaio))

def test_extract_money():
    value = '[罚款金额:0.02]'
    print(extract_money(value))


def test_read_excel():
    df = pd.read_excel('c:/users/杨也晰/desktop/t1.xlsx')
    df_person = df.query('status == "END" and product_name == "个人一级风险报告"')
    # for index,row in df_person.iterrows():
    #     resp = json.loads(row['resp'])
    #     Categories = resp['strategyResult']['StrategyOneResponse']['Body']['Application']['Categories']
    #     print(resp)
    #     print(Categories)
    for i in range(len(df_person)):
        if i==4:
            s = pd.read_json(df_person.ix[i, 'resp'], typ='series')
            res = s['strategyResult']['StrategyOneResponse']['Body']['Application']['Categories']
            print("1111", type(s))
            print("222", type(res))
        if i==5:
            s = pd.read_json(df_person.ix[i, 'resp'], typ='series')
            res = s['strategyResult']['StrategyOneResponse']['Body']['Application']['Categories']
            print("1111", type(s))
            print("222", type(res))


def test_ps_loan_apro_cnt_12m():
    ps = T09001()
    ps.run(user_name='', id_card_no='310115198208146818', phone='')
    print(ps.variables)
