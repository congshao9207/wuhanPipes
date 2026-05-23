import threading

import numpy

from jsonpath import jsonpath

from product.p_config import product_code_process_dict


def _build_request(req_no, product_code, variables=None):
    """
    根据http请求构建出决策需要的请求, 需要去查数据库获取相关的数据
    :return:
    """
    # 替換None值，因爲決策不認
    for key, value in variables.items():
        if value is None:
            variables[key] = ''
        if type(value) is numpy.float64:
            variables[key] = round(value, 2)
        if str(value) == 'nan':
            variables[key] = 0

    strategy_request = {
        "StrategyOneRequest": {
            "Header": {
                "InquiryCode": req_no,
                "ProcessCode": _get_process_code(product_code)
            },
            "Body": {
                "Application": {
                    "Variables": variables
                }
            }
        }
    }
    return strategy_request


def _get_process_code(product_code):
    """
    根据产品编码得到对应的process code： 决策引擎的流程编码
    :param product_code:
    :return:
    """
    if product_code in product_code_process_dict.keys():
        return product_code_process_dict.get(product_code)
    raise Exception("产品编码：{} 不能找到对应的决策流程".format(product_code))


def _get_biz_types(input_json):
    res = jsonpath(input_json, '$..out_strategyBranch')
    categories = jsonpath(input_json, '$..Categories')[0]
    if isinstance(res, list) and len(res) > 0:
        biz_types = res[0].split(',')
    else:
        biz_types = []
    return biz_types, categories


def _get_resp_field_value(resp_json, json_path):
    res = jsonpath(resp_json, json_path)
    if isinstance(res, list) and len(res) > 0:
        return res[0]
    return "N/A"


def _get_thread_id():
    return threading.currentThread().ident


def _append_rules(biz_types):
    rules = [
        {
            "code": "f0003",
            "data": [
                {
                    "relatedType": "SHAREHOLDER",
                    "rule": {
                        "entStatus": "OPENING",
                        "ratioOfInvestments": "0.2"
                    }
                },
                {
                    "relatedType": "LEGAL",
                    "rule": {
                        "entStatus": "OPENING"
                    }
                }
            ]
        }
    ]
    result = []
    for r in rules:
        if r['code'] in biz_types:
            result.append(r)
    return result


def score_to_int(strategy_resp):
    resp_variables = jsonpath(strategy_resp, '$..Application.Variables')
    if resp_variables is not None:
        variables_ = resp_variables[0]
        for key, value in variables_.items():
            if key.startswith('score') & ~key.endswith('level'):
                variables_[key] = int(round(value))


def _relation_risk_subject(strategy_resp, out_decision_code):
    branch_code = jsonpath(strategy_resp, '$.StrategyOneResponse.Body.Application.Categories..Variables')
    if type(branch_code) == bool:
        return

    for c in branch_code:
        code_key = c.get('out_decisionBranchCode')
        if code_key is not None and code_key in out_decision_code.keys():
            c['queryData'] = out_decision_code[code_key]
