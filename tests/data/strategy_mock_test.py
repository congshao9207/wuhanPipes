# -*- coding: utf-8 -*-
# @Time : 2019/12/18 2:54 PM
# @Author : lixiaobo
# @Site : 
# @File : strategy_mock_test.py
# @Software: PyCharm
import json

import pytest
import requests
from file_utils.files import file_content
from jsonpath import jsonpath

from product.p_utils import _build_request
from strategy_config import obtain_strategy_url


@pytest.fixture(name="mock_data")
def mock_data():
    v = file_content("../resource/", "mock_strategy_apply_amo.json")
    yield v


data = [("U_PERSON", 1000, ""), ("U_COMPANY", 10001, "KT012"), ("U_PERSONAL", 10001, "JT012")]


@pytest.mark.parametrize(argnames=["base_type", "val", "code"], argvalues=data)
def test_strategy_request(mock_data, base_type, val, code):
    origin_json = json.loads(mock_data)
    req_no = jsonpath(origin_json, "$.reqNo")[0]
    product_code = jsonpath(origin_json, "$.product_code")[0]

    subjects = jsonpath(origin_json, "$.subject")[0]
    for subject in subjects:
        variables = jsonpath(subject, "$.queryData.strategyInputVariables")[0]
        variables["base_type"] = base_type
        variables["court_pub_info_max"] = val
        variables["court_ent_pub_info_max"] = val
        variables["base_apply_amo"] = 20000

        origin_input = {}
        origin_input.update(variables)

        strategy_request = _build_request(req_no, product_code, origin_input)
        strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
        codes = jsonpath(strategy_response.json(), "$..out_decisionBranchCode")
        print("base_type:", base_type, " val:", val, "codes:", codes)
        if code and code != '' and codes and len(codes) > 0:
            print("begin assert..")
            assert code in codes
