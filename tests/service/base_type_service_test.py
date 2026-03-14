import json

import pytest
from jsonpath import jsonpath

from service.base_type_service import BaseTypeService
from service.base_type_service_v2 import BaseTypeServiceV2


@pytest.fixture(name="query_data")
def base_type_query_data():
    f = open("../resource/base_type_test_01.json")
    str_json = f.read()
    json_obj = json.loads(str_json)
    query_data = jsonpath(json_obj, "$.queryData")[0]
    yield query_data


@pytest.fixture(name="query_data_02")
def base_type_query_data_02():
    f = open("../resource/base_type_test_02.json")
    str_json = f.read()
    json_obj = json.loads(str_json)
    query_data = jsonpath(json_obj, "$.queryData")[0]
    yield query_data


@pytest.fixture(name="query_data_03")
def base_type_query_data_03():
    f = open("../resource/base_type_test_03.json")
    str_json = f.read()
    json_obj = json.loads(str_json)
    query_data = jsonpath(json_obj, "$.queryData")[0]
    yield query_data


def test_print_subject_base_type(query_data):
    base_type_service = BaseTypeService(query_data)
    for subject in query_data:
        base_type = base_type_service.parse_base_type(subject)
        print("subject:", subject["name"], " base_type:", base_type)


def test_print_subject_base_type_02(query_data_02):
    base_type_service = BaseTypeService(query_data_02)
    for subject in query_data_02:
        base_type = base_type_service.parse_base_type(subject)
        print("subject:", subject["name"], " base_type:", base_type)


def test_print_subject_base_type_03(query_data_03):
    base_type_service = BaseTypeService(query_data_03)
    base_type_service_v2 = BaseTypeServiceV2(query_data_03)

    print("base_type_service:", len(BaseTypeService.BASE_TYPE_MAPPING))
    print("base_type_service_v2:", len(BaseTypeServiceV2.BASE_TYPE_MAPPING_V2))

    for subject in query_data_03:
        base_type = base_type_service.parse_base_type(subject)
        base_type_v2 = base_type_service_v2.parse_base_type(subject)
        print("subject_v1:", subject["name"], " base_type:", base_type)
        print("subject_v2:", subject["name"], " base_type:", base_type_v2)
