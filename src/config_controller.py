# -*- coding: utf-8 -*-
# @Time : 2019/12/9 1:17 PM
# @Author : lixiaobo
# @Site : 
# @File : config_controller.py
# @Software: PyCharm
import json

from flask import Blueprint
from str_utils.str_utils import to_string

from resources.resource_util import get_config_content
from service.base_type_service import BaseTypeService

base_type_api = Blueprint('account_api', __name__)


@base_type_api.route("/base-type", methods=['GET'])
def base_type_mapping_info():
    base_type_service = BaseTypeService(None)
    return json.dumps(base_type_service.BASE_TYPE_MAPPING)


@base_type_api.route("/base-type-mapping", methods=['GET'])
def base_type_mapping_origin_data():
    mapping_data = get_config_content("base_type_mapping.json")
    return mapping_data


@base_type_api.route("/to-string", methods=['GET'])
def pipes_to_string():
    return to_string("Pipes to string.")
