# -*- coding: utf-8 -*-
import importlib

# from app import logger
import pkgutil
import traceback
from inspect import getmembers, isclass

import numpy

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import GroupedTransformer
from mapping.tranformer import Transformer, fix_cannot_to_json
from product.p_config import product_code_view_dict

logger = LoggerUtil().logger(__name__)

# 标识决策引擎交互完成。
STRATEGE_DONE = 'fffff'


def _get_codes_by_product_code(product_code):
    if product_code in product_code_view_dict.keys():
        return product_code_view_dict.get(product_code)
    else:
        return []


def extension_variables(variables):
    """
    构建展示变量
    :param variables:
    :return:
    """
    anti_apply_bank_7d = variables.get('anti_apply_bank_7d', 0)
    net_apply_bank_7d = variables.get('net_apply_bank_7d', 0)
    oth_loan_apply_bank_7d = variables.get('oth_loan_apply_bank_7d', 0)
    apply_bank_7d_arr = [anti_apply_bank_7d, net_apply_bank_7d, oth_loan_apply_bank_7d]
    anti_apply_bank_1m = variables.get('anti_apply_bank_1m', 0)
    net_apply_bank_1m = variables.get('net_apply_bank_1m', 0)
    oth_loan_apply_bank_1m = variables.get('oth_loan_apply_bank_1m', 0)
    apply_bank_1m_arr = [anti_apply_bank_1m, net_apply_bank_1m, oth_loan_apply_bank_1m]
    anti_apply_bank_3m = variables.get('anti_apply_bank_3m', 0)
    net_apply_bank_3m = variables.get('net_apply_bank_3m', 0)
    oth_loan_apply_bank_3m = variables.get('oth_loan_apply_bank_3m', 0)
    apply_bank_3m_arr = [anti_apply_bank_3m, net_apply_bank_3m, oth_loan_apply_bank_3m]
    apply_sloan_7d_arr = [variables.get('anti_apply_sloan_7d', 0), variables.get('net_apply_sloan_7d', 0),
                          variables.get('oth_loan_apply_sloan_7d', 0)]
    apply_sloan_1m_arr = [variables.get('anti_apply_sloan_1m', 0), variables.get('net_apply_sloan_1m', 0),
                          variables.get('oth_loan_apply_sloan_1m', 0)]
    apply_sloan_3m_arr = [variables.get('anti_apply_sloan_3m', 0), variables.get('net_apply_sloan_3m', 0),
                          variables.get('oth_loan_apply_sloan_3m', 0)]
    apply_p2p_7d_arr = [variables.get('anti_apply_p2p_7d', 0), variables.get('net_apply_p2p_7d', 0),
                        variables.get('oth_loan_apply_p2p_7d', 0)]
    apply_p2p_1m_arr = [variables.get('anti_apply_p2p_1m', 0), variables.get('net_apply_p2p_1m', 0),
                        variables.get('oth_loan_apply_p2p_1m', 0)]
    apply_p2p_3m_arr = [variables.get('anti_apply_p2p_3m', 0), variables.get('net_apply_p2p_3m', 0),
                        variables.get('oth_loan_apply_p2p_3m', 0)]
    apply_confin_7d_arr = [variables.get('anti_apply_confin_7d', 0), variables.get('net_apply_confin_7d', 0),
                           variables.get('oth_loan_apply_confin_7d', 0)]
    apply_confin_1m_arr = [variables.get('anti_apply_confin_1m', 0), variables.get('net_apply_confin_1m', 0),
                           variables.get('oth_loan_apply_confin_1m', 0)]
    apply_confin_3m_arr = [variables.get('anti_apply_confin_3m', 0), variables.get('net_apply_confin_3m', 0),
                           variables.get('oth_loan_apply_confin_3m', 0)]
    apply_other_7d_arr = [variables.get('anti_apply_other_7d', 0), variables.get('net_apply_other_7d', 0),
                          variables.get('oth_loan_apply_other_7d', 0)]
    apply_other_1m_arr = [variables.get('anti_apply_other_1m', 0), variables.get('net_apply_other_1m', 0),
                          variables.get('oth_loan_apply_other_1m', 0)]
    apply_other_3m_arr = [variables.get('anti_apply_other_3m', 0), variables.get('net_apply_other_3m', 0),
                          variables.get('oth_loan_apply_other_3m', 0)]
    apply_bank_6m_arr = [variables.get('oth_loan_apply_bank_6m', 0), variables.get('net_apply_bank_6m', 0)]
    apply_bank_12m_arr = [variables.get('oth_loan_apply_bank_12m', 0), variables.get('net_apply_bank_12m', 0)]
    apply_bank_his_arr = [variables.get('oth_loan_apply_bank_his', 0), variables.get('net_apply_bank_his', 0)]
    apply_sloan_6m_arr = [variables.get('oth_loan_apply_sloan_6m', 0), variables.get('net_apply_sloan_6m', 0)]
    apply_sloan_12m_arr = [variables.get('oth_loan_apply_sloan_12m', 0), variables.get('net_apply_sloan_12m', 0)]
    apply_sloan_his_arr = [variables.get('oth_loan_apply_sloan_his', 0), variables.get('net_apply_sloan_his', 0)]
    apply_p2p_6m_arr = [variables.get('oth_loan_apply_p2p_6m', 0), variables.get('net_apply_p2p_6m', 0)]
    apply_p2p_12m_arr = [variables.get('oth_loan_apply_p2p_12m', 0), variables.get('net_apply_p2p_12m', 0)]
    apply_p2p_his_arr = [variables.get('oth_loan_apply_p2p_his', 0), variables.get('net_apply_p2p_his', 0)]
    apply_confin_6m_arr = [variables.get('oth_loan_apply_confin_6m', 0), variables.get('net_apply_confin_6m', 0)]
    apply_confin_12m_arr = [variables.get('oth_loan_apply_confin_12m', 0), variables.get('net_apply_confin_12m', 0)]
    apply_confin_his_arr = [variables.get('oth_loan_apply_confin_his', 0), variables.get('net_apply_confin_his', 0)]
    apply_other_6m_arr = [variables.get('oth_loan_apply_other_6m', 0), variables.get('net_apply_other_6m', 0)]
    apply_other_12m_arr = [variables.get('oth_loan_apply_other_12m', 0), variables.get('net_apply_other_12m', 0)]
    apply_other_his_arr = [variables.get('oth_loan_apply_other_his', 0), variables.get('net_apply_other_his', 0)]
    extension = {
        'apply_bank_7d': round_max(apply_bank_7d_arr),
        'apply_bank_1m': round_max(apply_bank_1m_arr),
        'apply_bank_3m': round_max(apply_bank_3m_arr),
        'apply_sloan_7d': round_max(apply_sloan_7d_arr),
        'apply_sloan_1m': round_max(apply_sloan_1m_arr),
        'apply_sloan_3m': round_max(apply_sloan_3m_arr),
        'apply_p2p_7d': round_max(apply_p2p_7d_arr),
        'apply_p2p_1m': round_max(apply_p2p_1m_arr),
        'apply_p2p_3m': round_max(apply_p2p_3m_arr),
        'apply_confin_7d': round_max(apply_confin_7d_arr),
        'apply_confin_1m': round_max(apply_confin_1m_arr),
        'apply_confin_3m': round_max(apply_confin_3m_arr),
        'apply_other_7d': round_max(apply_other_7d_arr),
        'apply_other_1m': round_max(apply_other_1m_arr),
        'apply_other_3m': round_max(apply_other_3m_arr),
        'apply_bank_6m': round_max(apply_bank_6m_arr),
        'apply_bank_12m': round_max(apply_bank_12m_arr),
        'apply_bank_his': round_max(apply_bank_his_arr),
        'apply_sloan_6m': round_max(apply_sloan_6m_arr),
        'apply_sloan_12m': round_max(apply_sloan_12m_arr),
        'apply_sloan_his': round_max(apply_sloan_his_arr),
        'apply_p2p_6m': round_max(apply_p2p_6m_arr),
        'apply_p2p_12m': round_max(apply_p2p_12m_arr),
        'apply_p2p_his': round_max(apply_p2p_his_arr),
        'apply_confin_6m': round_max(apply_confin_6m_arr),
        'apply_confin_12m': round_max(apply_confin_12m_arr),
        'apply_confin_his': round_max(apply_confin_his_arr),
        'apply_other_6m': round_max(apply_other_6m_arr),
        'apply_other_12m': round_max(apply_other_12m_arr),
        'apply_other_his': round_max(apply_other_his_arr)
    }
    variables.update(extension)


def round_max(max_arr, median_arr=None, ratio=0.3):
    if median_arr is None:
        median_arr = max_arr
    return int(round(
        numpy.amax(max_arr) + ratio * numpy.median(median_arr),
        0
    ))


def translate_for_report_detail(product_code, user_name=None, id_card_no=None, phone=None, user_type=None,
                                base_type=None, origin_data=None, data_repository=None):
    """
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """

    variables = {}
    # 如果data_repository不为空，则由该函数的调用方释放
    no_data_repository = data_repository is None
    cached_data = {} if no_data_repository else data_repository
    try:
        codes = _get_codes_by_product_code(product_code)
        for c in codes:
            trans = get_transformer(c)
            trans_result = trans.run(user_name=user_name,
                                     id_card_no=id_card_no,
                                     phone=phone,
                                     user_type=user_type,
                                     base_type=base_type,
                                     origin_data=origin_data,
                                     cached_data=cached_data
                                     )
            variables.update(trans_result['variables'])

        # 特定的product_code会检测对应的view下的产品module.
        product_view_trans = get_product_transformers(product_code)
        if product_view_trans and len(product_view_trans) > 0:
            for product_view_tran in product_view_trans:
                trans_result = product_view_tran.run(user_name=user_name,
                                                     id_card_no=id_card_no,
                                                     phone=phone,
                                                     user_type=user_type,
                                                     base_type=base_type,
                                                     origin_data=origin_data,
                                                     cached_data=cached_data)
                variables.update(trans_result['variables'])

    except Exception as err:
        logger.error(">>> translate error: " + str(err))
        if no_data_repository:
            cached_data.clear()
        raise ServerException(code=500, description=str(err))

    if no_data_repository:
        cached_data.clear()
    if len(variables) > 0:
        extension_variables(variables)
        # 转换类型，这样解决tojson的问题
        fix_cannot_to_json(variables)
    return {
        'variables': variables
    }


def get_transformer(code) -> Transformer:
    """
    根据code构建对应的转换对象
    :param code:
    :return:
    """
    try:
        model = importlib.import_module("view.v" + str(code))
        api_class = getattr(model, "V" + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error("code:" + str(code) + "--" + str(err))
        return Transformer()


# 获取指定产品的view的transformers
def get_product_transformers(product_code, grouped=False):
    pkg = None
    try:
        pkg = importlib.import_module("view.p" + product_code)
    except ModuleNotFoundError as err:
        logger.error("product_code:%s, view transformers is empty.", product_code)
        return []

    try:
        transformer_list = []
        for importer, name, is_pkg in pkgutil.walk_packages(pkg.__path__, "%s." % pkg.__name__):
            if is_pkg:
                continue
            module = importlib.import_module(name)
            if grouped:
                object_list = [value() for (m_name, value) in getmembers(module) if isclass(value)
                               and issubclass(value, GroupedTransformer)]
            else:
                object_list = [value() for (_, value) in getmembers(module) if isclass(value) and value != Transformer]
            transformer_list.extend(object_list)
        return transformer_list
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
    return []
