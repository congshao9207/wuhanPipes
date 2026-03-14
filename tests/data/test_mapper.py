# -*- coding: utf-8 -*-
import importlib

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer, fix_cannot_to_json

# from app import logger

logger = LoggerUtil().logger(__name__)


def translate_for_strategy(product_code, codes, user_name=None, id_card_no=None, phone=None, user_type=None, origin_data=None):
    """
    根据产品编码对应的excel文件从Gears数据库里获取数据做转换处理。
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """
    variables = {}
    c = None
    try:
        for c in codes:
            if product_code is None:
                trans = get_transformer(c)
                trans_result = trans.run(user_name=user_name,
                                         id_card_no=id_card_no,
                                         phone=phone,
                                         user_type=user_type)
                variables.update(trans_result['variables'])
            else:
                product_trans = get_transformer(c, product_code)
                product_trans.product_code = product_code
                product_trans_result = product_trans.run(user_name=user_name,
                                                         id_card_no=id_card_no,
                                                         phone=phone,
                                                         user_type=user_type,
                                                         origin_data=origin_data)
                variables.update(product_trans_result['variables'])
    except Exception as err:
        logger.error(c + ">>> translate error: " + str(err))
        raise ServerException(code=500, description=str(err))
    # 转换类型，这样解决tojson的问题
    fix_cannot_to_json(variables)
    return variables


def get_transformer(code, product_code=None) -> Transformer:
    """
    根据code构建对应的转换对象
    :param product_code:
    :param code:
    :return:
    """
    try:
        model = None
        if product_code:
            model = importlib.import_module("mapping.p" + product_code + ".t" + str(code))
        else:
            model = importlib.import_module("mapping.t" + str(code))
        api_class = getattr(model, "T" + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Transformer()
