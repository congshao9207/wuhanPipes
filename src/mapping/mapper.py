# -*- coding: utf-8 -*-
import importlib

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer, fix_cannot_to_json

logger = LoggerUtil().logger(__name__)


def translate_for_strategy(product_code, codes, user_name=None, id_card_no=None, phone=None, user_type=None, base_type=None, df_client=None, origin_data=None, data_repository=None, full_msg=None):
    """
    根据产品编码对应的excel文件从Gears数据库里获取数据做转换处理。
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """
    variables = {}
    out_decision_code = {}
    c = None
    product_trans = []
    # 如果data_repository不为空，则由该函数的调用方释放
    no_data_repository = data_repository is None
    cached_data = {} if no_data_repository else data_repository

    try:
        for c in codes:
            trans = get_transformer(c)
            if trans:
                product_trans.append(trans)

            agg_trans = get_transformer(c, product_code)
            if agg_trans:
                product_trans.append(agg_trans)

        for trans in product_trans:
            logger.info("transformer begin: %s", str(trans))
            trans.df_client = df_client
            trans.product_code = product_code
            trans_result = trans.run(user_name=user_name,
                                     id_card_no=id_card_no,
                                     phone=phone,
                                     user_type=user_type,
                                     base_type=base_type,
                                     origin_data=origin_data,
                                     cached_data=cached_data,
                                     full_msg=full_msg)
            logger.info("transformer end: %s", str(trans))
            variables.update(trans_result['variables'])
            out_decision_code.update(trans_result['out_decision_code'])

    except Exception as err:
        if not no_data_repository:
            cached_data.clear()
        logger.error(c + ">>> translate error: " + str(err))
        raise ServerException(code=500, description=str(err))
    # 转换类型，这样解决tojson的问题
    if no_data_repository:
        cached_data.clear()
    fix_cannot_to_json(variables)
    return variables, out_decision_code


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
