# -*- coding: utf-8 -*-
import importlib

# from app import logger
import numpy

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer, fix_cannot_to_json

logger = LoggerUtil().logger(__name__)

# 标识决策引擎交互完成。
STRATEGE_DONE = 'fffff'




def translate_for_report_detail(codes, user_name=None, id_card_no=None, phone=None, user_type=None):
    """
    根据产品编码对应的excel文件从Gears数据库里获取数据做转换处理。
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """

    variables = {}

    try:
        for c in codes:
            trans = get_transformer(c)
            trans_result = trans.run(user_name=user_name,
                                     id_card_no=id_card_no,
                                     phone=phone,
                                     user_type=user_type)
            variables.update(trans_result['variables'])
    except Exception as err:
        logger.error(">>> translate error: " + str(err))
        raise ServerException(code=500, description=str(err))
    # 转换类型，这样解决tojson的问题
    fix_cannot_to_json(variables)
    return variables


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
        logger.error(str(err))
        return Transformer()
