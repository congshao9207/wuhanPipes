# -*- coding: utf-8 -*-
import traceback

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each
from mapping.tranformer import fix_cannot_to_json
from view.mapper_detail import get_product_transformers

logger = LoggerUtil().logger(__name__)


def view_variables_scheduler(product_code, full_msg=None, user_name=None, id_card_no=None, phone=None,
                                user_type=None,base_type=None, origin_data=None, invoke_style=invoke_each,
                                data_repository=None):
    """
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    特定的product_code会检测对应的view下的产品module.
    """

    variables = {}
    # 如果data_repository不为空，则由该函数的调用方释放
    no_data_repository = data_repository is None
    cached_data = {} if no_data_repository else data_repository
    try:
        view_transformers = get_product_transformers(product_code, True)
        if view_transformers and len(view_transformers) > 0:
            filtered_view_trans = filter(lambda x: x.invoke_style() & invoke_style > 0, view_transformers)
            for product_view_tran in filtered_view_trans:
                logger.info("product_view_tran begin :%s", str(product_view_tran))
                product_view_tran.full_msg = full_msg
                trans_result = product_view_tran.run(user_name=user_name,
                                                     id_card_no=id_card_no,
                                                     phone=phone,
                                                     user_type=user_type,
                                                     base_type=base_type,
                                                     origin_data=origin_data,
                                                     cached_data=cached_data)
                logger.info("product_view_tran end :%s", str(product_view_tran))
                group_name = product_view_tran.group_name()
                variables[group_name] = trans_result['variables']

    except Exception as err:
        logger.error(">>> translate error: " + str(err))
        if no_data_repository:
            cached_data.clear()
        logger.error(traceback.format_exc())
        raise ServerException(code=500, description=str(err))

    if no_data_repository:
        cached_data.clear()
    if len(variables) > 0:
        # extension_variables(variables)
        # 转换类型，这样解决toJson的问题
        fix_cannot_to_json(variables)
    return variables




