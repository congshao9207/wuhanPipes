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


# def get_transformer(code, product_code=None) -> Transformer:
#     """
#     根据code构建对应的转换对象
#     :param product_code:
#     :param code:
#     :return:
#     """
#     try:
#         model = None
#         if product_code:
#             model = importlib.import_module("mapping.p" + product_code + ".t" + str(code))
#         else:
#             model = importlib.import_module("mapping.t" + str(code))
#         api_class = getattr(model, "T" + str(code))
#         api_instance = api_class()
#         return api_instance
#     except ModuleNotFoundError as err:
#         logger.error(str(err))
#         return Transformer()


def get_transformer(code: str, product_code: str = None) -> Transformer:
    """
    根据code和product_code构建对应的转换对象

    逻辑：
    1. 如果提供了product_code，尝试导入 mapping.p{product_code}.t{code}
    2. 如果失败或未提供product_code，尝试导入 mapping.t{code}
    3. 如果都失败，返回默认Transformer

    注意：p{product_code}目录下可能没有对应的t{code}.py文件

    :param code: 转换器代码，如 "11002", "11003"
    :param product_code: 产品代码，如 "11002"。为None时使用通用转换器
    :return: Transformer实例
    """
    # 记录尝试的模块路径
    attempted_modules = []

    # 情况1：有product_code参数
    if product_code:
        try:
            # 先尝试产品特定的转换器：mapping.p11002.t11002
            module_name = f"mapping.p{product_code}.t{code}"
            attempted_modules.append(module_name)

            logger.debug(f"尝试导入产品特定转换器: {module_name}")
            model = importlib.import_module(module_name)

            # 获取对应的类
            class_name = f"T{code}"
            if hasattr(model, class_name):
                api_class = getattr(model, class_name)
                logger.info(f"✓ 成功加载产品{product_code}的转换器: {class_name}")
                return api_class()
            else:
                logger.warning(f"模块 {module_name} 中未找到类 {class_name}")

        except ModuleNotFoundError as err:
            logger.debug(f"产品特定转换器不存在: {err}")
            # 这是正常情况，继续尝试通用转换器

        except Exception as e:
            logger.error(f"导入产品特定转换器时出错: {e}")
            # 继续尝试通用转换器

    # 情况2：尝试通用转换器或作为备选
    try:
        # 尝试通用转换器：mapping.t11002
        module_name = f"mapping.t{code}"
        attempted_modules.append(module_name)

        logger.debug(f"尝试导入通用转换器: {module_name}")
        model = importlib.import_module(module_name)

        # 获取对应的类
        class_name = f"T{code}"
        if hasattr(model, class_name):
            api_class = getattr(model, class_name)

            if product_code:
                logger.info(f"✓ 使用通用转换器 {code} (产品{product_code}的特定转换器不存在)")
            else:
                logger.debug(f"✓ 成功加载通用转换器: {class_name}")

            return api_class()
        else:
            logger.error(f"通用转换器模块 {module_name} 中未找到类 {class_name}")

    except ModuleNotFoundError as err:
        logger.error(f"通用转换器不存在: {err}")
    except Exception as e:
        logger.error(f"导入通用转换器时出错: {e}")

    # 情况3：所有尝试都失败
    logger.warning(f"使用默认转换器，未找到合适的转换器")
    logger.debug(f"尝试过的模块路径: {attempted_modules}")
    logger.debug(f"参数: code={code}, product_code={product_code}")

    return Transformer()