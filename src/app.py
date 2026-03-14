import importlib
import json
import logging
import time
import traceback

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from py_eureka_client import eureka_client
from werkzeug.exceptions import HTTPException

from config import EUREKA_SERVER, version_info, GEARS_DB
from config_controller import base_type_api
from exceptions import APIException, ServerException
from fileparser.Parser import Parser
from logger.logger_util import LoggerUtil
from product.generate import Generate
from util.defensor_client import DefensorClient

logger = LoggerUtil().logger(__name__)

app = Flask(__name__)
app.register_blueprint(base_type_api)
start_time = time.localtime()

# logging.getLogger('sqlalchemy.engine.base.Engine').setLevel(logging.WARNING)

try:
    logger.info("init eureka client...")
    logger.info("EUREKA_SERVER:%s", EUREKA_SERVER)
    eureka_client.init(eureka_server=EUREKA_SERVER,
                       app_name="PIPES",
                       instance_port=8010)
    logger.info("eureka client started. center: %s", EUREKA_SERVER)
except Exception as e:
    logger.error(traceback.format_exc())


@app.route("/biz-types", methods=['POST'])
def shake_hand():
    """
    根据productCode调用对应的handler处理业务
    :return:
    """
    try:
        json_data = request.get_json()
        product_code = json_data.get('productCode')
        handler = _get_product_handler(product_code)
        df_client = DefensorClient(request.headers)
        handler.df_client = df_client
        handler.sql_db = sql_session

        resp = handler.shake_hand(json_data)
        logger.info("shake_hand------end-------")
        return jsonify(resp)
    except Exception as e:
        logger.error(traceback.format_exc())



@app.route("/strategy", methods=['POST'])
def strategy():
    try:
        logger.info("strategy begin...")
        json_data = request.get_json()
        logger.info("strategy param:%s", json_data)
        strategy_param = json_data.get('strategyParam')
        product_code = strategy_param.get('productCode')
        handler = _get_product_handler(product_code)
        df_client = DefensorClient(request.headers)
        handler.df_client = df_client
        handler.sql_db = sql_session

        resp = handler.call_strategy(json_data)
        return jsonify(resp)
    except Exception as e:
        logger.error(traceback.format_exc())


@app.route("/parse", methods=['POST'])
def parse():
    """
    流水解析，验真请求
    """
    file = request.files.get("file")
    function_code = request.args.get("parseCode")
    if function_code is None:
        function_code = request.form.get("parseCode")
    data = request.args.get("param")
    if data is None:
        data = request.form.get("param")

    if function_code is None:
        return "缺少 parseCode字段"
    elif data is None:
        return "缺少 param字段"
    elif file is None:
        return "缺少 file字段"

    handler = _get_handler("fileparser", "Parser", function_code)
    handler.init_param(json.loads(data), file)
    handler.sql_db = sql_session
    resp = handler.process()

    return jsonify(resp)


@app.route("/health", methods=['GET'])
def health_check():
    """
    检查当前应用的健康情况
    :return:
    """
    return 'pipes is running'


@app.route("/info", methods=['GET'])
def info():
    return 'pipes is running'


# 获取系统基本参数信息，用于系统监控
@app.route("/sys-basic-info", methods=['GET'])
def sys_basic_info():
    return jsonify({
        "SysName": "Pipes",
        "Version": version_info,
        "StartTime": time.strftime("%Y-%m-%d %H:%M:%S", start_time)
    })


@app.errorhandler(Exception)
def flask_global_exception_handler(e):
    # 判断异常是不是APIException
    if isinstance(e, APIException):
        return e
    # 判断异常是不是HTTPException
    if isinstance(e, HTTPException):
        error = APIException()
        error.code = e.code
        error.description = e.description
        return error
    # 异常肯定是Exception
    from flask import current_app
    # 如果是调试模式,则返回e的具体异常信息。否则返回json格式的ServerException对象！
    if current_app.config["DEBUG"]:
        return e
    return ServerException()


# def _get_product_handler(product_code) -> Generate:
#     model = None
#     try:
#         model = importlib.import_module("product.p" + str(product_code))
#     except ModuleNotFoundError as err:
#         try:
#             model = importlib.import_module("product.P" + str(product_code))
#         except ModuleNotFoundError as err:
#             logger.error(str(err))
#             return Generate()
#     try:
#         api_class = getattr(model, "P" + str(product_code))
#         api_instance = api_class()
#         return api_instance
#     except ModuleNotFoundError as err:
#         logger.error(str(err))
#         return Generate()

def _get_product_handler(product_code) -> Generate:
    """
    获取产品处理器

    Args:
        product_code: 产品代码，如 "11001"

    Returns:
        Generate实例
    """
    try:
        # 文件名是小写 p，所以使用小写导入
        module_name = f"product.p{product_code}"
        logger.debug(f"尝试导入产品模块: {module_name}")

        model = importlib.import_module(module_name)

        # 类名是大写 P
        class_name = f"P{product_code}"
        logger.debug(f"尝试获取类: {class_name}")

        if hasattr(model, class_name):
            api_class = getattr(model, class_name)
            api_instance = api_class()
            logger.info(f"成功加载产品处理器: {class_name}")
            return api_instance
        else:
            logger.error(f"模块 {module_name} 中未找到类 {class_name}")
            # 列出模块中可用的类
            available_classes = [attr for attr in dir(model) if not attr.startswith('_')]
            logger.debug(f"模块中可用的类: {available_classes}")
            return Generate()

    except ModuleNotFoundError as err:
        logger.error(f"产品模块不存在: {err}")
        # 可以添加文件路径调试信息
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        expected_file = os.path.join(current_dir, "product", f"p{product_code}.py")
        logger.debug(f"期望的文件路径: {expected_file}")
        logger.debug(f"文件是否存在: {os.path.exists(expected_file)}")
        return Generate()
    except Exception as err:
        logger.error(f"加载产品处理器时出错: {err}")
        return Generate()

def _get_handler(folder, prefix, code) -> Parser:
    try:
        model = importlib.import_module(folder + "." + prefix + str(code))
        api_class = getattr(model, prefix + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Parser()


def sql_db():
    db_url = 'dm+dmPython://%(user)s:%(pw)s@%(host)s:%(port)s?schema=%(db)s' % GEARS_DB
    # db_url = 'mysql+pymysql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % GEARS_DB
    app.config['SQLALCHEMY_DATABASE_URI'] = db_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ECHO'] = False
    db = SQLAlchemy(app)
    return db



sql_session = sql_db()

if __name__ == '__main__':
    logger.info('starting pipes...')
    app.run(host='0.0.0.0')
    logger.info('pipes started.')
