from flask import json
from flask import request
from werkzeug.exceptions import HTTPException


# 重写HTTPException异常中的get_body和get_headers方法,返回异常json格式
class APIException(HTTPException):

    # 重写父类的方法
    def get_body(self, environ=None, scope=None):
        return json.dumps(dict(
            code=self.code,
            name=self.name,
            requert=request.method + ">>" + request.url,
            description=self.get_description(environ)
        ))

    # 重写父类的方法
    def get_headers(self, *args, **kwargs):
        return [('Content-Type', 'application/json')]


class ServerException(APIException):
    # 重写父类的属性
    code = None
    description = "server unknown error..."

    def __init__(self, description=None, response=None, code=None):
        super().__init__(description, response)
        self.code = code
        self.description = description


class DataPreparedException(APIException):
    # 重写父类的属性
    code = None
    description = "data prepared error..."

    def __init__(self, description=None, response=None, code=None):
        super().__init__(description, response)
        self.code = code
        self.description = description


class DataExtractException(APIException):
    # 重写父类的属性
    code = None
    description = "data extract error..."

    def __init__(self, description=None, response=None, code=None):
        super().__init__(description, response)
        self.code = code
        self.description = description