# @Time : 2020/6/18 7:21 PM 
# @Author : lixiaobo
# @File : Parser.py 
# @Software: PyCharm
from abc import abstractmethod


class Parser(object):
    def __init__(self):
        self.param = None
        self.file = None

    # 初始化成员变量
    def init_param(self, param, file):
        self.param = param
        self.file = file

    # 解析处理逻辑
    @abstractmethod
    def process(self):
        pass
