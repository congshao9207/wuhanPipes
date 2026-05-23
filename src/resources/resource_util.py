# -*- coding: utf-8 -*-
# @Time : 2019/12/9 1:05 PM
# @Author : lixiaobo
# @Site : 
# @File : FileUtil.py
# @Software: PyCharm

from file_utils.files import read_content
from os.path import abspath, dirname
import os

def get_config_content(file_name):
    return read_content(file_name, __file__)


def get_config_content_gbk(file_name):
    BASE_DIR = dirname(abspath(__file__))
    resource_full_path = os.path.join(BASE_DIR, file_name)
    f = open(resource_full_path, 'r', encoding='gbk')
    return f