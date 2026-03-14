# -*- coding: utf-8 -*-
# @Time : 2020/1/17 3:31 PM
# @Author : lixiaobo
# @Site : 
# @File : conftest.py.py
# @Software: PyCharm
import os

import pytest

from app import app


@pytest.fixture
def client():
    client = app.test_client()
    yield client
