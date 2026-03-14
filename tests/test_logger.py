import pandas as pd

from logger.logger_util import LoggerUtil
from util.defensor_client import DefensorClient


def test_log_stash():
    logger_util = LoggerUtil()
    logger = logger_util.logger(__name__)
    logger.info("test my logger....")


def test_timestamp():
    a = pd.Timestamp("2018-08-01 12:00:00")
    if type(a) == pd.Timestamp:
        print('hhhhhhh')


def testClass():
    df_client = DefensorClient(headers=None)
    print(df_client)