# @Time : 2019/10/21 3:45 PM
# @Author : lixiaobo
# @Site :
# @File : t00001.py
# @Software: PyCharm
import json

import pandas as pd

from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)

class T00001(Transformer):
    """
    灰名单查询
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'mag_court_break_faith': 0,
            'mag_court_high_cons': 0,
            'mag_court_loan_con': 0,
            'mag_court_pop_loan': 0,
            'mag_fraudinfo_isMachdBlMakt': 0,
            'mag_social_idc_name_in_black': 0,
            'mag_social_name_tel_in_black': 0
        }

        self.variables_mapping = {
            "法院失信名单": "mag_court_break_faith",
            "限制高消费名单": "mag_court_high_cons",
            "借款合同纠纷": "mag_court_loan_con",
            "民间借贷纠纷": "mag_court_pop_loan",
            "第三方标注黑名单": "mag_fraudinfo_isMachdBlMakt",
            "身份证和姓名在黑名单": "mag_social_idc_name_in_black",
            "姓名和手机号在黑名单": "mag_social_name_tel_in_black"
        }

        self.df = None

    def _grey_register_df(self):
        if self.df is not None:
            return self.df

        if self.df_client is None:
            logger.error("t0001 df_client is not exists, nothing to do.")
            return

        id_type = None
        if self.user_type == "COMPANY":
            id_type = "CREDIT_CODE"
        else:
            id_type = "ID_CARD_NO"

        data_items = self.df_client.query_grey_list(self.user_name, self.id_card_no, id_type)
        if (data_items is None) or (len(data_items) <= 0):
            logger.warn("_grey_register_df data_items is None or empty.")
            return None

        info = json.dumps(data_items)
        self.df = pd.read_json(info)
        return self.df

    def _parse_to_strategy_variables(self, df=None):
        if df is None:
            logger.warn("_parse_to_strategy_variables df is none, nothing to do.")

        for risk_detail in self.variables_mapping:
            if df is not None and not df.query('riskDetail=="' + risk_detail + '"').empty:
                self.variables[self.variables_mapping.get(risk_detail)] = 1
            else:
                self.variables[self.variables_mapping.get(risk_detail)] = 0

    def transform(self):
        logger.info("T00001 transform begin.....")
        df = self._grey_register_df()
        self._parse_to_strategy_variables(df)
        logger.info("T00001 transform end, variables %s", self.variables)
