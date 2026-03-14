import datetime

from mapping.tranformer import Transformer
from util.id_card_info import GetInformation
from util.mysql_reader import sql_to_df
import re


# 婚姻状态取值如下：
# 未婚 UNMARRIED
# 已婚 MARRIED
# 初婚 CHUHUN
# 再婚 ZAIHUN
# 复婚 FUHUN
# 离异 DIVORCE
# 丧偶 WIDOWHOOD
# 未说明 UNKNOWN
class T00000(Transformer):
    """
    基本信息
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'base_date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'base_idno': '',
            'base_gender': 0,
            'base_age': 0,
            'base_black': 0,
            'base_type': 'PERSONAL',
            'user_type': '',
            'base_phone': '',
            'product_code': '',
            'base_marry_state': 'UNKNOWN',
            'strategy': "01",  # 是否过决策
            'education': '',  # 学历
            'auth_status': 'AUTHORIZED',  # 授权状态
            'base_idno_4': '',
            'base_idno_6': '',
            'base_industry': ''
        }

    def _base_black(self):
        sql = """
        SELECT count(1) as base_black FROM info_black_list
            WHERE valid > 0 AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        if df is not None and len(df) > 0:
            if df['base_black'][0] > 0:
                self.variables['base_black'] = 1

    def transform(self):
        if hasattr(self, "product_code"):
            self.variables["product_code"] = self.product_code
        self.variables['base_idno'] = self.id_card_no
        if self.phone is not None:
            self.variables['base_phone'] = self.phone
        # self.variables['base_phone'] = self.phone
        if self.base_type is not None:
            self.variables['base_type'] = self.base_type
        else:
            self.variables['base_type'] = self.user_type
        self.variables['user_type'] = self.user_type
        self._base_black()
        if self.user_type == 'PERSONAL' and self.id_card_no is not None and \
                re.search(r'(19\d{2}|20[012]\d)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])', str(self.id_card_no)[6:14]):
            information = GetInformation(self.id_card_no)
            self.variables['base_gender'] = information.get_sex()
            self.variables['base_age'] = information.get_age()
            self.variables['base_idno_4'] = self.id_card_no[0:4]
            self.variables['base_idno_6'] = self.id_card_no[0:6]

        if self.origin_data:
            apply_amount = self.origin_data.get("applyAmo")
            if apply_amount:
                self.variables["base_apply_amo"] = apply_amount

            # 授权状态
            auth_status = self.origin_data.get("authStatus")
            if auth_status:
                self.variables["auth_status"] = auth_status

            extra_param = self.origin_data.get("extraParam")
            if extra_param:
                marry_state = extra_param.get("marryState")
                industry = extra_param.get('industry')
                if marry_state:
                    self.variables["base_marry_state"] = marry_state
                if industry:
                    self.variables["base_industry"] = industry

                # 是否过决策
                strategy = extra_param.get("strategy")
                if strategy:
                    self.variables["strategy"] = strategy

                # 学历
                education = extra_param.get("education")
                if education:
                    self.variables["education"] = education



