from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T05001(Transformer):
    """
    手机状态
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'phone_check': 1
        }

    def _info_certification_df(self):
        info_certification = """
            SELECT phone, result FROM info_certification 
            WHERE certification_type = 'ID_NAME_MOBILE' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone=%(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "phone": self.phone,
                               "id_card_no": self.id_card_no})
        return df

    def _phone_check(self, df=None):
        """
        手机实名状态
        :param user_name:
        :param id_card_no:
        :return: 如果匹配返回 0， 不匹配返回 1
        """
        if df is not None and 'result' in df.columns:
            # result is True
            if len(df) == 1 and df['result'][0] == b'\x01':
                self.variables['phone_check'] = 0

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._phone_check(self._info_certification_df())
