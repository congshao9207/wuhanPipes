from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T01001(Transformer):
    """
    手机号在网状态
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'phone_on_line_state': 'UNKNOWN'
        }

    def _info_on_line_state_df(self):
        sql = """
            SELECT mobile_state FROM info_on_line_state 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND  id_card_no = %(id_card_no)s AND phone=%(phone)s;
        """
        df = sql_to_df(sql=sql,
                       params={"id_card_no": self.id_card_no,
                               "phone": self.phone})
        return df

    def _phone_on_line_state(self, df=None):
        """
        手机号在网状态
        :param user_name:
        :param id_card_no:
        """
        if df is not None and len(df) > 0:
            self.variables['phone_on_line_state'] = df['mobile_state'][0]

    def transform(self):
        self._phone_on_line_state(self._info_on_line_state_df())
