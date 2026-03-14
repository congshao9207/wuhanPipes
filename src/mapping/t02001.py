from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T02001(Transformer):
    """
    手机号在网时长
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'phone_on_line_days': 0
        }

    def _info_on_line_duration_df(self):
        sql = """
            SELECT on_line_days FROM info_on_line_duration 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND id_card_no = %(id_card_no)s AND phone=%(phone)s;
        """
        df = sql_to_df(sql=sql,
                       params={"id_card_no": self.id_card_no,
                               "phone": self.phone})
        return df

    def _phone_on_line_days(self, df=None):
        """
        手机号在网时长
        :param user_name:
        :param id_card_no:
        """
        if df is not None and len(df) > 0:
            self.variables['phone_on_line_days'] = df['on_line_days'][0]

    def transform(self):
        self._phone_on_line_days(self._info_on_line_duration_df())
