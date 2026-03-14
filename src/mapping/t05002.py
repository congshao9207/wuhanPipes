from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T05002(Transformer):
    """
    公安相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ps_name_id': 1
        }

    def _info_certification_df(self):
        info_certification = """
            SELECT result FROM info_certification 
            WHERE certification_type = 'ID_NAME' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _ps_name_id(self, df=None):
        """
        判断用户名和证件号是否匹配
        :param user_name:
        :param id_card_no:
        :return: 如果匹配返回 0， 不匹配返回 1
        """
        if df is not None and 'result' in df.columns:
            if len(df) == 1 and df['result'][0] == b'\x01':
                self.variables['ps_name_id'] = 0

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._ps_name_id(self._info_certification_df())
