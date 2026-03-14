from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T11001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'risk_score_loan': 0
        }

    def _info_risk_score_df(self):
        sql = """
            SELECT score FROM info_risk_score
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
                AND scorecard_type = 0
                AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no,
                               "phone": self.phone})
        return df

    def _risk_score_loan(self, df=None):
        """
        风险评分_小贷风险评分
        """
        if df is not None and len(df) > 0:
            self.variables['risk_score_loan'] = df['score'][0]

    def transform(self):
        self._risk_score_loan(df=self._info_risk_score_df())
