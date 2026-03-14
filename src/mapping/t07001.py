from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T07001(Transformer):
    """
    借贷统计
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'lend_score': None,
            'lend_fail_rate': None,
            'lend_cha_cnt_12m': None,
            'lend_chafail_cnt_12m': None
        }

    def _info_loan_stats_df(self):
        sql = """
            SELECT id, channel_score, fail_rate FROM info_loan_stats
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no,
                               "phone": self.phone})
        return df

    def _lend_score(self, df=None):
        """
        借贷统计_借贷统计分数
        """
        if df is not None and len(df) > 0:
            result = df['channel_score'][0]
            if result is not None:
                self.variables['lend_score'] = result

    def _lend_fail_rate(self, df=None):
        """
        借贷统计_贷款代扣失败率
        """
        if df is not None and len(df) > 0:
            result = df['fail_rate'][0]
            if result is not None:
                self.variables['lend_fail_rate'] = float(result)

    def _lend_cha_cnt_12m(self, df=None):
        """
        借贷统计_最近12个月扣款交易总次数
        :param df:
        :return:
        """
        if df is not None and len(df) > 0:
            sql = """
            select sum(latest_trans_count) as 'lend_cha_cnt_12m' from info_loan_stats_pay
            where loan_stats_id = %(info_loan_stats_id)s and recent_months = 'RECENTLY_12M';
            """
            df2 = sql_to_df(sql=sql,
                            params={"info_loan_stats_id": int(df['id'][0])})
            if df2 is not None:
                result = df2['lend_cha_cnt_12m'][0]
                if result is not None:
                    self.variables['lend_cha_cnt_12m'] = result

    def _lend_chafail_cnt_12m(self, df=None):
        """
        借贷统计_最近12个月扣款交易总次数
        :param df:
        :return:
        """
        if df is not None and len(df) > 0:
            sql = """
                select sum(low_balance_fail_count) as 'lend_chafail_cnt_12m' from info_loan_stats_pay 
                where loan_stats_id = %(info_loan_stats_id)s and recent_months='RECENTLY_12M'
            """
            df2 = sql_to_df(sql=sql,
                            params={"info_loan_stats_id": int(df['id'][0])})
            if df2 is not None:
                result = df2['lend_chafail_cnt_12m'][0]
                if result is not None:
                    self.variables['lend_chafail_cnt_12m'] = result

    def transform(self):
        df = self._info_loan_stats_df()
        self._lend_score(df=df)
        self._lend_fail_rate(df=df)
        self._lend_cha_cnt_12m(df=df)
        self._lend_chafail_cnt_12m(df=df)
