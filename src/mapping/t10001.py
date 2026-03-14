from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df
import numpy as np


class T10001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ovdu_sco_1y': 0,
            'ovdu_sco': 0,
            'ovdu_sco_time': 0
        }

    def _info_risk_overdue_df(self):
        info_certification = """
            SELECT b.risk_score, b.data_build_time, a.create_time FROM info_risk_overdue_record b,
            (SELECT risk_overdue_id,create_time FROM info_risk_overdue WHERE user_name = %(user_name)s 
            AND id_card_no = %(id_card_no)s
            AND unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY id  desc LIMIT 1) a
            WHERE b.risk_overdue_id = a.risk_overdue_id
            AND b.risk_score is not NULL
            AND b.risk_score != ''
            ORDER BY b.risk_score desc
            ;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        self._subtract_by_year(df)
        return df

    def _subtract_by_year(self, df):
        self.diff_year = subtract_datetime_col(df, 'create_time', 'data_build_time', 'Y')

    def _ovdu_sco_1y(self, df=None):
        """
        逾期核查_近1年内最大风险得分
        :param user_name:
        :param id_card_no:
        """
        if df is not None and len(df) > 0:
            df_out = df.query(self.diff_year + ' < 1')
            if df_out is not None and len(df_out) > 0:
                df_out['risk_score'].replace('', 0, inplace=True)
                df_out = df_out.fillna(0)
                self.variables['ovdu_sco_1y'] = df_out['risk_score'].max()

    def _ovdu_sco(self, df=None):
        """
        逾期核查_最大风险得分
        :param df:
        :return:
        """
        if df is not None and len(df) > 0:
            df['risk_score'].replace('', 0, inplace=True)
            df = df.fillna(0)
            self.variables['ovdu_sco'] = int(df['risk_score'].max())

    def _ovdu_sco_time(self, df=None):
        """
        逾期核查_最大风险得分
        :param df:
        :return:
        """
        if df is not None and len(df) > 0:
            # self.variables['ovdu_sco_time'] = df[self.diff_year][0]
            year = df[self.diff_year][0]
            if year < 1:
                self.variables['ovdu_sco_time'] = 1
            elif 1 <= year < 2:
                self.variables['ovdu_sco_time'] = 2
            elif 2 <= year < 3:
                self.variables['ovdu_sco_time'] = 3
            elif year >= 3:
                self.variables['ovdu_sco_time'] = 4

    def transform(self):
        df = self._info_risk_overdue_df()
        self._ovdu_sco_1y(df)
        self._ovdu_sco(df)
        self._ovdu_sco_time(df)