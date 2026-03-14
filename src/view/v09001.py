from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df


class V09001(Transformer):
    """
    他贷核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'oth_loan_apply_bank_7d': 0,
            'oth_loan_apply_bank_1m': 0,
            'oth_loan_apply_bank_3m': 0,
            'oth_loan_apply_sloan_7d': 0,
            'oth_loan_apply_sloan_1m': 0,
            'oth_loan_apply_sloan_3m': 0,
            'oth_loan_apply_p2p_7d': 0,
            'oth_loan_apply_p2p_1m': 0,
            'oth_loan_apply_p2p_3m': 0,
            'oth_loan_apply_confin_7d': 0,
            'oth_loan_apply_confin_1m': 0,
            'oth_loan_apply_confin_3m': 0,
            'oth_loan_apply_other_7d': 0,
            'oth_loan_apply_other_1m': 0,
            'oth_loan_apply_other_3m': 0,
            'oth_loan_apply_bank_6m': 0,
            'oth_loan_apply_bank_12m': 0,
            'oth_loan_apply_bank_his': 0,
            'oth_loan_apply_sloan_6m': 0,
            'oth_loan_apply_sloan_12m': 0,
            'oth_loan_apply_sloan_his': 0,
            'oth_loan_apply_p2p_6m': 0,
            'oth_loan_apply_p2p_12m': 0,
            'oth_loan_apply_p2p_his': 0,
            'oth_loan_apply_confin_6m': 0,
            'oth_loan_apply_confin_12m': 0,
            'oth_loan_apply_confin_his': 0,
            'oth_loan_apply_other_6m': 0,
            'oth_loan_apply_other_12m': 0,
            'oth_loan_apply_other_his': 0
        }

    def _risk_other_loan_record_df(self):
        info_risk_other_loan_record = """
            SELECT industry,data_build_time,create_time
            FROM info_risk_other_loan_record WHERE other_loan_id = (SELECT other_loan_id FROM info_risk_other_loan WHERE id_card_no = %(id_card_no)s 
            and user_name = %(user_name)s  and unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY id  desc LIMIT 1)
            and reason_code in ('01','99')
        """
        df = sql_to_df(sql=info_risk_other_loan_record,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        self.dff_day = subtract_datetime_col(df, 'create_time', 'data_build_time', 'D')
        self.dff_month = subtract_datetime_col(df, 'create_time', 'data_build_time', 'M')
        return df

    def _ps_risk_other_loan_record(self, df=None):
        if df is not None and len(df) > 0:
            day_7_df = df.query(self.dff_day + ' < 7')
            if day_7_df is not None and len(day_7_df) > 0:
                oth_loan_apply_bank_7d = len(day_7_df.query('industry=="BAK"'))
                self.variables['oth_loan_apply_bank_7d'] = oth_loan_apply_bank_7d
                oth_loan_apply_sloan_7d = len(day_7_df.query('industry=="MCL"'))
                self.variables['oth_loan_apply_sloan_7d'] = oth_loan_apply_sloan_7d
                oth_loan_apply_p2p_7d = len(day_7_df.query('industry=="P2P"'))
                self.variables['oth_loan_apply_p2p_7d'] = oth_loan_apply_p2p_7d
                oth_loan_apply_confin_7d = len(day_7_df.query('industry=="CNS"'))
                self.variables['oth_loan_apply_confin_7d'] = oth_loan_apply_confin_7d
                self.variables['oth_loan_apply_other_7d'] = len(
                    day_7_df) - oth_loan_apply_bank_7d - oth_loan_apply_sloan_7d - oth_loan_apply_p2p_7d - oth_loan_apply_confin_7d

            month_1_df = df.query(self.dff_month + ' < 1')
            if month_1_df is not None and len(month_1_df) > 0:
                oth_loan_apply_bank_1m = len(month_1_df.query('industry=="BAK"'))
                self.variables['oth_loan_apply_bank_1m'] = oth_loan_apply_bank_1m
                oth_loan_apply_sloan_1m = len(month_1_df.query('industry=="MCL"'))
                self.variables['oth_loan_apply_sloan_1m'] = oth_loan_apply_sloan_1m
                oth_loan_apply_p2p_1m = len(month_1_df.query('industry=="P2P"'))
                self.variables['oth_loan_apply_p2p_1m'] = oth_loan_apply_p2p_1m
                oth_loan_apply_confin_1m = len(month_1_df.query('industry=="CNS"'))
                self.variables['oth_loan_apply_confin_1m'] = oth_loan_apply_confin_1m
                self.variables['oth_loan_apply_other_1m'] = len(
                    month_1_df) - oth_loan_apply_bank_1m - oth_loan_apply_sloan_1m - oth_loan_apply_p2p_1m - oth_loan_apply_confin_1m

            month_3_df = df.query(self.dff_month + ' < 3')
            if month_3_df is not None and len(month_3_df) > 0:
                oth_loan_apply_bank_3m = len(month_3_df.query('industry=="BAK"'))
                self.variables['oth_loan_apply_bank_3m'] = oth_loan_apply_bank_3m
                oth_loan_apply_sloan_3m = len(month_3_df.query('industry=="MCL"'))
                self.variables['oth_loan_apply_sloan_3m'] = oth_loan_apply_sloan_3m
                oth_loan_apply_p2p_3m = len(month_3_df.query('industry=="P2P"'))
                self.variables['oth_loan_apply_p2p_3m'] = oth_loan_apply_p2p_3m
                oth_loan_apply_confin_3m = len(month_3_df.query('industry=="CNS"'))
                self.variables['oth_loan_apply_confin_3m'] = oth_loan_apply_confin_3m
                self.variables['oth_loan_apply_other_3m'] = len(
                    month_3_df) - oth_loan_apply_bank_3m - oth_loan_apply_sloan_3m - oth_loan_apply_p2p_3m - oth_loan_apply_confin_3m

            month_6_df = df.query(self.dff_month + ' < 6')
            if month_6_df is not None and len(month_6_df) > 0:
                oth_loan_apply_bank_6m = len(month_6_df.query('industry=="BAK"'))
                self.variables['oth_loan_apply_bank_6m'] = oth_loan_apply_bank_6m
                oth_loan_apply_sloan_6m = len(month_6_df.query('industry=="MCL"'))
                self.variables['oth_loan_apply_sloan_6m'] = oth_loan_apply_sloan_6m
                oth_loan_apply_p2p_6m = len(month_6_df.query('industry=="P2P"'))
                self.variables['oth_loan_apply_p2p_6m'] = oth_loan_apply_p2p_6m
                oth_loan_apply_confin_6m = len(month_6_df.query('industry=="CNS"'))
                self.variables['oth_loan_apply_confin_6m'] = oth_loan_apply_confin_6m
                self.variables['oth_loan_apply_other_6m'] = len(
                    month_6_df) - oth_loan_apply_bank_6m - oth_loan_apply_sloan_6m - oth_loan_apply_p2p_6m - oth_loan_apply_confin_6m

            month_12_df = df.query(self.dff_month + ' < 12')
            if month_12_df is not None and len(month_12_df) > 0:
                oth_loan_apply_bank_12m = len(month_12_df.query('industry=="BAK"'))
                self.variables['oth_loan_apply_bank_12m'] = oth_loan_apply_bank_12m
                oth_loan_apply_sloan_12m = len(month_12_df.query('industry=="MCL"'))
                self.variables['oth_loan_apply_sloan_12m'] = oth_loan_apply_sloan_12m
                oth_loan_apply_p2p_12m = len(month_12_df.query('industry=="P2P"'))
                self.variables['oth_loan_apply_p2p_12m'] = oth_loan_apply_p2p_12m
                oth_loan_apply_confin_12m = len(month_12_df.query('industry=="CNS"'))
                self.variables['oth_loan_apply_confin_12m'] = oth_loan_apply_confin_12m
                self.variables['oth_loan_apply_other_12m'] = len(
                    month_12_df) - oth_loan_apply_bank_12m - oth_loan_apply_sloan_12m - oth_loan_apply_p2p_12m - oth_loan_apply_confin_12m

            oth_loan_apply_bank_his = len(df.query('industry=="BAK"'))
            self.variables['oth_loan_apply_bank_his'] = oth_loan_apply_bank_his
            oth_loan_apply_sloan_his = len(df.query('industry=="MCL"'))
            self.variables['oth_loan_apply_sloan_his'] = oth_loan_apply_sloan_his
            oth_loan_apply_p2p_his = len(df.query('industry=="P2P"'))
            self.variables['oth_loan_apply_p2p_his'] = oth_loan_apply_p2p_his
            oth_loan_apply_confin_his = len(df.query('industry=="CNS"'))
            self.variables['oth_loan_apply_confin_his'] = oth_loan_apply_confin_his
            self.variables['oth_loan_apply_other_his'] = len(
                df) - oth_loan_apply_bank_his - oth_loan_apply_sloan_his - oth_loan_apply_p2p_his - oth_loan_apply_confin_his

    def transform(self):
        risk_other_loan_record_df = self._risk_other_loan_record_df()
        self._ps_risk_other_loan_record(df=risk_other_loan_record_df)
