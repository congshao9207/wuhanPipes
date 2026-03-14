import time

from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df


class T09001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'oth_loan_apro_cnt_6m': 0,
            'oth_loan_hit_org_cnt': 0,
            'oth_loan_hit_bank_cnt': 0,
            'oth_loan_hit_finance_cnt': 0,
            'oth_loan_hit_p2p_cnt': 0,
            'oth_loan_hit_org_cnt_3m': 0,
            'oth_loan_query_mac_cnt_6m': 0,
            'oth_loan_query_mac_cnt_3m': 0,
            'oth_loan_query_amount': 0,
            'oth_loan_apro_cnt_12m': 0,  # 近12个月贷款审批申请次数
        }

    def _loan_other_df(self):
        info_loan_other = """
            SELECT reason_code,industry,amount,bank_amount,
            consumer_finance_amount,p_2_p_amount,query_amount,query_amount_three_month,query_amount_six_month,data_build_time,create_time
            FROM info_risk_other_loan_record WHERE other_loan_id = (SELECT other_loan_id FROM info_risk_other_loan WHERE id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY id  desc LIMIT 1) LIMIT 1
        """
        df = sql_to_df(sql=info_loan_other, params={"id_card_no": self.id_card_no})
        return df

    # 该实现为实现细节
    def _oth_loan_apro_cnt_12m_backup(self):
        info_other_loan_sql = '''
                select other_loan_id, create_time  from info_risk_other_loan 
                    where id_card_no = %(id_card_no)s and 
                        unix_timestamp(NOW()) < unix_timestamp(expired_at) ORDER BY id  desc LIMIT 1
              '''

        df = sql_to_df(sql=info_other_loan_sql, params={"id_card_no": self.id_card_no})
        if df is None or df.empty:
            return

        other_loan_id = df.iloc[0].other_loan_id
        create_time = df.iloc[0].create_time
        end_line_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time.timestamp() - (365 * 24 * 60 * 60)))
        info_oth_loan_detail_sql = '''
                            select count(*) total_count from info_risk_other_loan_record where 
                                other_loan_id = %(oth_loan_id)s
                                and data_build_time > %(end_line_date)s 
                                and reason_code = %(reason_code)s
                        '''

        df = sql_to_df(sql=info_oth_loan_detail_sql, params={"oth_loan_id": str(other_loan_id),
                                                             "end_line_date": end_line_date,
                                                             "reason_code": "01"})
        return df

    def _oth_loan_apro_cnt_12m(self):
        info_other_loan_sql = '''
                select count(*) total_count from(
                    select other_loan_id, create_time  from info_risk_other_loan 
                    where id_card_no = %(id_card_no)s and 
                        unix_timestamp(NOW()) < unix_timestamp(expired_at) ORDER BY id  desc LIMIT 1
                    ) tab left join info_risk_other_loan_record olr on olr.other_loan_id = tab.other_loan_id 
                        where olr.data_build_time >= DATE_ADD(tab.create_time, Interval -1 year) and reason_code="01"
              '''

        df = sql_to_df(sql=info_other_loan_sql, params={"id_card_no": self.id_card_no})
        return df

    def _ps_loan_other(self, df=None):
        if df is not None and len(df) > 0:
            df = df.fillna(0)
            self.variables['oth_loan_hit_org_cnt'] = df['amount'][0]
            self.variables['oth_loan_hit_bank_cnt'] = df['bank_amount'][0]
            self.variables['oth_loan_hit_finance_cnt'] = df['consumer_finance_amount'][0]
            self.variables['oth_loan_hit_p2p_cnt'] = df['p_2_p_amount'][0]
            self.variables['oth_loan_query_mac_cnt_6m'] = df['query_amount_six_month'][0]
            self.variables['oth_loan_query_mac_cnt_3m'] = df['query_amount_three_month'][0]
            self.variables['oth_loan_query_amount'] = df['query_amount'][0]

    def _loan_date_df(self):
        info_loan_other = """
        SELECT B.data_build_time as data_build_time,B.reason_code as reason_code,A.create_time as create_time FROM info_risk_other_loan_record B ,(
            SELECT other_loan_id,create_time FROM info_risk_other_loan WHERE id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY id  desc LIMIT 1) A
        WHERE B.other_loan_id = A.other_loan_id            
        """
        df = sql_to_df(sql=info_loan_other,
                       params={"id_card_no": self.id_card_no})
        self.diff_month = subtract_datetime_col(df, 'create_time', 'data_build_time', 'M')
        return df

    def _ps_loan_date(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['oth_loan_apro_cnt_6m'] = df.query(self.diff_month + ' < 6 and reason_code=="01" ').shape[0]
            self.variables['oth_loan_hit_org_cnt_3m'] = df.query(self.diff_month + '< 3').shape[0]

    def _ps_loan_apro_cnt_12m(self, df=None):
        if df is not None and not df.empty:
            self.variables["oth_loan_apro_cnt_12m"] = df.iloc[0].total_count

    def transform(self):
        self._ps_loan_other(df=self._loan_other_df())
        self._ps_loan_date(df=self._loan_date_df())
        self._ps_loan_apro_cnt_12m(df=self._oth_loan_apro_cnt_12m())
