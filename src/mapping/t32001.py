from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def get_filed_value(df, key):
    df_temp = df[df['field_name'] == key]
    if not df_temp.empty:
        v = df_temp['field_value'].to_list()[0]
        return int(v) if v and v != "" else 0
    else:
        if key == "reg_unbank_history_day":
            return None
        return 0


class T32001(Transformer):

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'yf_bank_min_loan_max_interval_days': 0,
            'yf_overdue_org_cnt': 0,
            'yf_overdue_cnt': 0,
            'yf_non_bank_apply_org_cnt': 0,
            'yf_loan_org_24m_cnt': 0,
            'reg_unbank_history_day': None,
            'yf_loan_small_money_history_day':0
        }

    def _info_loan_statistics_item(self):
        sql = '''
            select field_name,field_value from info_risk_backtracking_item where risk_backtracking_id = (
                select id from info_risk_backtracking where user_name = %(user_name)s AND id_card_no = %(id_card_no)s 
                AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC LIMIT 1
            )
        '''
        df = sql_to_df(sql=sql,
                        params={"user_name": self.user_name,
                                "id_card_no": self.id_card_no})
        return df

    def clean_variables(self):
        df = self._info_loan_statistics_item()
        if df.empty:
            return
        self.variables['yf_bank_min_loan_max_interval_days'] = get_filed_value(df, 'loan_bank_small_money_history_day')
        self.variables['yf_overdue_org_cnt'] = get_filed_value(df, 'arrearage_platform_counts')
        self.variables['yf_overdue_cnt'] = get_filed_value(df, 'arrearage_counts')
        self.variables['yf_non_bank_apply_org_cnt'] = get_filed_value(df, 'app_unbank_counts')
        self.variables['yf_loan_org_24m_cnt'] = get_filed_value(df, 'loan_platform_month24')
        self.variables['reg_unbank_history_day'] = get_filed_value(df, 'reg_unbank_history_day')
        self.variables['yf_loan_small_money_history_day'] = get_filed_value(df, 'loan_small_money_history_day')

    def transform(self):
        self.clean_variables()

