from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def get_field_value(df, key1, key2):
    df_temp = df[(df['hit_type'] == key1) & (df['field_name'] == key2)]
    if not df_temp.empty:
        return float(df_temp['field_value'].to_list()[0])
    else:
        return 0


class T12002(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'bqs_cert_1m_org_cnt': 0,
            'bqs_cert_2m_petty_org_cnt': 0,
            'bqs_cert_3m_oth_org_cnt': 0,
            'bqs_cert_1m_oth_org_cnt': 0,
            'bqs_cert_2m_org_cnt': 0,
            'bqs_tel_3m_org_cnt': 0
        }

    def _info_stats_item(self):
        sql = '''
            select hit_type,field_name,field_value from info_stats_item where stats_id = (
                select id from info_stats where user_name = %(user_name)s and id_card_no = %(id_card_no)s
                and channel_api_no = '61001'
                and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 
            )
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def clean_variables(self):
        df = self._info_stats_item()
        self.variables['bqs_cert_1m_org_cnt'] = get_field_value(df, 'CERT_NO', 'monthly_multi_count_m01')
        self.variables['bqs_cert_2m_petty_org_cnt'] = get_field_value(df, 'CERT_NO',
                                                                      'monthly_mini_cash_loan_multi_count_m02')
        self.variables['bqs_cert_3m_oth_org_cnt'] = get_field_value(df, 'CERT_NO',
                                                                    'monthly_other_business_multi_count_m03')
        self.variables['bqs_cert_1m_oth_org_cnt'] = get_field_value(df, 'CERT_NO',
                                                                    'monthly_other_business_multi_count_m01')
        self.variables['bqs_cert_2m_org_cnt'] = get_field_value(df, 'CERT_NO', 'monthly_multi_count_m02')
        self.variables['bqs_tel_3m_org_cnt'] = get_field_value(df, 'MOBILE', 'monthly_multi_count_m02')

    def transform(self):
        self.clean_variables()
