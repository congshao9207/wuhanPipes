from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def check(df, key1, key2):
    df_temp = df[(df['field_name'] == key1) & (df['field_value'] == key2)]
    if not df_temp.empty:
        return 1
    else:
        return 0


class T12003(Transformer):
    """
       白骑士特殊识别
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'bqs_risk_cert_cnt': 0,
            'bqs_risk_tele_cnt': 0,
            'bqs_net_tele_cnt': 0,
            'bqs_net_cert_cnt': 0,
            'bqs_cross_event_risk': 0,
            'bqs_cross_equip_risk': 0,
            'bqs_model_middle_risk': 0,
            'bqs_model_high_risk': 0
        }

    def _info_stats_item(self):
        sql = '''
            select field_name,field_value from info_stats_item where stats_id = (
                select id from info_stats where user_name = %(user_name)s and id_card_no = %(id_card_no)s
                and (mobile_hit_type = '1' or cert_no_hit_type = '1')
                and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 
            )
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def clean_variables(self):
        df = self._info_stats_item()
        self.variables['bqs_risk_cert_cnt'] = check(df, 'secondType', '13001')
        self.variables['bqs_risk_tele_cnt'] = check(df, 'secondType', '13002')
        self.variables['bqs_net_tele_cnt'] = check(df, 'secondType', '32001')
        self.variables['bqs_net_cert_cnt'] = check(df, 'secondType', '32002')
        self.variables['bqs_cross_event_risk'] = check(df, 'secondType', '60001')
        self.variables['bqs_cross_equip_risk'] = check(df, 'secondType', '60002')
        self.variables['bqs_model_middle_risk'] = check(df, 'secondType', '61001')
        self.variables['bqs_model_high_risk'] = check(df, 'secondType', '61002')

    def transform(self):
        self.clean_variables()
