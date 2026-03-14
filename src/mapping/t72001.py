import re

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


def get_left_field_value(df, key):
    df_temp = df[df['field_name'] == key]
    if not df_temp.empty:
        value = df_temp['field_value'].to_list()[0]
        if pd.notna(value):
            if value == '-999' or value == '-1111':
                return 0
            else:
                find_list = value 
                if len(find_list) > 0:
                    return float(find_list[0])
                else:
                    return 0
        else:
            return 0
    else:
        return 0


class T72001(Transformer):
    """
    百融多头策略
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ae_m12_id_orgnum_d': 0,
            'ae_m12_id_nbank_orgnum': 0,
            'ae_m6_id_orgnum_d': 0,
            'ae_m12_id_nbank_max_monnum': 0,
            'ae_m6_cell_avg_monnum': 0,
            'ae_m3_cell_nbank_cons_allnum': 0,
            'ae_m6_cell_nbank_avg_monnum': 0,
            'ae_m6_cell_nbank_else_cons_orgnum': 0,
            'ae_m3_id_allnum_d': 0,
            'ae_m1_id_nbank_cons_allnum': 0,
            'ae_m1_cell_nbank_orgnum': 0,
        }

    def _info_risk_model_data(self):
        sql = '''
            select field_name,field_value from info_multi_header_data_item where main_id = (
            select id from info_multi_header_data where user_name = %(user_name)s and id_card_no = %(id_card_no)s
            and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 )
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def clean_risk_model_data(self):
        df = self._info_risk_model_data()
        if df.empty:
            return
        self.variables['ae_m12_id_orgnum_d'] = get_left_field_value(df, 'ae_m12_id_orgnum_d')
        self.variables['ae_m12_id_nbank_orgnum'] = get_left_field_value(df,
                                                                        'ae_m12_id_nbank_orgnum')
        self.variables['ae_m6_id_orgnum_d'] = get_left_field_value(df,
                                                                   'ae_m6_id_orgnum_d')
        self.variables['ae_m12_id_nbank_max_monnum'] = get_left_field_value(df,
                                                                            'ae_m12_id_nbank_max_monnum')
        self.variables['ae_m6_cell_avg_monnum'] = get_left_field_value(df,
                                                                       'ae_m6_cell_avg_monnum')
        self.variables['ae_m3_cell_nbank_cons_allnum'] = get_left_field_value(df,
                                                                              'ae_m3_cell_nbank_cons_allnum')
        self.variables['ae_m6_cell_nbank_avg_monnum'] = get_left_field_value(df,
                                                                             'ae_m6_cell_nbank_avg_monnum')
        self.variables['ae_m6_cell_nbank_else_cons_orgnum'] = get_left_field_value(df,
                                                                                   'ae_m6_cell_nbank_else_cons_orgnum')
        self.variables['ae_m3_id_allnum_d'] = get_left_field_value(df,
                                                                   'ae_m3_id_allnum_d')
        self.variables['ae_m1_id_nbank_cons_allnum'] = get_left_field_value(df,
                                                                            'ae_m1_id_nbank_cons_allnum')
        self.variables['ae_m1_cell_nbank_orgnum'] = get_left_field_value(df,
                                                                         'ae_m1_cell_nbank_orgnum')

    def transform(self):
        self.clean_risk_model_data()
