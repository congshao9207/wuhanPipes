from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import numpy as np
import pandas as pd


def get_field_value(df, key):
    df_temp = df[df['field_name'] == key]
    if not df_temp.empty and pd.notna(df_temp['field_value'].to_list()[0]):
        return np.round(float(df_temp['field_value'].to_list()[0]),4)
    else:
        return 0


class T34001(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'cts_act_047': 0,
            'cts_act_044': 0,
            'cts_act_156': 0,
            'cts_reg_008': 0,
            'cts_app_114': 0,
            'cts_app_254': 0,
            'cts_app_135': 0,
            'cts_app_145': 0,
            'cts_msg_017': 0
        }

    def _info_risk_cts_item(self):
        sql = '''
            select field_name,field_value from info_risk_cts_item where risk_cts_id = (
                select id from info_risk_cts where mobile = %(mobile)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            )
        '''
        df = sql_to_df(sql=sql,
                       params={"mobile": self.phone})
        return df

    def clean_variables(self):
        df = self._info_risk_cts_item()
        if df.empty:
            return
        self.variables['cts_act_047'] = get_field_value(df, 'cts_act_047')
        self.variables['cts_act_044'] = get_field_value(df, 'cts_act_044')
        self.variables['cts_act_156'] = get_field_value(df, 'cts_act_156')
        self.variables['cts_reg_008'] = get_field_value(df, 'cts_reg_008')
        self.variables['cts_app_114'] = get_field_value(df, 'cts_app_114')
        self.variables['cts_app_254'] = get_field_value(df, 'cts_app_254')
        self.variables['cts_app_135'] = get_field_value(df, 'cts_app_135')
        self.variables['cts_app_145'] = get_field_value(df, 'cts_app_145')
        self.variables['cts_msg_017'] = get_field_value(df, 'cts_msg_017')

    def transform(self):
        self.clean_variables()
