from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def get_field_value(df, key):
    df_temp = df[df['field_name'] == key]
    if not df_temp.empty:
        return float(df_temp['field_value'].to_list()[0])
    else:
        if key == "FIN_Loan_uninstall_0M_1M":
            return None
        return 0


class T35001(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'FIN_Loan_uninstall_0M_1M':None,
            'FIN_Loan_all_0M_1M': 0,
            'FIN_Debit_PCT_all_1M_2M': 0,
            'FIN_Loan_Small_uninstall_0M_1M': 0,
            'FIN_Loan_Small_all_0M_1M': 0,
            'FIN_Loan_P2P_uninstall_0M_1M': 0,
            'FIN_Loan_P2P_all_0M_1M': 0
        }

    def _info_risk_factor_item(self):
        sql = '''
            select field_name,field_value from info_risk_factor_item where risk_factor_id = (
                select id from info_risk_factor where mobile = %(mobile)s 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            )
        '''
        df = sql_to_df(sql=sql,
                       params={"mobile": self.phone})
        return df

    def clean_variables(self):
        df = self._info_risk_factor_item()
        if df.empty:
            return
        self.variables['FIN_Loan_uninstall_0M_1M'] = get_field_value(df, 'FIN_Loan_uninstall_0M_1M')
        self.variables['FIN_Loan_all_0M_1M'] = get_field_value(df, 'FIN_Loan_all_0M_1M')
        self.variables['FIN_Debit_PCT_all_1M_2M'] = get_field_value(df, 'FIN_Debit_PCT_all_1M_2M')
        self.variables['FIN_Loan_Small_uninstall_0M_1M'] = get_field_value(df, 'FIN_Loan_Small_uninstall_0M_1M')
        self.variables['FIN_Loan_Small_all_0M_1M'] = get_field_value(df, 'FIN_Loan_Small_all_0M_1M')
        self.variables['FIN_Loan_P2P_uninstall_0M_1M'] = get_field_value(df, 'FIN_Loan_P2P_uninstall_0M_1M')
        self.variables['FIN_Loan_P2P_all_0M_1M'] = get_field_value(df, 'FIN_Loan_P2P_all_0M_1M')

    def transform(self):
        self.clean_variables()
