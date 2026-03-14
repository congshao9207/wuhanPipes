from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


def get_filed_value(df, key1, key2):
    df_temp = df[(df['group_name'] == key1) & (df['field_name'] == key2)]
    if not df_temp.empty:
        v = df_temp['field_value'].to_list()[0]
        return v if v and v != "" else 0

    else:
        return 0


class T33001(Transformer):

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'yf_repayment_index': 0,
            'yf_large_loan_installment_score': 0,
            'yf_small_loan_installment_score': 0,
            'yf_small_loan_guar_score': 0
        }

    def _info_loan_statistics_item(self):
        sql = '''
            select group_name,field_name,field_value from info_loan_statistics_item where loan_statistics_id = (
                select id from info_loan_statistics where user_name = %(user_name)s AND id_card_no = %(id_card_no)s 
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
        self.variables['yf_repayment_index'] = float(get_filed_value(df, 'general_info', 'performance_amount_index'))
        self.variables['yf_large_loan_installment_score'] = int(get_filed_value(df, 'general_info', 'large_stages_score'))
        self.variables['yf_small_loan_installment_score'] = int(get_filed_value(df, 'general_info', 'small_stages_score'))
        self.variables['yf_small_loan_guar_score'] = int(get_filed_value(df, 'general_info', 'loan_guarantee_score'))

    def transform(self):
        self.clean_variables()
