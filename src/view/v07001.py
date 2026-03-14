import numpy as np
import pandas as pd

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class V07001(Transformer):
    """
    信贷统计
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'loan_analyst_overdue_time_amt': None
        }

    # 读取目标数据集
    def _info_loan_stats_df(self):
        sql = """
               SELECT recent_months,low_balance_fail_count,success_pay_avg_amount 
               FROM info_loan_stats_pay  
               WHERE  loan_stats_id 
               IN (
                   SELECT fv.id 
                   FROM (
                       SELECT id 
                       FROM info_loan_stats 
                       WHERE 
                           user_name = %(user_name)s 
                           AND id_card_no = %(id_card_no)s 
                           AND phone = %(phone)s
                           AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                       ORDER BY id DESC 
                       LIMIT 1
                   ) as fv
               ) and success_pay_avg_amount IS NOT NULL and low_balance_fail_count IS NOT NULL;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算字段
    def _info_loan_stats(self, df=None):
        df.dropna(axis=0, how='any', inplace=True)
        if df is not None and len(df) > 0:
            df['recent_mth'] = df['recent_months'].map({'RECENTLY_1M': '最近1个月', 'RECENTLY_3M': '最近3个月',
                                                        'RECENTLY_6M': '最近6个月', 'RECENTLY_12M': '最近12个月', '': ''})
            df = df.fillna(0)

            df['value'] = df.apply(lambda x: x['low_balance_fail_count'] * x['success_pay_avg_amount'], axis=1)
            df['overdue_value'] = pd.cut(df['value'],
                                         [-1, 0, 1999.99, 4999.99, 9999.99, 29999.99, 49999.99, 99999.99, np.inf],
                                         right=True,
                                         labels=['', '0~0.2万', '0.2万~0.5万', '0.5万~1万', '1万~3万', '3万~5万', '5万~10万',
                                                 '10万以上'])
            df['list'] = df.apply(lambda x: x['recent_mth'] + ':' + x['overdue_value'], axis=1)
            if len(df[df['value'] > 0]) > 0:
                self.variables['loan_analyst_overdue_time_amt'] = df[df['value'] > 0]['list'].tolist()


    def transform(self):
        self._info_loan_stats(self._info_loan_stats_df())
