import pandas as pd

from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class V10001(Transformer):
    """
    逾期核查展示
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ovdu_overdue_time_amt': None
        }

    # 读取目标数据集
    def _info_risk_overdue_df(self):
        sql = """
            SELECT b.risk_score, b.data_build_time, a.create_time 
            FROM info_risk_overdue_record b
            LEFT JOIN info_risk_overdue a 
            ON b.risk_overdue_id = a.risk_overdue_id
            WHERE a.risk_overdue_id = (
                SELECT risk_overdue_id FROM info_risk_overdue
                WHERE unix_timestamp(NOW())  < unix_timestamp(expired_at)
                    AND user_name = %(user_name)s 
                    AND id_card_no = %(id_card_no)s
                ORDER BY id DESC 
                LIMIT 1
                );
        """
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'data_build_time', 'M')]
        return df

    # 计算“逾期核查_逾期时间_金额”字段
    def _ovdu_overdue_time_amt(self, df=None):
        new_list = list()
        df.dropna(axis=0, how='any', inplace=True)
        if df is not None and len(df) > 0:
            df['risk_score'] = df['risk_score'].replace(to_replace='11', value='0W～0.1W')
            df['risk_score'] = df['risk_score'].replace(to_replace='12', value='0.1W～0.5W')
            df['risk_score'] = df['risk_score'].replace(to_replace='13', value='0.5W～2W')
            df['risk_score'] = df['risk_score'].replace(to_replace='14', value='2W～10W')
            df['risk_score'] = df['risk_score'].replace(to_replace='15', value='10W以上')
            df['risk_score'] = df['risk_score'].replace(to_replace='21', value='0W～0.1W')
            df['risk_score'] = df['risk_score'].replace(to_replace='22', value='0.1W～0.5W')
            df['risk_score'] = df['risk_score'].replace(to_replace='23', value='0.5W～2W')
            df['risk_score'] = df['risk_score'].replace(to_replace='24', value='2W～10W')
            df['risk_score'] = df['risk_score'].replace(to_replace='25', value='10W以上')
            df['risk_score'] = df['risk_score'].replace(to_replace='31', value='0W～0.1W')
            df['risk_score'] = df['risk_score'].replace(to_replace='32', value='0.1W～0.5W')
            df['risk_score'] = df['risk_score'].replace(to_replace='33', value='0.5W～2W')
            df['risk_score'] = df['risk_score'].replace(to_replace='34', value='2W～10W')
            df['risk_score'] = df['risk_score'].replace(to_replace='35', value='10W以上')
            df['risk_score'] = df['risk_score'].replace(to_replace='41', value='0W～0.1W')
            df['risk_score'] = df['risk_score'].replace(to_replace='42', value='0.1W～0.5W')
            df['risk_score'] = df['risk_score'].replace(to_replace='43', value='0.5W～2W')
            df['risk_score'] = df['risk_score'].replace(to_replace='44', value='2W～10W')
            df['risk_score'] = df['risk_score'].replace(to_replace='45', value='10W以上')

            for i in range(len(df)):
                if df.iloc[i, 4] < 1 and df.iloc[i, 0] not in ['10', '20', '30', '40']:
                    new_list.append('最近1个月' + ':' + df.iloc[i, 0])
                elif df.iloc[i, 4] < 3 and df.iloc[i, 0] not in ['10', '20', '30', '40']:
                    new_list.append('最近3个月' + ':' + df.iloc[i, 0])
                elif df.iloc[i, 4] < 6 and df.iloc[i, 0] not in ['10', '20', '30', '40']:
                    new_list.append('最近6个月' + ':' + df.iloc[i, 0])
                elif df.iloc[i, 4] < 12 and df.iloc[i, 0] not in ['10', '20', '30', '40']:
                    new_list.append('最近12个月' + ':' + df.iloc[i, 0])
            if len(new_list) > 0:
                self.variables['ovdu_overdue_time_amt'] = new_list

    def transform(self):
        self._ovdu_overdue_time_amt(self._info_risk_overdue_df())
