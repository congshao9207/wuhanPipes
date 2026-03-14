import pandas as pd

from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class V13001(Transformer):
    """
    短信核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'sms_reg_org_cnt': 0,  # 短信核查_注册总机构次数
            'sms_app_org_cnt': 0,  # 短信核查_申请总机构次数
            'sms_loan_org_cnt': 0,  # 短信核查_放款总机构次数
            'sms_reject_org_cnt': 0,  # 短信核查_驳回总机构次数
            'sms_loan_amout_sum': None,  # 短信核查_放款总金额
            'sms_loan_amout_avg': None,  # 短信核查_放款笔均金额
            'sms_overdue_time_amt': None,  # 短信核查_逾期时间_金额
            'sms_debt_time_amt': None  # 短信核查_欠款时间_金额
        }

    # 获取目标数据集1
    def _info_sms_loan_platform(self):

        sql = '''
            SELECT sms_id,platform_code 
            FROM info_sms_loan_platform  
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name,
                                        "id_card_no": self.id_card_no,
                                        "phone": self.phone})
        return df

    # 短信核查_注册总机构次数
    def _sms_reg_org_cnt(self, df=None):
        if not df.empty:
            df = df.drop_duplicates(subset=['platform_code'], keep='first')
            self.variables['sms_reg_org_cnt'] = len(df)

    # 获取目标数据集2
    def _info_sms_loan_apply(self):
        sql = '''
            SELECT sms_id, platform_code
            FROM info_sms_loan_apply
            WHERE sms_id 
            IN (
                SELECT sms.sms_id
                FROM (
                    SELECT sms_id
                    FROM info_sms
                    WHERE 
                        user_name = %(user_name)s
                        AND id_card_no = %(id_card_no)s
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC
                    LIMIT 1
                ) sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算短信核查_申请总机构次数
    def _sms_app_org_cnt(self, df=None):
        df = df.drop_duplicates(subset=['platform_code'], keep='first')
        self.variables['sms_app_org_cnt'] = len(df)

    # 获取目标数据集3
    def _info_sms_loan(self):

        sql = '''
            SELECT sms_id,platform_code,loan_amount
            FROM info_sms_loan 
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算短信核查_放款总机构次数
    def _sms_loan_org_cnt(self, df=None):
        df = df.drop_duplicates(subset=['platform_code'], keep='first')
        self.variables['sms_loan_org_cnt'] = len(df)

    # 计算短信核查_放款总金额
    def _sms_loan_amout_sum(self, df=None):
        if len(df) != 0:
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0W～0.2W", value=0.1)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0.2W～0.5W", value=0.35)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0.5W～1W", value=0.75)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="1W～3W", value=2)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="3W～5W", value=4)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="5W～10W", value=7.5)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="10W以上", value=15)
            self.variables['sms_loan_amout_sum'] = str(df['loan_amount'].sum()) + 'W'

    # 计算短信核查_放款笔均金额
    def _sms_loan_amout_avg(self, df=None):
        if len(df) != 0:
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0W～0.2W", value=0.1)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0.2W～0.5W", value=0.35)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0.5W～1W", value=0.75)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="1W～3W", value=2)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="3W～5W", value=4)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="5W～10W", value=7.5)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="10W以上", value=15)
            self.variables['sms_loan_amout_avg'] = str(round(df['loan_amount'].mean(), 1)) + 'W'

    # 获取目标数据集4
    def _info_sms_loan_reject(self):

        sql = '''
            SELECT sms_id,platform_code
            FROM info_sms_loan_reject 
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算短信核查_驳回总机构次数
    def _sms_reject_org_cnt(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['platform_code'], keep='first')
            self.variables['sms_reject_org_cnt'] = len(df)

    # 获取目标数据集5
    def _info_sms_overdue_platform(self):

        sql = '''
            SELECT a.overdue_money,a.overdue_time,b.create_time
            FROM info_sms_overdue_platform a 
            LEFT JOIN info_sms b
            ON a.sms_id = b.sms_id
            WHERE a.sms_id
            IN (
                SELECT sms.sms_id
                FROM (
                    SELECT sms_id
                    FROM info_sms
                    WHERE
                        user_name = %(user_name)s
                        AND id_card_no = %(id_card_no)s
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'overdue_time', 'M')]
        return df

    # 计算短信核查_逾期时间_金额
    def _sms_overdue_time_amt(self, df=None):
        df = df.dropna(subset=['overdue_money'], how='any')
        if df is not None and len(df) > 0:
            new_list = list()
            for i in range(len(df)):
                if df.iloc[i, 4] < 1:
                    new_list.append('最近1个月' + ':' + df.iloc[i, 0])
                elif df.iloc[i, 4] < 3:
                    new_list.append('最近3个月' + ':' + df.iloc[i, 0])
                elif df.iloc[i, 4] < 6:
                    new_list.append('最近6个月' + ':' + df.iloc[i, 0])
                elif df.iloc[i, 4] < 12:
                    new_list.append('最近12个月' + ':' + df.iloc[i, 0])
            if len(new_list) > 0:
                self.variables['sms_overdue_time_amt'] = new_list

    # 获取目标数据集6
    def _info_sms_debt(self):

        sql1 = '''
            SELECT sms_id,platform_code,debt_money
            FROM info_sms_debt
            WHERE sms_id
            IN (
                SELECT sms.sms_id
                FROM (
                    SELECT sms_id
                    FROM info_sms
                    WHERE
                        user_name = %(user_name)s
                        AND id_card_no = %(id_card_no)s
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC
                    LIMIT 1
                ) as sms
            );
        '''
        sql2 = '''
             SELECT platform_code,overdue_time
             FROM info_sms_overdue_platform
             WHERE sms_id
             IN (
                 SELECT sms.sms_id
                 FROM (
                     SELECT sms_id
                     FROM info_sms
                     WHERE
                         user_name = %(user_name)s
                         AND id_card_no = %(id_card_no)s
                         AND phone = %(phone)s
                         AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                     ORDER BY id DESC
                     LIMIT 1
                 ) as sms
             );
        '''
        sql3 = '''
            SELECT sms_id,create_time
            FROM info_sms
            WHERE
                user_name = %(user_name)s
                AND id_card_no = %(id_card_no)s
                AND phone = %(phone)s
                AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
        '''
        df1 = sql_to_df(sql=sql1,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df2 = sql_to_df(sql=sql2,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df3 = sql_to_df(sql=sql3,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        merge1 = pd.merge(df1, df2, how='left', on='platform_code')
        df = pd.merge(merge1, df3, how='left', on='sms_id')
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'overdue_time', 'M')]
        return df

    # 计算短信核查_欠款时间_金额
    def _sms_debt_time_amt(self, df=None):
        df = df.dropna(subset=['debt_money'], how='any')
        if df is not None and len(df) > 0:
            new_list = list()
            for i in range(len(df)):
                if df.iloc[i, 6] < 1:
                    new_list.append('最近1个月' + ':' + df.iloc[i, 2])
                elif df.iloc[i, 6] < 3:
                    new_list.append('最近3个月' + ':' + df.iloc[i, 2])
                elif df.iloc[i, 6] < 6:
                    new_list.append('最近6个月' + ':' + df.iloc[i, 2])
                elif df.iloc[i, 6] < 12:
                    new_list.append('最近12个月' + ':' + df.iloc[i, 2])
            if len(new_list) > 0:
                self.variables['sms_debt_time_amt'] = new_list

    #  执行变量转换
    def transform(self):
        self._sms_reg_org_cnt(self._info_sms_loan_platform())
        self._sms_app_org_cnt(self._info_sms_loan_apply())
        info_sms_loan = self._info_sms_loan()
        if info_sms_loan is not None and len(info_sms_loan) > 0:
            self._sms_loan_org_cnt(info_sms_loan)
            self._sms_loan_amout_sum(info_sms_loan)
            self._sms_loan_amout_avg(info_sms_loan)
        self._sms_reject_org_cnt(self._info_sms_loan_reject())
        self._sms_overdue_time_amt(self._info_sms_overdue_platform())
        self._sms_debt_time_amt(self._info_sms_debt())
