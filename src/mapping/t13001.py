import pandas as pd
from pandas.tseries import offsets

from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df


class T13001(Transformer):
    """
    短信核查相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'sms_reg_cnt': 0,  # 短信核查_注册总次数
            'sms_reg_cnt_bank_3m': 0,  # 短信核查_近3个月内银行类注册次数
            'sms_reg_cnt_other_3m': 0,  # 短信核查_近3个月内非银行类注册次数
            'sms_app_cnt': 0,  # 短信核查_申请总次数
            'sms_max_apply': 0,  # 短信核查_申请金额最大等级
            'sms_loan_cnt': 0,  # 短信核查_放款总次数
            'sms_max_loan': 0,  # 短信核查_放款金额最大等级
            'sms_reject_cnt': 0,  # 短信核查_驳回总次数
            'sms_overdue_cnt': 0,  # 短信核查_逾期总次数
            'sms_max_overdue': 0,  # 短信核查_逾期金额最大等级
            'sms_owe_cnt': 0,  # 短信核查_欠款总次数
            'sms_max_owe': 0,  # 短信核查_欠款金额最大等级
            'sms_owe_cnt_6m': 0,  # 短信核查_近6个月内欠款次数
            'sms_owe_cnt_6_12m': 0,  # 短信核查_近6-12个月内欠款次数
            'sms_max_owe_6m': 0,  # 短信核查_近6个月内欠款金额最大等级
            'sms_app_cnt_3m': 0,  # 短信核查_近三个月内申请总次数
            'sms_loan_cnt_3m': 0,  # 短信核查_近三个月内放款总次数
            'hd_regi_p2p_3m': 0,
            'hd_loan_non_bank_6m': 0,
            'hd_loan_month_std_24m': 0,
            'hd_apply_month_std_12m': 0,
            'hd_loan_month_std_12m': 0,
            'hd_regi_non_bank_3m': 0,
            'hd_loan_total_weight_amt_9m': 0,
            'regi_month_interval_max_2m': None
        }
        self.two_years_ago = ''
        self.sms_id = ''
        self.end_date = None

    # 获取目标数据集1
    def _info_sms_loan_platform(self):

        sql1 = '''
            SELECT sms_id,platform_type,register_time 
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
        sql2 = '''
            SELECT sms_id,create_time 
            FROM info_sms 
            WHERE 
                user_name = %(user_name)s 
                AND id_card_no = %(id_card_no)s 
                AND phone = %(phone)s 
                AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
        '''
        df1 = sql_to_df(sql=sql1,
                        params={"user_name": self.user_name,
                                "id_card_no": self.id_card_no,
                                "phone": self.phone})
        df2 = sql_to_df(sql=sql2,
                        params={"user_name": self.user_name,
                                "id_card_no": self.id_card_no,
                                "phone": self.phone})
        df = pd.merge(df1, df2, how='left', on='sms_id')
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'register_time', 'M')]
        return df

    # 计算短信核查_注册总次数
    def _sms_reg_cnt(self, df=None):
        self.variables['sms_reg_cnt'] = len(df)

    # 计算短信核查_近3个月内银行类注册次数
    def _sms_reg_cnt_bank_3m(self, df=None):
        if len(df) != 0:
            df_1 = df.loc[(df['date_dif'] < 3) & (df['platform_type'] == 'BANK'), :].copy()
            self.variables['sms_reg_cnt_bank_3m'] = len(df_1)

    # 计算短信核查_近3个月内非银行类注册次数
    def _sms_reg_cnt_other_3m(self, df=None):
        if len(df) != 0:
            df_2 = df.loc[(df['date_dif'] < 3) & (df['platform_type'] == 'NON_BANK'), :].copy()
            self.variables['sms_reg_cnt_other_3m'] = len(df_2)

    # 获取目标数据集2
    def _info_sms_loan_apply(self):

        sql = '''
            SELECT sms_id, apply_amount
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

    # 计算短信核查_申请总次数
    def _sms_app_cnt(self, df=None):

        self.variables['sms_app_cnt'] = len(df)

    # 计算短信核查_申请金额最大等级
    def _sms_max_apply(self, df=None):
        if len(df) != 0:
            df['apply_amount'] = df['apply_amount'].replace(to_replace="0W～0.2W", value=1)
            df['apply_amount'] = df['apply_amount'].replace(to_replace="0.2W～0.5W", value=2)
            df['apply_amount'] = df['apply_amount'].replace(to_replace="0.5W～1W", value=3)
            df['apply_amount'] = df['apply_amount'].replace(to_replace="1W～3W", value=4)
            df['apply_amount'] = df['apply_amount'].replace(to_replace="3W～5W", value=5)
            df['apply_amount'] = df['apply_amount'].replace(to_replace="5W～10W", value=6)
            df['apply_amount'] = df['apply_amount'].replace(to_replace="10W以上", value=7)
            self.variables['sms_max_apply'] = df['apply_amount'].max()

    # 获取目标数据集3
    def _info_sms_loan(self):

        sql = '''
            SELECT sms_id,loan_amount 
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

    # 计算短信核查_放款总次数
    def _sms_loan_cnt(self, df=None):

        self.variables['sms_loan_cnt'] = len(df)

    # 计算短信核查_放款金额最大等级
    def _sms_max_loan(self, df=None):
        if len(df) != 0:
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0W～0.2W", value=1)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0.2W～0.5W", value=2)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="0.5W～1W", value=3)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="1W～3W", value=4)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="3W～5W", value=5)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="5W～10W", value=6)
            df['loan_amount'] = df['loan_amount'].replace(to_replace="10W以上", value=7)
            self.variables['sms_max_loan'] = df['loan_amount'].max()

    # 获取目标数据集4
    def _info_sms_loan_reject(self):

        sql = '''
            SELECT sms_id 
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

    # 计算短信核查_驳回总次数
    def _sms_reject_cnt(self, df=None):

        self.variables['sms_reject_cnt'] = len(df)

    # 获取目标数据集5
    def _info_sms_overdue_platform(self):

        sql = '''
            SELECT sms_id,overdue_money 
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
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算短信核查_逾期总次数
    def _sms_overdue_cnt(self, df=None):

        self.variables['sms_overdue_cnt'] = len(df)

    # 计算短信核查_逾期金额最大等级
    def _sms_max_overdue(self, df=None):
        if len(df) != 0:
            df['overdue_money'] = df['overdue_money'].replace(to_replace="0W～0.2W", value=1)
            df['overdue_money'] = df['overdue_money'].replace(to_replace="0.2W～0.5W", value=2)
            df['overdue_money'] = df['overdue_money'].replace(to_replace="0.5W～1W", value=3)
            df['overdue_money'] = df['overdue_money'].replace(to_replace="1W～3W", value=4)
            df['overdue_money'] = df['overdue_money'].replace(to_replace="3W～5W", value=5)
            df['overdue_money'] = df['overdue_money'].replace(to_replace="5W～10W", value=6)
            df['overdue_money'] = df['overdue_money'].replace(to_replace="10W以上", value=7)
            self.variables['sms_max_overdue'] = df['overdue_money'].max()

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

    # 计算短信核查_欠款总次数
    def _sms_owe_cnt(self, df=None):
        self.variables['sms_owe_cnt'] = len(df)

    # 计算短信核查_欠款金额最大等级
    def _sms_max_owe(self, df=None):
        if len(df) != 0:
            df['debt_money'] = df['debt_money'].replace(to_replace="0W～0.2W", value=1)
            df['debt_money'] = df['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
            df['debt_money'] = df['debt_money'].replace(to_replace="0.5W～1W", value=3)
            df['debt_money'] = df['debt_money'].replace(to_replace="1W～3W", value=4)
            df['debt_money'] = df['debt_money'].replace(to_replace="3W～5W", value=5)
            df['debt_money'] = df['debt_money'].replace(to_replace="5W～10W", value=6)
            df['debt_money'] = df['debt_money'].replace(to_replace="10W以上", value=7)
            self.variables['sms_max_owe'] = df['debt_money'].max()

    # 计算短信核查_近6个月内欠款次数
    def _sms_owe_cnt_6m(self, df=None):
        if len(df) != 0:
            df_1 = df.loc[df['date_dif'] < 6, :].copy()
            self.variables['sms_owe_cnt_6m'] = len(df_1)

    # 计算短信核查_近6-12个月内欠款次数
    def _sms_owe_cnt_6_12m(self, df=None):

        if len(df) != 0:
            df_2 = df.loc[(df['date_dif'] >= 6) & (df['date_dif'] < 12), :].copy()
            self.variables['sms_owe_cnt_6_12m'] = len(df_2)

    # 计算短信核查_近6个月内欠款金额最大等级
    def _sms_max_owe_6m(self, df=None):

        if len(df) != 0:
            df_3 = df.loc[df['date_dif'] < 6, :].copy()
            if len(df_3) != 0:
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0W～0.2W", value=1)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.5W～1W", value=3)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="1W～3W", value=4)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="3W～5W", value=5)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="5W～10W", value=6)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="10W以上", value=7)
                self.variables['sms_max_owe_6m'] = df_3['debt_money'].max()

    def _sms_app_cnt_3m(self):
        sql = '''
                select count(*) total_count from(
                    SELECT sms_id, create_time
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                    ) sms left join info_sms_loan_apply slp on slp.sms_id = sms.sms_id 
                        where apply_time >= DATE_ADD(sms.create_time, Interval -3 month)
                '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        if df is None or df.empty:
            self.variables["sms_app_cnt_3m"] = df.iloc[0].total_count

    def _sms_loan_cnt_3m(self):
        sql = '''
                    select count(*) total_count from(
                        SELECT sms_id, create_time
                        FROM info_sms 
                        WHERE 
                            user_name = %(user_name)s 
                            AND id_card_no = %(id_card_no)s 
                            AND phone = %(phone)s
                            AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        ORDER BY id DESC 
                        LIMIT 1
                        ) sms left join info_sms_loan sla on sla.sms_id = sms.sms_id 
                            where loan_time >= DATE_ADD(sms.create_time, Interval -3 month)
                    '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        if df is None or df.empty:
            self.variables["sms_loan_cnt_3m"] = df.iloc[0].total_count

    def _info_sms_id(self):
        id_no = self.id_card_no
        sql = """
            select sms_id, create_time
            from info_sms
            where id_card_no = %(id_no)s 
            and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df = sql_to_df(sql=sql, params={"id_no": str(id_no)})
        if df.shape[0] == 0:
            return '', None
        sms_id = str(df['sms_id'].to_list()[0])
        end_date = pd.to_datetime(df['create_time'].to_list()[0])
        return sms_id, end_date

    @staticmethod
    def datediff(data_df, date_col, end_date):
        if date_col not in data_df.columns:
            return data_df
        data_df[date_col] = data_df[date_col].apply(pd.to_datetime)
        data_df.sort_values(by=date_col, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df['month_from_now'] = data_df[date_col].apply(
            lambda x: (end_date.year - x.year) * 12 + end_date.month - x.month + (end_date.day - x.day) // 100)
        data_df.loc[0, 'monthdiff'] = -1
        for i in data_df.index[1:]:
            data_df.loc[i, 'monthdiff'] = abs(data_df.loc[i, 'month_from_now'] - data_df.loc[i - 1, 'month_from_now'])
        return data_df

    def _info_regi_data(self):
        sql = """
            select * 
            from info_sms_loan_platform
            where sms_id = %(sms_id)s and register_time > %(two_years_ago)s
        """
        df = sql_to_df(sql=sql, params={"sms_id": self.sms_id,
                                        "two_years_ago": self.two_years_ago})
        if df.shape[0] == 0:
            return
        df = self.datediff(df, 'register_time', self.end_date)
        regi_month_interval_max_2m = df[df['month_from_now'] < 2]['monthdiff'].max()
        self.variables['hd_regi_non_bank_3m'] = df[(df['month_from_now'] < 3) &
                                                   (df['platform_type'] == 'NON_BANK')].shape[0]
        self.variables['hd_regi_p2p_3m'] = df[(df['month_from_now'] < 3) &
                                              (df['platform_type'] == 'P2P')].shape[0]
        if pd.notna(regi_month_interval_max_2m):
            self.variables['regi_month_interval_max_2m'] = int(regi_month_interval_max_2m)

    def _info_apply_data(self):
        sql = """
            select *
            from info_sms_loan_apply
            where sms_id = %(sms_id)s and apply_time > %(two_years_ago)s
        """
        df = sql_to_df(sql=sql, params={"sms_id": self.sms_id,
                                        "two_years_ago": self.two_years_ago})
        if df.shape[0] == 0:
            return
        df = self.datediff(df, 'apply_time', self.end_date)
        hd_apply_month_std_12m = df[df['month_from_now'] < 12].groupby(
            'month_from_now').agg({'apply_time': len})['apply_time'].std()
        if pd.notna(hd_apply_month_std_12m):
            self.variables['hd_apply_month_std_12m'] = hd_apply_month_std_12m

    def _info_loan_data(self):
        sql = """
            select * 
            from info_sms_loan
            where sms_id = %(sms_id)s and loan_time > %(two_years_ago)s
        """
        df = sql_to_df(sql=sql, params={"sms_id": self.sms_id,
                                        "two_years_ago": self.two_years_ago})
        if df.shape[0] == 0:
            return
        df = self.datediff(df, 'loan_time', self.end_date)
        self.variables['hd_loan_non_bank_6m'] = df[(df['month_from_now'] < 6) &
                                                   (df['platform_type'] == 'NON_BANK')].shape[0]
        hd_loan_month_std_24m = df[df['month_from_now'] < 24].groupby(
            'month_from_now').agg({'loan_time': len})['loan_time'].std()
        hd_loan_month_std_12m = df[df['month_from_now'] < 12].groupby(
            'month_from_now').agg({'loan_time': len})['loan_time'].std()
        if pd.notna(hd_loan_month_std_12m):
            self.variables['hd_loan_month_std_12m'] = hd_loan_month_std_12m
        if pd.notna(hd_loan_month_std_24m):
            self.variables['hd_loan_month_std_24m'] = hd_loan_month_std_24m
        weight_mapping = {
            "0W～0.2W": 1000,
            "0.2W～0.5W": 3500,
            "0.5W～1W": 7500,
            "1W～3W": 20000,
            "3W～5W": 40000,
            "5W～10W": 75000,
            "10W以上": 100000
        }
        df['weight_amt'] = df['loan_amount'].map(weight_mapping)
        self.variables['hd_loan_total_weight_amt_9m'] = df[df['month_from_now'] < 9]['weight_amt'].sum()

    def transform(self):
        platform_df = self._info_sms_loan_platform()
        self._sms_reg_cnt(platform_df)
        self._sms_reg_cnt_bank_3m(platform_df)
        self._sms_reg_cnt_other_3m(platform_df)
        apply = self._info_sms_loan_apply()
        self._sms_app_cnt(apply)
        self._sms_max_apply(apply)
        loan = self._info_sms_loan()
        self._sms_loan_cnt(loan)
        self._sms_max_loan(loan)
        self._sms_reject_cnt(self._info_sms_loan_reject())
        platform = self._info_sms_overdue_platform()
        self._sms_overdue_cnt(platform)
        self._sms_max_overdue(platform)
        debt = self._info_sms_debt()
        self._sms_owe_cnt(debt)
        self._sms_max_owe(debt)
        self._sms_owe_cnt_6m(debt)
        self._sms_owe_cnt_6_12m(debt)
        self._sms_max_owe_6m(debt)
        self._sms_app_cnt_3m()
        self._sms_loan_cnt_3m()
        self.sms_id, self.end_date = self._info_sms_id()
        if self.end_date is None:
            return
        two_years_ago = self.end_date - offsets.DateOffset(years=2)
        self.two_years_ago = two_years_ago.strftime('%Y-%m-%d')
        self._info_apply_data()
        self._info_regi_data()
        self._info_loan_data()
