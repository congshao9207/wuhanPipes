import pandas as pd

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from util.common_util import exception

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class T12001(Transformer):
    """
    反欺诈相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'anti_idno_consume_black': 0,  # 反欺诈_身份证比对信贷行业信用消费黑名单
            'anti_idno_P2P_black': 0,  # 反欺诈_身份证比对信贷行业P2P黑名单
            'anti_idno_lost_black': 0,  # 反欺诈_身份证比对信贷行业失联名单
            'anti_idno_fraud_black': 0,  # 反欺诈_身份证比对信贷行业欺诈名单
            'anti_idno_loan_black': 0,  # 反欺诈_身份证比对信贷行业信贷黑名单
            'anti_tel_industy_black': 0,  # 反欺诈_手机号比对信贷行业黑名单
            'anti_tel_P2P_black': 0,  # 反欺诈_手机号比对信贷行业P2P黑名单
            'anti_tel_lost_black': 0,  # 反欺诈_手机号比对信贷行业失联黑名单
            'anti_tel_fraud_black': 0,  # 反欺诈_手机号比对信贷行业欺诈黑名单
            'anti_tel_small_no': 0,  # 反欺诈_手机号比对通信小号名单
            'anti_idno_court_execu': 0,  # 反欺诈_身份证比对法院执行名单
            'anti_idno_court_closure': 0,  # 反欺诈_身份证比对法院结案名单
            'anti_idno_court_break_faith': 0,  # 反欺诈_身份证比对法院失信名单
            'anti_idno_legal_break_faith': 0,  # 反欺诈_身份证比对法人失信名单
            'anti_idno_apply_1m': 0,  # 反欺诈_身份证一个月内多头申请过多_次数
            'anti_idno_apply_3m': 0,  # 反欺诈_身份证三个月内多头申请过多_次数
            'anti_tel_apply_1m': 0,  # 反欺诈_手机号一个月内多头申请过多_次数
            'anti_tel_apply_3m': 0,  # 反欺诈_手机号三个月内多头申请过多_次数
            'anti_idno_apply_3d': 0,  # 反欺诈_身份证三天内多头申请过多_次数
            'anti_idno_apply_7d': 0,  # 反欺诈_身份证七天内多头申请过多_次数
            'anti_tel_apply_3d': 0,  # 反欺诈_手机号三天内多头申请过多_次数
            'anti_tel_apply_7d': 0,  # 反欺诈_手机号七天内多头申请过多_次数
        }

    ## 获取目标数据集1
    def _info_anti_fraud_rule(self):
        sql = '''
            SELECT DISTINCT(rule_id),info_anti_fraud_rule.rule_name,info_anti_fraud_rule.rule_memo
            FROM info_anti_fraud_rule 
            LEFT JOIN info_anti_fraud_strategy 
            ON info_anti_fraud_rule.anti_fraud_rule_id = info_anti_fraud_strategy.anti_fraud_rule_id
            WHERE info_anti_fraud_strategy.anti_fraud_id 
            IN (
                SELECT af.anti_fraud_id 
                FROM (
                    SELECT anti_fraud_id 
                    FROM info_anti_fraud 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        ORDER BY id DESC 
                        LIMIT 1
                ) as af
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})

        return df

    # 计算反欺诈_身份证比对信贷行业信用消费黑名单
    def _anti_idno_consume_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对信贷行业信用消费黑名单')]
            self.variables['anti_idno_consume_black'] = len(df)

    # 计算反欺诈_身份证比对信贷行业P2P黑名单
    def _anti_idno_P2P_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对信贷行业P2P黑名单')]
            self.variables['anti_idno_P2P_black'] = len(df)

    # 计算反欺诈_身份证比对信贷行业失联名单
    def _anti_idno_lost_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对信贷行业失联名单')]
            self.variables['anti_idno_lost_black'] = len(df)

    # 计算反欺诈_身份证比对信贷行业欺诈名单
    def _anti_idno_fraud_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对信贷行业欺诈名单')]
            self.variables['anti_idno_fraud_black'] = len(df)

    # 计算反欺诈_身份证比对信贷行业信贷黑名单
    def _anti_idno_loan_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对信贷行业信贷黑名单')]
            self.variables['anti_idno_loan_black'] = len(df)

    # 计算反欺诈_手机号比对信贷行业黑名单
    def _anti_tel_industy_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('手机号比对信贷行业黑名单')]
            self.variables['anti_tel_industy_black'] = len(df)

    # 计算手机号比对信贷行业P2P黑名单
    def _anti_tel_P2P_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('手机号比对信贷行业P2P黑名单')]
            self.variables['anti_tel_P2P_black'] = len(df)

    # 计算反欺诈_手机号比对信贷行业失联黑名单
    def _anti_tel_lost_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('手机号比对信贷行业失联黑名单')]
            self.variables['anti_tel_lost_black'] = len(df)

    # 计算反欺诈_手机号比对信贷行业欺诈黑名单
    def _anti_tel_fraud_black(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('手机号比对信贷行业欺诈黑名单')]
            self.variables['anti_tel_fraud_black'] = len(df)

    # 计算反欺诈_手机号比对通信小号名单
    def _anti_tel_small_no(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('手机号比对通信小号名单')]
            self.variables['anti_tel_small_no'] = len(df)

    # 计算反欺诈_身份证比对法院执行名单
    def _anti_idno_court_execu(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对法院执行名单')]
            self.variables['anti_idno_court_execu'] = len(df)

    # 计算反欺诈_身份证比对法院结案名单
    def _anti_idno_court_closure(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对法院结案名单')]
            self.variables['anti_idno_court_closure'] = len(df)

    # 计算反欺诈_身份证比对法院失信名单
    def _anti_idno_court_break_faith(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对法院失信名单')]
            self.variables['anti_idno_court_break_faith'] = len(df)

    # 计算反欺诈_身份证比对法人失信名单
    def _anti_idno_legal_break_faith(self, df=None):
        df = df.dropna(subset=['rule_name'], how='any')
        if df.empty is not True:
            df = df[df['rule_name'].str.contains('身份证比对法人失信名单')]
            self.variables['anti_idno_legal_break_faith'] = len(df)

    # 计算反欺诈_身份证一个月内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_idno_apply_1m(self, df=None):

        df = df.loc[df['rule_name'] == '身份证一个月内多头申请过多', 'rule_memo'].copy()
        if len(df) != 0:
            self.variables['anti_idno_apply_1m'] = int(df.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_身份证三个月内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_idno_apply_3m(self, df=None):

        df2 = df.loc[df['rule_name'] == '身份证三个月内多头申请过多', 'rule_memo'].copy()
        if len(df2) != 0:
            self.variables['anti_idno_apply_3m'] = int(df2.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_手机号一个月内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_tel_apply_1m(self, df=None):

        df3 = df.loc[df['rule_name'] == '手机号一个月内多头申请过多', 'rule_memo'].copy()
        if len(df3) != 0:
            self.variables['anti_tel_apply_1m'] = int(df3.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_手机号三个月内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_tel_apply_3m(self, df=None):

        df4 = df.loc[df['rule_name'] == '手机号三个月内多头申请过多', 'rule_memo'].copy()
        if len(df4) != 0:
            self.variables['anti_tel_apply_3m'] = int(df4.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_身份证三天内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_idno_apply_3d(self, df=None):

        df5 = df.loc[df['rule_name'] == '身份证三天内多头申请过多', 'rule_memo'].copy()
        if len(df5) != 0:
            self.variables['anti_idno_apply_3d'] = int(df5.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_身份证七天内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_idno_apply_7d(self, df=None):

        df6 = df.loc[df['rule_name'] == '身份证七天内多头申请过多', 'rule_memo'].copy()
        if len(df6) != 0:
            self.variables['anti_idno_apply_7d'] = int(df6.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_手机号三天内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_tel_apply_3d(self, df=None):

        df7 = df.loc[df['rule_name'] == '手机号三天内多头申请过多', 'rule_memo'].copy()
        if len(df7) != 0:
            self.variables['anti_tel_apply_3d'] = int(df7.values[0].split(',')[0].split(':')[1])

    # 计算反欺诈_手机号七天内多头申请过多_次数
    @exception('purpose= 反欺诈&author=gulongwei')
    def _anti_tel_apply_7d(self, df=None):

        df8 = df.loc[df['rule_name'] == '手机号七天内多头申请过多', 'rule_memo'].copy()
        if len(df8) != 0:
            self.variables['anti_tel_apply_7d'] = int(df8.values[0].split(',')[0].split(':')[1])

    #  执行变量转换
    def transform(self):
        rule_df = self._info_anti_fraud_rule()
        self._anti_idno_consume_black(rule_df)
        self._anti_idno_P2P_black(rule_df)
        self._anti_idno_lost_black(rule_df)
        self._anti_idno_fraud_black(rule_df)
        self._anti_idno_loan_black(rule_df)
        self._anti_tel_industy_black(rule_df)
        self._anti_tel_P2P_black(rule_df)
        self._anti_tel_lost_black(rule_df)
        self._anti_tel_fraud_black(rule_df)
        self._anti_tel_small_no(rule_df)
        self._anti_idno_court_execu(rule_df)
        self._anti_idno_court_closure(rule_df)
        self._anti_idno_court_break_faith(rule_df)
        self._anti_idno_legal_break_faith(rule_df)
        self._anti_idno_apply_1m(rule_df)
        self._anti_idno_apply_3m(rule_df)
        self._anti_tel_apply_1m(rule_df)
        self._anti_tel_apply_3m(rule_df)
        self._anti_idno_apply_3d(rule_df)
        self._anti_idno_apply_7d(rule_df)
        self._anti_tel_apply_3d(rule_df)
        self._anti_tel_apply_7d(rule_df)
