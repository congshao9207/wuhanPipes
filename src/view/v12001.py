import re

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def get_bank_count(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=银行\:)\d+\.?\d*").search(var) is not None:
            value = int(re.compile(r"(?<=银行\:)\d+\.?\d*").search(var).group(0))
            return value
        else:
            return 0
    else:
        return 0


def get_sloan_count(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=线下小贷\:)\d+\.?\d*").search(var) is not None:
            value = int(re.compile(r"(?<=线下小贷\:)\d+\.?\d*").search(var).group(0))
            return value
        else:
            return 0
    else:
        return 0


def get_p2p_count(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=P2P理财\:)\d+\.?\d*").search(var) is not None:
            value = int(re.compile(r"(?<=P2P理财\:)\d+\.?\d*").search(var).group(0))
            return value
        else:
            return 0
    else:
        return 0


def get_confin1_count(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=持牌消费金融\:)\d+\.?\d*").search(var) is not None:
            value = int(re.compile(r"(?<=持牌消费金融\:)\d+\.?\d*").search(var).group(0))
            return value
        else:
            return 0
    else:
        return 0


def get_confin2_count(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=信贷/消费金融\:)\d+\.?\d*").search(var) is not None:
            value = int(re.compile(r"(?<=信贷/消费金融\:)\d+\.?\d*").search(var).group(0))
            return value
        else:
            return 0
    else:
        return 0


def get_all_count(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=总数\:)\d+\.?\d*").search(var) is not None:
            value = int(re.compile(r"(?<=总数\:)\d+\.?\d*").search(var).group(0))
            return value
        else:
            return 0
    else:
        return 0


class V12001(Transformer):
    """
    反欺诈相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'anti_tel_apply_bank_7d': 0,
            'anti_id_apply_bank_7d': 0,
            'anti_apply_bank_7d': 0,
            'anti_tel_apply_bank_1m': 0,
            'anti_id_apply_bank_1m': 0,
            'anti_apply_bank_1m': 0,
            'anti_tel_apply_bank_3m': 0,
            'anti_id_apply_bank_3m': 0,
            'anti_apply_bank_3m': 0,
            'anti_tel_apply_sloan_7d': 0,
            'anti_id_apply_sloan_7d': 0,
            'anti_apply_sloan_7d': 0,
            'anti_tel_apply_sloan_1m': 0,
            'anti_id_apply_sloan_1m': 0,
            'anti_apply_sloan_1m': 0,
            'anti_tel_apply_sloan_3m': 0,
            'anti_id_apply_sloan_3m': 0,
            'anti_apply_sloan_3m': 0,
            'anti_tel_apply_p2p_7d': 0,
            'anti_id_apply_p2p_7d': 0,
            'anti_apply_p2p_7d': 0,
            'anti_tel_apply_p2p_1m': 0,
            'anti_id_apply_p2p_1m': 0,
            'anti_apply_p2p_1m': 0,
            'anti_tel_apply_p2p_3m': 0,
            'anti_id_apply_p2p_3m': 0,
            'anti_apply_p2p_3m': 0,
            'anti_tel_apply_confin_7d': 0,
            'anti_id_apply_confin_7d': 0,
            'anti_apply_confin_7d': 0,
            'anti_tel_apply_confin_1m': 0,
            'anti_id_apply_confin_1m': 0,
            'anti_apply_confin_1m': 0,
            'anti_tel_apply_confin_3m': 0,
            'anti_id_apply_confin_3m': 0,
            'anti_apply_confin_3m': 0,
            'anti_tel_apply_other_7d': 0,
            'anti_id_apply_other_7d': 0,
            'anti_apply_other_7d': 0,
            'anti_tel_apply_other_1m': 0,
            'anti_id_apply_other_1m': 0,
            'anti_apply_other_1m': 0,
            'anti_tel_apply_other_3m': 0,
            'anti_id_apply_other_3m': 0,
            'anti_apply_other_3m': 0
        }

    ## 获取目标数据集
    def _info_anti_fraud_rule(self):
        sql = '''
            SELECT info_anti_fraud_rule.rule_name,info_anti_fraud_rule.rule_memo 
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

    # 计算反欺诈_手机号7天内申请银行次数  ***
    def _anti_tel_apply_bank_7d(self, df=None):
        df1 = df.loc[df['rule_name'] == '手机号七天内多头申请过多', 'rule_memo'].copy()
        if len(df1) > 0:
            self.variables['anti_tel_apply_bank_7d'] = get_bank_count(df1.values[0])

    # 计算反欺诈_身份证7天内申请银行次数   ***
    def _anti_id_apply_bank_7d(self, df=None):
        df2 = df.loc[df['rule_name'] == '身份证七天内多头申请过多', 'rule_memo'].copy()
        if len(df2) > 0:
            self.variables['anti_id_apply_bank_7d'] = get_bank_count(df2.values[0])

    # 计算反欺诈_手机号1个月内申请银行次数  ***
    def _anti_tel_apply_bank_1m(self, df=None):
        df3 = df.loc[df['rule_name'] == '手机号一个月内多头申请过多', 'rule_memo'].copy()
        if len(df3) > 0:
            self.variables['anti_tel_apply_bank_1m'] = get_bank_count(df3.values[0])

    # 计算反欺诈_身份证1个月内申请银行次数   ***
    def _anti_id_apply_bank_1m(self, df=None):
        df4 = df.loc[df['rule_name'] == '身份证一个月内多头申请过多', 'rule_memo'].copy()
        if len(df4) > 0:
            self.variables['anti_id_apply_bank_1m'] = get_bank_count(df4.values[0])

    # 计算反欺诈_手机号3个月内申请银行次数  ***
    def _anti_tel_apply_bank_3m(self, df=None):
        df5 = df.loc[df['rule_name'] == '手机号三个月内多头申请过多', 'rule_memo'].copy()
        if len(df5) > 0:
            self.variables['anti_tel_apply_bank_3m'] = get_bank_count(df5.values[0])

    # 计算反欺诈_身份证3个月内申请银行次数   ***
    def _anti_id_apply_bank_3m(self, df=None):
        df6 = df.loc[df['rule_name'] == '身份证三个月内多头申请过多', 'rule_memo'].copy()
        if len(df6) > 0:
            self.variables['anti_id_apply_bank_3m'] = get_bank_count(df6.values[0])

    # 计算反欺诈_手机号7天内申请小贷次数  ***
    def _anti_tel_apply_sloan_7d(self, df=None):
        df1 = df.loc[df['rule_name'] == '手机号七天内多头申请过多', 'rule_memo'].copy()
        if len(df1) > 0:
            self.variables['anti_tel_apply_sloan_7d'] = get_sloan_count(df1.values[0])

    # 计算反欺诈_身份证7天内申请小贷次数   ***
    def _anti_id_apply_sloan_7d(self, df=None):
        df2 = df.loc[df['rule_name'] == '身份证七天内多头申请过多', 'rule_memo'].copy()
        if len(df2) > 0:
            self.variables['anti_id_apply_sloan_7d'] = get_sloan_count(df2.values[0])

    # 计算反欺诈_手机号1个月内申请小贷次数  ***
    def _anti_tel_apply_sloan_1m(self, df=None):
        df3 = df.loc[df['rule_name'] == '手机号一个月内多头申请过多', 'rule_memo'].copy()
        if len(df3) > 0:
            self.variables['anti_tel_apply_sloan_1m'] = get_sloan_count(df3.values[0])

    # 计算反欺诈_身份证1个月内申请小贷次数   ***
    def _anti_id_apply_sloan_1m(self, df=None):
        df4 = df.loc[df['rule_name'] == '身份证一个月内多头申请过多', 'rule_memo'].copy()
        if len(df4) > 0:
            self.variables['anti_id_apply_sloan_1m'] = get_sloan_count(df4.values[0])

    # 计算反欺诈_手机号3个月内申请小贷次数  ***
    def _anti_tel_apply_sloan_3m(self, df=None):
        df5 = df.loc[df['rule_name'] == '手机号三个月内多头申请过多', 'rule_memo'].copy()
        if len(df5) > 0:
            self.variables['anti_tel_apply_sloan_3m'] = get_sloan_count(df5.values[0])

    # 计算反欺诈_身份证3个月内申请小贷次数   ***
    def _anti_id_apply_sloan_3m(self, df=None):
        df6 = df.loc[df['rule_name'] == '身份证三个月内多头申请过多', 'rule_memo'].copy()
        if len(df6) > 0:
            self.variables['anti_id_apply_sloan_3m'] = get_sloan_count(df6.values[0])

    # 计算反欺诈_手机号7天内申请p2p次数  ***
    def _anti_tel_apply_p2p_7d(self, df=None):
        df1 = df.loc[df['rule_name'] == '手机号七天内多头申请过多', 'rule_memo'].copy()
        if len(df1) > 0:
            self.variables['anti_tel_apply_p2p_7d'] = get_p2p_count(df1.values[0])

    # 计算反欺诈_身份证7天内申请p2p次数   ***
    def _anti_id_apply_p2p_7d(self, df=None):
        df2 = df.loc[df['rule_name'] == '身份证七天内多头申请过多', 'rule_memo'].copy()
        if len(df2) > 0:
            self.variables['anti_id_apply_p2p_7d'] = get_p2p_count(df2.values[0])

    # 计算反欺诈_手机号1个月内申请p2p次数  ***
    def _anti_tel_apply_p2p_1m(self, df=None):
        df3 = df.loc[df['rule_name'] == '手机号一个月内多头申请过多', 'rule_memo'].copy()
        if len(df3) > 0:
            self.variables['anti_tel_apply_p2p_1m'] = get_p2p_count(df3.values[0])

    # 计算反欺诈_身份证1个月内申请p2p次数   ***
    def _anti_id_apply_p2p_1m(self, df=None):
        df4 = df.loc[df['rule_name'] == '身份证一个月内多头申请过多', 'rule_memo'].copy()
        if len(df4) > 0:
            self.variables['anti_id_apply_p2p_1m'] = get_p2p_count(df4.values[0])

    # 计算反欺诈_手机号3个月内申请p2p次数  ***
    def _anti_tel_apply_p2p_3m(self, df=None):
        df5 = df.loc[df['rule_name'] == '手机号三个月内多头申请过多', 'rule_memo'].copy()
        if len(df5) > 0:
            self.variables['anti_tel_apply_p2p_3m'] = get_p2p_count(df5.values[0])

    # 计算反欺诈_身份证3个月内申请p2p次数   ***
    def _anti_id_apply_p2p_3m(self, df=None):
        df6 = df.loc[df['rule_name'] == '身份证三个月内多头申请过多', 'rule_memo'].copy()
        if len(df6) > 0:
            self.variables['anti_id_apply_p2p_3m'] = get_p2p_count(df6.values[0])

    # 计算反欺诈_手机号7天内申请消费金融次数  ***
    def _anti_tel_apply_confin_7d(self, df=None):
        df1 = df.loc[df['rule_name'] == '手机号七天内多头申请过多', 'rule_memo'].copy()
        if len(df1) > 0:
            self.variables['anti_tel_apply_confin_7d'] = get_confin1_count(df1.values[0]) + get_confin2_count(
                df1.values[0])

    # 计算反欺诈_身份证7天内申请消费金融次数   ***
    def _anti_id_apply_confin_7d(self, df=None):
        df2 = df.loc[df['rule_name'] == '身份证七天内多头申请过多', 'rule_memo'].copy()
        if len(df2) > 0:
            self.variables['anti_id_apply_confin_7d'] = get_confin1_count(df2.values[0]) + get_confin2_count(
                df2.values[0])

    # 计算反欺诈_手机号1个月内申请消费金融次数  ***
    def _anti_tel_apply_confin_1m(self, df=None):
        df3 = df.loc[df['rule_name'] == '手机号一个月内多头申请过多', 'rule_memo'].copy()
        if len(df3) > 0:
            self.variables['anti_tel_apply_confin_1m'] = get_confin1_count(df3.values[0]) + get_confin2_count(
                df3.values[0])

    # 计算反欺诈_身份证1个月内申请消费金融次数   ***
    def _anti_id_apply_confin_1m(self, df=None):
        df4 = df.loc[df['rule_name'] == '身份证一个月内多头申请过多', 'rule_memo'].copy()
        if len(df4) > 0:
            self.variables['anti_id_apply_confin_1m'] = get_confin1_count(df4.values[0]) + get_confin2_count(
                df4.values[0])

    # 计算反欺诈_手机号3个月内申请消费金融次数  ***
    def _anti_tel_apply_confin_3m(self, df=None):
        df5 = df.loc[df['rule_name'] == '手机号三个月内多头申请过多', 'rule_memo'].copy()
        if len(df5) > 0:
            self.variables['anti_tel_apply_confin_3m'] = get_confin1_count(df5.values[0]) + get_confin2_count(
                df5.values[0])

    # 计算反欺诈_身份证3个月内申请消费金融次数   ***
    def _anti_id_apply_confin_3m(self, df=None):
        df6 = df.loc[df['rule_name'] == '身份证三个月内多头申请过多', 'rule_memo'].copy()
        if len(df6) > 0:
            self.variables['anti_id_apply_confin_3m'] = get_confin1_count(df6.values[0]) + get_confin2_count(
                df6.values[0])

    # 计算反欺诈_手机号7天内申请其他机构次数  ***
    def _anti_tel_apply_other_7d(self, df=None):
        df1 = df.loc[df['rule_name'] == '手机号七天内多头申请过多', 'rule_memo'].copy()
        if len(df1) > 0:
            self.variables['anti_tel_apply_other_7d'] = get_all_count(df1.values[0]) - (
                    get_bank_count(df1.values[0]) + get_sloan_count(df1.values[0]) + get_p2p_count(
                df1.values[0]) + get_confin1_count(df1.values[0]) + get_confin2_count(df1.values[0]))

    # 计算反欺诈_身份证7天内申请其他机构次数   ***
    def _anti_id_apply_other_7d(self, df=None):
        df2 = df.loc[df['rule_name'] == '身份证七天内多头申请过多', 'rule_memo'].copy()
        if len(df2) > 0:
            self.variables['anti_id_apply_other_7d'] = get_all_count(df2.values[0]) - (
                    get_bank_count(df2.values[0]) + get_sloan_count(df2.values[0]) + get_p2p_count(
                df2.values[0]) + get_confin1_count(df2.values[0]) + get_confin2_count(df2.values[0]))

    # 计算反欺诈_手机号1个月内申请其他机构次数  ***
    def _anti_tel_apply_other_1m(self, df=None):
        df3 = df.loc[df['rule_name'] == '手机号一个月内多头申请过多', 'rule_memo'].copy()
        if len(df3) > 0:
            self.variables['anti_tel_apply_other_1m'] = get_all_count(df3.values[0]) - (
                    get_bank_count(df3.values[0]) + get_sloan_count(df3.values[0]) + get_p2p_count(
                df3.values[0]) + get_confin1_count(df3.values[0]) + get_confin2_count(df3.values[0]))

    # 计算反欺诈_身份证1个月内申请其他机构次数   ***
    def _anti_id_apply_other_1m(self, df=None):
        df4 = df.loc[df['rule_name'] == '身份证一个月内多头申请过多', 'rule_memo'].copy()
        if len(df4) > 0:
            self.variables['anti_id_apply_other_1m'] = get_all_count(df4.values[0]) - (
                    get_bank_count(df4.values[0]) + get_sloan_count(df4.values[0]) + get_p2p_count(
                df4.values[0]) + get_confin1_count(df4.values[0]) + get_confin2_count(df4.values[0]))

    # 计算反欺诈_手机号3个月内申请其他机构次数  ***
    def _anti_tel_apply_other_3m(self, df=None):
        df5 = df.loc[df['rule_name'] == '手机号三个月内多头申请过多', 'rule_memo'].copy()
        if len(df5) > 0:
            self.variables['anti_tel_apply_other_3m'] = get_all_count(df5.values[0]) - (
                    get_bank_count(df5.values[0]) + get_sloan_count(df5.values[0]) + get_p2p_count(
                df5.values[0]) + get_confin1_count(df5.values[0]) + get_confin2_count(df5.values[0]))

    # 计算反欺诈_身份证3个月内申请其他机构次数   ***
    def _anti_id_apply_other_3m(self, df=None):
        df6 = df.loc[df['rule_name'] == '身份证三个月内多头申请过多', 'rule_memo'].copy()
        if len(df6) > 0:
            self.variables['anti_id_apply_other_3m'] = get_all_count(df6.values[0]) - (
                    get_bank_count(df6.values[0]) + get_sloan_count(df6.values[0]) + get_p2p_count(
                df6.values[0]) + get_confin1_count(df6.values[0]) + get_confin2_count(df6.values[0]))

    # 计算反欺诈_7天内申请银行次数   ***
    def _anti_apply_bank_7d(self):
        self.variables['anti_apply_bank_7d'] = max(self.variables['anti_tel_apply_bank_7d'],
                                                   self.variables['anti_id_apply_bank_7d'])

    # 计算反欺诈_1个月内申请银行次数   ***
    def _anti_apply_bank_1m(self):
        self.variables['anti_apply_bank_1m'] = max(self.variables['anti_tel_apply_bank_1m'],
                                                   self.variables['anti_id_apply_bank_1m'])

    # 计算反欺诈_3个月内申请银行次数   ***
    def _anti_apply_bank_3m(self):
        self.variables['anti_apply_bank_3m'] = max(self.variables['anti_tel_apply_bank_3m'],
                                                   self.variables['anti_id_apply_bank_3m'])

    # 计算反欺诈_7天内申请小贷次数   ***
    def _anti_apply_sloan_7d(self):
        self.variables['anti_apply_sloan_7d'] = max(self.variables['anti_tel_apply_sloan_7d'],
                                                    self.variables['anti_id_apply_sloan_7d'])

    # 计算反欺诈_1个月内申请小贷次数   ***
    def _anti_apply_sloan_1m(self):
        self.variables['anti_apply_sloan_1m'] = max(self.variables['anti_tel_apply_sloan_1m'],
                                                    self.variables['anti_id_apply_sloan_1m'])

    # 计算反欺诈_3个月内申请小贷次数   ***
    def _anti_apply_sloan_3m(self):
        self.variables['anti_apply_sloan_3m'] = max(self.variables['anti_tel_apply_sloan_3m'],
                                                    self.variables['anti_id_apply_sloan_3m'])

    # 计算反欺诈_7天内申请p2p次数   ***
    def _anti_apply_p2p_7d(self):
        self.variables['anti_apply_p2p_7d'] = max(self.variables['anti_tel_apply_p2p_7d'],
                                                  self.variables['anti_id_apply_p2p_7d'])

    # 计算反欺诈_1个月内申请p2p次数   ***
    def _anti_apply_p2p_1m(self):
        self.variables['anti_apply_p2p_1m'] = max(self.variables['anti_tel_apply_p2p_1m'],
                                                  self.variables['anti_id_apply_p2p_1m'])

    # 计算反欺诈_3个月内申请p2p次数   ***
    def _anti_apply_p2p_3m(self):
        self.variables['anti_apply_p2p_3m'] = max(self.variables['anti_tel_apply_p2p_3m'],
                                                  self.variables['anti_id_apply_p2p_3m'])

    # 计算反欺诈_7天内申请消费金融次数   ***
    def _anti_apply_confin_7d(self):
        self.variables['anti_apply_confin_7d'] = max(self.variables['anti_tel_apply_confin_7d'],
                                                     self.variables['anti_id_apply_confin_7d'])

    # 计算反欺诈_1个月内申请消费金融次数   ***
    def _anti_apply_confin_1m(self):
        self.variables['anti_apply_confin_1m'] = max(self.variables['anti_tel_apply_confin_1m'],
                                                     self.variables['anti_id_apply_confin_1m'])

    # 计算反欺诈_3个月内申请消费金融次数   ***
    def _anti_apply_confin_3m(self):
        self.variables['anti_apply_confin_3m'] = max(self.variables['anti_tel_apply_confin_3m'],
                                                     self.variables['anti_id_apply_confin_3m'])

    # 计算反欺诈_7天内申请其他机构次数   ***
    def _anti_apply_other_7d(self):
        self.variables['anti_apply_other_7d'] = max(self.variables['anti_tel_apply_other_7d'],
                                                    self.variables['anti_id_apply_other_7d'])

    # 计算反欺诈_1个月内申请其他机构次数   ***
    def _anti_apply_other_1m(self):
        self.variables['anti_apply_other_1m'] = max(self.variables['anti_tel_apply_other_1m'],
                                                    self.variables['anti_id_apply_other_1m'])

    # 计算反欺诈_3个月内申请其他机构次数   ***
    def _anti_apply_other_3m(self):
        self.variables['anti_apply_other_3m'] = max(self.variables['anti_tel_apply_other_3m'],
                                                    self.variables['anti_id_apply_other_3m'])

    #  执行变量转换
    def transform(self):
        info_anti_fraud_rule = self._info_anti_fraud_rule()
        if info_anti_fraud_rule is not None and len(info_anti_fraud_rule) > 0:
            self._anti_tel_apply_bank_7d(info_anti_fraud_rule)
            self._anti_id_apply_bank_7d(info_anti_fraud_rule)
            self._anti_tel_apply_bank_1m(info_anti_fraud_rule)
            self._anti_id_apply_bank_1m(info_anti_fraud_rule)
            self._anti_tel_apply_bank_3m(info_anti_fraud_rule)
            self._anti_id_apply_bank_3m(info_anti_fraud_rule)
            self._anti_tel_apply_sloan_7d(info_anti_fraud_rule)
            self._anti_id_apply_sloan_7d(info_anti_fraud_rule)
            self._anti_tel_apply_sloan_1m(info_anti_fraud_rule)
            self._anti_id_apply_sloan_1m(info_anti_fraud_rule)
            self._anti_tel_apply_sloan_3m(info_anti_fraud_rule)
            self._anti_id_apply_sloan_3m(info_anti_fraud_rule)
            self._anti_tel_apply_p2p_7d(info_anti_fraud_rule)
            self._anti_id_apply_p2p_7d(info_anti_fraud_rule)
            self._anti_tel_apply_p2p_1m(info_anti_fraud_rule)
            self._anti_id_apply_p2p_1m(info_anti_fraud_rule)
            self._anti_tel_apply_p2p_3m(info_anti_fraud_rule)
            self._anti_id_apply_p2p_3m(info_anti_fraud_rule)
            self._anti_tel_apply_confin_7d(info_anti_fraud_rule)
            self._anti_id_apply_confin_7d(info_anti_fraud_rule)
            self._anti_tel_apply_confin_1m(info_anti_fraud_rule)
            self._anti_id_apply_confin_1m(info_anti_fraud_rule)
            self._anti_tel_apply_confin_3m(info_anti_fraud_rule)
            self._anti_id_apply_confin_3m(info_anti_fraud_rule)
            self._anti_tel_apply_other_7d(info_anti_fraud_rule)
            self._anti_id_apply_other_7d(info_anti_fraud_rule)
            self._anti_tel_apply_other_1m(info_anti_fraud_rule)
            self._anti_id_apply_other_1m(info_anti_fraud_rule)
            self._anti_tel_apply_other_3m(info_anti_fraud_rule)
            self._anti_id_apply_other_3m(info_anti_fraud_rule)
        self._anti_apply_bank_7d()
        self._anti_apply_bank_1m()
        self._anti_apply_bank_3m()
        self._anti_apply_sloan_7d()
        self._anti_apply_sloan_1m()
        self._anti_apply_sloan_3m()
        self._anti_apply_p2p_7d()
        self._anti_apply_p2p_1m()
        self._anti_apply_p2p_3m()
        self._anti_apply_confin_7d()
        self._anti_apply_confin_1m()
        self._anti_apply_confin_3m()
        self._anti_apply_other_7d()
        self._anti_apply_other_1m()
        self._anti_apply_other_3m()
