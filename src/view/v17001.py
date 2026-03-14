import json as simplejson
import re

import pandas as pd

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def get_js(df, value):
    index_name = df.index.name
    row_list = []
    for index, col in df.iterrows():
        row_str = dict(col).get(value, '{}')
        row_dict = simplejson.loads(row_str)
        row_dict[index_name] = str(index)
        row_list.append(row_dict)
    df_re = pd.DataFrame(row_list)
    return df_re


def get_money(var, name):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=%s:)\d+\.?\d*" % name).search(var) is not None:
            value = int(re.compile(r"(?<=%s\:)\d+\.?\d*" % name).search(var).group(0))
        else:
            value = int(0)
        return value
    else:
        return 0


class V17001(Transformer):
    """
    网申核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'net_apply_bank_7d': 0,
            'net_apply_bank_1m': 0,
            'net_apply_bank_3m': 0,
            'net_apply_sloan_7d': 0,
            'net_apply_sloan_1m': 0,
            'net_apply_sloan_3m': 0,
            'net_apply_p2p_7d': 0,
            'net_apply_p2p_1m': 0,
            'net_apply_p2p_3m': 0,
            'net_apply_confin_7d': 0,
            'net_apply_confin_1m': 0,
            'net_apply_confin_3m': 0,
            'net_apply_other_7d': 0,
            'net_apply_other_1m': 0,
            'net_apply_other_3m': 0,
            'net_apply_bank_6m': 0,
            'net_apply_bank_12m': 0,
            'net_apply_bank_his': 0,
            'net_apply_sloan_6m': 0,
            'net_apply_sloan_12m': 0,
            'net_apply_sloan_his': 0,
            'net_apply_p2p_6m': 0,
            'net_apply_p2p_12m': 0,
            'net_apply_p2p_his': 0,
            'net_apply_confin_6m': 0,
            'net_apply_confin_12m': 0,
            'net_apply_confin_his': 0,
            'net_apply_other_6m': 0,
            'net_apply_other_12m': 0,
            'net_apply_other_his': 0
        }

    # 读取目标数据集
    def _info_fraud_verification_df(self):
        sql = """
               SELECT item_name,item_group,item_detail 
               FROM info_fraud_verification_item  
               WHERE  fraud_verification_id 
               IN (
                   SELECT fv.fraud_verification_id 
                   FROM (
                       SELECT id fraud_verification_id 
                       FROM info_fraud_verification 
                       WHERE 
                           user_name = %(user_name)s 
                           AND id_card_no = %(id_card_no)s 
                           AND phone = %(phone)s
                           AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                       ORDER BY id DESC 
                       LIMIT 1
                   ) as fv
               );
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算字段
    def _info_fraud_verification(self, df=None):
        if df is not None and len(df) > 0:
            df1 = get_js(df[df['item_name'] == '7天内申请人在多个平台申请借款'], 'item_detail')
            if len(df1) > 0:
                lst = ';'.join(df1['platform_detail'][0])
                self.variables['net_apply_bank_7d'] = get_money(lst, '网上银行') + get_money(lst, '信用卡中心') + get_money(lst,
                                                                                                                   '银行消费金融公司') + get_money(
                    lst, '银行对公业务') + get_money(lst, '银行个人业务') + get_money(lst, '银行小微贷款') + get_money(lst, '直销银行')
                self.variables['net_apply_sloan_7d'] = get_money(lst, '小额贷款公司')
                self.variables['net_apply_p2p_7d'] = get_money(lst, 'P2P网贷')
                self.variables['net_apply_confin_7d'] = get_money(lst, '大型消费金融公司') + get_money(lst, '一般消费分期平台')
                self.variables['net_apply_other_7d'] = df1['platform_count'][0] - get_money(lst, '网上银行') - get_money(
                    lst, '信用卡中心') - get_money(lst, '银行消费金融公司') - get_money(lst, '银行对公业务') - get_money(lst,
                                                                                                      '银行个人业务') - get_money(
                    lst, '银行小微贷款') - get_money(lst, '直销银行') - get_money(lst, '小额贷款公司') - get_money(lst,
                                                                                                   'P2P网贷') - get_money(
                    lst, '大型消费金融公司') - get_money(lst, '一般消费分期平台')

            df2 = get_js(df[df['item_name'] == '1个月内申请人在多个平台申请借款'], 'item_detail')
            if len(df2) > 0:
                lst = ';'.join(df2['platform_detail'][0])
                self.variables['net_apply_bank_1m'] = get_money(lst, '网上银行') + get_money(lst, '信用卡中心') + get_money(lst,
                                                                                                                   '银行消费金融公司') + get_money(
                    lst, '银行对公业务') + get_money(lst, '银行个人业务') + get_money(lst, '银行小微贷款') + get_money(lst, '直销银行')
                self.variables['net_apply_sloan_1m'] = get_money(lst, '小额贷款公司')
                self.variables['net_apply_p2p_1m'] = get_money(lst, 'P2P网贷')
                self.variables['net_apply_confin_1m'] = get_money(lst, '大型消费金融公司') + get_money(lst, '一般消费分期平台')
                self.variables['net_apply_other_1m'] = df2['platform_count'][0] - get_money(lst, '网上银行') - get_money(
                    lst, '信用卡中心') - get_money(lst, '银行消费金融公司') - get_money(lst, '银行对公业务') - get_money(lst,
                                                                                                      '银行个人业务') - get_money(
                    lst, '银行小微贷款') - get_money(lst, '直销银行') - get_money(lst, '小额贷款公司') - get_money(lst,
                                                                                                   'P2P网贷') - get_money(
                    lst, '大型消费金融公司') - get_money(lst, '一般消费分期平台')

            df3 = get_js(df[df['item_name'] == '3个月内申请人在多个平台申请借款'], 'item_detail')
            if len(df3) > 0:
                lst = ';'.join(df3['platform_detail'][0])
                self.variables['net_apply_bank_3m'] = get_money(lst, '网上银行') + get_money(lst, '信用卡中心') + get_money(lst,
                                                                                                                   '银行消费金融公司') + get_money(
                    lst, '银行对公业务') + get_money(lst, '银行个人业务') + get_money(lst, '银行小微贷款') + get_money(lst, '直销银行')
                self.variables['net_apply_sloan_3m'] = get_money(lst, '小额贷款公司')
                self.variables['net_apply_p2p_3m'] = get_money(lst, 'P2P网贷')
                self.variables['net_apply_confin_3m'] = get_money(lst, '大型消费金融公司') + get_money(lst, '一般消费分期平台')
                self.variables['net_apply_other_3m'] = df3['platform_count'][0] - get_money(lst, '网上银行') - get_money(
                    lst, '信用卡中心') - get_money(lst, '银行消费金融公司') - get_money(lst, '银行对公业务') - get_money(lst,
                                                                                                      '银行个人业务') - get_money(
                    lst, '银行小微贷款') - get_money(lst, '直销银行') - get_money(lst, '小额贷款公司') - get_money(lst,
                                                                                                   'P2P网贷') - get_money(
                    lst, '大型消费金融公司') - get_money(lst, '一般消费分期平台')

            df4 = get_js(df[df['item_name'] == '6个月内申请人在多个平台申请借款'], 'item_detail')
            if len(df4) > 0:
                lst = ';'.join(df4['platform_detail'][0])
                self.variables['net_apply_bank_6m'] = get_money(lst, '网上银行') + get_money(lst, '信用卡中心') + get_money(lst,
                                                                                                                   '银行消费金融公司') + get_money(
                    lst, '银行对公业务') + get_money(lst, '银行个人业务') + get_money(lst, '银行小微贷款') + get_money(lst, '直销银行')
                self.variables['net_apply_sloan_6m'] = get_money(lst, '小额贷款公司')
                self.variables['net_apply_p2p_6m'] = get_money(lst, 'P2P网贷')
                self.variables['net_apply_confin_6m'] = get_money(lst, '大型消费金融公司') + get_money(lst, '一般消费分期平台')
                self.variables['net_apply_other_6m'] = df4['platform_count'][0] - get_money(lst, '网上银行') - get_money(
                    lst, '信用卡中心') - get_money(lst, '银行消费金融公司') - get_money(lst, '银行对公业务') - get_money(lst,
                                                                                                      '银行个人业务') - get_money(
                    lst, '银行小微贷款') - get_money(lst, '直销银行') - get_money(lst, '小额贷款公司') - get_money(lst,
                                                                                                   'P2P网贷') - get_money(
                    lst, '大型消费金融公司') - get_money(lst, '一般消费分期平台')

            df5 = get_js(df[df['item_name'] == '12个月内申请人在多个平台申请借款'], 'item_detail')
            if len(df5) > 0:
                lst = ';'.join(df5['platform_detail'][0])
                self.variables['net_apply_bank_12m'] = get_money(lst, '网上银行') + get_money(lst, '信用卡中心') + get_money(lst,
                                                                                                                    '银行消费金融公司') + get_money(
                    lst, '银行对公业务') + get_money(lst, '银行个人业务') + get_money(lst, '银行小微贷款') + get_money(lst, '直销银行')
                self.variables['net_apply_sloan_12m'] = get_money(lst, '小额贷款公司')
                self.variables['net_apply_p2p_12m'] = get_money(lst, 'P2P网贷')
                self.variables['net_apply_confin_12m'] = get_money(lst, '大型消费金融公司') + get_money(lst, '一般消费分期平台')
                self.variables['net_apply_other_12m'] = df5['platform_count'][0] - get_money(lst, '网上银行') - get_money(
                    lst, '信用卡中心') - get_money(lst, '银行消费金融公司') - get_money(lst, '银行对公业务') - get_money(lst,
                                                                                                      '银行个人业务') - get_money(
                    lst, '银行小微贷款') - get_money(lst, '直销银行') - get_money(lst, '小额贷款公司') - get_money(lst,
                                                                                                   'P2P网贷') - get_money(
                    lst, '大型消费金融公司') - get_money(lst, '一般消费分期平台')

            df6 = get_js(df[df['item_name'] == '近60个月以上申请人在多个平台申请借款'], 'item_detail')
            if len(df6) > 0:
                lst = ';'.join(df6['platform_detail'][0])
                self.variables['net_apply_bank_his'] = get_money(lst, '网上银行') + get_money(lst, '信用卡中心') + get_money(lst,
                                                                                                                    '银行消费金融公司') + get_money(
                    lst, '银行对公业务') + get_money(lst, '银行个人业务') + get_money(lst, '银行小微贷款') + get_money(lst, '直销银行')
                self.variables['net_apply_sloan_his'] = get_money(lst, '小额贷款公司')
                self.variables['net_apply_p2p_his'] = get_money(lst, 'P2P网贷')
                self.variables['net_apply_confin_his'] = get_money(lst, '大型消费金融公司') + get_money(lst, '一般消费分期平台')
                self.variables['net_apply_other_his'] = df6['platform_count'][0] - get_money(lst, '网上银行') - get_money(
                    lst, '信用卡中心') - get_money(lst, '银行消费金融公司') - get_money(lst, '银行对公业务') - get_money(lst,
                                                                                                      '银行个人业务') - get_money(
                    lst, '银行小微贷款') - get_money(lst, '直销银行') - get_money(lst, '小额贷款公司') - get_money(lst,
                                                                                                   'P2P网贷') - get_money(
                    lst, '大型消费金融公司') - get_money(lst, '一般消费分期平台')

    def transform(self):
        self._info_fraud_verification(self._info_fraud_verification_df())
