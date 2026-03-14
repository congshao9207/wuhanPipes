# @Time : 2020/10/21 3:31 PM 
# @Author : lixiaobo
# @File : fin.py.py 
# @Software: PyCharm

import json

import jsonpath
import numpy as np
import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each, invoke_union
from util.mysql_reader import sql_to_df


class Fin(GroupedTransformer):
    """
    财务风险分析_fin
    """

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "fin"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'fin_qh_p2p_category_12m_cnt': 0,
            'fin_qh_small_loan_category_12m_cnt': 0,
            'fin_qh_insure_category_12m_cnt': 0,
            'fin_qh_bank_category_12m_cnt': 0,
            'fin_qh_rent_category_12m_cnt': 0,
            'fin_qh_consume_category_12m_cnt': 0,
            'fin_qh_consume_cns_category_12m_cnt': 0,
            'fin_qh_consume_htl_category_12m_cnt': 0,
            'fin_qh_other_category_12m_cnt': 0,
            'fin_qh_other_ins_category_12m_cnt': 0,
            'fin_qh_other_asm_category_12m_cnt': 0,
            'fin_qh_other_inv_category_12m_cnt': 0,
            'fin_qh_other_crf_category_12m_cnt': 0,
            'fin_qh_other_fac_category_12m_cnt': 0,
            'fin_qh_other_car_category_12m_cnt': 0,
            'fin_qh_other_tru_category_12m_cnt': 0,
            'fin_qh_other_thr_category_12m_cnt': 0,
            'fin_qh_other_oth_category_12m_cnt':0,
            'fin_qh_inc_3_6_org_cnt': 0,
            'fin_qh_dec_3_6_org_cnt': 0,
            'fin_qh_inc_6_12_org_cnt': 0,
            'fin_qh_dec_6_12_org_cnt': 0,
            'fin_qh_inc_12_24_org_cnt': 0,
            'fin_qh_dec_12_24_org_cnt': 0,
            'fin_qh_p2p_category_1m_cnt': 0,
            'fin_qh_small_loan_category_1m_cnt': 0,
            'fin_qh_insure_category_1m_cnt': 0,
            'fin_qh_bank_category_1m_cnt': 0,
            'fin_qh_rent_category_1m_cnt': 0,
            'fin_qh_consume_category_1m_cnt': 0,
            'fin_qh_other_category_1m_cnt': 0,
            'fin_qh_p2p_category_3m_cnt': 0,
            'fin_qh_small_loan_category_3m_cnt': 0,
            'fin_qh_insure_category_3m_cnt': 0,
            'fin_qh_bank_category_3m_cnt': 0,
            'fin_qh_rent_category_3m_cnt': 0,
            'fin_qh_consume_category_3m_cnt': 0,
            'fin_qh_other_category_3m_cnt': 0,
            'fin_qh_p2p_category_6m_cnt': 0,
            'fin_qh_small_loan_category_6m_cnt': 0,
            'fin_qh_insure_category_6m_cnt': 0,
            'fin_qh_bank_category_6m_cnt': 0,
            'fin_qh_rent_category_6m_cnt': 0,
            'fin_qh_consume_category_6m_cnt': 0,
            'fin_qh_other_category_6m_cnt': 0,
            'fin_qh_p2p_category_9m_cnt': 0,
            'fin_qh_small_loan_category_9m_cnt': 0,
            'fin_qh_insure_category_9m_cnt': 0,
            'fin_qh_bank_category_9m_cnt': 0,
            'fin_qh_rent_category_9m_cnt': 0,
            'fin_qh_consume_category_9m_cnt': 0,
            'fin_qh_other_category_9m_cnt': 0,
            'fin_hd_register_3m_cnt': 0,
            'fin_hd_apply_3m_cnt': 0,
            'fin_hd_loan_3m_cnt': 0,
            'fin_hd_overdue_3m_cnt': 0,
            'fin_hd_register_6m_cnt': 0,
            'fin_hd_apply_6m_cnt': 0,
            'fin_hd_loan_6m_cnt': 0,
            'fin_hd_overdue_6m_cnt': 0,
            'fin_hd_register_12m_cnt': 0,
            'fin_hd_apply_12m_cnt': 0,
            'fin_hd_loan_12m_cnt': 0,
            'fin_hd_overdue_12m_cnt': 0,
            'fin_hd_register_24m_cnt': 0,
            'fin_hd_apply_24m_cnt': 0,
            'fin_hd_loan_24m_cnt': 0,
            'fin_hd_overdue_24m_cnt': 0,
            'fin_hd_register_3m_org_cnt': 0,
            'fin_hd_apply_3m_org_cnt': 0,
            'fin_hd_loan_3m_org_cnt': 0,
            'fin_hd_overdue_3m_org_cnt': 0,
            'fin_hd_register_6m_org_cnt': 0,
            'fin_hd_apply_6m_org_cnt': 0,
            'fin_hd_loan_6m_org_cnt': 0,
            'fin_hd_overdue_6m_org_cnt': 0,
            'fin_hd_register_12m_org_cnt': 0,
            'fin_hd_apply_12m_org_cnt': 0,
            'fin_hd_loan_12m_org_cnt': 0,
            'fin_hd_overdue_12m_org_cnt': 0,
            'fin_hd_register_24m_org_cnt': 0,
            'fin_hd_apply_24m_org_cnt': 0,
            'fin_hd_loan_24m_org_cnt': 0,
            'fin_hd_overdue_24m_org_cnt': 0,
            'fin_hd_apply_3m_amt': 0,
            'fin_hd_loan_3m_amt': 0,
            'fin_hd_overdue_3m_amt': 0,
            'fin_hd_apply_6m_amt': 0,
            'fin_hd_loan_6m_amt': 0,
            'fin_hd_overdue_6m_amt': 0,
            'fin_hd_apply_12m_amt': 0,
            'fin_hd_loan_12m_amt': 0,
            'fin_hd_overdue_12m_amt': 0,
            'fin_hd_apply_24m_amt': 0,
            'fin_hd_loan_24m_amt': 0,
            'fin_hd_overdue_24m_amt': 0,
            'fin_hd_apply_3m_avg_amt': 0,
            'fin_hd_loan_3m_avg_amt': 0,
            'fin_hd_overdue_3m_avg_amt': 0,
            'fin_hd_apply_6m_avg_amt': 0,
            'fin_hd_loan_6m_avg_amt': 0,
            'fin_hd_overdue_6m_avg_amt': 0,
            'fin_hd_apply_12m_avg_amt': 0,
            'fin_hd_loan_12m_avg_amt': 0,
            'fin_hd_overdue_12m_avg_amt': 0,
            'fin_hd_apply_24m_avg_amt': 0,
            'fin_hd_loan_24m_avg_amt': 0,
            'fin_hd_overdue_24m_avg_amt': 0,
            'fin_qh_query_time': [],
            'fin_qh_industry_code': [],
            'fin_qh_query_reason': []
        }

    # 解析传入的json串
    def _jsonpath_load(self, json_str) -> list:
        """
        :param str: json串
        :return list: 返回字典列表，包含类型和相关数据数据
        """
        json_dic = json.loads(json_str)
        res = list()
        for i in jsonpath.jsonpath(json_dic, "$.queryData[*]"):
            t = dict()
            t['baseType'] = i.get('userType')
            t['name'] = i.get('name')
            t['idno'] = i.get('idno')
            t['phone'] = i.get('phone')
            res.append(t)
        return res

    # 读取前海 info_oth_loan_summary 数据集
    def _load_detail_info_data_df(self, dict_in) -> pd.DataFrame:
        sql = """
               SELECT detail_info_data
               FROM info_other_loan_summary  
               WHERE  user_name = %(user_name)s and id_card_no = %(id_card_no)s 
               and unix_timestamp(NOW()) < unix_timestamp(expired_at)
               ORDER BY id DESC LIMIT 1;
        """
        df = sql_to_df(sql=sql, params={"user_name": dict_in['name'], "id_card_no": dict_in['idno']})
        if df is not None and len(df) > 0:
            json_str = df['detail_info_data'].iloc[0]
            res = pd.read_json(json_str)
            if res.shape[0] > 0:
                res = res[~res['var1'].astype(str).str.contains('806460')]
            return res
        return None

    # 读取华道 sms_id 数据
    def _load_sms_id(self) -> int:
        sql = """
               SELECT sms_id
               FROM info_sms  
               WHERE  user_name = %(user_name)s and id_card_no = %(id_card_no)s
               and unix_timestamp(NOW()) < unix_timestamp(expired_at)
               ORDER BY id DESC LIMIT 1;
        """
        # 为了方便，直接用sql_to_df
        # todo 改为效率更高的SQL直接查询方式
        df_sms_id = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        if df_sms_id is not None and len(df_sms_id) > 0:
            return int(df_sms_id['sms_id'].iloc[0])
        return None

    # 读取华道 info_sms_loan_platform 数据
    def _load_info_sms_loan_platform_df(self, sms_id: int) -> pd.DataFrame:
        if sms_id is not None:
            sql = """               
               SELECT *
               FROM info_sms_loan_platform  
               WHERE sms_id = %(sms_id)s;
            """
            # 返回 info_sms_loan_platform 中该用户 sms_id 的所有数据
            df = sql_to_df(sql=sql, params={"sms_id": sms_id})
            return df
        return None

    # 读取华道 info_sms_loan_apply 数据
    def _load_info_sms_loan_apply_df(self, sms_id: int) -> pd.DataFrame:
        if sms_id is not None:
            sql = """               
               SELECT *
               FROM info_sms_loan_apply  
               WHERE sms_id = %(sms_id)s;
            """
            # 返回 info_sms_loan_apply 中该用户 sms_id 的所有数据
            df = sql_to_df(sql=sql, params={"sms_id": sms_id})
            return df
        return None

    # 读取华道 info_sms_loan 数据
    def _load_info_sms_loan_df(self, sms_id: int) -> pd.DataFrame:
        if sms_id is not None:
            sql = """
               SELECT *
               FROM info_sms_loan
               WHERE sms_id = %(sms_id)s;
            """
            # 返回 info_sms_loan_apply 中该用户 sms_id 的所有数据
            df = sql_to_df(sql=sql, params={"sms_id": sms_id})
            return df
        return None

    # 读取华道 info_sms_overdue_platform 数据
    def _load_info_sms_overdue_platform_df(self, sms_id: int) -> pd.DataFrame:
        if sms_id is not None:
            sql = """               
               SELECT *
               FROM info_sms_overdue_platform  
               WHERE sms_id = %(sms_id)s;
            """
            # 返回 info_sms_loan_apply 中该用户 sms_id 的所有数据
            df = sql_to_df(sql=sql, params={"sms_id": sms_id})
            return df
        return None

    # 计算 info_oth_loan_summary 相关字段
    def _info_oth_loan_summary(self, df=None):
        if df is not None and len(df) > 0:
            df['dateUpdated'] = pd.to_datetime(df['dateUpdated'])
            current_datetime = pd.datetime.now()
            df_12m = df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=12)]
            self.variables['fin_qh_p2p_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "P2P"])
            self.variables['fin_qh_small_loan_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "MCL"])
            self.variables['fin_qh_insure_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "FGC"])
            self.variables['fin_qh_bank_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "BAK"])
            self.variables['fin_qh_rent_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "LEA"])
            self.variables['fin_qh_consume_category_12m_cnt'] = len(
                df_12m[(df_12m['industryCode'] == "CNS") | (df_12m['industryCode'] == "HTL")])
            self.variables['fin_qh_consume_cns_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "CNS"])
            self.variables['fin_qh_consume_htl_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "HTL"])
            self.variables['fin_qh_other_category_12m_cnt'] = len(
                df_12m[df_12m['industryCode'].isin(['INS', 'ASM', 'INV', 'CRF', 'FAC', 'CAR', 'TRU', 'THR'])])
            self.variables['fin_qh_other_ins_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "INS"])
            self.variables['fin_qh_other_asm_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "ASM"])
            self.variables['fin_qh_other_inv_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "INS"])
            self.variables['fin_qh_other_crf_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "CRF"])
            self.variables['fin_qh_other_fac_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "FAC"])
            self.variables['fin_qh_other_car_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "CAR"])
            self.variables['fin_qh_other_tru_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "TRU"])
            self.variables['fin_qh_other_thr_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "THR"])
            self.variables['fin_qh_other_oth_category_12m_cnt'] = len(df_12m[df_12m['industryCode'] == "OTH"])

            var1_g3m = \
                df[df['dateUpdated'] >= (current_datetime - pd.offsets.DateOffset(months=3))][
                    'var1'].unique()
            var1_g6m_l3m = \
                df[(df['dateUpdated'] >= (current_datetime - pd.offsets.DateOffset(months=6))) & (df[
                                                                                                      'dateUpdated'] < (
                                                                                                          current_datetime - pd.offsets.DateOffset(
                                                                                                      months=3)))][
                    'var1'].unique()
            var1_g6m = \
                df[(df['dateUpdated'] >= (current_datetime - pd.offsets.DateOffset(months=6)))][
                    'var1'].unique()
            var1_g12m_l6m = \
                df[(df['dateUpdated'] >= (current_datetime - pd.offsets.DateOffset(months=12))) & (df[
                                                                                                       'dateUpdated'] < (
                                                                                                           current_datetime - pd.offsets.DateOffset(
                                                                                                       months=6)))][
                    'var1'].unique()
            var1_g12m = \
                df[(df['dateUpdated'] >= (current_datetime - pd.offsets.DateOffset(months=12)))][
                    'var1'].unique()
            var1_g24m_l12m = \
                df[(df['dateUpdated'] >= (current_datetime - pd.offsets.DateOffset(months=24))) & (df[
                                                                                                       'dateUpdated'] < (
                                                                                                           current_datetime - pd.offsets.DateOffset(
                                                                                                       months=12)))][
                    'var1'].unique()

            self.variables['fin_qh_inc_3_6_org_cnt'] = len(set(var1_g3m) - set(var1_g6m_l3m))
            self.variables['fin_qh_dec_3_6_org_cnt'] = len(set(var1_g6m_l3m) - set(var1_g3m))
            self.variables['fin_qh_inc_6_12_org_cnt'] = len(set(var1_g6m) - set(var1_g12m_l6m))
            self.variables['fin_qh_dec_6_12_org_cnt'] = len(set(var1_g12m_l6m) - set(var1_g6m))
            self.variables['fin_qh_inc_12_24_org_cnt'] = len(set(var1_g12m) - set(var1_g24m_l12m))
            self.variables['fin_qh_dec_12_24_org_cnt'] = len(set(var1_g24m_l12m) - set(var1_g12m))

            df_1m = df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=1)]
            self.variables['fin_qh_p2p_category_1m_cnt'] = len(df_1m[df_1m['industryCode'] == "P2P"])
            self.variables['fin_qh_small_loan_category_1m_cnt'] = len(df_1m[df_1m['industryCode'] == "MCL"])
            self.variables['fin_qh_insure_category_1m_cnt'] = len(df_1m[df_1m['industryCode'] == "FGC"])
            self.variables['fin_qh_bank_category_1m_cnt'] = len(df_1m[df_1m['industryCode'] == "BAK"])
            self.variables['fin_qh_rent_category_1m_cnt'] = len(df_1m[df_1m['industryCode'] == "LEA"])
            self.variables['fin_qh_consume_category_1m_cnt'] = len(
                df_1m[(df_1m['industryCode'] == "CNS") | (df_1m['industryCode'] == "HTL")])
            self.variables['fin_qh_other_category_1m_cnt'] = len(
                df_1m[df_1m['industryCode'].isin(['INS', 'ASM', 'INV', 'CRF', 'FAC', 'CAR', 'TRU', 'THR'])])

            df_3m = df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=3)]
            self.variables['fin_qh_p2p_category_3m_cnt'] = len(df_3m[df_3m['industryCode'] == "P2P"])
            self.variables['fin_qh_small_loan_category_3m_cnt'] = len(df_3m[df_3m['industryCode'] == "MCL"])
            self.variables['fin_qh_insure_category_3m_cnt'] = len(df_3m[df_3m['industryCode'] == "FGC"])
            self.variables['fin_qh_bank_category_3m_cnt'] = len(df_3m[df_3m['industryCode'] == "BAK"])
            self.variables['fin_qh_rent_category_3m_cnt'] = len(df_3m[df_3m['industryCode'] == "LEA"])
            self.variables['fin_qh_consume_category_3m_cnt'] = len(
                df_3m[(df_3m['industryCode'] == "CNS") | (df_3m['industryCode'] == "HTL")])
            self.variables['fin_qh_other_category_3m_cnt'] = len(
                df_3m[df_3m['industryCode'].isin(['INS', 'ASM', 'INV', 'CRF', 'FAC', 'CAR', 'TRU', 'THR'])])

            df_6m = df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=6)]
            self.variables['fin_qh_p2p_category_6m_cnt'] = len(df_6m[df_6m['industryCode'] == "P2P"])
            self.variables['fin_qh_small_loan_category_6m_cnt'] = len(df_6m[df_6m['industryCode'] == "MCL"])
            self.variables['fin_qh_insure_category_6m_cnt'] = len(df_6m[df_6m['industryCode'] == "FGC"])
            self.variables['fin_qh_bank_category_6m_cnt'] = len(df_6m[df_6m['industryCode'] == "BAK"])
            self.variables['fin_qh_rent_category_6m_cnt'] = len(df_6m[df_6m['industryCode'] == "LEA"])
            self.variables['fin_qh_consume_category_6m_cnt'] = len(
                df_6m[(df_6m['industryCode'] == "CNS") | (df_6m['industryCode'] == "HTL")])
            self.variables['fin_qh_other_category_6m_cnt'] = len(
                df_6m[df_6m['industryCode'].isin(['INS', 'ASM', 'INV', 'CRF', 'FAC', 'CAR', 'TRU', 'THR'])])

            df_9m = df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=9)]
            self.variables['fin_qh_p2p_category_9m_cnt'] = len(df_9m[df_9m['industryCode'] == "P2P"])
            self.variables['fin_qh_small_loan_category_9m_cnt'] = len(df_9m[df_9m['industryCode'] == "MCL"])
            self.variables['fin_qh_insure_category_9m_cnt'] = len(df_9m[df_9m['industryCode'] == "FGC"])
            self.variables['fin_qh_bank_category_9m_cnt'] = len(df_9m[df_9m['industryCode'] == "BAK"])
            self.variables['fin_qh_rent_category_9m_cnt'] = len(df_9m[df_9m['industryCode'] == "LEA"])
            self.variables['fin_qh_consume_category_9m_cnt'] = len(
                df_9m[(df_9m['industryCode'] == "CNS") | (df_9m['industryCode'] == "HTL")])
            self.variables['fin_qh_other_category_9m_cnt'] = len(
                df_9m[df_9m['industryCode'].isin(['INS', 'ASM', 'INV', 'CRF', 'FAC', 'CAR', 'TRU', 'THR'])])

            self.variables['fin_qh_query_time'] = \
                df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=24)]['dateUpdated'].apply(
                    lambda x: x.strftime('%Y-%m-%d')).to_list()

            industry_rename_dir = {
                'MCL': '小贷',
                'CNS': '消金',
                'HTL': '消金',
                'BAK': '银行',
                'LEA': '融资租赁',
                'FGC': '担保',
                'INS': '其他',
                'ASM': '其他',
                'INV': '其他',
                'CRF': '其他',
                'FAC': '其他',
                'CAR': '其他',
                'TRU': '其他',
                'THR': '其他',
                'P2P': 'P2P'
            }

            self.variables['fin_qh_industry_code'] = \
                df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=24)]['industryCode'].map(
                    industry_rename_dir).to_list()

            reason_rename_dir = {
                '1': "贷前查询",
                '2': '贷中查询',
                '3': '贷后查询',
                '99': '其他'
            }

            self.variables['fin_qh_query_reason'] = \
                df[df['dateUpdated'] >= current_datetime - pd.offsets.DateOffset(months=24)]['reasonCode'].map(
                    lambda x: reason_rename_dir[str(x)]).to_list()

    # 计算 info_sms_loan_platform 相关字段
    def _info_sms_loan_platform(self, df=None):
        if df is not None and len(df) > 0:
            df['register_time'] = pd.to_datetime(df['register_time'])
            current_datetime = pd.datetime.now()
            df_3m = df[df['register_time'] >= current_datetime - pd.offsets.DateOffset(months=3)]
            self.variables['fin_hd_register_3m_cnt'] = len(df_3m)
            self.variables['fin_hd_register_3m_org_cnt'] = len(df_3m['platform_code'].unique())

            df_6m = df[df['register_time'] >= current_datetime - pd.offsets.DateOffset(months=6)]
            self.variables['fin_hd_register_6m_cnt'] = len(df_6m)
            self.variables['fin_hd_register_6m_org_cnt'] = len(df_6m['platform_code'].unique())

            df_12m = df[df['register_time'] >= current_datetime - pd.offsets.DateOffset(months=12)]
            self.variables['fin_hd_register_12m_cnt'] = len(df_12m)
            self.variables['fin_hd_register_12m_org_cnt'] = len(df_12m['platform_code'].unique())

            df_24m = df[df['register_time'] >= current_datetime - pd.offsets.DateOffset(months=24)]
            self.variables['fin_hd_register_24m_cnt'] = len(df_24m)
            self.variables['fin_hd_register_24m_org_cnt'] = len(df_24m['platform_code'].unique())

    # 计算 info_sms_loan_apply 相关字段
    def _info_sms_loan_apply(self, df=None):
        if df is not None and len(df) > 0:
            reset_dir = {
                '0W～0.2W': 1000,
                '0.2W～0.5W': 3500,
                '0.5W～1W': 7500,
                '1W～3W': 20000,
                '3W～5W': 40000,
                '5W～10W': 75000,
                '10W以上': 100000
            }
            df['apply_time'] = pd.to_datetime(df['apply_time'])
            df['apply_amount'] = df['apply_amount'].map(reset_dir)
            current_datetime = pd.datetime.now()
            df_3m = df[df['apply_time'] >= current_datetime - pd.offsets.DateOffset(months=3)]
            if df_3m is not None and len(df_3m) > 0:
                self.variables['fin_hd_apply_3m_cnt'] = len(df_3m)
                self.variables['fin_hd_apply_3m_org_cnt'] = len(df_3m['platform_code'].unique())
                self.variables['fin_hd_apply_3m_amt'] = df_3m['apply_amount'].sum()
                self.variables['fin_hd_apply_3m_avg_amt'] = np.round(df_3m['apply_amount'].mean(), 2)

            df_6m = df[df['apply_time'] >= current_datetime - pd.offsets.DateOffset(months=6)]
            if df_6m is not None and len(df_6m) > 0:
                self.variables['fin_hd_apply_6m_cnt'] = len(df_6m)
                self.variables['fin_hd_apply_6m_org_cnt'] = len(df_6m['platform_code'].unique())
                self.variables['fin_hd_apply_6m_amt'] = df_6m['apply_amount'].sum()
                self.variables['fin_hd_apply_6m_avg_amt'] = np.round(df_6m['apply_amount'].mean(), 2)

            df_12m = df[df['apply_time'] >= current_datetime - pd.offsets.DateOffset(months=12)]
            if df_12m is not None and len(df_12m) > 0:
                self.variables['fin_hd_apply_12m_cnt'] = len(df_12m)
                self.variables['fin_hd_apply_12m_org_cnt'] = len(df_12m['platform_code'].unique())
                self.variables['fin_hd_apply_12m_amt'] = df_12m['apply_amount'].sum()
                self.variables['fin_hd_apply_12m_avg_amt'] = np.round(df_12m['apply_amount'].mean(), 2)

            df_24m = df[df['apply_time'] >= current_datetime - pd.offsets.DateOffset(months=24)]
            if df_24m is not None and len(df_24m) > 0:
                self.variables['fin_hd_apply_24m_cnt'] = len(df_24m)
                self.variables['fin_hd_apply_24m_org_cnt'] = len(df_24m['platform_code'].unique())
                self.variables['fin_hd_apply_24m_amt'] = df_24m['apply_amount'].sum()
                self.variables['fin_hd_apply_24m_avg_amt'] = np.round(df_24m['apply_amount'].mean(), 2)

    # 计算 info_sms_loan 相关字段
    def _info_sms_loan(self, df=None):
        if df is not None and len(df) > 0:
            reset_dir = {
                '0W～0.2W': 1000,
                '0.2W～0.5W': 3500,
                '0.5W～1W': 7500,
                '1W～3W': 20000,
                '3W～5W': 40000,
                '5W～10W': 75000,
                '10W以上': 100000
            }
            df['loan_time'] = pd.to_datetime(df['loan_time'])
            df['loan_amount'] = df['loan_amount'].map(reset_dir)
            current_datetime = pd.datetime.now()

            df_3m = df[df['loan_time'] >= current_datetime - pd.offsets.DateOffset(months=3)]
            if df_3m is not None and len(df_3m) > 0:
                self.variables['fin_hd_loan_3m_cnt'] = len(df_3m)
                self.variables['fin_hd_loan_3m_org_cnt'] = len(df_3m['platform_code'].unique())
                self.variables['fin_hd_loan_3m_amt'] = df_3m['loan_amount'].sum()
                self.variables['fin_hd_loan_3m_avg_amt'] = np.round(df_3m['loan_amount'].mean(), 2)

            df_6m = df[df['loan_time'] >= current_datetime - pd.offsets.DateOffset(months=6)]
            if df_6m is not None and len(df_6m) > 0:
                self.variables['fin_hd_loan_6m_cnt'] = len(df_6m)
                self.variables['fin_hd_loan_6m_org_cnt'] = len(df_6m['platform_code'].unique())
                self.variables['fin_hd_loan_6m_amt'] = df_6m['loan_amount'].sum()
                self.variables['fin_hd_loan_6m_avg_amt'] = np.round(df_6m['loan_amount'].mean(), 2)

            df_12m = df[df['loan_time'] >= current_datetime - pd.offsets.DateOffset(months=12)]
            if df_12m is not None and len(df_12m) > 0:
                self.variables['fin_hd_loan_12m_cnt'] = len(df_12m)
                self.variables['fin_hd_loan_12m_org_cnt'] = len(df_12m['platform_code'].unique())
                self.variables['fin_hd_loan_12m_amt'] = df_12m['loan_amount'].sum()
                self.variables['fin_hd_loan_12m_avg_amt'] = np.round(df_12m['loan_amount'].mean(), 2)

            df_24m = df[df['loan_time'] >= current_datetime - pd.offsets.DateOffset(months=24)]
            if df_24m is not None and len(df_24m) > 0:
                self.variables['fin_hd_loan_24m_cnt'] = len(df_24m)
                self.variables['fin_hd_loan_24m_org_cnt'] = len(df_24m['platform_code'].unique())
                self.variables['fin_hd_loan_24m_amt'] = df_24m['loan_amount'].sum()
                self.variables['fin_hd_loan_24m_avg_amt'] = np.round(df_24m['loan_amount'].mean(), 2)

    # 计算 info_sms_overdue_platform 相关字段
    def _info_sms_overdue_platform(self, df=None):
        if df is not None and len(df) > 0:
            reset_dir = {
                '0W～0.2W': 1000,
                '0.2W～0.5W': 3500,
                '0.5W～1W': 7500,
                '1W～3W': 20000,
                '3W～5W': 40000,
                '5W～10W': 75000,
                '10W以上': 100000
            }
            df['overdue_time'] = pd.to_datetime(df['overdue_time'])
            df['overdue_money'] = df['overdue_money'].map(reset_dir)
            current_datetime = pd.datetime.now()

            df_3m = df[df['overdue_time'] >= current_datetime - pd.offsets.DateOffset(months=3)]
            if df_3m is not None and len(df_3m) > 0:
                self.variables['fin_hd_overdue_3m_cnt'] = len(df_3m)
                self.variables['fin_hd_overdue_3m_org_cnt'] = len(df_3m['platform_code'].unique())
                self.variables['fin_hd_overdue_3m_amt'] = df_3m['overdue_money'].sum()
                self.variables['fin_hd_overdue_3m_avg_amt'] = np.round(df_3m['overdue_money'].mean(), 2)

            df_6m = df[df['overdue_time'] >= current_datetime - pd.offsets.DateOffset(months=6)]
            if df_6m is not None and len(df_6m) > 0:
                self.variables['fin_hd_overdue_6m_cnt'] = len(df_6m)
                self.variables['fin_hd_overdue_6m_org_cnt'] = len(df_6m['platform_code'].unique())
                self.variables['fin_hd_overdue_6m_amt'] = df_6m['overdue_money'].sum()
                self.variables['fin_hd_overdue_6m_avg_amt'] = np.round(df_6m['overdue_money'].mean(), 2)

            df_12m = df[df['overdue_time'] >= current_datetime - pd.offsets.DateOffset(months=12)]
            if df_12m is not None and len(df_12m) > 0:
                self.variables['fin_hd_overdue_12m_cnt'] = len(df_12m)
                self.variables['fin_hd_overdue_12m_org_cnt'] = len(df_12m['platform_code'].unique())
                self.variables['fin_hd_overdue_12m_amt'] = df_12m['overdue_money'].sum()
                self.variables['fin_hd_overdue_12m_avg_amt'] = np.round(df_12m['overdue_money'].mean(), 2)

            df_24m = df[df['overdue_time'] >= current_datetime - pd.offsets.DateOffset(months=24)]
            if df_24m is not None and len(df_24m) > 0:
                self.variables['fin_hd_overdue_24m_cnt'] = len(df_24m)
                self.variables['fin_hd_overdue_24m_org_cnt'] = len(df_24m['platform_code'].unique())
                self.variables['fin_hd_overdue_24m_amt'] = df_24m['overdue_money'].sum()
                self.variables['fin_hd_overdue_24m_avg_amt'] = np.round(df_24m['overdue_money'].mean(), 2)

    def transform(self):
        each = self.origin_data
        strategy = self.origin_data.get("extraParam")['strategy']
        if "PERSONAL" in each['baseType'].upper() and strategy == '01':
            df = self._load_detail_info_data_df(each)
            self._info_oth_loan_summary(df)

            sms_id = self._load_sms_id()

            df = self._load_info_sms_loan_platform_df(sms_id)
            self._info_sms_loan_platform(df)

            df = self._load_info_sms_loan_apply_df(sms_id)
            self._info_sms_loan_apply(df)

            df = self._load_info_sms_loan_df(sms_id)
            self._info_sms_loan(df)

            df = self._load_info_sms_overdue_platform_df(sms_id)
            self._info_sms_overdue_platform(df)


