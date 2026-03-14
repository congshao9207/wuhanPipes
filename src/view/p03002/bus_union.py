# @Time : 2020/10/21 3:31 PM 
# @Author : lixiaobo
# @File : bus.py.py 
# @Software: PyCharm


import json

import jsonpath
import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each, invoke_union
from util.common_util import get_industry_risk_level, get_industry_risk_tips
from util.mysql_reader import sql_to_df


class Bus(GroupedTransformer):
    """
    经营风险分析_bus
    """

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "bus"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'bus_industry_cnt': 0,
            'bus_abnomal_cnt': 0,
            'bus_change_record_cnt': 0,
            'bus_invest_cnt': 0,
            'bus_industry_ent_name': "",
            'bus_industry_industry': "",
            'bus_industry_grade': "",
            'bus_industry_hint': [],
            'bus_abnormal_name': [],
            'bus_abnormal_cause': [],
            'bus_abnormal_date': [],
            'bus_abnormal_org': [],
            'bus_abnormal_clear_cause': [],
            'bus_abnormal_clear_date': [],
            'bus_abnormal_clear_org': [],
            'bus_change_name': [],
            'bus_change_category': [],
            'bus_change_date': [],
            'bus_change_content_before': [],
            'bus_change_content_after': [],
            'bus_invest_name': [],
            'bus_invest_code': [],
            'bus_invest_legal_rep': [],
            'bus_invest_regist': [],
            'bus_invest_type': [],
            'bus_invest_capital': [],
            'bus_invest_status': [],
            'bus_invest_date': [],
            'bus_invest_com_cnt': [],
            'bus_invest_proportion': [],
            'bus_invest_form': []
        }

    def _jsonpath_load(self, json_dic) -> list:
        """
        :param str: json串
        :return list: 返回字典列表，包含类型和相关数据数据
        """
        res = list()
        for i in jsonpath.jsonpath(json_dic, "$..queryData[*]"):
            t = dict()
            t['baseType'] = i.get('userType')
            t['name'] = i.get('name')
            t['idno'] = i.get('idno')
            t['phone'] = i.get('phone')
            t['strategy'] = i.get('extraParam')['strategy']
            res.append(t)
        return res

    # 读取企业数据 info_com_bus_basic 读取企业数据主键
    def _load_info_com_bus_basic_id(self, dict_in) -> int:
        if len(dict_in['idno']) != 0:
            sql = """
               SELECT *
               FROM info_com_bus_basic WHERE ent_name = %(ent_name)s
               and credit_code = %(credit_code)s
               and channel_api_no = '24001'
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"ent_name": dict_in['name'], "credit_code": dict_in['idno']})
            if df is not None and len(df) > 0:
                df.sort_values(by=['expired_at'], ascending=False, inplace=True)
                return int(df['id'].iloc[0])
        else:
            sql = """
               SELECT *
               FROM info_com_bus_basic WHERE ent_name = %(ent_name)s
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"ent_name": dict_in['name']})
            if df is not None and len(df) > 0:
                return int(df['id'].iloc[0])
        return None



    # 读取 info_com_bus_exception 数据
    def _load_info_com_bus_exception_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT b.ent_name,a.result_in,a.date_in,a.org_name_in,a.result_out,a.date_out,a.org_name_out
               FROM info_com_bus_exception as a inner join info_com_bus_basic as b on a.basic_id = b.id
               WHERE basic_id = %(id)s;
        """
        info_com_bus_exception_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_exception_df is not None and len(info_com_bus_exception_df) > 0:
            return info_com_bus_exception_df
        return None

    # 读取 info_com_bus_alter 数据
    def _load_info_com_bus_alter_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT b.ent_name,a.alt_item,a.alt_date,a.alt_be,a.alt_af
               FROM info_com_bus_alter as a inner join info_com_bus_basic as b on a.basic_id = b.id
               WHERE basic_id = %(id)s and alt_be != alt_af ;
        """
        info_com_bus_alter_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_alter_df is not None and len(info_com_bus_alter_df) > 0:
            return info_com_bus_alter_df
        return None

    # 读取 info_com_bus_entinvitem 数据
    def _load_info_com_bus_entinvitem_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT b.ent_name,a.credit_code,a.fr_name,a.reg_no,a.ent_type,a.reg_cap,a.ent_status,a.es_date,a.pinv_amount,a.funded_ratio,a.con_form
               FROM info_com_bus_entinvitem as a inner join info_com_bus_basic as b on a.basic_id = b.id
               WHERE basic_id = %(id)s;
        """
        info_com_bus_entinvitem_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_entinvitem_df is not None and len(info_com_bus_entinvitem_df) > 0:
            return info_com_bus_entinvitem_df
        return None

    # 计算 bus_abnormal 相关字段
    def _bus_abnormal(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['ent_name', 'date_in'])
            self.variables['bus_abnomal_cnt'] += len(df)
            self.variables['bus_abnormal_name'] += df['ent_name'].to_list()
            self.variables['bus_abnormal_cause'] += df['result_in'].to_list()
            self.variables['bus_abnormal_date'] += df['date_in'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['bus_abnormal_org'] += df['org_name_in'].to_list()
            self.variables['bus_abnormal_clear_cause'] += df['result_out'].to_list()
            self.variables['bus_abnormal_clear_date'] += df['date_out'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['bus_abnormal_clear_org'] += df['org_name_out'].to_list()

    # 计算 bus_change 相关字段
    def _bus_change(self, df=None):
        if df is not None:
            df = df[pd.notna(df.alt_item)]
            if len(df) > 0:
                df = df.drop_duplicates().sort_values(by=['alt_date'], ascending=False)
                self.variables['bus_change_record_cnt'] += len(df)
                self.variables['bus_change_name'] += df['ent_name'].to_list()
                self.variables['bus_change_category'] += df['alt_item'].to_list()
                self.variables['bus_change_date'] += df['alt_date'].apply(
                    lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
                self.variables['bus_change_content_before'] += df['alt_be'].to_list()
                self.variables['bus_change_content_after'] += df['alt_af'].to_list()

    # 计算 bus_invest 相关字段
    def _bus_invest(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['ent_name','credit_code','es_date']).sort_values(by=['credit_code'])
            self.variables['bus_invest_cnt'] += len(df)
            self.variables['bus_invest_name'] += df['ent_name'].to_list()
            self.variables['bus_invest_code'] += df['credit_code'].to_list()
            self.variables['bus_invest_legal_rep'] += df['fr_name'].to_list()
            self.variables['bus_invest_regist'] += df['reg_no'].to_list()
            self.variables['bus_invest_type'] += df['ent_type'].to_list()
            self.variables['bus_invest_capital'] += df['reg_cap'].to_list()
            self.variables['bus_invest_status'] += df['ent_status'].to_list()
            self.variables['bus_invest_date'] += df['es_date'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['bus_invest_com_cnt'] += df['pinv_amount'].to_list()
            self.variables['bus_invest_proportion'] += df['funded_ratio'].to_list()
            self.variables['bus_invest_form'] += df['con_form'].to_list()

    def _get_industry_name(self, industry):
        sql = '''
            select cn_name from tree_dic where opt_type="STD_GB_4754-2017" and en_name=%(industry)s order by id desc limit 1
        '''
        df = sql_to_df(sql=sql, params={"industry":industry})
        if not df.empty:
            return df.loc[0, 'cn_name']
        else:
            return ''


    def transform(self):
        query_list = self._jsonpath_load(self.full_msg)
        industry = self.full_msg.get('strategyParam').get('industry') if self.full_msg.get('strategyParam') is not None else ''
        for each in query_list:
            if "PERSONAL" not in each['baseType'].upper() and each['strategy'] == '01':
                com_id = self._load_info_com_bus_basic_id(each)

                df = self._load_info_com_bus_exception_df(com_id)
                self._bus_abnormal(df)

                df = self._load_info_com_bus_alter_df(com_id)
                self._bus_change(df)

                df = self._load_info_com_bus_entinvitem_df(com_id)
                self._bus_invest(df)
        if industry is not None:
            self.variables['bus_industry_industry'] = self._get_industry_name(industry)
            self.variables['bus_industry_grade'] = get_industry_risk_level(industry)
            self.variables['bus_industry_hint'] = get_industry_risk_tips(industry)
            self.variables['bus_industry_cnt'] = len(self.variables['bus_industry_hint'])




