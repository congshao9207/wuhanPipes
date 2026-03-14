# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:23 AM
# @Author : lixiaobo
# @Site : 
# @File : tf0001.py
# @Software: PyCharm
import pandas as pd

from mapping.tranformer import Transformer
from mapping.utils.df_comparator_util import df_compare, to_df


class Tf0001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.pre_biz_date = ""
        '''self.variables = {
            'relent_court_open_owed_owe_laf': '0',  # 联企法院_在营企业_欠款欠费名单命中次数_贷后新增
            'relent_court_open_court_dishonesty_laf': '0',  # 联企法院_在营企业_失信老赖名单命中次数_贷后新增
            'relent_court_open_rest_entry_laf': '0',  # 联企法院_在营企业_限制出入境名单命中次数_贷后新增
            'relent_court_open_high_cons_laf': '0',  # 联企法院_在营企业_限制高消费名单命中次数_贷后新增
            'relent_court_open_cri_sus_laf': '0',  # 联企法院_在营企业_罪犯及嫌疑人名单命中次数_贷后新增
            'relent_court_open_fin_loan_con_laf': '0',  # 联企法院_在营企业_金融借款合同纠纷_贷后新增
            'relent_court_open_loan_con_laf': '0',  # 联企法院_在营企业_借款合同纠纷_贷后新增
            'relent_court_open_pop_loan': '0',  # 联企法院_在营企业_民间借贷纠纷_贷后新增
            'relent_court_open_tax_arrears_laf': '0',  # 联企法院_在营企业_欠税名单命中次数_贷后新增
            'relent_court_open_pub_info_laf': '0',  # 联企法院_在营企业_执行公开信息命中次数_贷后新增
            'relent_court_open_admi_violation_laf': '0',  # 联企法院_在营企业_行政违法记录命中次数_贷后新增
            'relent_court_open_judge_docu_laf': '0',  # 联企法院_在营企业_民商事裁判文书命中次数_贷后新增
            'relent_court_open_judge_proc_laf': '0',  # 联企法院_在营企业_民商事审判流程命中次数_贷后新增
            'relent_court_open_tax_pay_laf': '0',  # 联企法院_在营企业_纳税非正常户命中次数_贷后新增
        }
        '''

    def _relent_court_open_owed_owe_laf(self, variable_name):
        before_df = self._relent_court_open_owed_owe_laf_before()
        after_df = self._relent_court_open_owed_owe_laf_after()

        # print("before_df", before_df)
        # print("after_df", after_df)
        df_compare(self.variables, before_df, after_df, variable_name)

    def _relent_court_open_owed_owe_laf_before(self):
        sql = '''
                        select le.ent_name as ent_name from(
                        select * from info_per_bus_basic where name=%(user_name)s and id_card_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                        )tab inner join info_per_bus_legal le on tab.id=le.basic_id where le.ent_status REGEXP '在营（开业）|存续（在营、开业、在册）'
                        union
                        select sh.ent_name as ent_name from(
                        select * from info_per_bus_basic where name=%(user_name)s and id_card_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                        )tab inner join info_per_bus_shareholder sh on sh.basic_id = tab.id and sh.ent_status REGEXP '在营（开业）|存续（在营、开业、在册）' and sub_conam/reg_cap >= 0
                        '''
        biz_sql = '''
                        select contract_no from info_court_arrearage arr inner join(
                        select * from info_court  where unique_name=%(ent_name)s and create_time < %(pre_biz_date)s order by create_time limit 1
                        ) tab on arr.court_id = tab.id;
                        '''
        return self.__search_biz_no(sql, biz_sql, "contract_no")

    def _relent_court_open_owed_owe_laf_after(self):
        sql = '''
                        select le.ent_name as ent_name from(
                            select * from info_per_bus_basic where `name`=%(user_name)s and id_card_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                            )tab inner join info_per_bus_legal le on tab.id = le.basic_id where le.ent_status REGEXP '在营（开业）|存续（在营、开业、在册）'
                            union
                            select sh.ent_name as ent_name from(
                            select * from info_per_bus_basic where `name`=%(user_name)s and id_card_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                            )tab inner join info_per_bus_shareholder sh on sh.basic_id = tab.id and sh.ent_status REGEXP '在营（开业）|存续（在营、开业、在册）' and sub_conam/reg_cap >= 0
                        '''
        biz_sql = '''
                        select contract_no from info_court_arrearage arr inner join(
                        select * from info_court where unique_name=%(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by create_time limit 1
                        ) tab on arr.court_id = tab.id;
                        '''
        return self.__search_biz_no(sql, biz_sql, "contract_no")

    def __search_biz_no(self, ent_name_sql, biz_sql, biz_no_type):
        ent_list = self.__get_ent_list(ent_name_sql)
        # print("ent_list:", ent_list)
        result_df = pd.DataFrame(columns=[biz_no_type])
        for ent in ent_list:
            biz_no_df = to_df(self, biz_sql, {"ent_name": ent})
            # print("biz_no_df:", biz_no_df)
            if biz_no_df.shape[0] > 0:
                result_df = result_df.append(biz_no_df)
        return result_df

    def __get_ent_list(self, sql):
        df = to_df(self, sql, {})
        if df.shape[0] > 0:
            return df["ent_name"].to_list()
        return []

    def transform(self):
        # 在贷后报告中，不用清洗
        pass
        # 前一个业务的创建时间
        # self.pre_biz_date = self.origin_data.get('preBizDate')
        # self._relent_court_open_owed_owe_laf("relent_court_open_owed_owe_laf")
