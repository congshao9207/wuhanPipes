# -*- coding: utf-8 -*-
# @Time : 2020/2/24 9:20 AM
# @Author : lixiaobo
# @Site : 
# @File : t16001.py
# @Software: PyCharm

from mapping.tranformer import Transformer
from mapping.utils.df_comparator_util import df_compare, var_compare, to_df


class T16001(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'court_owed_owe_laf': '0',  # 法院核查_个人_欠款欠费名单贷后新增
            'court_dishonesty_laf': '0',  # 法院核查_个人_失信老赖名单_贷后新增
            'court_limit_entry_laf': '0',  # 法院核查_个人_限制出入境名单_贷后新增
            'court_high_cons_laf': '0',  # 法院核查_个人_限制高消费名单命中次数_贷后新增
            'court_cri_sus_laf': '0',  # 法院核查_企业_罪犯及嫌疑人名单命中次数_贷后新增
            'court_fin_loan_con_laf': '0',  # 法院核查_个人_金融借款合同纠纷_贷后新增
            'court_loan_con_laf': '0',  # 法院核查_个人_借款合同纠纷_贷后新增
            'court_pop_loan_laf': '0',  # 法院核查_个人_民间借贷纠纷_贷后新增
            'court_admi_vio_laf': '0',  # 法院核查_个人_行政违法记录命中次数_贷后新增
            'court_judge_laf': '0',  # 法院核查_个人_民商事裁判文书命中次数_贷后新增
            'court_trial_proc_laf': '0',  # 法院核查_个人_民商事审判流程命中次数_贷后新增
            'court_tax_pay_laf': '0',  # 法院核查_个人_纳税非正常户命中次数_贷后新增
            'court_pub_info_laf': '0',  # 法院核查_个人_执行公开信息命中次数_贷后新增
            'court_tax_arrears_laf': '0',  # 法院核查_个人_欠税名单命中次数_贷后新增
        }
        self.pre_biz_date = None

    def _court_owed_owe_laf(self, variable_name):
        old_sql = '''
        select contract_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
            ) tab left join info_court_arrearage arr on arr.court_id = tab.id where contract_no is not null;
        '''
        new_sql = '''
        select contract_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            ) tab left join info_court_arrearage t on t.court_id = tab.id where contract_no is not null;
        '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_dishonesty_laf(self, variable_name):
        old_sql = '''
        select execute_case_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
            ) tab left join info_court_deadbeat t on t.court_id = tab.id where execute_case_no is not null;
        '''
        new_sql = '''
        select execute_case_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            ) tab left join info_court_deadbeat t on t.court_id = tab.id where execute_case_no is not null;
        '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_limit_entry_laf(self, variable_name):
        old_sql = '''
        select execute_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
            ) tab left join info_court_limited_entry_exit t on t.court_id = tab.id where execute_no is not null;
        '''
        new_sql = '''
        select execute_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            ) tab left join info_court_limited_entry_exit t on t.court_id = tab.id where execute_no is not null;
        '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_high_cons_laf(self, variable_name):
        old_sql = '''
        select execute_case_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
            ) tab left join info_court_limit_hignspending t on t.court_id = tab.id where execute_case_no is not null;
        '''
        new_sql = '''
        select execute_case_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            ) tab left join info_court_limit_hignspending t on t.court_id = tab.id where execute_case_no is not null;
        '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_cri_sus_laf(self, variable_name):
        old_sql = '''
        select case_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
            ) tab left join info_court_criminal_suspect t on t.court_id = tab.id where case_no is not null;
        '''
        new_sql = '''
        select case_no from(
            select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
            ) tab left join info_court_criminal_suspect t on t.court_id = tab.id where case_no is not null;
        '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_fin_loan_stats(self, variable_name, case_reason):
        old_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_judicative_pape t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "%%被告%%" and case_no is not null
                union
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join info_court_trial_process t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "%%被告%%" and case_no is not null;
                '''

        new_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_judicative_pape t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "%%被告%%" and case_no is not null
                union
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_trial_process t on t.court_id = tab.id where case_reason regexp %(case_reason)s and legal_status like "%%被告%%" and case_no is not null;
                '''
        old_df = to_df(self, old_sql, {"case_reason": case_reason})
        new_df = to_df(self, new_sql, {"case_reason": case_reason})
        df_compare(self.variables, old_df, new_df, variable_name)

    def _court_admi_vio_laf(self, variable_name):
        old_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join  info_court_administrative_violation t on t.court_id = tab.id where case_no is not null;
                '''
        new_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_administrative_violation t on t.court_id = tab.id where case_no is not null;
                '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_judge_laf(self, variable_name):
        old_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join  info_court_judicative_pape t on t.court_id = tab.id where case_no is not null;
                '''
        new_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_judicative_pape t on t.court_id = tab.id where case_no is not null;
                '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_trial_proc_laf(self, variable_name):
        old_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join  info_court_trial_process t on t.court_id = tab.id where case_no is not null;
                '''
        new_sql = '''
                select case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_trial_process t on t.court_id = tab.id where case_no is not null;
                '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_tax_pay_laf(self, variable_name):
        new_sql = '''
                    select confirm_date from(
                        select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                        ) tab inner join info_court_taxable_abnormal_user t on t.court_id = tab.id where confirm_date > %(pre_biz_date)s;
                    '''

        new_df = to_df(self, new_sql, {})
        if not new_df.empty:
            self.variables[variable_name] = 1

    def _court_pub_info_laf(self, variable_name):
        old_sql = '''
                select execute_case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and create_time < %(pre_biz_date)s order by create_time desc limit 1
                    ) tab left join  info_court_excute_public t on t.court_id = tab.id where execute_case_no is not null;
                '''
        new_sql = '''
                select execute_case_no from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    ) tab left join info_court_excute_public t on t.court_id = tab.id where execute_case_no is not null;
                '''
        var_compare(self, new_sql, old_sql, variable_name)

    def _court_tax_arrears_laf(self, variable_name):
        new_sql = '''
                select taxes_time from(
                    select id from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                ) tab inner join info_court_tax_arrears t on t.court_id = tab.id where t.taxes_time > %(pre_biz_date)s ;
                '''
        new_df = to_df(self, new_sql, {})
        if not new_df.empty:
            self.variables[variable_name] = 1

    def transform(self):
        # 前一个业务的创建时间
        self.pre_biz_date = self.origin_data.get('preBizDate')

        # 法院核查_个人_欠款欠费名单贷后新增
        self._court_owed_owe_laf("court_owed_owe_laf")
        # 法院核查_个人_失信老赖名单_贷后新增
        self._court_dishonesty_laf("court_dishonesty_laf")
        # 法院核查_个人_限制出入境名单_贷后新增
        self._court_limit_entry_laf("court_limit_entry_laf")
        # 法院核查_个人_限制高消费名单命中次数_贷后新增
        self._court_high_cons_laf("court_high_cons_laf")
        # 法院核查_企业_罪犯及嫌疑人名单命中次数_贷后新增
        self._court_cri_sus_laf("court_cri_sus_laf")

        # 法院核查_个人_金融借款合同纠纷_贷后新增
        self._court_fin_loan_stats("court_fin_loan_con_laf", "金融借款合同纠纷")
        # 法院核查_个人_借款合同纠纷_贷后新增
        self._court_fin_loan_stats("court_loan_con_laf", "借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷")
        # 法院核查_个人_民间借贷纠纷_贷后新增
        self._court_fin_loan_stats("court_pop_loan_laf", "民间借贷纠纷")

        # 法院核查_个人_行政违法记录命中次数_贷后新增
        self._court_admi_vio_laf("court_admi_vio_laf")
        # 法院核查_个人_民商事裁判文书命中次数_贷后新增
        self._court_judge_laf("court_judge_laf")
        # 法院核查_个人_民商事审判流程命中次数_贷后新增
        self._court_trial_proc_laf("court_trial_proc_laf")

        # 法院核查_个人_纳税非正常户命中次数_贷后新增
        self._court_tax_pay_laf("court_tax_pay_laf")
        # 法院核查_个人_执行公开信息命中次数_贷后新增
        self._court_pub_info_laf("court_pub_info_laf")
        # 法院核查_个人_欠税名单命中次数_贷后新增
        self._court_tax_arrears_laf("court_tax_arrears_laf")
