from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from mapping.utils.df_comparator_util import df_compare
import pandas as pd


class T16002(Transformer):
    """
    法院核查:企业,贷后新增
    """

    def __init__(self):
        super().__init__()
        self.variables = {
            "court_ent_owed_owe_laf": 0,  # 法院核查_企业_欠款欠费名单命中次数_贷后新增
            "court_ent_dishonesty_laf": 0,  # 法院核查_企业_失信老赖名单命中次数_贷后新增
            "court_ent_limit_entry_laf": 0,  # 法院核查_企业_限制出入境名单命中次数_贷后新增
            "court_ent_high_cons_laf": 0,  # 法院核查_企业_限制高消费名单命中次数_贷后新增
            "court_ent_cri_sus_laf": 0,  # 法院核查_企业_罪犯及嫌疑人名单命中次数_贷后新增
            "court_ent_fin_loan_con_laf": 0,  # 法院核查_企业_金融借款合同纠纷_贷后新增
            "court_ent_loan_con_laf": 0,  # 法院核查_企业_借款合同纠纷_贷后新增
            "court_ent_pop_loan_laf": 0,  # 法院核查_企业_民间借贷纠纷_贷后新增
            "court_ent_admi_vio_laf": 0,  # 法院核查_企业_行政违法记录命中次数_贷后新增
            "court_ent_judge_laf": 0,  # 法院核查_企业_民商事裁判文书命中次数_贷后新增
            "court_ent_trial_proc_laf": 0,  # 法院核查_企业_民商事审判流程命中次数_贷后新增
            "court_ent_tax_pay_laf": 0,  # 法院核查_企业_纳税非正常户命中次数_贷后新增
            "court_ent_pub_info_laf": 0,  # 法院核查_企业_执行公开信息命中次数_贷后新增
            "court_ent_tax_arrears_laf": 0  # 法院核查_企业_欠税名单命中次数_贷后新增
        }
        self.pre_biz_date = None
        self.id_list = list()

    # 获取贷前和贷后的info_court表格对应id
    def _get_info_court_id(self):
        sql1 = """
            select 
                id 
            FROM 
                info_court 
            where 
                create_time < %(result_date)s
                and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                order by id desc limit 1
        """
        sql2 = """
            select 
                id 
            FROM 
                info_court 
            where 
                create_time < NOW()
                and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                order by id desc limit 1
        """
        df1 = sql_to_df(sql=sql1,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no,
                                'result_date': self.pre_biz_date})
        df2 = sql_to_df(sql=sql2,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no})
        if len(df1) > 0:
            self.id_list.append(str(df1.values[0][0]))
        else:
            self.id_list.append('Na')
        if len(df2) > 0:
            self.id_list.append(str(df2.values[0][0]))
        else:
            self.id_list.append('Na')

    # 判断各个变量命中次数是否有变化
    def _hit_list_times(self):
        hit_list = {
            "court_ent_owed_owe_laf": {"table_name": "info_court_arrearage",
                                       "column_name": "contract_no"},  # 法院核查_企业_欠款欠费名单命中次数_贷后新增
            "court_ent_dishonesty_laf": {"table_name": "info_court_deadbeat",
                                         "column_name": "execute_case_no"},  # 法院核查_企业_失信老赖名单命中次数_贷后新增
            "court_ent_limit_entry_laf": {"table_name": "info_court_limited_entry_exit",
                                          "column_name": "execute_no"},  # 法院核查_企业_限制出入境名单命中次数_贷后新增
            "court_ent_high_cons_laf": {"table_name": "info_court_limit_hignspending",
                                        "column_name": "execute_case_no"},  # 法院核查_企业_限制高消费名单命中次数_贷后新增
            "court_ent_cri_sus_laf": {"table_name": "info_court_criminal_suspect",
                                      "column_name": "case_no"},  # 法院核查_企业_罪犯及嫌疑人名单命中次数_贷后新增
            "court_ent_admi_vio_laf": {"table_name": "info_court_administrative_violation",
                                       "column_name": "case_no"},  # 法院核查_企业_行政违法记录命中次数_贷后新增
            "court_ent_judge_laf": {"table_name": "info_court_judicative_pape",
                                    "column_name": "case_no"},  # 法院核查_企业_民商事裁判文书命中次数_贷后新增
            "court_ent_trial_proc_laf": {"table_name": "info_court_trial_process",
                                         "column_name": "case_no"},  # 法院核查_企业_民商事审判流程命中次数_贷后新增
            "court_ent_pub_info_laf": {"table_name": "info_court_excute_public",
                                       "column_name": "execute_case_no"}  # 法院核查_企业_执行公开信息命中次数_贷后新增
        }
        for var in hit_list.keys():
            sql_before_loan = """
                select 
                    a.%s""" % hit_list[var]["column_name"] + """ as key_no
                from 
                    %s a""" % hit_list[var]["table_name"] + """
                where
                    a.court_id=%(id)s
                """
            sql_after_loan = """
                select 
                    a.%s""" % hit_list[var]["column_name"] + """ as key_no
                from 
                    %s a""" % hit_list[var]["table_name"] + """
                where
                    a.court_id=%(id)s
                """
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'id': self.id_list[0]})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'id': self.id_list[1]})
            df_compare(self.variables, df_before_loan, df_after_loan, var)

    # 判断命中合同纠纷次数相关变量是否发生变化
    def _hit_contract_dispute_times(self):
        hit_list = {
            "court_ent_fin_loan_con_laf": "金融借款合同纠纷",  # 法院核查_企业_金融借款合同纠纷_贷后新增
            # "court_ent_loan_con_laf": "借款合同纠纷",  # 法院核查_企业_借款合同纠纷_贷后新增
            "court_ent_pop_loan_laf": "民间借贷纠纷"  # 法院核查_企业_民间借贷纠纷_贷后新增
        }
        for var in hit_list.keys():
            sql_before_loan1 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_judicative_pape a
                where
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like "%%被告%%"
                """
            sql_after_loan1 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_judicative_pape a
                where
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like "%%被告%%"
                """
            sql_before_loan2 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_trial_process a
                where
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like "%%被告%%"
                """
            sql_after_loan2 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_trial_process a
                where
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like "%%被告%%"
                """
            df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                        params={'id': self.id_list[0],
                                                "case_reason": "%"+hit_list[var]+"%"})
            df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                       params={'id': self.id_list[1],
                                               "case_reason": "%"+hit_list[var]+"%"})
            df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                        params={'id': self.id_list[0],
                                                "case_reason": "%"+hit_list[var]+"%"})
            df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                       params={'id': self.id_list[1],
                                               "case_reason": "%"+hit_list[var]+"%"})
            df_before_loan = pd.concat([df_before_loan1, df_before_loan2], axis=0, ignore_index=True, sort=False)
            df_after_loan = pd.concat([df_after_loan1, df_after_loan2], axis=0, ignore_index=True, sort=False)

            df_compare(self.variables, df_before_loan, df_after_loan, var)

    # 判断借款合同纠纷命中数量是否有新增
    def _court_ent_loan_con_laf(self):
        sql_before_loan1 = """
            select 
                a.case_no as key_no
            from 
                info_court_judicative_pape a
            where
                a.court_id=%(id)s and
                a.case_reason regexp "借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷"
                and a.legal_status like "%%被告%%"
            """
        sql_after_loan1 = """
            select 
                a.case_no as key_no
            from 
                info_court_judicative_pape a
            where
                a.court_id=%(id)s and
                a.case_reason regexp "借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷"
                and a.legal_status like "%%被告%%"
            """
        sql_before_loan2 = """
            select 
                a.case_no as key_no
            from 
                info_court_trial_process a
            where
                a.court_id=%(id)s and
                a.case_reason regexp "借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷"
                and a.legal_status like "%%被告%%"
            """
        sql_after_loan2 = """
            select 
                a.case_no as key_no
            from 
                info_court_trial_process a
            where
                a.court_id=%(id)s and
                a.case_reason regexp "借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷"
                and a.legal_status like "%%被告%%"
            """
        df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                    params={'id': self.id_list[0]})
        df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                   params={'id': self.id_list[1]})
        df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                    params={'id': self.id_list[0]})
        df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                   params={'id': self.id_list[1]})
        df_before_loan = pd.concat([df_before_loan1, df_before_loan2], axis=0, ignore_index=True, sort=False)
        df_after_loan = pd.concat([df_after_loan1, df_after_loan2], axis=0, ignore_index=True, sort=False)

        df_compare(self.variables, df_before_loan, df_after_loan, "court_ent_loan_con_laf")

    # 统计纳税欠税名单新增命中次数
    def _count_hit_times_newly_increased(self):
        hit_list = {
            "court_ent_tax_pay_laf": {"table_name": "info_court_taxable_abnormal_user",
                                      "column_name": "confirm_date"},  # 法院核查_企业_纳税非正常户命中次数_贷后新增
            "court_ent_tax_arrears_laf": {"table_name": "info_court_tax_arrears",
                                          "column_name": "taxes_time"}  # 法院核查_企业_欠税名单命中次数_贷后新增
        }
        for var in hit_list.keys():
            sql = """
                select 
                    count(*) as cnt
                from
                    %s a""" % hit_list[var]["table_name"] + """
                where
                    a.court_id=%(id)s
                and """ + "a.%s" % hit_list[var]["column_name"] + """ between %(result_date)s and NOW()
            """
            df = sql_to_df(sql=sql,
                           params={"result_date": self.pre_biz_date,
                                   'id': self.id_list[1]})
            self.variables[var] = df.values[0][0]

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get("preBizDate")
        if self.user_name is None or len(self.user_name) == 0:
            self.user_name = "Na"
        if self.id_card_no is None or len(self.id_card_no) == 0:
            self.id_card_no = "Na"

        self.variables["variable_product_code"] = "06001"
        self._get_info_court_id()
        self._hit_list_times()
        self._hit_contract_dispute_times()
        self._count_hit_times_newly_increased()
        self._court_ent_loan_con_laf()
