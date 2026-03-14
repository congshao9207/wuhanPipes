import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from mapping.utils.df_comparator_util import df_compare
import time


class Tf0004(Transformer):
    """
    企业工商法院核查,贷后新增
    """
    def __init__(self):
        super().__init__()
        self.variables = {
            "com_bus_court_open_owed_owe_laf": 0,  # 工商法院_在营企业_欠款欠费名单命中次数_贷后新增
            "com_bus_court_open_court_dishonesty_laf": 0,  # 工商法院_在营企业_失信老赖名单命中次数_贷后新增
            "com_bus_court_open_rest_entry_laf": 0,  # 工商法院_在营企业_限制出入境名单命中次数_贷后新增
            "com_bus_court_open_high_cons_laf": 0,  # 工商法院_在营企业_限制高消费名单命中次数_贷后新增
            "com_bus_court_open_cri_sus_laf": 0,  # 工商法院_在营企业_罪犯及嫌疑人名单命中次数_贷后新增
            "com_bus_court_open_fin_loan_con_laf": 0,  # 工商法院_在营企业_金融借款合同纠纷_贷后新增
            "com_bus_court_open_loan_con_laf": 0,  # 工商法院_在营企业_借款合同纠纷_贷后新增
            "com_bus_court_open_pop_loan_laf": 0,  # 工商法院_在营企业_民间借贷纠纷_贷后新增
            "com_bus_court_open_tax_arrears_laf": 0,  # 工商法院_在营企业_欠税名单命中次数_贷后新增
            "com_bus_court_open_pub_info_laf": 0,  # 工商法院_在营企业_执行公开信息命中次数_贷后新增
            "com_bus_court_open_admi_violation_laf": 0,  # 工商法院_在营企业_行政违法记录命中次数_贷后新增
            "com_bus_court_open_judge_docu_laf": 0,  # 工商法院_在营企业_民商事裁判文书命中次数_贷后新增
            "com_bus_court_open_judge_proc_laf": 0,  # 工商法院_在营企业_民商事审判流程命中次数_贷后新增
            "com_bus_court_open_tax_pay_laf": 0  # 工商法院_在营企业_纳税非正常户命中次数_贷后新增
        }
        self.pre_biz_date = None
        self.ent_list = list()
        self.id_list = list()

    # 获取企业对外投资企业中占股比例大于20%且在营的企业
    def _get_ent_list(self):
        t1 = time.time()
        sql1 = """
            select 
                distinct a.ent_name,a.credit_code
            from
                info_com_bus_entinvitem a,
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by id desc limit 1) b 
            where
                a.basic_id=b.id  and
                a.funded_ratio >= 0.2 and 
                (a.ent_status='在营（开业）' or a.ent_status='存续（在营、开业、在册）')
        """
        sql2 = """
            select 
                distinct a.ent_name,a.credit_code
            from
                info_com_bus_frinv a,
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by id desc limit 1) b 
            where
                a.basic_id=b.id  and
                a.funded_ratio >= 0.2 and 
                (a.ent_status='在营（开业）' or a.ent_status='存续（在营、开业、在册）')
        """
        df1 = sql_to_df(sql=sql1,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no})
        df2 = sql_to_df(sql=sql2,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no})
        df = pd.concat([df1, df2], axis=0, ignore_index=True, sort=False)
        if len(df) > 0:
            df.fillna('Na', inplace=True)
            df.replace('', 'Na', inplace=True)
            ent_str = '|'.join(list(df['ent_name']))
            credit_str = '|'.join(list(df['credit_code']))
            self.ent_list.append(ent_str)
            self.ent_list.append(credit_str)
        else:
            self.ent_list = ['Na', 'Na']
        # print("获取ent_list耗时:%.4f" % (time.time()-t1))
        return

    # 根据ent_list获取需要info_court表中对应的记录id
    def _get_info_court_id(self):
        sql1 = """
            select 
                max(id) as id 
            FROM 
                info_court 
            where 
                create_time < %(result_date)s
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no
        """
        sql2 = """
            select 
                max(id) as id 
            FROM 
                info_court 
            where 
                create_time < NOW()
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no
        """
        df1 = sql_to_df(sql=sql1,
                        params={'user_name': self.ent_list[0],
                                'id_card_no': self.ent_list[1],
                                'result_date': self.pre_biz_date})
        df2 = sql_to_df(sql=sql2,
                        params={'user_name': self.ent_list[0],
                                'id_card_no': self.ent_list[1]})
        if len(df1) > 0:
            temp = [str(_) for _ in df1['id']]
            self.id_list.append(','.join(temp))
        else:
            self.id_list.append('Na')
        if len(df2) > 0:
            temp = [str(_) for _ in df2['id']]
            self.id_list.append(','.join(temp))
        else:
            self.id_list.append('Na')

    # 判断各个变量命中次数是否有变化
    def _hit_list_times(self):
        hit_list = {
            'com_bus_court_open_owed_owe_laf': {'table_name': 'info_court_arrearage',
                                                'column_name': 'contract_no'},  # 工商法院_在营企业_欠款欠费名单命中次数_贷后新增
            'com_bus_court_open_court_dishonesty_laf': {'table_name': 'info_court_deadbeat',
                                                        'column_name': 'execute_case_no'},  # 工商法院_在营企业_失信老赖名单命中次数_贷后新增
            'com_bus_court_open_rest_entry_laf': {'table_name': 'info_court_limited_entry_exit',
                                                  'column_name': 'execute_no'},  # 工商法院_在营企业_限制出入境名单命中次数_贷后新增
            'com_bus_court_open_high_cons_laf': {'table_name': 'info_court_limit_hignspending',
                                                 'column_name': 'execute_case_no'},  # 工商法院_在营企业_限制高消费名单命中次数_贷后新增
            'com_bus_court_open_cri_sus_laf': {'table_name': 'info_court_criminal_suspect',
                                               'column_name': 'case_no'},  # 工商法院_在营企业_罪犯及嫌疑人名单命中次数_贷后新增
            'com_bus_court_open_judge_docu_laf': {'table_name': 'info_court_judicative_pape',
                                                  'column_name': 'case_no'},  # 工商法院_在营企业_民商事裁判文书命中次数_贷后新增
            'com_bus_court_open_judge_proc_laf': {'table_name': 'info_court_trial_process',
                                                  'column_name': 'case_no'},  # 工商法院_在营企业_民商事审判流程命中次数_贷后新增
            'com_bus_court_open_admi_violation_laf': {'table_name': 'info_court_administrative_violation',
                                                      'column_name': 'case_no'}  # 工商法院_在营企业_执行公开信息命中次数_贷后新增
        }
        for var in hit_list.keys():
            t1 = time.time()
            sql_before_loan = """
                select 
                    a.%s""" % hit_list[var]['column_name'] + """ as key_no
                from 
                    %s a""" % hit_list[var]['table_name'] + """
                where
                    a.court_id in (%(id)s)
                """
            sql_after_loan = """
                select 
                    a.%s""" % hit_list[var]['column_name'] + """ as key_no
                from 
                    %s a""" % hit_list[var]['table_name'] + """
                where
                    a.court_id in (%(id)s)
                """
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'id': self.id_list[0]})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'id': self.id_list[1]})
            df_compare(self.variables, df_before_loan, df_after_loan, var)
            # print("变量%s赋值耗时:%.4f" % (var, time.time()-t1))

    # 判断命中合同纠纷次数相关变量是否发生变化
    def _hit_contract_dispute_times(self):
        hit_list = {
            'com_bus_court_open_fin_loan_con_laf': '金融借款合同纠纷',  # 工商法院_在营企业_金融借款合同纠纷_贷后新增
            'com_bus_court_open_pop_loan_laf': '民间借贷纠纷'  # 工商法院_在营企业_民间借贷纠纷_贷后新增
        }
        for var in hit_list.keys():
            t1 = time.time()
            sql_before_loan1 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_judicative_pape a
                where
                    a.court_id in (%(id)s)  and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan1 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_judicative_pape a
                where
                    a.court_id in (%(id)s)  and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_before_loan2 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_trial_process a 
                where
                    a.court_id in (%(id)s)  and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan2 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_trial_process a
                where
                    a.court_id in (%(id)s)  and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                        params={'id': self.id_list[0],
                                                'case_reason': '%'+hit_list[var]+'%'})
            df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                       params={'id': self.id_list[1],
                                               'case_reason': '%'+hit_list[var]+'%'})
            df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                        params={'id': self.id_list[0],
                                                'case_reason': '%'+hit_list[var]+'%'})
            df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                       params={'id': self.id_list[1],
                                               'case_reason': '%'+hit_list[var]+'%'})
            df_before_loan = pd.concat([df_before_loan1, df_before_loan2],
                                       axis=0, ignore_index=True, sort=False)
            df_after_loan = pd.concat([df_after_loan1, df_after_loan2],
                                      axis=0, ignore_index=True, sort=False)
            df_compare(self.variables, df_before_loan, df_after_loan, var)
            # print("变量%s赋值耗时:%.4f" % (var, time.time() - t1))

    # 判断借款合同纠纷命中数是否有新增
    def _com_bus_court_open_loan_con_laf(self):
        t1 = time.time()
        sql_before_loan1 = """
            select 
                a.case_no as key_no
            from 
                info_court_judicative_pape a
            where
                a.court_id in (%(id)s)  and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan1 = """
            select 
                a.case_no as key_no
            from 
                info_court_judicative_pape a
            where
                a.court_id in (%(id)s)  and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_before_loan2 = """
            select 
                a.case_no as key_no
            from 
                info_court_trial_process a
            where
                a.court_id in (%(id)s)  and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan2 = """
            select 
                a.case_no as key_no
            from 
                info_court_trial_process a
            where
                a.court_id in (%(id)s)  and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                    params={'id': self.id_list[0],
                                            'id_card_no': self.ent_list[1]})
        df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                   params={'id': self.id_list[1]})
        df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                    params={'id': self.id_list[0],
                                            'id_card_no': self.ent_list[1]})
        df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                   params={'id': self.id_list[1]})
        df_before_loan = pd.concat([df_before_loan1, df_before_loan2],
                                   axis=0, ignore_index=True, sort=False)
        df_after_loan = pd.concat([df_after_loan1, df_after_loan2],
                                  axis=0, ignore_index=True, sort=False)
        df_compare(self.variables, df_before_loan, df_after_loan, 'com_bus_court_open_loan_con_laf')
        # print("变量%s赋值耗时:%.4f" % ('com_bus_court_open_loan_con_laf', time.time() - t1))

    # 统计欠税名单,纳税非正常户新增命中次数
    def _count_hit_times_newly_increased(self):
        t1 = time.time()
        hit_list = {
            'com_bus_court_open_tax_arrears_laf': {'table_name': 'info_court_tax_arrears',
                                                   'column_name': 'taxes_time'},  # 工商法院_在营企业_欠税名单命中次数_贷后新增
            'com_bus_court_open_tax_pay_laf': {'table_name': 'info_court_taxable_abnormal_user',
                                               'column_name': 'confirm_date'}  # 工商法院_在营企业_纳税非正常户命中次数_贷后新增
        }
        for var in hit_list.keys():
            sql = """
                select 
                    count(*) as cnt
                from
                    %s a""" % hit_list[var]['table_name'] + """
                where
                    a.court_id in (%(id)s)  and
                     a.""" + hit_list[var]['column_name'] + """ between %(result_date)s and NOW()
            """
            df = sql_to_df(sql=sql,
                           params={'id': self.id_list[1],
                                   'result_date': self.pre_biz_date})
            self.variables[var] = df.values[0][0]
            # print("变量%s赋值耗时:%.4f" % (var, time.time() - t1))

    # 统计执行公开信息新增命中次数
    def _com_bus_court_open_pub_info_laf(self):
        t1 = time.time()
        sql = """
            select 
                count(*) as cnt
            from
                info_court_excute_public a
            where
                a.court_id in (%(id)s) 
        """
        df = sql_to_df(sql=sql,
                       params={'id': self.id_list[1]})
        self.variables['com_bus_court_open_pub_info_laf'] = df.values[0][0]
        # print("变量%s赋值耗时:%.4f" % ('com_bus_court_open_pub_info_laf', time.time() - t1))

    # 执行变量转换
    def transform(self):
        t0 = time.time()
        self.pre_biz_date = self.origin_data.get('preBizDate')
        if self.user_name is None or len(self.user_name) == 0:
            self.user_name = 'Na'
        if self.id_card_no is None or len(self.id_card_no) == 0:
            self.id_card_no = 'Na'

        self.variables["variable_product_code"] = "06001"

        self._get_ent_list()
        t1 = time.time()
        # print("方法_get_ent_list耗时:%.4f" % (t1 - t0))
        self._get_info_court_id()
        tx = time.time()
        # print("方法_get_info_court_id耗时:%.4f" % (tx-t1))
        self._hit_list_times()
        t2 = time.time()
        # print("方法_hit_list_times耗时:%.4f" % (t2 - tx))
        self._hit_contract_dispute_times()
        t3 = time.time()
        # print("方法_hit_contract_dispute_times耗时:%.4f" % (t3 - t2))
        self._com_bus_court_open_loan_con_laf()
        t4 = time.time()
        # print("方法_com_bus_court_open_loan_con_laf耗时:%.4f" % (t4 - t3))
        self._count_hit_times_newly_increased()
        t5 = time.time()
        # print("方法_count_hit_times_newly_increased耗时:%.4f" % (t5 - t4))
        self._com_bus_court_open_pub_info_laf()
        t6 = time.time()
        # print("方法_com_bus_court_open_pub_info_laf耗时:%.4f" % (t6 - t5))
