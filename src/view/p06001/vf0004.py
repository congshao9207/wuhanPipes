import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import time


class Vf0004(Transformer):
    """
    企业工商法院核查,贷前+贷后详细信息展示
    """

    def __init__(self):
        super().__init__()
        self.variables = {'info_com_bus_court': []}
        # self.variables = {
        #     'info_com_bus_court_open_owed_owe': None,  # 工商法院_在营企业_欠款欠费名单_贷后+贷前
        #     'info_com_bus_court_open_court_dishonesty': None,  # 工商法院_在营企业_失信老赖名单_贷后+贷前
        #     'info_com_bus_court_open_rest_entry': None,  # 工商法院_在营企业_限制出入境名单_贷后+贷前
        #     'info_com_bus_court_open_high_cons': None,  # 工商法院_在营企业_限制高消费名单_贷后+贷前
        #     'info_com_bus_court_open_cri_sus': None,  # 工商法院_在营企业_罪犯及嫌疑人名单_贷后+贷前
        #     'info_com_bus_court_open_fin_loan_con': None,  # 工商法院_在营企业_金融借款合同纠纷_贷后+贷前
        #     'info_com_bus_court_open_loan_con': None,  # 工商法院_在营企业_借款合同纠纷_贷后+贷前
        #     'info_com_bus_court_open_pop_loan': None,  # 工商法院_在营企业_民间借贷纠纷_贷后+贷前
        #     'info_com_bus_court_open_tax_arrears': None,  # 工商法院_在营企业_欠税名单_贷后+贷前
        #     'info_com_bus_court_open_pub_info': None,  # 工商法院_在营企业_执行公开信息_贷后+贷前
        #     'info_com_bus_court_open_admi_violation': None,  # 工商法院_在营企业_行政违法记录_贷后+贷前
        #     'info_com_bus_court_open_judge_docu': None,  # 工商法院_在营企业_民商事裁判文书_贷后+贷前
        #     'info_com_bus_court_open_judge_proc': None,  # 工商法院_在营企业_民商事审判流程_贷后+贷前
        #     'info_com_bus_court_open_tax_pay': None  # 工商法院_在营企业_纳税非正常户_贷后+贷前
        # }
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
                a.basic_id=b.id and
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
                a.basic_id=b.id and
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
        # print("获取ent_list耗时:%.4f" % (time.time() - t1))
        return

    # 根据ent_list获取需要info_court表中对应的记录id
    def _get_info_court_id(self):
        sql1 = """
            select 
                max(id) as id 
            FROM 
                info_court 
            where 
                %(result_date)s between create_time and expired_at
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no
        """
        sql2 = """
            select 
                max(id) as id 
            FROM 
                info_court 
            where 
                NOW() between create_time and expired_at
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

    # 企业对外投资企业命中各类名单详细信息展示
    def _hit_list_details(self):
        hit_list = {
            'info_com_bus_court_open_owed_owe': {'table_name': 'info_court_arrearage',
                                                 'type': '工商法院在营企业欠款欠费名单'},  # 工商法院_在营企业_欠款欠费名单_贷后+贷前
            'info_com_bus_court_open_court_dishonesty': {'table_name': 'info_court_deadbeat',
                                                         'type': '工商法院在营企业失信老赖名单'},  # 工商法院_在营企业_失信老赖名单_贷后+贷前
            'info_com_bus_court_open_rest_entry': {'table_name': 'info_court_limited_entry_exit',
                                                   'type': '工商法院在营企业限制出入境名单'},  # 工商法院_在营企业_限制出入境名单_贷后+贷前
            'info_com_bus_court_open_high_cons': {'table_name': 'info_court_limit_hignspending',
                                                  'type': '工商法院在营企业限制高消费名单'},  # 工商法院_在营企业_限制高消费名单_贷后+贷前
            'info_com_bus_court_open_cri_sus': {'table_name': 'info_court_criminal_suspect',
                                                'type': '工商法院在营企业罪犯及嫌疑人名单'},  # 工商法院_在营企业_罪犯及嫌疑人名单_贷后+贷前
            'info_com_bus_court_open_tax_arrears': {'table_name': 'info_court_tax_arrears',
                                                    'type': '工商法院在营企业欠税名单'},  # 工商法院_在营企业_欠税名单_贷后+贷前
            'info_com_bus_court_open_pub_info': {'table_name': 'info_court_excute_public',
                                                 'type': '工商法院在营企业执行公开信息'},  # 工商法院_在营企业_执行公开信息_贷后+贷前
            'info_com_bus_court_open_admi_violation': {'table_name': 'info_court_administrative_violation',
                                                       'type': '工商法院在营企业行政违法记录'},  # 工商法院_在营企业_行政违法记录_贷后+贷前
            'info_com_bus_court_open_judge_docu': {'table_name': 'info_court_judicative_pape',
                                                   'type': '工商法院在营企业民商事裁判文书'},  # 工商法院_在营企业_民商事裁判文书_贷后+贷前
            'info_com_bus_court_open_judge_proc': {'table_name': 'info_court_trial_process',
                                                   'type': '工商法院在营企业民商事审判流程'},  # 工商法院_在营企业_民商事审判流程_贷后+贷前
            'info_com_bus_court_open_tax_pay': {'table_name': 'info_court_taxable_abnormal_user',
                                                'type': '工商法院在营企业纳税非正常户'}  # 工商法院_在营企业_纳税非正常户_贷后+贷前
        }
        for var in hit_list.keys():
            t1 = time.time()
            sql_before_loan = """
                select 
                    a.*
                from 
                   %s a""" % hit_list[var]['table_name'] + """
                where
                    a.court_id in (%(id)s)
                """
            sql_after_loan = """
                select 
                    a.*
                from 
                    %s a""" % hit_list[var]['table_name'] + """
                where
                    a.court_id in (%(id)s)
                """
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'id': self.id_list[0]})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'id': self.id_list[1]})

            df_before_loan.fillna('', inplace=True)
            df_after_loan.fillna('', inplace=True)
            self.variables['info_com_bus_court'].append({'variable': var,
                                                         'type': hit_list[var]['type'],
                                                         'before': [],
                                                         'after': []})
            if len(df_before_loan) > 0:
                for row in df_before_loan.itertuples():
                    self.variables['info_com_bus_court'][-1]['before'].append({})
                    for col in df_before_loan.columns:
                        self.variables['info_com_bus_court'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan) > 0:
                for row in df_after_loan.itertuples():
                    self.variables['info_com_bus_court'][-1]['after'].append({})
                    for col in df_after_loan.columns:
                        self.variables['info_com_bus_court'][-1]['after'][-1][col] = str(getattr(row, col))
            # print("变量%s赋值耗时:%.4f" % (var, time.time() - t1))
        return

    # 企业对外投资企业金融借款合同纠纷和民间借贷纠纷详细信息展示
    def _hit_contract_dispute_details(self):
        hit_list = {
            'info_com_bus_court_open_fin_loan_con': {'case_reason': '金融借款合同纠纷',
                                                     'type': '工商法院在营企业金融借款合同纠纷'},  # 工商法院_在营企业_金融借款合同纠纷_贷后+贷前
            'info_com_bus_court_open_pop_loan': {'case_reason': '民间借贷纠纷',
                                                 'type': '工商法院在营企业民间借贷纠纷'}  # 工商法院_在营企业_民间借贷纠纷_贷后+贷前
        }
        for var in hit_list.keys():
            t1 = time.time()
            sql_before_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a
                where
                    a.court_id in (%(id)s) and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a
                where
                    a.court_id in (%(id)s) and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_before_loan2 = """
                select 
                    a.*
                from 
                    info_court_trial_process a
                where
                    a.court_id in (%(id)s) and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan2 = """
                select 
                    a.*
                from 
                    info_court_trial_process a
                where
                    a.court_id in (%(id)s) and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                        params={'id': self.id_list[0],
                                                'case_reason': '%'+hit_list[var]['case_reason']+'%'})
            df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                       params={'id': self.id_list[1],
                                               'case_reason': '%'+hit_list[var]['case_reason']+'%'})
            df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                        params={'id': self.id_list[0],
                                                'case_reason': '%'+hit_list[var]['case_reason']+'%'})
            df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                       params={'id': self.id_list[1],
                                               'case_reason': '%'+hit_list[var]['case_reason']+'%'})

            df_before_loan1.fillna('', inplace=True)
            df_after_loan1.fillna('', inplace=True)
            df_before_loan2.fillna('', inplace=True)
            df_after_loan2.fillna('', inplace=True)
            self.variables['info_com_bus_court'].append({'variable': var,
                                                         'type': hit_list[var]['type'],
                                                         'before': [],
                                                         'after': []})
            if len(df_before_loan1) > 0:
                for row in df_before_loan1.itertuples():
                    self.variables['info_com_bus_court'][-1]['before'].append({})
                    for col in df_before_loan1.columns:
                        self.variables['info_com_bus_court'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan1) > 0:
                for row in df_after_loan1.itertuples():
                    self.variables['info_com_bus_court'][-1]['after'].append({})
                    for col in df_after_loan1.columns:
                        self.variables['info_com_bus_court'][-1]['after'][-1][col] = str(getattr(row, col))
            if len(df_before_loan2) > 0:
                for row in df_before_loan2.itertuples():
                    self.variables['info_com_bus_court'][-1]['before'].append({})
                    for col in df_before_loan2.columns:
                        self.variables['info_com_bus_court'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan2) > 0:
                for row in df_after_loan2.itertuples():
                    self.variables['info_com_bus_court'][-1]['after'].append({})
                    for col in df_after_loan2.columns:
                        self.variables['info_com_bus_court'][-1]['after'][-1][col] = str(getattr(row, col))
            # print("变量%s赋值耗时:%.4f" % (var, time.time() - t1))
        return

    # 企业对外投资企业借款合同纠纷详细信息展示
    def _info_com_bus_court_open_loan_con(self):
        t1 = time.time()
        sql_before_loan1 = """
            select 
                a.*
            from 
                info_court_judicative_pape a
            where
                a.court_id in (%(id)s) and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                            金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan1 = """
            select 
                a.*
            from 
                info_court_judicative_pape a
            where
                a.court_id in (%(id)s) and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                            金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_before_loan2 = """
            select 
                a.*
            from 
                info_court_trial_process a
            where
                a.court_id in (%(id)s) and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                            金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan2 = """
            select 
                a.*
            from 
                info_court_trial_process a
            where
                a.court_id in (%(id)s) and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                            金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                    params={'id': self.id_list[0]})
        df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                   params={'id': self.id_list[1]})
        df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                    params={'id': self.id_list[0]})
        df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                   params={'id': self.id_list[1]})

        df_before_loan1.fillna('', inplace=True)
        df_after_loan1.fillna('', inplace=True)
        df_before_loan2.fillna('', inplace=True)
        df_after_loan2.fillna('', inplace=True)
        self.variables['info_com_bus_court'].append({'variable': 'info_com_bus_court_open_loan_con',
                                                     'type': '工商法院在营企业借款合同纠纷',
                                                     'before': [],
                                                     'after': []})
        if len(df_before_loan1) > 0:
            for row in df_before_loan1.itertuples():
                self.variables['info_com_bus_court'][-1]['before'].append({})
                for col in df_before_loan1.columns:
                    self.variables['info_com_bus_court'][-1]['before'][-1][col] = str(getattr(row, col))
        if len(df_after_loan1) > 0:
            for row in df_after_loan1.itertuples():
                self.variables['info_com_bus_court'][-1]['after'].append({})
                for col in df_after_loan1.columns:
                    self.variables['info_com_bus_court'][-1]['after'][-1][col] = str(getattr(row, col))
        if len(df_before_loan2) > 0:
            for row in df_before_loan2.itertuples():
                self.variables['info_com_bus_court'][-1]['before'].append({})
                for col in df_before_loan2.columns:
                    self.variables['info_com_bus_court'][-1]['before'][-1][col] = str(getattr(row, col))
        if len(df_after_loan2) > 0:
            for row in df_after_loan2.itertuples():
                self.variables['info_com_bus_court'][-1]['after'].append({})
                for col in df_after_loan2.columns:
                    self.variables['info_com_bus_court'][-1]['after'][-1][col] = str(getattr(row, col))
        # print("变量%s赋值耗时:%.4f" % ('info_com_bus_court_open_loan_con', time.time() - t1))
        return

    # 执行变量转换
    def transform(self):
        t0 = time.time()
        self.pre_biz_date = self.origin_data.get('preBizDate')
        if self.user_name is None or len(self.user_name) == 0:
            self.user_name = 'Na'
        if self.id_card_no is None or len(self.id_card_no) == 0:
            self.id_card_no = 'Na'

        # self.variables["variable_product_code"] = "06001"

        self._get_ent_list()
        t1 = time.time()
        # print("方法_get_ent_list耗时:%.4f" % (t1-t0))
        self._get_info_court_id()
        tx = time.time()
        # print("方法_get_info_court_id耗时:%.4f" % (tx - t1))
        self._hit_list_details()
        t2 = time.time()
        # print("方法_hit_list_details耗时:%.4f" % (t2 - tx))
        self._hit_contract_dispute_details()
        t3 = time.time()
        # print("方法_hit_contract_dispute_details耗时:%.4f" % (t3 - t2))
        self._info_com_bus_court_open_loan_con()
        t4 = time.time()
        # print("方法_info_com_bus_court_open_loan_con耗时:%.4f" % (t4 - t3))
