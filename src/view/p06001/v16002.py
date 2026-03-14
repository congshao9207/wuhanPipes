from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


class V16002(Transformer):
    """
    法院核查:贷前+贷后详细信息展示
    """
    def __init__(self):
        super().__init__()
        self.variables = {'info_court': []}
        # self.variables = {
        #     'info_court_ent_owed_owe': None,  # 法院核查_企业_欠款欠费名单_贷后+贷前
        #     'info_court_ent_dishonesty': None,  # 法院核查_企业_失信老赖名单_贷后+贷前
        #     'info_court_ent_limit_entry': None,  # 法院核查_企业_限制出入境名单_贷后+贷前
        #     'info_court_ent_high_cons': None,  # 法院核查_企业_限制高消费名单_贷后+贷前
        #     'info_court_ent_cri_sus': None,  # 法院核查_企业_罪犯及嫌疑人名单_贷后+贷前
        #     'info_court_ent_fin_loan_con': None,  # 法院核查_企业_金融借款合同纠纷_贷后+贷前
        #     'info_court_ent_loan_con': None,  # 法院核查_企业_借款合同纠纷_贷后+贷前
        #     'info_court_ent_pop_loan': None,  # 法院核查_企业_民间借贷纠纷_贷后+贷前
        #     'info_court_ent_admi_vio': None,  # 法院核查_企业_行政违法记录_贷后+贷前
        #     'info_court_ent_judge': None,  # 法院核查_企业_民商事裁判文书_贷后+贷前
        #     'info_court_ent_trial_proc': None,  # 法院核查_企业_民商事审判流程_贷后+贷前
        #     'info_court_ent_tax_pay': None,  # 法院核查_企业_纳税非正常户_贷后+贷前
        #     'info_court_ent_pub_info': None,  # 法院核查_企业_执行公开信息_贷后+贷前
        #     'info_court_ent_tax_arrears': None  # 法院核查_企业_欠税名单_贷后+贷前
        # }
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

    # 命中各类名单详细信息展示
    def _hit_list_details(self):
        hit_list = {
            'info_court_ent_owed_owe': {'table_name': 'info_court_arrearage',
                                        'type': '法院核查企业欠款欠费名单'},  # 法院核查_企业_欠款欠费名单_贷后+贷前
            'info_court_ent_dishonesty': {'table_name': 'info_court_deadbeat',
                                          'type': '法院核查企业失信老赖名单'},  # 法院核查_企业_失信老赖名单_贷后+贷前
            'info_court_ent_limit_entry': {'table_name': 'info_court_limited_entry_exit',
                                           'type': '法院核查企业限制出入境名单'},  # 法院核查_企业_限制出入境名单_贷后+贷前
            'info_court_ent_high_cons': {'table_name': 'info_court_limit_hignspending',
                                         'type': '法院核查企业限制高消费名单'},  # 法院核查_企业_限制高消费名单_贷后+贷前
            'info_court_ent_cri_sus': {'table_name': 'info_court_criminal_suspect',
                                       'type': '法院核查企业罪犯及嫌疑人名单'},  # 法院核查_企业_罪犯及嫌疑人名单_贷后+贷前
            'info_court_ent_admi_vio': {'table_name': 'info_court_administrative_violation',
                                        'type': '法院核查企业行政违法记录'},  # 法院核查_企业_行政违法记录_贷后+贷前
            'info_court_ent_judge': {'table_name': 'info_court_judicative_pape',
                                     'type': '法院核查企业民商事裁判文书'},  # 法院核查_企业_民商事裁判文书_贷后+贷前
            'info_court_ent_trial_proc': {'table_name': 'info_court_trial_process',
                                          'type': '法院核查企业民商事审判流程'},  # 法院核查_企业_民商事审判流程_贷后+贷前
            'info_court_ent_tax_pay': {'table_name': 'info_court_taxable_abnormal_user',
                                       'type': '法院核查企业纳税非正常户'},  # 法院核查_企业_纳税非正常户_贷后+贷前
            'info_court_ent_pub_info': {'table_name': 'info_court_excute_public',
                                        'type': '法院核查企业执行公开信息'},  # 法院核查_企业_执行公开信息_贷后+贷前
            'info_court_ent_tax_arrears': {'table_name': 'info_court_tax_arrears',
                                           'type': '法院核查企业欠税名单'}  # 法院核查_企业_欠税名单_贷后+贷前
        }
        for var in hit_list.keys():
            sql_before_loan = """
                select 
                    a.*
                from 
                    %s a """ % hit_list[var]['table_name'] + """
                where
                    a.court_id=%(id)s
                """
            sql_after_loan = """
                select 
                    a.*
                from 
                    %s a """ % hit_list[var]['table_name'] + """
                where
                    a.court_id=%(id)s
                """
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'id': self.id_list[0]})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'id': self.id_list[1]})

            df_before_loan.fillna('', inplace=True)
            df_after_loan.fillna('', inplace=True)
            self.variables['info_court'].append({'variable': var,
                                                 'type': hit_list[var]['type'],
                                                 'before': [],
                                                 'after': []})
            if len(df_before_loan) > 0:
                for row in df_before_loan.itertuples():
                    self.variables['info_court'][-1]['before'].append({})
                    for col in df_before_loan.columns:
                        self.variables['info_court'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan) > 0:
                for row in df_after_loan.itertuples():
                    self.variables['info_court'][-1]['after'].append({})
                    for col in df_after_loan.columns:
                        self.variables['info_court'][-1]['after'][-1][col] = str(getattr(row, col))
        return

    # 金融借款合同纠纷和民间借贷纠纷详细信息展示
    def _hit_contract_dispute_details(self):
        hit_list = {
            'info_court_ent_fin_loan_con': {'case_reason': '金融借款合同纠纷',
                                            'type': '法院核查企业金融借款合同纠纷'},  # 法院核查_企业_金融借款合同纠纷_贷后+贷前
            'info_court_ent_pop_loan': {'case_reason': '民间借贷纠纷',
                                        'type': '法院核查企业民间借贷纠纷'}  # 法院核查_企业_民间借贷纠纷_贷后+贷前
        }
        for var in hit_list.keys():
            sql_before_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a
                where 
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a
                where 
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_before_loan2 = """
                select 
                    a.*
                from 
                    info_court_trial_process a
                where 
                    a.court_id=%(id)s and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan2 = """
                select 
                    a.*
                from 
                    info_court_trial_process a
                where 
                    a.court_id=%(id)s and
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
            self.variables['info_court'].append({'variable': var,
                                                 'type': hit_list[var]['type'],
                                                 'before': [],
                                                 'after': []})
            if len(df_before_loan1) > 0:
                for row in df_before_loan1.itertuples():
                    self.variables['info_court'][-1]['before'].append({})
                    for col in df_before_loan1.columns:
                        self.variables['info_court'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan1) > 0:
                for row in df_after_loan1.itertuples():
                    self.variables['info_court'][-1]['after'].append({})
                    for col in df_after_loan1.columns:
                        self.variables['info_court'][-1]['after'][-1][col] = str(getattr(row, col))
            if len(df_before_loan2) > 0:
                for row in df_before_loan2.itertuples():
                    self.variables['info_court'][-1]['before'].append({})
                    for col in df_before_loan2.columns:
                        self.variables['info_court'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan2) > 0:
                for row in df_after_loan2.itertuples():
                    self.variables['info_court'][-1]['after'].append({})
                    for col in df_after_loan2.columns:
                        self.variables['info_court'][-1]['after'][-1][col] = str(getattr(row, col))
        return

    # 借款合同纠纷详细信息展示
    def _info_court_ent_loan_con(self):
        sql_before_loan1 = """
            select 
                a.*
            from 
                info_court_judicative_pape a
            where 
                a.court_id=%(id)s and
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
                a.court_id=%(id)s and
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
                a.court_id=%(id)s and
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
                a.court_id=%(id)s and
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
        self.variables['info_court'].append({'variable': 'info_court_ent_loan_con',
                                             'type': '法院核查企业借款合同纠纷',
                                             'before': [],
                                             'after': []})
        if len(df_before_loan1) > 0:
            for row in df_before_loan1.itertuples():
                self.variables['info_court'][-1]['before'].append({})
                for col in df_before_loan1.columns:
                    self.variables['info_court'][-1]['before'][-1][col] = str(getattr(row, col))
        if len(df_after_loan1) > 0:
            for row in df_after_loan1.itertuples():
                self.variables['info_court'][-1]['after'].append({})
                for col in df_after_loan1.columns:
                    self.variables['info_court'][-1]['after'][-1][col] = str(getattr(row, col))
        if len(df_before_loan2) > 0:
            for row in df_before_loan2.itertuples():
                self.variables['info_court'][-1]['before'].append({})
                for col in df_before_loan2.columns:
                    self.variables['info_court'][-1]['before'][-1][col] = str(getattr(row, col))
        if len(df_after_loan2) > 0:
            for row in df_after_loan2.itertuples():
                self.variables['info_court'][-1]['after'].append({})
                for col in df_after_loan2.columns:
                    self.variables['info_court'][-1]['after'][-1][col] = str(getattr(row, col))
        return

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')
        if self.user_name is None or len(self.user_name) == 0:
            self.user_name = 'Na'
        if self.id_card_no is None or len(self.id_card_no) == 0:
            self.id_card_no = 'Na'

        # self.variables["variable_product_code"] = "06001"
        self._get_info_court_id()
        self._hit_list_details()
        self._hit_contract_dispute_details()
        self._info_court_ent_loan_con()
