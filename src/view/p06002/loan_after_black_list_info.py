import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_union
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LABlackListInfo(GroupedTransformer):
    """
    黑灰名单分析，此文件仅调用一次
    """

    def __init__(self):
        super().__init__()
        self.last_report_time = None
        self.loan_after_obj = None
        self.variables = {
            # 企业不良记录
            "black_list_name": [],  # 姓名/公司名称
            "black_list_tyle": [],  # 名单类型
            "black_list_case_no": [],  # 案号
            "black_list_detail": [],  # 详情
            # 法院公告信息
            "black_overt_name": [],  # 姓名/公司名称
            "black_overt_reason": [],  # 涉案事由
            "black_overt_type": [],  # 诉讼身份
            "black_overt_authority": [],  # 审理机关
            "black_overt_case_no": [],  # 案号
            "black_overt_status": [],  # 审理状态
            "black_overt_date": [],  # 审理时间
            # 裁判文书
            "black_judge_name": [],  # 姓名/公司名称
            "black_judge_reason": [],  # 涉案事由
            "black_judge_authority": [],  # 审理机关
            "black_judge_case_no": [],  # 案号
            "black_judge_time": [],  # 结案时间
            "black_judge_url": [],  # 详情
            # 执行公开
            "black_exec_name": [],  # 姓名/公司名称
            "black_exec_authority": [],  # 执行机关
            "black_exec_case_no": [],  # 案号
            "black_exec_date": [],  # 执行日期
            "black_exec_content": [],  # 执行标的
            "black_exec_type": [],  # 执行状态
            # 行政违法
            "black_illegal_name": [],  # 企业名称
            "black_illegal_reason": [],  # 违法事由
            "black_illegal_datetime": [],  # 具体日期
            "black_illegal_case_no": [],  # 案号
            # 严重违法失信
            "black_legal_name": [],  # 企业名称
            "black_legal_cause": [],  # 列入原因
            "black_legal_date": [],  # 列入日期
            "black_legal_org": [],  # 列入作出决定机关
            "black_legal_clear_cause": [],  # 移出原因
            "black_legal_clear_date": [],  # 移出日期
            "black_legal_clear_org": [],  # 移出作出决定机关
            # 股权冻结
            "black_froz_name": [],  # 企业名称
            "black_froz_role": [],  # 企业角色
            "black_froz_status": [],  # 状态
            "black_froz_execute_no": [],  # 协助公示通知文书号
            "black_froz_amt": [],  # 股权数额
            "black_froz_inv": [],  # 被执行人
            "black_froz_auth": [],  # 执行法院
            "black_froz_public_date": [],  # 公示日期
            "black_froz_time": [],  # 冻结期限
            "black_froz_thaw_date": [],  # 解冻日期
            "black_froz_invalid_date": [],  # 失效时间
            "black_froz_invalid_reason": [],  # 失效原因
            "black_keep_froz_time": [],  # 续行冻结期限
        }

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "black"

    def get_com_list(self):
        after_df = pd.DataFrame(self.loan_after_obj)
        com_list = after_df[after_df['userType'] == 'COMPANY']['idno'].tolist()
        all_list = after_df['idno'].tolist()
        return com_list, all_list

    def get_court_id(self, com_list):
        inter_com_str = '"' + '","'.join(com_list) + '"'
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select max(id) as id 
                from info_court 
                where unique_id_no in (%s) and '%s' between create_time and expired_at
                group by unique_id_no
            """ % (inter_com_str, self.last_report_time)
            df1 = sql_to_df(sql1)
        sql2 = """
            select max(id) as id 
            from info_court 
            where unique_id_no in (%s) and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            group by unique_id_no
        """ % inter_com_str
        df2 = sql_to_df(sql2)
        court_id1 = None
        if df1 is not None and df1.shape[0] > 0:
            df1['id'] = df1['id'].astype(str)
            court_id1 = df1['id'].tolist()
        court_id2 = None
        if df2.shape[0] > 0:
            df2['id'] = df2['id'].astype(str)
            court_id2 = df2['id'].tolist()
        return court_id1, court_id2

    def get_basic_id(self, com_list):
        inter_com_str = '"' + '","'.join(com_list) + '"'
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select max(id) as id 
                from info_com_bus_basic 
                where credit_code in (%s) and '%s' between create_time and expired_at
                group by credit_code
            """ % (inter_com_str, self.last_report_time)
            df1 = sql_to_df(sql1)
        sql2 = """
            select max(id) as id 
            from info_com_bus_basic 
            where credit_code in (%s) and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            group by credit_code
        """ % inter_com_str
        df2 = sql_to_df(sql2)
        basic_id1 = None
        if df1 is not None and df1.shape[0] > 0:
            df1['id'] = df1['id'].astype(str)
            basic_id1 = df1['id'].tolist()
        basic_id2 = None
        if df2.shape[0] > 0:
            df2['id'] = df2['id'].astype(str)
            basic_id2 = df2['id'].tolist()
        return basic_id1, basic_id2

    def info_court_black_list(self, court_id1, court_id2):
        sql = """
            select name, '罪犯及嫌疑人' as type, case_no as case_no, criminal_reason as detail, trial_date as detail_time
            from info_court_criminal_suspect where court_id in (%(court_id)s)
            union all 
            select name, '失信老赖' as type, execute_case_no as case_no, 
            execute_content as detail, execute_date as detail_time
            from info_court_deadbeat where court_id in (%(court_id)s) and execute_status != '已结案'
            union all 
            select name, '限制高消费' as type, execute_case_no as case_no, 
            execute_content as detail, specific_date as detail_time
            from info_court_limit_hignspending where court_id in (%(court_id)s)
            union all 
            select name, '限制出入境' as type, execute_no as case_no, 
            execute_content as detail, specific_date as detail_time
            from info_court_limited_entry_exit where court_id in (%(court_id)s)
        """
        if court_id1 is not None:
            court_id1 = ','.join(court_id1)
            court_df1 = sql_to_df(sql % {'court_id': str(court_id1)})
        else:
            court_df1 = None
        if court_id2 is not None:
            court_id2 = ','.join(court_id2)
            court_df2 = sql_to_df(sql % {'court_id': str(court_id2)})
        else:
            court_df2 = None
        if court_df2 is not None:
            if court_df1 is None:
                court_df1 = pd.DataFrame(columns=court_df2.columns)
            for row in court_df2.itertuples():
                temp_name = getattr(row, 'name')
                temp_type = getattr(row, 'type')
                temp_case_no = getattr(row, 'case_no')
                temp_date = getattr(row, 'detail_time')
                temp_df = court_df1[(court_df1['name'] == temp_name) &
                                    (court_df1['type'] == temp_type) &
                                    (court_df1['case_no'] == temp_case_no) &
                                    (court_df1['detail_time'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['black_list_name'].append(temp_name)
                    self.variables['black_list_tyle'].append(temp_type)
                    self.variables['black_list_case_no'].append(temp_case_no)
                    self.variables['black_list_detail'].append(temp_date)

    def info_court_trial_process(self, court_id1, court_id2):
        sql = """
            select *
            from info_court_trial_process
            where court_id in (%s)
        """
        if court_id1 is not None:
            court_id1 = ','.join(court_id1)
            court_df1 = sql_to_df(sql % str(court_id1))
        else:
            court_df1 = None
        if court_id2 is not None:
            court_id2 = ','.join(court_id2)
            court_df2 = sql_to_df(sql % str(court_id2))
        else:
            court_df2 = None
        if court_df2 is not None:
            if court_df1 is None:
                court_df1 = pd.DataFrame(columns=court_df2.columns)
            for row in court_df2.itertuples():
                temp_id = getattr(row, 'id_no')
                temp_case_no = getattr(row, 'case_no')
                temp_date = getattr(row, 'specific_date')
                temp_df = court_df1[(court_df1['id_no'] == temp_id) &
                                    (court_df1['case_no'] == temp_case_no) &
                                    (court_df1['specific_date'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['black_overt_name'].append(getattr(row, 'name'))
                    self.variables['black_overt_reason'].append(getattr(row, 'case_reason'))
                    self.variables['black_overt_type'].append(getattr(row, 'legal_status'))
                    self.variables['black_overt_authority'].append(getattr(row, 'trial_authority'))
                    self.variables['black_overt_case_no'].append(temp_case_no)
                    self.variables['black_overt_status'].append('')
                    self.variables['black_overt_date'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)

    def info_court_judicative_pape(self, court_id1, court_id2):
        sql = """
            select *
            from info_court_judicative_pape
            where court_id in (%s)
        """
        if court_id1 is not None:
            court_id1 = ','.join(court_id1)
            court_df1 = sql_to_df(sql % str(court_id1))
        else:
            court_df1 = None
        if court_id2 is not None:
            court_id2 = ','.join(court_id2)
            court_df2 = sql_to_df(sql % str(court_id2))
        else:
            court_df2 = None
        if court_df2 is not None:
            if court_df1 is None:
                court_df1 = pd.DataFrame(columns=court_df2.columns)
            for row in court_df2.itertuples():
                temp_id = getattr(row, 'id_no')
                temp_case_no = getattr(row, 'case_no')
                temp_date = getattr(row, 'closed_time')
                temp_df = court_df1[(court_df1['id_no'] == temp_id) &
                                    (court_df1['case_no'] == temp_case_no) &
                                    (court_df1['closed_time'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['black_judge_name'].append(getattr(row, 'name'))
                    self.variables['black_judge_reason'].append(getattr(row, 'case_reason'))
                    self.variables['black_judge_authority'].append(getattr(row, 'trial_authority'))
                    self.variables['black_judge_case_no'].append(temp_case_no)
                    self.variables['black_judge_url'].append(getattr(row, 'url'))
                    self.variables['black_judge_time'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)

    def info_court_excute_public(self, court_id1, court_id2):
        sql = """
            select *
            from info_court_excute_public
            where court_id in (%s)
        """
        if court_id1 is not None:
            court_id1 = ','.join(court_id1)
            court_df1 = sql_to_df(sql % str(court_id1))
        else:
            court_df1 = None
        if court_id2 is not None:
            court_id2 = ','.join(court_id2)
            court_df2 = sql_to_df(sql % str(court_id2))
        else:
            court_df2 = None
        if court_df2 is not None:
            if court_df1 is None:
                court_df1 = pd.DataFrame(columns=court_df2.columns)
            for row in court_df2.itertuples():
                temp_id = getattr(row, 'id_no')
                temp_case_no = getattr(row, 'execute_case_no')
                temp_date = getattr(row, 'filing_time')
                temp_df = court_df1[(court_df1['id_no'] == temp_id) &
                                    (court_df1['execute_case_no'] == temp_case_no) &
                                    (court_df1['filing_time'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['black_exec_name'].append(getattr(row, 'name'))
                    self.variables['black_exec_authority'].append(getattr(row, 'execute_court'))
                    self.variables['black_exec_case_no'].append(temp_case_no)
                    self.variables['black_exec_date'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)
                    self.variables['black_exec_content'].append(getattr(row, 'execute_content'))
                    self.variables['black_exec_type'].append(getattr(row, 'execute_status'))

    def info_court_administrative_violation(self, court_id1, court_id2):
        sql = """
            select *
            from info_court_administrative_violation
            where court_id in (%s)
        """
        if court_id1 is not None:
            court_id1 = ','.join(court_id1)
            court_df1 = sql_to_df(sql % str(court_id1))
        else:
            court_df1 = None
        if court_id2 is not None:
            court_id2 = ','.join(court_id2)
            court_df2 = sql_to_df(sql % str(court_id2))
        else:
            court_df2 = None
        if court_df2 is not None:
            if court_df1 is None:
                court_df1 = pd.DataFrame(columns=court_df2.columns)
            for row in court_df2.itertuples():
                temp_id = getattr(row, 'id_no')
                temp_case_no = getattr(row, 'case_no')
                temp_date = getattr(row, 'specific_date')
                temp_df = court_df1[(court_df1['id_no'] == temp_id) &
                                    (court_df1['case_no'] == temp_case_no) &
                                    (court_df1['specific_date'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['black_illegal_name'].append(getattr(row, 'name'))
                    self.variables['black_illegal_reason'].append(getattr(row, 'illegalreason'))
                    self.variables['black_illegal_datetime'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)
                    self.variables['black_illegal_case_no'].append(temp_case_no)

    def info_com_bus_illegal(self, basic_id1, basic_id2):
        sql = """
            select b.ent_name, a.*
            from info_com_bus_illegal a inner join info_com_bus_basic b 
            on a.basic_id = b.id and b.id in (%s)
        """
        if basic_id1 is not None:
            basic_id1 = ','.join(basic_id1)
            basic_df1 = sql_to_df(sql % str(basic_id1))
        else:
            basic_df1 = None
        if basic_id2 is not None:
            basic_id2 = ','.join(basic_id2)
            basic_df2 = sql_to_df(sql % str(basic_id2))
        else:
            basic_df2 = None
        if basic_df2 is not None:
            if basic_df1 is None:
                basic_df1 = pd.DataFrame(columns=basic_df2.columns)
            for row in basic_df2.itertuples():
                temp_name = getattr(row, 'ent_name')
                temp_org = getattr(row, 'illegal_org_name_in')
                temp_date = getattr(row, 'illegal_date_in')
                temp_df = basic_df1[(basic_df1['ent_name'] == temp_name) &
                                    (basic_df1['illegal_org_name_in'] == temp_org) &
                                    (basic_df1['illegal_date_in'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['black_legal_name'].append(temp_name)
                    self.variables['black_legal_cause'].append(getattr(row, 'illegal_result_in'))
                    self.variables['black_legal_date'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)
                    self.variables['black_legal_org'].append(temp_org)
                    self.variables['black_legal_clear_cause'].append(getattr(row, 'illegal_rresult_out'))
                    self.variables['black_legal_clear_date'].append(
                        format(pd.to_datetime(getattr(row, 'illegal_date_out')), '%Y-%m-%d')
                        if pd.notna(getattr(row, 'illegal_date_out')) else None)
                    self.variables['black_legal_clear_org'].append(getattr(row, 'illegal_org_name_out'))

    def info_com_bus_shares_frost(self, basic_id1, basic_id2):
        sql = """
            select b.ent_name, a.*
            from info_com_bus_shares_frost a inner join info_com_bus_basic b 
            on a.basic_id = b.id and b.id in (%s) 
        """
        # where a.judicial_froz_state not in ('解冻', '解除冻结', '失效')
        if basic_id1 is not None:
            basic_id1 = ','.join(basic_id1)
            basic_df1 = sql_to_df(sql % str(basic_id1))
        else:
            basic_df1 = None
        if basic_id2 is not None:
            basic_id2 = ','.join(basic_id2)
            basic_df2 = sql_to_df(sql % str(basic_id2))
        else:
            basic_df2 = None
        if basic_df2 is not None:
            if basic_df1 is None:
                basic_df1 = pd.DataFrame(columns=basic_df2.columns)
            for row in basic_df2.itertuples():
                temp_name = getattr(row, 'ent_name')
                temp_no = getattr(row, 'froz_execute_no')
                temp_date = getattr(row, 'froz_from')
                temp_df = basic_df1[(basic_df1['ent_name'] == temp_name) &
                                    (basic_df1['froz_execute_no'] == temp_no) &
                                    (basic_df1['froz_from'] == temp_date)]
                if temp_df.shape[0] == 0:
                    froz_from = getattr(row, 'froz_from')
                    froz_to = getattr(row, 'froz_to')
                    keep_froz_from = getattr(row, 'keep_froz_from')
                    keep_froz_to = getattr(row, 'keep_froz_to')
                    if pd.notna(froz_from) or pd.notna(froz_to):
                        froz_time = '%s至%s' % (format(froz_from, '%Y-%m-%d') if pd.notna(froz_from) else '',
                                               format(froz_to, '%Y-%m-%d') if pd.notna(froz_to) else '')
                    else:
                        froz_time = ''
                    if pd.notna(keep_froz_from) or pd.notna(keep_froz_to):
                        keep_froz_time = '%s至%s' % \
                                         (format(keep_froz_from, '%Y-%m-%d') if pd.notna(keep_froz_from) else '',
                                          format(keep_froz_to, '%Y-%m-%d') if pd.notna(keep_froz_to) else '')
                    else:
                        keep_froz_time = ''
                    self.variables['black_froz_name'].append(temp_name)
                    self.variables['black_froz_role'].append(getattr(row, 'jhi_role'))
                    self.variables['black_froz_status'].append(getattr(row, 'judicial_froz_state'))
                    self.variables['black_froz_execute_no'].append(temp_no)
                    self.variables['black_froz_amt'].append(getattr(row, 'judicial_fro_am'))
                    self.variables['black_froz_inv'].append(getattr(row, 'judicial_inv'))
                    self.variables['black_froz_auth'].append(getattr(row, 'froz_auth'))
                    self.variables['black_froz_public_date'].append(
                        format(pd.to_datetime(getattr(row, 'public_date')), '%Y-%m-%d')
                        if pd.notna(getattr(row, 'public_date')) else None)
                    self.variables['black_froz_time'].append(froz_time)
                    self.variables['black_froz_thaw_date'].append(
                        format(pd.to_datetime(getattr(row, 'thaw_date')), '%Y-%m-%d')
                        if pd.notna(getattr(row, 'thaw_date')) else None)
                    self.variables['black_froz_invalid_date'].append(
                        format(pd.to_datetime(getattr(row, 'invalid_time')), '%Y-%m-%d')
                        if pd.notna(getattr(row, 'invalid_time')) else None)
                    self.variables['black_froz_invalid_reason'].append(getattr(row, 'invalid_reason'))
                    self.variables['black_keep_froz_time'].append(keep_froz_time)

    def info_com_bus_mort_basic(self, basic_id1, basic_id2):
        sql = """
            select 
                b.ent_name, a.mort_gager, d.mort_org, a.mort_reg_no, a.reg_date, a.mort_status, a.reg_org,
                c.pri_cla_sec_am, c.pri_clasec_kind, c.pef_per_from, c.pef_per_to, 
                e.gua_name, e.gua_own, e.gua_des, f.can_date
            from info_com_bus_mort_basic a 
            inner join info_com_bus_basic b on a.basic_id = b.id and b.id in (%s)
            left join info_com_bus_mort_creditor c on a.id = c.mort_id
            left join info_com_bus_mort_holder d on a.id = d.mort_id
            left join info_com_bus_mort_collateral e on a.id = e.mort_id
            left join info_com_bus_mort_cancel f on a.id = f.mort_id
        """
        if basic_id1 is not None:
            basic_id1 = ','.join(basic_id1)
            basic_df1 = sql_to_df(sql % str(basic_id1))
        else:
            basic_df1 = None
        if basic_id2 is not None:
            basic_id2 = ','.join(basic_id2)
            basic_df2 = sql_to_df(sql % str(basic_id2))
        else:
            basic_df2 = None
        if basic_df2 is not None:
            if basic_df1 is None:
                basic_df1 = pd.DataFrame(columns=basic_df2.columns)
            for row in basic_df2.itertuples():
                temp_name = getattr(row, 'ent_name')
                temp_no = getattr(row, 'mort_reg_no')
                temp_date = getattr(row, 'reg_date')
                temp_df = basic_df1[(basic_df1['ent_name'] == temp_name) &
                                    (basic_df1['mort_reg_no'] == temp_no) &
                                    (basic_df1['reg_date'] == temp_date)]
                if temp_df.shape[0] == 0:
                    date_from = getattr(row, 'pef_per_from')
                    date_to = getattr(row, 'pef_per_to')
                    if pd.notna(date_from) or pd.notna(date_to):
                        date_range = '%s至%s' % (format(date_from, '%Y-%m-%d') if pd.notna(date_from) else '',
                                                format(date_to, '%Y-%m-%d') if pd.notna(date_to) else '')
                    else:
                        date_range = ''
                    self.variables['fin_mort_name'].append(getattr(row, 'mort_gager'))
                    self.variables['fin_mort_to_name'].append(getattr(row, 'mort_org'))
                    self.variables['fin_mort_reg_no'].append(temp_no)
                    self.variables['fin_mort_reg_date'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)
                    self.variables['fin_mort_status'].append(getattr(row, 'mort_status'))
                    self.variables['fin_mort_reg_org'].append(getattr(row, 'reg_org'))
                    self.variables['fin_mab_guar_amt'].append(getattr(row, 'pri_cla_sec_am'))
                    self.variables['fin_mab_guar_type'].append(getattr(row, 'pri_clasec_kind'))
                    self.variables['fin_pef_date_range'].append(date_range)
                    self.variables['fin_gua_name'].append(getattr(row, 'gua_name'))
                    self.variables['fin_gua_own'].append(getattr(row, 'gua_own'))
                    self.variables['fin_gua_des'].append(getattr(row, 'gua_des'))
                    self.variables['fin_cancle_date'].append(
                        format(pd.to_datetime(getattr(row, 'can_date')), '%Y-%m-%d')
                        if pd.notna(getattr(row, 'can_date')) else None)

    def info_com_bus_shares_impawn(self, basic_id1, basic_id2):
        sql = """
            select b.ent_name, a.*
            from info_com_bus_shares_impawn a inner join info_com_bus_basic b 
            on a.basic_id = b.id and b.id in (%s)
        """
        if basic_id1 is not None:
            basic_id1 = ','.join(basic_id1)
            basic_df1 = sql_to_df(sql % str(basic_id1))
        else:
            basic_df1 = None
        if basic_id2 is not None:
            basic_id2 = ','.join(basic_id2)
            basic_df2 = sql_to_df(sql % str(basic_id2))
        else:
            basic_df2 = None
        if basic_df2 is not None:
            if basic_df1 is None:
                basic_df1 = pd.DataFrame(columns=basic_df2.columns)
            for row in basic_df2.itertuples():
                temp_name = getattr(row, 'ent_name')
                temp_pled_gor = getattr(row, 'imp_pled_gor')
                temp_am = getattr(row, 'imp_am')
                temp_org = getattr(row, 'imp_org')
                temp_date = getattr(row, 'imp_equple_date')
                temp_df = basic_df1[(basic_df1['ent_name'] == temp_name) &
                                    (basic_df1['imp_pled_gor'] == temp_pled_gor) &
                                    (basic_df1['imp_am'] == temp_am) &
                                    (basic_df1['imp_org'] == temp_org) &
                                    (basic_df1['imp_equple_date'] == temp_date)]
                if temp_df.shape[0] == 0:
                    self.variables['fin_impawn_name'].append(temp_name)
                    self.variables['fin_impawn_role'].append(getattr(row, 'jhi_role'))
                    self.variables['fin_impawn_equity_no'].append(getattr(row, 'imp_equity_no'))
                    self.variables['fin_impawn_pled_gor'].append(temp_pled_gor)
                    self.variables['fin_impawn_am'].append(temp_am)
                    self.variables['fin_impawn_org'].append(temp_org)
                    self.variables['fin_impawn_state'].append(getattr(row, 'imp_exe_state'))
                    self.variables['fin_impawn_equple_date'].append(
                        format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)
                    self.variables['fin_impawn_pub_date'].append(
                        format(pd.to_datetime(getattr(row, 'imp_pub_date')), '%Y-%m-%d')
                        if pd.notna(getattr(row, 'imp_pub_date')) else None)

    def transform(self):
        LoanAfter.init_grouped_transformer(self)
        com_list, all_list = self.get_com_list()
        com_courd_id1, com_court_id2 = self.get_court_id(com_list)
        all_court_id1, all_court_id2 = self.get_court_id(all_list)
        com_basic_id1, com_basic_id2 = self.get_basic_id(com_list)
        self.info_court_black_list(com_courd_id1, com_court_id2)
        self.info_court_trial_process(all_court_id1, all_court_id2)
        self.info_court_judicative_pape(all_court_id1, all_court_id2)
        self.info_court_excute_public(all_court_id1, all_court_id2)
        self.info_court_administrative_violation(all_court_id1, all_court_id2)
        self.info_com_bus_illegal(com_basic_id1, com_basic_id2)
        self.info_com_bus_shares_frost(com_basic_id1, com_basic_id2)
        # self.info_com_bus_mort_basic(com_basic_id1, com_basic_id2)
        # self.info_com_bus_shares_impawn(com_basic_id1, com_basic_id2)
