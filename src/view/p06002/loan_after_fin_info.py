import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_union
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LABFinInfo(GroupedTransformer):
    """
    财务分析，此文件仅调用一次
    """

    def __init__(self):
        super().__init__()
        self.last_report_time = None
        self.loan_after_obj = None
        self.variables = {
            # 动产抵押
            "fin_mort_name": [],  # 抵押人
            "fin_mort_to_name": [],  # 抵押权人
            "fin_mort_reg_no": [],  # 登记编号
            "fin_mort_reg_date": [],  # 登记日期
            "fin_mort_status": [],  # 登记状态
            "fin_mort_reg_org": [],  # 登记机关
            "fin_mab_guar_amt": [],  # 被担保债权数额
            "fin_mab_guar_type": [],  # 被担保债权种类
            "fin_pef_date_range": [],  # 债务人履行债务的期限
            "fin_gua_name": [],  # 抵押物名称
            "fin_gua_own": [],  # 抵押物所有权或使用权归属权
            "fin_gua_des": [],  # 抵押物详情
            "fin_cancle_date": [],  # 注销日期
            # 股权出质
            "fin_impawn_name": [],  # 企业名称
            "fin_impawn_role": [],  # 企业角色
            "fin_impawn_equity_no": [],  # 登记编号
            "fin_impawn_pled_gor": [],  # 出质人
            "fin_impawn_am": [],  # 出质股权数
            "fin_impawn_org": [],  # 质权人
            "fin_impawn_state": [],  # 状态
            "fin_impawn_equple_date": [],  # 股权出质登记日期
            "fin_impawn_pub_date": []  # 公示日期
        }

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "fin"

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
        com_basic_id1, com_basic_id2 = self.get_basic_id(com_list)
        self.info_com_bus_mort_basic(com_basic_id1, com_basic_id2)
        self.info_com_bus_shares_impawn(com_basic_id1, com_basic_id2)
